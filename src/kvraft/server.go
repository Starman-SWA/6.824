package kvraft

import (
	"bytes"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false
const Debug2 = false
const Debug4 = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func DPrintf2(format string, a ...interface{}) (n int, err error) {
	if Debug2 {
		log.Printf(format, a...)
	}
	return
}

func DPrintf4(format string, a ...interface{}) (n int, err error) {
	if Debug4 {
		log.Printf(format, a...)
	}
	return
}

const CHECK_CLIENTAPPLIED_TIMEOUT = 300 // ms
const CHECK_SNAPSHOT_INTERVAL = 50      // ms

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Data []byte
}

type OpFields struct {
	opIndex int
	oper    string
	key     string
	value   string
	ckId    int
}

type OpRecv struct {
	opFields  *OpFields
	opTerm    int
	committed chan byte // to wake up all the clerks waiting for log committed at this index

	wrongLeader  bool
	getKeyExists bool
	getValue     string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	table map[string]string // the database
	// appliedLogs   string // for test
	clientMaxOpIndexs map[int]int     // the latest operation that server has applied for each client
	indexOpRecvs      map[int]*OpRecv // op received from Raft at index

	maxCommandIndex int

	ps *raft.Persister
}

func (op *Op) EncodeOp(opf OpFields) {
	DPrintf("EncodeOp %v\n", opf)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(opf.opIndex)
	e.Encode(opf.oper)
	e.Encode(opf.key)
	if opf.oper == "Put" || opf.oper == "Append" {
		e.Encode(opf.value)
	}
	e.Encode(opf.ckId)
	op.Data = w.Bytes()
}

func (op *Op) DecodeOp() OpFields {
	opf := OpFields{}

	r := bytes.NewBuffer(op.Data)
	d := labgob.NewDecoder(r)

	if d.Decode(&opf.opIndex) != nil {
		log.Fatal(errors.New("op opIndex decoding error"))
	}

	if d.Decode(&opf.oper) != nil {
		log.Fatal(errors.New("op oper decoding error"))
	}

	if d.Decode(&opf.key) != nil {
		log.Fatal(errors.New("op key decoding error"))
	}

	if opf.oper == "Put" || opf.oper == "Append" {
		if d.Decode(&opf.value) != nil {
			log.Fatal(errors.New("op value decoding error"))
		}
	}

	if d.Decode(&opf.ckId) != nil {
		log.Fatal(errors.New("op ckId decoding error"))
	}

	DPrintf("Decode %v\n", opf)
	return opf
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("%v server: handling Get RPC, OpIndex: %v, Key: %v, ClientId: %v\n", kv.me, args.OpIndex, args.Key, args.ClientId)
	//DPrintf("%v server: applied log %v", kv.me, kv.appliedLogs)

	op := Op{}
	opFields := OpFields{opIndex: args.OpIndex, oper: "Get", key: args.Key, ckId: args.ClientId}
	op.EncodeOp(opFields)
	startIndex, startTerm, isLeader := kv.rf.Start(op)

	if !isLeader {
		DPrintf("%v server: Get RPC reply ErrWrongLeader\n", kv.me)
		*reply = GetReply{Err: ErrWrongLeader}
		return
	}

	DPrintf("%v server: Get RPC start process\n", kv.me)

	opRecv := OpRecv{opFields: &opFields, committed: make(chan byte), opTerm: startTerm}
	kv.mu.Lock()
	kv.indexOpRecvs[startIndex] = &opRecv
	kv.mu.Unlock()

	DPrintf("%v server: Get RPC waiting\n", kv.me)

	select {
	case <-opRecv.committed:
		if opRecv.wrongLeader {
			*reply = GetReply{Err: ErrWrongLeader}
			DPrintf("%v server: Get RPC reply ErrWrongLeader after commited, OpIndex: %v, Key: %v, ClientId: %v\n", kv.me, args.OpIndex, args.Key, args.ClientId)
		} else if !opRecv.getKeyExists {
			*reply = GetReply{Err: ErrNoKey}
			DPrintf("%v server: Get RPC reply ErrNoKey, OpIndex: %v, Key: %v, ClientId: %v\n", kv.me, args.OpIndex, args.Key, args.ClientId)
		} else {
			*reply = GetReply{Err: OK, Value: opRecv.getValue}
			DPrintf("%v server: Get RPC reply value, OpIndex: %v, Key: %v, ClientId: %v, Value: %v\n", kv.me, args.OpIndex, args.Key, args.ClientId, opRecv.getValue)
		}
	case <-time.After(CHECK_CLIENTAPPLIED_TIMEOUT * time.Millisecond):
		DPrintf("%v server: Get RPC reply ErrWrongLeader due to timeout, OpIndex: %v, Key: %v, ClientId: %v\n", kv.me, args.OpIndex, args.Key, args.ClientId)
		*reply = GetReply{Err: ErrWrongLeader}
	}

	kv.mu.Lock()
	if kv.indexOpRecvs[startIndex].opFields == &opFields {
		delete(kv.indexOpRecvs, args.ClientId)
	}
	kv.mu.Unlock()

	DPrintf("%v server: PutAppend RPC end", kv.me)
}

// Operate Put or Append Op
func (kv *KVServer) OperatePutAppend(opf OpFields) {
	if opf.oper == "Put" {
		kv.table[opf.key] = opf.value
	} else if opf.oper == "Append" {
		kv.table[opf.key] += opf.value
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("%v server: handling PutAppend RPC, args:%v\n", kv.me, args)
	//DPrintf("%v server: applied log %v", kv.me, kv.appliedLogs)

	op := Op{}
	opFields := OpFields{opIndex: args.OpIndex, oper: args.Op, key: args.Key, value: args.Value, ckId: args.ClientId}
	op.EncodeOp(opFields)
	startIndex, startTerm, isLeader := kv.rf.Start(op)

	if !isLeader {
		DPrintf("%v server: PutAppend RPC reply ErrWrongLeader\n", kv.me)
		*reply = PutAppendReply{Err: ErrWrongLeader}
		return
	}

	DPrintf("%v server: PutAppend RPC start process\n", kv.me)

	opRecv := OpRecv{opFields: &opFields, committed: make(chan byte), opTerm: startTerm}
	kv.mu.Lock()
	kv.indexOpRecvs[startIndex] = &opRecv
	kv.mu.Unlock()

	DPrintf("%v server: PutAppend RPC waiting\n", kv.me)
	select {
	case <-opRecv.committed:
		if opRecv.wrongLeader {
			*reply = PutAppendReply{Err: ErrWrongLeader}
			DPrintf("%v server: PutAppend RPC reply ErrWrongLeader after committed, OpIndex: %v, Key: %v, ClientId: %v\n", kv.me, args.OpIndex, args.Key, args.ClientId)
		} else {
			*reply = PutAppendReply{Err: OK}
			DPrintf("%v server: PutAppend RPC reply success, OpIndex: %v, Key: %v, ClientId: %v\n", kv.me, args.OpIndex, args.Key, args.ClientId)
		}
	case <-time.After(CHECK_CLIENTAPPLIED_TIMEOUT * time.Millisecond):
		DPrintf("%v server: PutAppend RPC reply ErrWrongLeader due to timeout, OpIndex: %v, Key: %v, ClientId: %v\n", kv.me, args.OpIndex, args.Key, args.ClientId)
		*reply = PutAppendReply{Err: ErrWrongLeader}
	}

	kv.mu.Lock()
	if kv.indexOpRecvs[startIndex].opFields == &opFields {
		delete(kv.indexOpRecvs, args.ClientId)
	}
	kv.mu.Unlock()

	DPrintf("%v server: PutAppend RPC end", kv.me)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applyLogs() {
	count := 0
	for kv.killed() == false {
		applyMsg := <-kv.applyCh
		count++
		DPrintf4("%v server: <- count:%v, applyMsg:%v\n", kv.me, count, applyMsg)

		if applyMsg.CommandValid {
			DPrintf4("%v server: <- count:%v, command start\n", kv.me, count)
			op, ok := applyMsg.Command.(Op)
			if !ok {
				log.Fatal(errors.New("applyMsg type error"))
			}
			opf := op.DecodeOp()

			kv.mu.Lock()

			opRecv, opRecvExists := kv.indexOpRecvs[applyMsg.CommandIndex]
			prevOpIndex, prevOpIndexExists := kv.clientMaxOpIndexs[opf.ckId]
			DPrintf("%v server: applyLogs decode op, CommandIndex:%v, opf:%v", kv.me, applyMsg.CommandIndex, opf)
			if opRecvExists {
				DPrintf("%v server: applyLogs opRecv:%v", kv.me, opRecv)
			}
			if prevOpIndexExists {
				DPrintf("%v server: applyLogs prevOpIndex:%v", kv.me, prevOpIndex)
			}
			kv.clientMaxOpIndexs[opf.ckId] = opf.opIndex

			kv.maxCommandIndex = applyMsg.CommandIndex

			if opRecvExists && opRecv.opTerm != applyMsg.CommandTerm {
				opRecv.wrongLeader = true
			}

			if opf.oper == "Put" || opf.oper == "Append" {
				if !prevOpIndexExists || prevOpIndex < opf.opIndex {
					DPrintf("%v server: operate putappend: %v %v %v\n", kv.me, opf.key, opf.oper, opf.value)
					kv.OperatePutAppend(opf)
					DPrintf("%v server: result of put: table[%v]=%v\n", kv.me, opf.key, kv.table[opf.key])
				} else if opRecvExists {
					//opRecv.wrongLeader = true
				}
			} else if opf.oper == "Get" && opRecvExists {
				opRecv.getValue, opRecv.getKeyExists = kv.table[opf.key]
				DPrintf("%v server: result of get: table[%v]=%v\n", kv.me, opf.key, kv.table[opf.key])
			}

			kv.mu.Unlock()
			DPrintf("%v server: applyLogs process done", kv.me)

			if opRecvExists {
				close(opRecv.committed)
			}
			DPrintf("%v server: applyLogs close committed done", kv.me)
			DPrintf4("%v server: <- count:%v, command end\n", kv.me, count)
		} else if applyMsg.SnapshotValid {
			DPrintf4("%v server: <- count:%v, snapshot start\n", kv.me, count)

			// if applyMsg.CommandIndex <= kv.maxCommandIndex {
			// 	kv.mu.Unlock()
			// 	continue
			// }

			DPrintf4("%v server: condinstallsnapshot wait, applyMsg:%v\n", kv.me, applyMsg)
			if kv.rf.CondInstallSnapshot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex, applyMsg.Snapshot) {
				DPrintf4("%v server: condinstallsnapshot ok", kv.me)
				kv.decodeAndApplySnapshot(applyMsg.Snapshot)
			}
			DPrintf4("%v server: condinstallsnapshot done", kv.me)

			DPrintf4("%v server: <- count:%v, snapshot end\n", kv.me, count)
		}
	}
}

func (kv *KVServer) checkSnapshot() {
	for !kv.killed() {
		DPrintf4("%v server: too large size sleeping\n", kv.me)
		time.Sleep(CHECK_SNAPSHOT_INTERVAL * time.Millisecond)

		DPrintf4("%v server: too large size reading\n", kv.me)
		size := kv.ps.RaftStateSize()
		DPrintf4("%v server: too large size:%v?\n", kv.me, size)
		if size >= kv.maxraftstate {
			DPrintf4("%v server: too large size:%v, waiting for lock\n", kv.me, size)
			kv.mu.Lock()
			DPrintf4("%v server: too large size:%v, get lock\n", kv.me, size)
			maxCommandIndex := kv.maxCommandIndex
			snapshot := kv.encodeSnapshot()
			kv.mu.Unlock()
			DPrintf4("%v server: too large size:%v, snapshot index:%v\n", kv.me, size, maxCommandIndex)
			kv.rf.Snapshot(maxCommandIndex, snapshot)
			DPrintf4("%v server: too large size:%v, snapshot index:%v okk\n", kv.me, size, maxCommandIndex)
		}
	}
}

// call with lock
func (kv *KVServer) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.maxCommandIndex)
	e.Encode(kv.table)
	e.Encode(kv.clientMaxOpIndexs)
	snapshot := w.Bytes()
	DPrintf("%v server: encoding snapshot, maxCommandIndex:%v, table:%v, clientMaxOpIndexs:%v\n", kv.me, kv.maxCommandIndex, kv.table, kv.clientMaxOpIndexs)

	return snapshot
}

func (kv *KVServer) decodeAndApplySnapshot(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var maxCommandIndex int
	var table map[string]string
	var clientMaxOpIndexs map[int]int

	if d.Decode(&maxCommandIndex) != nil ||
		d.Decode(&table) != nil ||
		d.Decode(&clientMaxOpIndexs) != nil {
		DPrintf("server %d: snapshot decoding error", kv.me)
		return
	}

	kv.mu.Lock()
	kv.maxCommandIndex = maxCommandIndex
	kv.table = table
	kv.clientMaxOpIndexs = clientMaxOpIndexs

	DPrintf("%v server: decoding snapshot, maxCommandIndex:%v, table:%v, clientMaxOpIndexs:%v\n", kv.me, kv.maxCommandIndex, kv.table, kv.clientMaxOpIndexs)
	kv.mu.Unlock()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.table = make(map[string]string)
	// kv.maxOpIndexs = make(map[int]int)
	// kv.clientApplied = make(map[int]chan OpFields)
	kv.ps = persister
	kv.maxCommandIndex = 0
	kv.clientMaxOpIndexs = make(map[int]int)
	kv.indexOpRecvs = make(map[int]*OpRecv)

	snapshot := persister.ReadSnapshot()
	if snapshot != nil {
		kv.decodeAndApplySnapshot(snapshot)
	}

	go kv.applyLogs()
	if maxraftstate != -1 {
		go kv.checkSnapshot()
	}

	DPrintf("%v server: server start\n", kv.me)
	return kv
}
