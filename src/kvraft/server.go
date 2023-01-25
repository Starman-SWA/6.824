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

const CHECK_APPLYCH_INTERVAL = 100      // μs
const CHECK_CLIENTAPPLIED_TIMEOUT = 500 // ms
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
	maxOpIndexs   map[int]int // the latest operation that server has applied for each client
	clientApplied map[int]chan OpFields

	ps              *raft.Persister
	maxCommandIndex int
}

func (op *Op) EncodeOp(opf OpFields) {
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

	return opf
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("%v server: handling Get RPC, OpIndex: %v, Key: %v, ClientId: %v, maxOpIndex:%v\n", kv.me, args.OpIndex, args.Key, args.ClientId, kv.maxOpIndexs[args.ClientId])
	//DPrintf("%v server: applied log %v", kv.me, kv.appliedLogs)

	op := Op{}
	op.EncodeOp(OpFields{opIndex: args.OpIndex, oper: "Get", key: args.Key, ckId: args.ClientId})
	_, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		DPrintf("%v server: Get RPC reply ErrWrongLeader\n", kv.me)
		*reply = GetReply{Err: ErrWrongLeader}
		return
	}

	kv.mu.Lock()
	if args.OpIndex == kv.maxOpIndexs[args.ClientId] {
		if value, ok := kv.table[args.Key]; ok {
			DPrintf("%v server: Get RPC return OK, OpIndex: %v, Key: %v, ClientId: %v\n", kv.me, args.OpIndex, args.Key, args.ClientId)
			*reply = GetReply{Err: OK, Value: value}
		} else {
			DPrintf("%v server: Get RPC return ErrNoKey, OpIndex: %v, Key: %v, ClientId: %v\n", kv.me, args.OpIndex, args.Key, args.ClientId)
			*reply = GetReply{Err: ErrNoKey}
		}
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	kv.mu.Lock()
	DPrintf2("%v server: Get RPC get lock, Key: %v, OpIndex: %v, ClientId: %v\n", kv.me, args.Key, args.OpIndex, args.ClientId)
	if kv.clientApplied[args.ClientId] == nil {
		kv.clientApplied[args.ClientId] = make(chan OpFields)
	}
	kv.mu.Unlock()
	DPrintf2("%v server: Get RPC release lock, args==%v\n", kv.me, args)

	select {
	case opf := <-kv.clientApplied[args.ClientId]:
		kv.mu.Lock()
		if opf.opIndex >= args.OpIndex {
			if value, ok := kv.table[args.Key]; ok {
				DPrintf("%v server: Get RPC return OK, OpIndex: %v, Key: %v, ClientId: %v\n", kv.me, args.OpIndex, args.Key, args.ClientId)
				*reply = GetReply{Err: OK, Value: value}
				// DPrintf("%v server: (%v,%v)\n", kv.me, opf.key, kv.table[opf.key])
				// kv.appliedLogs += fmt.Sprintf("(%v,%v,%v,%v,%v) ", opf.opIndex, opf.oper, opf.key, opf.value, opf.ckId)
				// DPrintf("%v server: applied log %v", kv.me, kv.appliedLogs)
			} else {
				DPrintf("%v server: Get RPC return ErrNoKey, OpIndex: %v, Key: %v, ClientId: %v\n", kv.me, args.OpIndex, args.Key, args.ClientId)
				*reply = GetReply{Err: ErrNoKey}
			}
			kv.maxOpIndexs[args.ClientId] = opf.opIndex

		} else { // should not exist
			DPrintf("%v server: Get RPC reply ErrWrongLeader due to out-of-date, OpIndex: %v, Key: %v, ClientId: %v\n", kv.me, args.OpIndex, args.Key, args.ClientId)
			*reply = GetReply{Err: ErrWrongLeader}
		}
		kv.mu.Unlock()
	case <-time.After(CHECK_CLIENTAPPLIED_TIMEOUT * time.Millisecond):
		DPrintf("%v server: Get RPC reply ErrWrongLeader due to timeout, OpIndex: %v, Key: %v, ClientId: %v\n", kv.me, args.OpIndex, args.Key, args.ClientId)
		*reply = GetReply{Err: ErrWrongLeader}
	}

	kv.mu.Lock()
	delete(kv.clientApplied, args.ClientId)
	kv.mu.Unlock()
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
	DPrintf("%v server: handling PutAppend RPC, OpIndex: %v, Key: %v, ClientId: %v, maxOpIndex:%v\n", kv.me, args.OpIndex, args.Key, args.ClientId, kv.maxOpIndexs[args.ClientId])

	//DPrintf("%v server: applied log %v", kv.me, kv.appliedLogs)

	op := Op{}
	op.EncodeOp(OpFields{opIndex: args.OpIndex, oper: args.Op, key: args.Key, value: args.Value, ckId: args.ClientId})
	_, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		DPrintf("%v server: PutAppend RPC reply ErrWrongLeader, OpIndex: %v, Key: %v, value: %v, ClientId: %v\n", kv.me, args.OpIndex, args.Key, args.Value, args.ClientId)
		*reply = PutAppendReply{Err: ErrWrongLeader}
		return
	}

	kv.mu.Lock()
	if args.OpIndex == kv.maxOpIndexs[args.ClientId] {
		DPrintf("%v server: PutAppend RPC return OK, OpIndex: %v, Key: %v, value: %v, ClientId: %v\n", kv.me, args.OpIndex, args.Key, args.Value, args.ClientId)
		*reply = PutAppendReply{Err: OK}
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	kv.mu.Lock()
	DPrintf2("%v server: PutAppend RPC get lock, Key: %v, Value: %v, Op: %v, OpIndex: %v, ClientId: %v\n", kv.me, args.Key, args.Value, args.Op, args.OpIndex, args.ClientId)
	if kv.clientApplied[args.ClientId] == nil {
		kv.clientApplied[args.ClientId] = make(chan OpFields)
	}
	kv.mu.Unlock()
	DPrintf2("%v server: PutAppend RPC release lock, args==%v\n", kv.me, args)

	select {
	case opf := <-kv.clientApplied[args.ClientId]:
		kv.mu.Lock()
		if opf.opIndex >= args.OpIndex {
			DPrintf("%v server: PutAppend RPC return OK, OpIndex: %v, Key: %v, value: %v, ClientId: %v\n", kv.me, args.OpIndex, args.Key, args.Value, args.ClientId)
			// kv.OperatePutAppend(opf)
			// kv.maxOpIndexs[args.ClientId] = opf.opIndex
			*reply = PutAppendReply{Err: OK}
			// DPrintf("%v server: (%v,%v)\n", kv.me, opf.key, kv.table[opf.key])
			// kv.appliedLogs += fmt.Sprintf("(%v,%v,%v,%v,%v) ", opf.opIndex, opf.oper, opf.key, opf.value, opf.ckId)
			// DPrintf("%v server: applied log %v", kv.me, kv.appliedLogs)
		} else { // should not exist
			DPrintf("%v server: PutAppend RPC reply ErrWrongLeader due to out-of-date, OpIndex: %v, Key: %v, value: %v, ClientId: %v\n", kv.me, args.OpIndex, args.Key, args.Value, args.ClientId)
			*reply = PutAppendReply{Err: ErrWrongLeader}
		}
		kv.mu.Unlock()
	case <-time.After(CHECK_CLIENTAPPLIED_TIMEOUT * time.Millisecond):
		DPrintf("%v server: PutAppend RPC reply ErrWrongLeader due to timeout, OpIndex: %v, Key: %v, value: %v, ClientId: %v\n", kv.me, args.OpIndex, args.Key, args.Value, args.ClientId)
		*reply = PutAppendReply{Err: ErrWrongLeader}
	}

	kv.mu.Lock()
	delete(kv.clientApplied, args.ClientId)
	kv.mu.Unlock()
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
	for kv.killed() == false {
		applyMsg := <-kv.applyCh

		if applyMsg.CommandValid {
			op, ok := applyMsg.Command.(Op)
			if !ok {
				log.Fatal(errors.New("applyMsg type error"))
			}
			opf := op.DecodeOp()

			DPrintf("%v server: applyCh receive logIdx: %v, OpIndex: %v, Key: %v, value: %v, ClientId: %v\n", kv.me, applyMsg.CommandIndex, opf.opIndex, opf.key, opf.value, opf.ckId)
			kv.mu.Lock()
			kv.maxCommandIndex = applyMsg.CommandIndex
			if opf.opIndex > kv.maxOpIndexs[opf.ckId] {
				kv.OperatePutAppend(opf)
				kv.maxOpIndexs[opf.ckId] = opf.opIndex
			}
			kv.mu.Unlock()

			select {
			case kv.clientApplied[opf.ckId] <- opf:
			default:
			}
		} else if applyMsg.SnapshotValid {
			kv.decodeAndApplySnapshot(applyMsg.Snapshot)
		}
	}
}

func (kv *KVServer) checkSnapshot() {
	for !kv.killed() {
		time.Sleep(CHECK_SNAPSHOT_INTERVAL * time.Millisecond)

		size := kv.ps.RaftStateSize()
		if size >= kv.maxraftstate {
			kv.mu.Lock()
			maxCommandIndex := kv.maxCommandIndex
			snapshot := kv.encodeSnapshot()
			kv.mu.Unlock()
			kv.rf.Snapshot(maxCommandIndex, snapshot)
		}
	}
}

// call with lock
func (kv *KVServer) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.table)
	e.Encode(kv.maxOpIndexs)
	e.Encode(kv.maxCommandIndex)
	snapshot := w.Bytes()

	return snapshot
}

func (kv *KVServer) decodeAndApplySnapshot(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var table map[string]string
	var maxOpIndexs map[int]int
	var maxCommandIndex int
	if d.Decode(&table) != nil ||
		d.Decode(&maxOpIndexs) != nil ||
		d.Decode(&maxCommandIndex) != nil {
		DPrintf("server %d: snapshot decoding error", kv.me)
		return
	}
	kv.mu.Lock()
	kv.table = table
	kv.maxOpIndexs = maxOpIndexs
	kv.maxCommandIndex = maxCommandIndex
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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.table = make(map[string]string)
	kv.maxOpIndexs = make(map[int]int)
	kv.clientApplied = make(map[int]chan OpFields)
	kv.ps = persister
	kv.maxCommandIndex = 0

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
