package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	maxOpIndex int
	mu         sync.Mutex
	id         int
	leaderId   int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.maxOpIndex = 0
	ck.id = time.Now().Nanosecond()
	DPrintf("making clerk, id==%v\n", ck.id)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.mu.Lock()
	ck.maxOpIndex++
	opIndex := ck.maxOpIndex
	ck.mu.Unlock()

	i := ck.leaderId
	args := GetArgs{Key: key, OpIndex: opIndex, ClientId: ck.id}
	reply := GetReply{}

	for {
		DPrintf("client: calling Get to server %v, Key: %v, OpIndex: %v, ClientId: %v\n", i, args.Key, args.OpIndex, args.ClientId)
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			DPrintf("client: Get to server %v return wrongleader, Key: %v, OpIndex: %v, ClientId: %v\n", i, args.Key, args.OpIndex, args.ClientId)
			i = (i + 1) % len(ck.servers)
		} else {
			ck.leaderId = i
			break
		}
	}

	DPrintf("client: Get to server %v ok, Key: %v, OpIndex: %v, ClientId: %v\n", i, args.Key, args.OpIndex, args.ClientId)
	if reply.Err == ErrNoKey {
		return ""
	} else {
		return reply.Value
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	ck.maxOpIndex++
	opIndex := ck.maxOpIndex
	ck.mu.Unlock()

	i := ck.leaderId
	args := PutAppendArgs{Key: key, Value: value, Op: op, OpIndex: opIndex, ClientId: ck.id}
	reply := PutAppendReply{}

	for {
		DPrintf("client: calling PutAppend to server %v, Key: %v, Value: %v, Op: %v, OpIndex: %v, ClientId: %v\n", i, args.Key, args.Value, args.Op, args.OpIndex, args.ClientId)
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			DPrintf("client: PutAppend to server %v return wrongleader, Key: %v, Value: %v, Op: %v, OpIndex: %v, ClientId: %v\n", i, args.Key, args.Value, args.Op, args.OpIndex, args.ClientId)
			i = (i + 1) % len(ck.servers)
		} else {
			ck.leaderId = i
			break
		}
	}
	DPrintf("client: PutAppend to server %v ok, Key: %v, Value: %v, Op: %v, OpIndex: %v, ClientId: %v\n", i, args.Key, args.Value, args.Op, args.OpIndex, args.ClientId)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
