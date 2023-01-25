package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	S    *ExampleSt
	STRR string
	Y    int
}

type ExampleSt struct {
	SS string
}

// Add your RPC definitions here.
// RPC type enum definitions
const (
	TASK_MAP = iota
	TASK_REDUCE
	TASK_NONE
	TASK_WAIT
)

type RPCArgs struct {
	RPCType     int
	TaskType    int
	ReduceIndex int
	FileName    string
	NodeId      int
}

type RPCReply struct {
	TaskType    int
	NReduce     int
	MapIndex    int
	ReduceIndex int
	TotalMapNum int
	FileName    string
	NodeId      int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
