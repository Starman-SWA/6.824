package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	files   []string
	nReduce int

	mapStates []int // 0:not dispatched, 1:dispatched, 2:done
	mapDone   bool
	mapMutex  sync.Mutex

	reduceStates []int // 0:not dispatched, 1:dispatched, 2:done
	reduceDone   bool
	reduceMutex  sync.Mutex
}

func (c *Coordinator) searchFileName(filename string) int {
	for i, file := range c.files {
		if file == filename {
			return i
		}
	}
	return -1
}

func (c *Coordinator) checkMapDone() bool {
	for _, state := range c.mapStates {
		if state != 2 {
			return false
		}
	}
	return true
}

func (c *Coordinator) checkReduceDone() bool {
	for _, state := range c.reduceStates {
		if state != 2 {
			return false
		}
	}
	return true
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	st := &ExampleSt{"i win"}
	reply.S = st
	reply.Y = 222
	reply.STRR = "GSG"
	// fmt.Printf("reply:%v\n", reply)

	return nil
}

func (c *Coordinator) DispatchTask(args *RPCArgs, reply *RPCReply) error {
	c.mapMutex.Lock()
	//fmt.Printf("locked\n")
	//fmt.Printf("c.mapStates: %v\n", c.mapStates)
	if !c.mapDone {
		// find a map task
		for i, state := range c.mapStates {
			if state == 0 {
				//fmt.Printf("dispatching map task\n")
				//fmt.Printf("args:%v\n", args)
				//fmt.Printf("origin reply:%v\n", reply)
				c.mapStates[i] = 1
				reply.TaskType = TASK_MAP
				reply.NReduce = c.nReduce
				reply.MapIndex = i
				reply.FileName = c.files[i]
				//fmt.Printf("mapindex:%v, reply dispatched:%v\n", i, reply)
				//c.mutex.Unlock()
				//fmt.Printf("unlocked\n")
				c.mapMutex.Unlock()
				return nil
			}
		}
		c.mapMutex.Unlock()
	} else {
		c.mapMutex.Unlock()
		c.reduceMutex.Lock()
		// find a reduce task
		for i, state := range c.reduceStates {
			if state == 0 {
				//fmt.Printf("dispatching reduce task\n")
				c.reduceStates[i] = 1
				reply.TaskType = TASK_REDUCE
				reply.ReduceIndex = i
				reply.TotalMapNum = len(c.files)
				//c.mutex.Unlock()
				//fmt.Printf("unlocked\n")
				c.reduceMutex.Unlock()
				return nil
			}
		}
		c.reduceMutex.Unlock()
	}

	c.reduceMutex.Lock()
	if c.reduceDone {
		reply.TaskType = TASK_NONE
	} else {
		reply.TaskType = TASK_WAIT
	}
	c.reduceMutex.Unlock()
	//c.mutex.Unlock()
	//fmt.Printf("unlocked\n")
	return nil
}

func (c *Coordinator) HandleTaskDone(args *RPCArgs, reply *RPCReply) error {
	if args.TaskType == TASK_MAP {
		i := c.searchFileName(args.FileName)
		if i == -1 {
			return errors.New("handleTaskDone task index error")
		}
		c.mapMutex.Lock()
		c.mapStates[i] = 2

		if c.checkMapDone() {
			c.mapDone = true
		}
		c.mapMutex.Unlock()
	} else if args.TaskType == TASK_REDUCE {
		c.reduceMutex.Lock()
		c.reduceStates[args.ReduceIndex] = 2
		if c.checkReduceDone() {
			c.reduceDone = true
		}
		c.reduceMutex.Unlock()
	} else {
		return errors.New("handleTaskDone task type error")
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// your code here
	c.reduceMutex.Lock()
	defer c.reduceMutex.Unlock()
	return c.reduceDone
}

// func (c *Coordinator) checkDispatchedTasks() {

// 	time.Sleep(10 * time.Second)
// }

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.mapStates = make([]int, len(files))
	//fmt.Println(c.mapStates)
	c.reduceStates = make([]int, nReduce)
	//fmt.Println(c.reduceStates)
	c.nReduce = nReduce
	//fmt.Printf("nreduce %v\n", c.nReduce)
	c.mapDone = false
	c.reduceDone = false

	c.server()
	return &c
}
