package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		reply := SendRequest()
		if reply == nil || reply.TaskType == TASK_NONE {
			break
		} else if reply.TaskType == TASK_MAP {
			doMap(reply, mapf)
		} else if reply.TaskType == TASK_REDUCE {
			doReduce(reply, reducef)
		} else { // TASK_WAIT
			time.Sleep(100 * time.Millisecond)
		}
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		//fmt.Printf("reply %v\n", reply)
		//fmt.Printf("str %v\n", reply.S.SS)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func SendRequest() *RPCReply {
	args := RPCArgs{}
	reply := RPCReply{}

	ok := call("Coordinator.DispatchTask", &args, &reply)
	if ok {
		//fmt.Printf("args in sendRequest:%v", args)
		//fmt.Printf("reply in sendRequest:%v", reply)
		return &reply
	} else {
		fmt.Print("SendRequest call return error")
		return nil
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func doMap(reply *RPCReply, mapf func(string, string) []KeyValue) {
	//fmt.Printf("doing map\n")
	//fmt.Printf("reply: %v\n", reply)
	file, err := os.Open(reply.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", reply.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.FileName)
	}
	file.Close()
	kva := mapf(reply.FileName, string(content))

	sort.Sort(ByKey(kva))
	for i := 0; i < len(kva); {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}

		filename := fmt.Sprintf("mr-%d-%d-%d", reply.MapIndex, ihash(kva[i].Key)%reply.NReduce, reply.NodeId)
		//filename := fmt.Sprintf("mr-%d-%d", reply.MapIndex, ihash(kva[i].Key)%reply.NReduce)
		file, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
		if err != nil {
			log.Fatalf("cannot open %v here", filename)
		}
		enc := json.NewEncoder(file)
		for ; i < j; i++ {
			err := enc.Encode(&kva[i])
			if err != nil {
				log.Fatalf("cannot write json to file %v", filename)
			}
		}
		file.Close()
	}

	SendMapDone(reply.FileName, reply.NodeId)
	//fmt.Printf("map done\n")
}

func SendMapDone(filename string, nodeId int) {
	args := &RPCArgs{TaskType: TASK_MAP, FileName: filename, NodeId: nodeId}
	reply := &RPCReply{}

	ok := call("Coordinator.HandleTaskDone", &args, &reply)
	if !ok {
		fmt.Print("unable to call handleTaskDone")
	}
}

func doReduce(reply *RPCReply, reducef func(string, []string) string) {
	//fmt.Printf("doing reduce\n")
	//fmt.Printf("reply: %v\n", reply)
	var kva []KeyValue

	for i := 0; i < reply.TotalMapNum; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, reply.ReduceIndex)
		file, err := os.Open(filename)
		if err != nil {
			//fmt.Printf("cannot open file %v\n", filename)
			continue
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}

		file.Close()
	}

	sort.Sort(ByKey(kva))

	filename := fmt.Sprintf("mr-out-%d-%d", reply.ReduceIndex, reply.NodeId)
	//filename := fmt.Sprintf("mr-out-%d", reply.ReduceIndex)
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}

	for i := 0; i < len(kva); {
		values := make([]string, 0)
		values = append(values, kva[i].Value)
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			values = append(values, kva[j].Value)
			j++
		}

		result := reducef(kva[i].Key, values)
		fmt.Fprintf(file, "%v %v\n", kva[i].Key, result)

		i = j
	}

	file.Close()

	SendReduceDone(reply.ReduceIndex, reply.NodeId)
	//fmt.Printf("reduce done\n")
}

func SendReduceDone(reduceIndex int, nodeId int) {
	args := &RPCArgs{TaskType: TASK_REDUCE, ReduceIndex: reduceIndex, NodeId: nodeId}
	reply := &RPCReply{}

	ok := call("Coordinator.HandleTaskDone", &args, &reply)
	if !ok {
		fmt.Print("unable to call handleTaskDone")
	}
}
