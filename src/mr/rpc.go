package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"log"
	"net/rpc"
	"os"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

//
// Add your RPC definitions here.
//

type NewMapArgs struct {
}

type NewMapReply struct {
	Filename          string
	TmpFilenamePrefix string
	NReduce           int
	IMap              int
	AllMapping        bool
}

func CallNewMap(args *NewMapArgs) (NewMapReply, bool) {
	reply := NewMapReply{}
	ok := call("Coordinator.NewMap", args, &reply)
	return reply, ok
}

type DoneMapArgs struct {
}

type DoneMapReply struct {
}

func CallDoneMap(args *DoneMapArgs) (DoneMapReply, bool) {
	reply := DoneMapReply{}
	ok := call("Coordinator.DoneMap", args, &reply)
	return reply, ok
}

type NewReduceArgs struct {
}

type NewReduceReply struct {
	Filename     string
	TmpFilenames []string
	AllReducing  bool
	MapDone      bool
}

func CallNewReduce(args *NewReduceArgs) (NewReduceReply, bool) {
	reply := NewReduceReply{}
	ok := call("Coordinator.NewReduce", args, &reply)
	return reply, ok
}

type DoneReduceArgs struct {
}

type DoneReduceReply struct {
}

func CallDoneReduce(args *DoneReduceArgs) (DoneReduceReply, bool) {
	reply := DoneReduceReply{}
	ok := call("Coordinator.DoneReduce", args, &reply)
	return reply, ok
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	//c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
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

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
