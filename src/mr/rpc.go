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
// Add your RPC definitions here.
//

type NewWorkArgs struct {
}

type NewWorkReply struct {
	WorkType int
	WorkDone bool
	IWork    int
	NMap     int
	NReduce  int
	Filename string
}

func CallNewWork(args *NewWorkArgs) (NewWorkReply, bool) {
	reply := NewWorkReply{}
	ok := call("Coordinator.NewWork", args, &reply)
	return reply, ok
}

type DoneWorkArgs struct {
	IWork    int
	WorkType int
}

type DoneWorkReply struct {
}

func CallDoneWork(args *DoneWorkArgs) (DoneWorkReply, bool) {
	reply := DoneWorkReply{}
	ok := call("Coordinator.DoneWork", args, &reply)
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
