package mr

import (
	"log"
	"os"
	"strconv"
	"sync/atomic"
)
import "net"
import "net/rpc"
import "net/http"

var (
	OutFilePrefix = "mr-out-"
	TmpFilePrefix = "mr-tmp-"
)

type Coordinator struct {
	// Your definitions here.
	nMap, nReduce       int32
	iMap, iReduce       int32
	dMap, dReduce       int32
	mapDone, reduceDone bool
	files               []string
}

func DefaultCoordinator() *Coordinator {
	return &Coordinator{
		nMap:       10,
		nReduce:    10,
		iMap:       0,
		iReduce:    0,
		mapDone:    false,
		reduceDone: false,
		files:      []string{},
	}
}

// Your code here -- RPC handlers for the worker to call.

// Example
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) NewMap(args *NewMapArgs, reply *NewMapReply) error {
	//log.Println("NewMap c", c)
	for atomic.LoadInt32(&c.iMap) < c.nMap {
		n := atomic.AddInt32(&c.iMap, 1)
		log.Println("NewMap n", n)
		if n <= int32(len(c.files)) {
			n = n - 1
			reply.Filename = c.files[n]
			reply.TmpFilenamePrefix = TmpFilePrefix
			reply.NReduce = int(c.nReduce)
			reply.IMap = int(n)
			return nil
		}
	}
	reply.AllMapping = true
	return nil
}

func (c *Coordinator) DoneMap(args *DoneMapArgs, reply *DoneMapReply) error {
	atomic.AddInt32(&c.dMap, 1)
	if atomic.LoadInt32(&c.dMap) == c.nMap {
		c.mapDone = true
	}
	return nil
}

func (c *Coordinator) NewReduce(args *NewReduceArgs, reply *NewReduceReply) error {
	if !c.mapDone {
		reply.MapDone = false
		return nil
	}
	for atomic.LoadInt32(&c.iReduce) < c.nReduce {
		n := atomic.AddInt32(&c.iReduce, 1)
		if n <= c.nReduce {
			n = n - 1
			reply.Filename = OutFilePrefix + strconv.Itoa(int(n))
			for i := int32(0); i < c.nMap; i++ {
				reply.TmpFilenames = append(reply.TmpFilenames, TmpFilePrefix+strconv.Itoa(int(i))+"-"+strconv.Itoa(int(n)))
			}
			return nil
		}
	}
	reply.AllReducing = true
	return nil
}

func (c *Coordinator) DoneReduce(args *DoneReduceArgs, reply *DoneReduceReply) error {
	atomic.AddInt32(&c.dReduce, 1)
	if atomic.LoadInt32(&c.dReduce) == c.nReduce {
		c.reduceDone = true
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.reduceDone
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := DefaultCoordinator()

	c.nMap = int32(len(files))
	c.nReduce = int32(nReduce)
	c.files = files

	c.server()
	return c
}
