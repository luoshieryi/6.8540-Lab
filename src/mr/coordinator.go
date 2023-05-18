package mr

import (
	"log"
	"math"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)
import "net"
import "net/rpc"
import "net/http"

var (
	OutFilePrefix         = "mr-out-"
	TmpFilePrefix         = "mr-tmp-"
	WorkTypeMap           = "map"
	WorkTypeReduce        = "reduce"
	workStatusNone  int32 = 0
	workStatusDoing int32 = 1
	workStatusDone  int32 = 2
	workStatusFail  int32 = 3
)

func GenerateTmpFileNames(iMap, nReduce int32) (tmpFiles []string) {
	for i := int32(0); i < nReduce; i++ {
		tmpFiles = append(tmpFiles, TmpFilePrefix+strconv.Itoa(int(iMap))+"-"+strconv.Itoa(int(i)))
	}
	return tmpFiles
}

func GenerateReduceFileNames(iReduce, nMap int32) (reduceFiles []string) {
	for i := int32(0); i < nMap; i++ {
		reduceFiles = append(reduceFiles, TmpFilePrefix+strconv.Itoa(int(i))+"-"+strconv.Itoa(int(iReduce)))
	}
	return reduceFiles
}

func GenerateOutFileName(iReduce int32) string {
	return OutFilePrefix + strconv.Itoa(int(iReduce))
}

func initWork(c *Coordinator, workType string, n int32) {
	for i := int32(0); i < n; i++ {
		c.workQueue <- i
		switch workType {
		case WorkTypeMap:
			c.mapStatus[i] = workStatusNone
		case WorkTypeReduce:
			c.reduceStatus[i] = workStatusNone
		}
	}
}

type Coordinator struct {
	// Your definitions here.
	nMap, nReduce           int32
	mapStatus, reduceStatus []int32
	dMapLock, dReduceLock   sync.Mutex
	dMapCount, dReduceCount int32
	mapDone, reduceDone     bool
	filenames               []string
	workQueue               chan int32
}

func DefaultCoordinator() *Coordinator {
	return &Coordinator{
		nMap:         0,
		nReduce:      10,
		mapStatus:    []int32{}, // mapStatus[iMap], map任务状态
		reduceStatus: []int32{}, // reduceStatus[iReduce], reduce任务状态
		mapDone:      false,
		reduceDone:   false,
		filenames:    []string{},           // filenames[iMap], 输入文件
		workQueue:    make(chan int32, 10), // 正在工作的任务队列
	}
}

func (c *Coordinator) NewMap(args *NewMapArgs, reply *NewMapReply) error {

	//log.Println("NewMapRequest", c)

	if c.mapDone {
		reply.MapDone = true
		return nil
	}
	select {
	case iMap := <-c.workQueue:
		if c.mapStatus[iMap] == workStatusDoing || c.mapStatus[iMap] == workStatusDone {
			return nil
		}
		c.mapStatus[iMap] = workStatusDoing
		reply.Filename = c.filenames[iMap]
		reply.NReduce = int(c.nReduce)
		reply.IMap = int(iMap)
		time.AfterFunc(time.Second*10, func() {
			c.dMapLock.Lock()
			if c.mapStatus[iMap] == workStatusDoing {
				c.mapStatus[iMap] = workStatusFail
				c.workQueue <- iMap
			}
			c.dMapLock.Unlock()
		})
	default:
		reply.AllMapping = true
	}
	return nil
}

func (c *Coordinator) DoneMap(args *DoneMapArgs, reply *DoneMapReply) error {

	//log.Println("DoneMapRequest", c)

	c.dMapLock.Lock()
	if c.mapStatus[args.IMap] == workStatusDone {
		c.dMapLock.Unlock()
		return nil
	}
	c.mapStatus[args.IMap] = workStatusDone
	c.dMapLock.Unlock()

	if count := atomic.AddInt32(&c.dMapCount, 1); count == c.nMap {
		c.mapDone = true
		initWork(c, WorkTypeReduce, c.nReduce)
	}

	//log.Println("DoneMapRequestEnd", c)

	return nil
}

func (c *Coordinator) NewReduce(args *NewReduceArgs, reply *NewReduceReply) error {
	if c.reduceDone {
		reply.ReduceDone = true
		return nil
	}
	select {
	case iReduce := <-c.workQueue:
		if c.reduceStatus[iReduce] == workStatusDoing || c.reduceStatus[iReduce] == workStatusDone {
			return nil
		}
		c.reduceStatus[iReduce] = workStatusDoing
		reply.IReduce = int(iReduce)
		reply.NMap = int(c.nMap)
		reply.Filename = GenerateOutFileName(iReduce)
		time.AfterFunc(time.Second*10, func() {
			c.dReduceLock.Lock()
			if c.reduceStatus[iReduce] == workStatusDoing {
				c.reduceStatus[iReduce] = workStatusFail
				c.workQueue <- iReduce
			}
			c.dReduceLock.Unlock()
		})
	default:
		reply.AllReducing = true
	}
	return nil
}

func (c *Coordinator) DoneReduce(args *DoneReduceArgs, reply *DoneReduceReply) error {
	c.dReduceLock.Lock()
	if c.reduceStatus[args.IReduce] == workStatusDone {
		c.dReduceLock.Unlock()
		return nil
	}
	c.reduceStatus[args.IReduce] = workStatusDone
	c.dReduceLock.Unlock()

	if count := atomic.AddInt32(&c.dReduceCount, 1); count == c.nReduce {
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
	c.filenames = files
	c.mapStatus = make([]int32, c.nMap)
	c.reduceStatus = make([]int32, c.nReduce)
	c.workQueue = make(chan int32, int(math.Max(float64(c.nMap), float64(c.nReduce))))

	initWork(c, WorkTypeMap, c.nMap)

	c.server()
	return c
}
