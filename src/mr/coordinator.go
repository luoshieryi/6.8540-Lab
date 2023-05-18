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
	OutFilePrefix   = "mr-out-"
	TmpFilePrefix   = "mr-tmp-"
	WorkTypeNone    = 0
	WorkTypeMap     = 1
	WorkTypeReduce  = 2
	WorkTypeDone    = 3
	workStatusNone  = 0
	workStatusDoing = 1
	workStatusDone  = 2
	workStatusFail  = 3
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

func initWork(c *Coordinator, workType int, nWork int32) {
	c.workType = workType
	c.dWorkCount = 0
	for i := int32(0); i < nWork; i++ {
		c.workQueue <- i
		c.workStatus[i] = workStatusNone
	}
}

type Coordinator struct {
	// Your definitions here.
	nMap, nReduce int32
	filenames     []string
	workType      int
	workStatus    []int
	dWorkLock     sync.Mutex
	dWorkCount    int32
	workQueue     chan int32
}

func DefaultCoordinator() *Coordinator {
	return &Coordinator{
		nMap:       0,
		nReduce:    10,
		filenames:  []string{}, // filenames[iWork], 输入文件
		workType:   WorkTypeNone,
		workStatus: []int{}, // workStatus[iWork], 任务状态
		dWorkLock:  sync.Mutex{},
		dWorkCount: 0,
		workQueue:  make(chan int32, 10), // 正在工作的任务队列
	}
}

func (c *Coordinator) NewWork(args *NewWorkArgs, reply *NewWorkReply) error {

	//log.Println("new work", c)

	if c.workType == WorkTypeDone {
		reply.WorkType = WorkTypeDone
		return nil
	}

	select {
	case iWork := <-c.workQueue:
		if c.workStatus[iWork] == workStatusDoing || c.workStatus[iWork] == workStatusDone {
			reply.WorkType = WorkTypeNone
			return nil
		}

		c.workStatus[iWork] = workStatusDoing
		reply.WorkType = c.workType
		reply.IWork = int(iWork)
		reply.NMap = int(c.nMap)
		reply.NReduce = int(c.nReduce)
		switch c.workType {
		case WorkTypeMap:
			reply.Filename = c.filenames[iWork]
		case WorkTypeReduce:
			reply.Filename = GenerateOutFileName(iWork)
		}

		time.AfterFunc(time.Second*10, func() {
			c.dWorkLock.Lock()
			if reply.WorkType == c.workType && c.workStatus[iWork] == workStatusDoing {
				c.workStatus[iWork] = workStatusFail
				c.workQueue <- iWork
			}
			c.dWorkLock.Unlock()
		})
	default:
		reply.WorkType = WorkTypeNone
	}

	return nil
}

func (c *Coordinator) DoneWork(args *DoneWorkArgs, reply *DoneWorkReply) error {
	c.dWorkLock.Lock()
	// WorkType避免过期的任务提交
	if c.workStatus[args.IWork] == workStatusDone || c.workType != args.WorkType {
		c.dWorkLock.Unlock()
		return nil
	}
	c.workStatus[args.IWork] = workStatusDone
	c.dWorkLock.Unlock()

	switch c.workType {
	case WorkTypeMap:
		if count := atomic.AddInt32(&c.dWorkCount, 1); count == c.nMap {
			initWork(c, WorkTypeReduce, c.nReduce)
		}
	case WorkTypeReduce:
		if count := atomic.AddInt32(&c.dWorkCount, 1); count == c.nReduce {
			c.workType = WorkTypeDone
		}
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
	return c.workType == WorkTypeDone
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
	mxN := int(math.Max(float64(c.nMap), float64(c.nReduce)))
	c.workStatus = make([]int, mxN)
	c.workQueue = make(chan int32, mxN)

	initWork(c, WorkTypeMap, c.nMap)

	c.server()
	return c
}
