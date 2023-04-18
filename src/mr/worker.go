package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"time"
)

// KeyValue
// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// ByKey for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker
// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	//log.Println("Worker started")
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		reply, _ := CallNewMap(&NewMapArgs{})
		//log.Println("NewMapReply: ", reply)
		if reply.AllMapping {
			break
		}

		file, err := os.Open(reply.Filename)
		if err != nil {
			log.Fatalf("cannot open %v", reply.Filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", reply.Filename)
		}
		file.Close()
		kva := mapf(reply.Filename, string(content))
		intermediates := make([][]KeyValue, reply.NReduce)
		for _, kv := range kva {
			iReduce := ihash(kv.Key) % reply.NReduce
			intermediates[iReduce] = append(intermediates[iReduce], kv)
		}
		CallDoneMap(&DoneMapArgs{intermediates})
	}

	for {
		reply, _ := CallNewReduce(&NewReduceArgs{})

		//log.Println("NewReduceFile: ", reply.Filename)

		if reply.MapDone {
			time.Sleep(time.Second)
		}

		if reply.AllReducing {
			break
		}

		intermediate := reply.Intermediate
		sort.Sort(ByKey(intermediate))
		oname := reply.Filename
		ofile, _ := os.Create(oname)

		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			output := reducef(intermediate[i].Key, values)
			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
			i = j
		}

		ofile.Close()
		CallDoneReduce(&DoneReduceArgs{})
	}

}
