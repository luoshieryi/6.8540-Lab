package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
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

// DoneMap
// map后产生的中间文件
func DoneMap(intermediates [][]KeyValue, iMap int) {
	tmpFiles := GenerateTmpFileNames(int32(iMap), int32(len(intermediates)))
	for iReduce, intermediate := range intermediates {

		//log.Println("TmpMap: ", tmpFiles[iReduce])	// Log

		file, _ := os.Create(tmpFiles[iReduce])
		enc := json.NewEncoder(file)
		for _, kv := range intermediate {
			enc.Encode(&kv)
		}
		file.Close()
	}
}

// Worker
// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.
	// log.Println("Worker started")
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		reply, _ := CallNewMap(&NewMapArgs{})

		//log.Println("NewMapRequest: ", reply) // Log

		if reply.AllMapping {
			time.Sleep(time.Second)
			continue
		}
		if reply.MapDone {
			break
		}

		//log.Println("NewMapWork: ", reply) // Log

		file, err := os.Open(reply.Filename)
		if err != nil {
			log.Fatalf("cannot open %v", reply.Filename)
		}
		content, err := io.ReadAll(file)
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

		//log.Println("DoneMap: ", reply.IMap) // Log

		DoneMap(intermediates, reply.IMap)

		//log.Println("CallDoneMap: ", reply.IMap) // Log

		CallDoneMap(&DoneMapArgs{IMap: int32(reply.IMap)})
	}

	for {
		reply, _ := CallNewReduce(&NewReduceArgs{})

		//log.Println("NewReduceWork: ", reply)

		if reply.AllReducing {
			time.Sleep(time.Second)
			continue
		}

		if reply.ReduceDone {
			break
		}

		intermediate := make([]KeyValue, 0)
		//log.Println("ReduceFileNames: ", GenerateReduceFileNames(int32(reply.IReduce), int32(reply.NMap)))
		for _, filename := range GenerateReduceFileNames(int32(reply.IReduce), int32(reply.NMap)) {
			file, _ := os.Open(filename)
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				intermediate = append(intermediate, kv)
			}
			file.Close()
		}
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
		CallDoneReduce(&DoneReduceArgs{IReduce: int32(reply.IReduce)})
	}

}
