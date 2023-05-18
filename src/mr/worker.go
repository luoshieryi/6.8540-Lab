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

func doMap(iMap, nReduce int, filename string, mapf func(string, string) []KeyValue) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	kva := mapf(filename, string(content))
	intermediates := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		iReduce := ihash(kv.Key) % nReduce
		intermediates[iReduce] = append(intermediates[iReduce], kv)
	}

	tmpFiles := GenerateTmpFileNames(int32(iMap), int32(len(intermediates)))
	for iReduce, intermediate := range intermediates {

		//log.Println("TmpMap: ", tmpFiles[iReduce])

		file, _ := os.Create(tmpFiles[iReduce])
		enc := json.NewEncoder(file)
		for _, kv := range intermediate {
			enc.Encode(&kv)
		}
		file.Close()
	}

	return
}

func doReduce(iReduce int, nMap int, filename string, reducef func(string, []string) string) {
	intermediate := make([]KeyValue, 0)
	//log.Println("ReduceFileNames: ", GenerateReduceFileNames(int32(reply.IReduce), int32(reply.NMap)))
	for _, filename := range GenerateReduceFileNames(int32(iReduce), int32(nMap)) {
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
	oname := filename
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
}

// Worker
// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.
	// log.Println("Worker started")
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		reply, _ := CallNewWork(&NewWorkArgs{})

		//log.Println("new work: ", reply)

		switch reply.WorkType {
		case WorkTypeNone:
			time.Sleep(time.Second)
		case WorkTypeMap:
			doMap(reply.IWork, reply.NReduce, reply.Filename, mapf)
			CallDoneWork(&DoneWorkArgs{IWork: reply.IWork, WorkType: WorkTypeMap})
		case WorkTypeReduce:
			doReduce(reply.IWork, reply.NMap, reply.Filename, reducef)
			CallDoneWork(&DoneWorkArgs{IWork: reply.IWork, WorkType: WorkTypeReduce})
		case WorkTypeDone:
			return
		}
	}

}
