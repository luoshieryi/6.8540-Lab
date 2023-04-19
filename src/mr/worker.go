package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
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
func DoneMap(intermediates [][]KeyValue, tmpFilenamePrefix string, iMap int) {
	for iReduce, intermediate := range intermediates {
		filename := tmpFilenamePrefix + strconv.Itoa(iMap) + "-" + strconv.Itoa(iReduce)
		log.Println("TmpMap: ", filename)
		file, _ := os.Create(filename)
		enc := json.NewEncoder(file)
		for _, kv := range intermediate {
			enc.Encode(&kv)
		}
		file.Close()
	}
}

// Worker
// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// log.Println("Worker started")
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
		DoneMap(intermediates, reply.TmpFilenamePrefix, reply.IMap)
		CallDoneMap(&DoneMapArgs{})
	}

	for {
		reply, _ := CallNewReduce(&NewReduceArgs{})

		// log.Println("NewReduceFile: ", reply.Filename)

		if reply.MapDone {
			time.Sleep(time.Second)
		}

		if reply.AllReducing {
			break
		}

		intermediate := make([]KeyValue, 0)
		for _, filename := range reply.TmpFilenames {
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
		CallDoneReduce(&DoneReduceArgs{})
	}

}
