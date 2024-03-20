package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func partition(kva []KeyValue, nReduce int) map[int][]KeyValue {
    kvag := make(map[int][]KeyValue)
    for _, kv := range kva {
        id := ihash(kv.Key) % nReduce
        kvag[id] = append(kvag[id], kv)
    } 
    return kvag
}

func pollMapTask(mapf func(string, string) []KeyValue) Err {
    args := PollMapTaskArgs{}
    reply := PollMapTaskReply{}
    for {
        if ok := call("Coordinator.PollMapTask", &args, &reply); ok {
            break
        }
        time.Sleep(time.Millisecond * 500)
    }
    if reply.Error != OK {
        return reply.Error
    }

    filename := reply.File
    file, err := os.Open(filename)
    if err != nil {
        log.Fatalf("cannot open %v", filename)
    }
    content, err := ioutil.ReadAll(file)
    if err != nil {
        log.Fatalf("cannot read %v", filename)
    }
    file.Close()

    kva := mapf(filename, string(content))
    kvag := partition(kva, reply.NReduce)
    ofiles := make(map[int]string)
    for reduceTaskId, kva := range kvag {
        tempFile, err := os.CreateTemp("", "")
        if err != nil {
            log.Fatal("cannot open temp file")
        }
        enc := json.NewEncoder(tempFile)
        for _, kv := range kva {
            if err := enc.Encode(&kv); err != nil {
                log.Fatal("cannot encode kv")
            }
        }
        tempFile.Close()

        oname := fmt.Sprintf("mr-%v-%v", reply.MapTaskId, reduceTaskId)
        if err := os.Rename(tempFile.Name(), oname); err != nil {
            log.Fatal("cannot rename temp file")
        }
        ofiles[reduceTaskId] = oname
    }
    notifyMapTaskDone(ofiles, reply.MapTaskId)  
    return OK
}


func pollReduceTask(reducef func(string, []string) string) Err {
    args := PollReduceTaskArgs {}
    reply := PollReduceTaskReply{}
    for {
        if ok := call("Coordinator.PollReduceTask", &args, &reply); ok {
            break
        }
        time.Sleep(time.Millisecond * 500)
    }
    if reply.Error != OK {
        return reply.Error
    }

    kvag := make(map[string][]string)
    for _, filename := range reply.Files {
        file, err := os.Open(filename)
        if err != nil {
            log.Fatalf("cannot open %v", filename)
        }
        dec := json.NewDecoder(file)
        var kv KeyValue
        for {
            err = dec.Decode(&kv)
            if err != nil {
                break
            }
            kvag[kv.Key] = append(kvag[kv.Key], kv.Value)
        }
        file.Close()
    }

    tempFile, err := os.CreateTemp("", "")
    if err != nil {
        log.Fatal("cannot open temp file")
    }
    for k, va := range kvag {
        v := reducef(k, va)
        fmt.Fprintf(tempFile, "%v %v\n", k, v)
    }
    tempFile.Close()

    oname := fmt.Sprintf("mr-out-%v", reply.ReduceTaskId)
    if err := os.Rename(tempFile.Name(), oname); err != nil {
        log.Fatal("cannot rename temp file")
    }
    notifyReduceTaskDone(reply.ReduceTaskId)
    return OK
}

//
// It is not feasible to use channel or conditional variable to notify 
// the coordinator when we finish the task, since both of them are 
// in-memory data structure and can only works in a single process.
// So we use RPC instead. Note that this one should be idempotent like 
// other RPC.
//
func notifyMapTaskDone(outputFiles map[int]string, mapTaskId int) {
    args := NotifyMapTaskDoneArgs {
        OutputFiles: outputFiles,
        MapTaskId: mapTaskId,
    }
    reply := NotifyMapTaskDoneReply{}

    for {
        if ok := call("Coordinator.NotifyMapTaskDone", &args, &reply); ok {
            break
        }
        time.Sleep(time.Millisecond * 500)
    }
}

func notifyReduceTaskDone(reduceTaskId int) {
    args := NotifyReduceTaskDoneArgs {
        ReduceTaskId: reduceTaskId,
    }
    reply := PollReduceTaskReply{}
    for {
        if ok := call("Coordinator.NotifyReducdeTaskDone", &args, &reply); ok {
            break
        }
        time.Sleep(time.Millisecond * 500)
    }
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
    for {
        err := pollMapTask(mapf)
        if err == ErrNotAvail {
            time.Sleep(5 * time.Second)
        }
        if err == EOF {
            break
        }
    }

    for {
        err := pollReduceTask(reducef)
        if err == ErrNotAvail {
            time.Sleep(5 * time.Second)
        }
        if err == EOF {
            break
        }
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
