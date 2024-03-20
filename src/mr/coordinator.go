package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)


type Coordinator struct {
	// Your definitions here.
    mu sync.Mutex
    nReduce int

    MapTasks map[int]*MapTask
    ReduceTasks map[int]*ReduceTask
}

type MapTask struct {
    inputFile string
    pending bool
}

type ReduceTask struct {
    inputFiles []string
    pending bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) PollMapTask(args *PollMapTaskArgs, 
    reply *PollMapTaskReply) error {
    c.mu.Lock()
    defer c.mu.Unlock()
    // All Map Tasks are done?
    if len(c.MapTasks) == 0 {
        reply.Error = EOF
        return nil
    }
    for mapTaskId, task := range c.MapTasks {
        if task.pending {
            continue
        }
        reply.File = task.inputFile
        reply.MapTaskId = mapTaskId
        reply.NReduce = c.nReduce
        task.pending = true
        go func(mapTaskId int) {
            <- time.After(time.Second * 10)
            c.mu.Lock()
            defer c.mu.Unlock()
            if task, ok := c.MapTasks[mapTaskId]; ok {
                // mark the task ready to be rescheduled
                task.pending = false
            }
        }(mapTaskId)
        reply.Error = OK
        return nil
    }
    reply.Error = ErrNotAvail
    return nil
}

func (c *Coordinator) PollReduceTask(args *PollReduceTaskArgs, 
    reply *PollReduceTaskReply) error {
    c.mu.Lock()
    defer c.mu.Unlock()
    // Some Map Tasks not finished yet?
    if len(c.MapTasks) > 0 {
        reply.Error = ErrNotAvail
        return nil
    }
    // All Reduce Tasks done?
    if len(c.ReduceTasks) == 0 {
        reply.Error = EOF
        return nil
    }
    for reduceTaskId, task := range c.ReduceTasks {
        if task.pending {
            continue
        }
        reply.Files = task.inputFiles
        reply.ReduceTaskId = reduceTaskId
        task.pending = true
        go func(reduceTaskId int) {
            <- time.After(time.Second * 10)
            c.mu.Lock()
            defer c.mu.Unlock()
            // Marks the task as ready to be reschedule.
            // It is fine if the previous worker replies success
            // after we assign the task to a different worker.
            if task, ok := c.ReduceTasks[reduceTaskId]; ok {
                task.pending = false
            }
        }(reduceTaskId)
        reply.Error = OK
        return nil
    }
    reply.Error = ErrNotAvail
    return nil
}

func (c *Coordinator) NotifyMapTaskDone(args *NotifyMapTaskDoneArgs, 
    reply *NotifyMapTaskDoneReply) error {
    c.mu.Lock()
    defer c.mu.Unlock()
    // It is fine if the task is assigned to multiple workers 
    // and we get multiple success replies.
    if _, ok := c.MapTasks[args.MapTaskId]; ok {
        for id, file := range args.OutputFiles {
            task := c.ReduceTasks[id]
            task.inputFiles = append(task.inputFiles, file)
        }
        delete(c.MapTasks, args.MapTaskId)
    }
    return nil
}

func (c *Coordinator) NotifyReducdeTaskDone(args *NotifyReduceTaskDoneArgs, 
    reply *NotifyReduceTaskDoneReply) error {
    c.mu.Lock()
    defer c.mu.Unlock()
    if _, ok := c.ReduceTasks[args.ReduceTaskId]; ok {
        delete(c.ReduceTasks, args.ReduceTaskId)
    }
    return nil
}



//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
    c.mu.Lock()
    defer c.mu.Unlock()
    return len(c.MapTasks) == 0 && len(c.ReduceTasks) == 0
}


//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
    c.nReduce = nReduce
    c.MapTasks = make(map[int]*MapTask)
    c.ReduceTasks = make(map[int]*ReduceTask)
    for id, file := range files {
        c.MapTasks[id] = &MapTask {
            inputFile: file,
            pending: false,
        }
    }
    for id := 0; id < nReduce; id++ {
        c.ReduceTasks[id] = &ReduceTask {
            inputFiles: make([]string, 0),
            pending: false,
        }
    }
	c.server()
	return &c
}
