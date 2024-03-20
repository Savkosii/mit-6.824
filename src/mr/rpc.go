package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Add your RPC definitions here.
type PollMapTaskArgs struct {
}

type PollMapTaskReply struct {
    Error Err
    File string
    MapTaskId int
    NReduce int
}

type PollReduceTaskArgs struct {
}

type PollReduceTaskReply struct {
    Error Err
    Files []string
    ReduceTaskId int
}

type NotifyMapTaskDoneArgs struct {
    MapTaskId int
    OutputFiles map[int]string
}

type NotifyMapTaskDoneReply struct {

}

type NotifyReduceTaskDoneArgs struct {
    ReduceTaskId int
}

type NotifyReduceTaskDoneReply struct {

}

const (
	OK             = "OK"
	ErrNotAvail     = "ErrNotAvail"
    EOF            = "EOF"
)

type Err string


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
