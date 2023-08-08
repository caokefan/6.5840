package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"
import "time"
import "fmt"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
const (
	MAP   	= "MAP"
	REDUCE 	= "REDUCE"
	DONE	= "DONE"
)

type Task struct {
	Id				int
	Type			string
	MapInputFile	string
	WorkerId		int
	DeadLine		time.Time
}

type ApplyForTaskArgs struct {
	WorkerId		int
	LastTaskId		int
	LastTaskType	string
}

type ApplyForTaskReply struct {
	TaskId			int
	TaskType		string
	MapInputFile	string
	NREDUCE			int
	NMap			int
}

func tmpMapOutFile(workerId, mapId, reduceId int) string {
	return fmt.Sprintf("tmp-worker-%d-%d-%d", workerId, mapId, reduceId)
}

func finalMapOutFile(mapId, reduceId int) string {
	return fmt.Sprintf("mr-%d-%d", mapId, reduceId)
}

func tmpReduceOutFile(workerId, reduceId int) string {
	return fmt.Sprintf("tmp-worker-%d-out-%d", workerId, reduceId)
}

func finalReduceOutFile(reduceId int) string {
	return fmt.Sprintf("mr-out-%d", reduceId)
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
