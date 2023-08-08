package mr

import (
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)


type Coordinator struct {
	// Your definitions here.
	lock		sync.Mutex
	stage		string
	nMap		int
	nReduce		int
	tasks 		map[string]Task
	toDoTasks	chan Task
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) ApplyForTask(args *ApplyForTaskArgs, reply *ApplyForTaskReply) error {
	if args.LastTaskId != -1 {
		c.lock.Lock()
		taskId := createTaskId(args.LastTaskType, args.LastTaskId)
		if task, ok := c.tasks[taskId]; ok && task.WorkerId == args.WorkerId {
			log.Printf("%d complete %s-%d task", args.WorkerId, args.LastTaskType, args.LastTaskId)
			if args.LastTaskType == MAP {
				for i := 0; i < c.nReduce; i++ {
					err := os.Rename(tmpMapOutFile(args.WorkerId, args.LastTaskId, i), finalMapOutFile(args.LastTaskId, i))
					if err != nil {

					}
				}
			} else if args.LastTaskType == REDUCE {
				err := os.Rename(tmpReduceOutFile(args.WorkerId, args.LastTaskId), finalReduceOutFile(args.LastTaskId))
				if err != nil {

				}
			}
			delete(c.tasks, taskId)
			if len(c.tasks) == 0 {
				c.cutover()
			}
		}
		c.lock.Unlock()

	}
	task, ok := <- c.toDoTasks
		if !ok {
			return nil
		}

		c.lock.Lock()
		defer c.lock.Unlock()
		log.Printf("Assign %s task %d to worker %d", task.Type, task.Id, args.WorkerId)

		task.WorkerId = args.WorkerId
		task.DeadLine = time.Now().Add(10 * time.Second)
		c.tasks[createTaskId(task.Type, task.Id)] = task
		reply.TaskId = task.Id
		reply.TaskType = task.Type
		reply.MapInputFile = task.MapInputFile
		reply.NMap = c.nMap
		reply.NREDUCE =	c.nReduce
		return nil
}

func (c *Coordinator) cutover() {
	if c.stage == MAP {
		c.stage = REDUCE
		for i := 0;i < c.nReduce; i++ {
			task := Task{Id: i, Type: REDUCE, WorkerId: -1}
			c.tasks[createTaskId(task.Type, i)] = task
			c.toDoTasks <- task
		}
	} else if c.stage == REDUCE {
		close(c.toDoTasks)
		c.stage = DONE
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	c.lock.Lock()
	ret := c.stage == DONE
	defer c.lock.Unlock()

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		stage: 		MAP,
		nMap: 		len(files),
		nReduce: 	nReduce,
		tasks: 		make(map[string]Task),
		toDoTasks: 	make(chan Task, int(math.Max(float64(len(files)), float64(nReduce)))),	
	}

	// Your code here.
	fmt.Printf("%v length is %v\n", files, len(files))

	for i, file := range files {
		task := Task {
			Id: 			i,
			Type: 			MAP,
			WorkerId: 		-1,
			MapInputFile: 	file,
		}
		c.tasks[createTaskId(task.Type, task.Id)] = task
		c.toDoTasks <- task
	}

	c.server()
	go func() {
		for {
			time.Sleep(500 * time.Millisecond)
			c.lock.Lock()
			for _, task := range c.tasks {
				if task.WorkerId != -1 && time.Now().After(task.DeadLine) {
					task.WorkerId = -1
					c.toDoTasks <- task
				}
			}
			c.lock.Unlock()
		}
	}()
	return &c
}

func createTaskId(t string, id int) string {
	return fmt.Sprintf("%v%d", t, id)
}