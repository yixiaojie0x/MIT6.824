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
	nReduce     int
	nMap        int
	files       []string
	mapfinished int
	// idle, in-pregress, complete
	mapTaskState    []int
	reducefinished  int
	reduceTaskState []int

	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AllocateTask(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	if c.mapfinished < c.nMap {
		allocate := -1
		for i := 0; i < c.nMap; i++ {
			if c.mapTaskState[i] == 0 {
				allocate = i
				break
			}
		}
		if allocate == -1 {
			// waiting for unfinished map jobs
			reply.TaskType = 2
			c.mu.Unlock()
		} else {
			// allocate map job
			reply.NReduce = c.nReduce
			reply.TaskType = 0
			reply.MapTaskNumber = allocate
			reply.Filename = c.files[allocate]
			c.mapTaskState[allocate] = 1
			c.mu.Unlock()
			go func() {
				time.Sleep(time.Duration(10) * time.Second)
				c.mu.Lock()
				if c.mapTaskState[allocate] == 1 {
					// assume map job died
					c.mapTaskState[allocate] = 0
				}
				c.mu.Unlock()
			}()
		}
	} else if c.mapfinished == c.nMap && c.reducefinished < c.nReduce {
		allocate := -1
		for i := 0; i < c.nReduce; i++ {
			if c.reduceTaskState[i] == 0 {
				allocate = i
				break
			}
		}
		if allocate == -1 {
			reply.TaskType = 2
			c.mu.Unlock()
		} else {
			reply.NMap = c.nMap
			reply.TaskType = 1
			reply.ReduceTaskNumber = allocate
			c.reduceTaskState[allocate] = 1
			c.mu.Unlock()
			go func() {
				time.Sleep(time.Duration(10) * time.Second)
				c.mu.Lock()
				if c.reduceTaskState[allocate] == 1 {
					c.reduceTaskState[allocate] = 0
				}
				c.mu.Unlock()
			}()
		}
	} else {
		reply.TaskType = 3
		c.mu.Unlock()
	}

	c.mu.Unlock()
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
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.server()
	return &c
}
