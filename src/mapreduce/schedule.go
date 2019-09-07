package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//



//TODO:
//	1) Adding workers back to the channel once they are free
//	2) Shutdown reply has some issue

func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	type Response struct {
		Message string
	}
	response := new(Response)
	switch phase {
	case mapPhase:
		var wg sync.WaitGroup
		ntasks = len(mapFiles)
		n_other = nReduce
		wg.Add(ntasks)

		for i := 0; i < ntasks; i++ {
			for rpcAddress := range registerChan {
				fmt.Println("Worker Address:", rpcAddress)
				go func(taskNumber int) {
					taskArgs := DoTaskArgs{jobName, mapFiles[taskNumber], phase, taskNumber, n_other}
					call(rpcAddress, "Worker.DoTask", taskArgs, response)
					wg.Done()
				}(i)
				break
			}
		}
		wg.Wait()
		return
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
		var wg sync.WaitGroup
		wg.Add(ntasks)
		for i := 0; i < ntasks; i++ {
			for rpcAddress := range registerChan {
				go func(taskNumber int) {
					taskArgs := DoTaskArgs{jobName, mapFiles[taskNumber], phase, taskNumber, n_other}
					call(rpcAddress, "Worker.DoTask", taskArgs, response)
					wg.Done()
				}(i)
				break
			}
		}
		wg.Wait()
		return
	}
	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	fmt.Printf("Schedule: %v done\n", phase)
}
