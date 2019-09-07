package mapreduce

import (
	"fmt"
	"sync"
)

type Response struct {
	Message string
}

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//

func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	response := new(Response)
	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
		registerChan = distributeTasks(ntasks, registerChan, jobName, mapFiles, phase, n_other, response)
		return
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
		registerChan = distributeTasks(ntasks, registerChan, jobName, mapFiles, phase, n_other, response)
		return
	}
	fmt.Printf("Schedule: %v done\n", phase)
}

func distributeTasks(ntasks int, registerChan chan string, jobName string,
	mapFiles []string, phase jobPhase, n_other int, response *Response) chan string {
	var wg sync.WaitGroup
	wg.Add(ntasks)
	for i := 0; i < ntasks; i++ {
		for rpcAddress := range registerChan {
			go func(taskNumber int) {
				taskArgs := DoTaskArgs{jobName, mapFiles[taskNumber], phase, taskNumber, n_other}
				call(rpcAddress, "Worker.DoTask", taskArgs, response)
				wg.Done()
				registerChan <- rpcAddress
			}(i)
			break
		}
	}
	wg.Wait()
	return registerChan
}
