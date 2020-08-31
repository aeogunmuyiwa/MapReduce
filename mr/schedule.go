package mr

import (
	"fmt"
	"net/rpc"
	"sync"
	"time"
)

/***************check the job state *********/
func (m *Master) schedule(phase jobPhase,) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)

	switch phase {
	case mapPhase:
		ntasks = len(m.files)
		n_other = m.nReduce
	case reducePhase:
		ntasks = m.nReduce
		n_other = len(m.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	// CUSTOM
	// 1. get next workerName from registerChan
	// 2. use custom_rpc.call() to send rpc to worker
	// call(workerAddress, Worker.DoTask, DoTaskArgs, nil)
	//

	var wg sync.WaitGroup
	wg.Add(ntasks)
	taskIdx := 0
	for taskIdx < ntasks {
		fmt.Println("in schedle func with ntask", ntasks)
		go func(idx int) {

			defer wg.Done()

			var doTaskArgs DoTaskArgs
			doTaskArgs.JobName = m.jobName
			if phase == mapPhase {
				doTaskArgs.File = m.files[idx]
				fmt.Println("map phase files", m.files[idx])
				fmt.Println("map phase")
			}else{
				fmt.Println("Reduce phase")
			}
			doTaskArgs.Phase = phase
			doTaskArgs.TaskNumber = idx
			doTaskArgs.NumOtherPhase = n_other
			workerName :=  <- m.registerChannel
			//doTaskCallRes := false
			fmt.Println("calling worker. doTask")
			fmt.Println("Worker name ", workerName)
			args := DoTaskArgs{
				JobName:       m.jobName,
				File:          m.files[idx],
				Phase:         phase,
				TaskNumber:    idx,
				NumOtherPhase: n_other,
			}
			rpcs := rpc.NewServer()
			rpcs.Register(m)
			doTaskCallRes := ServiceCall(workerName, "Worker.DoTask", args, nil)
			go func() {
				m.registerChannel <- workerName
				fmt.Println(" worker. done task")
			}()
			if doTaskCallRes == false {
			//	go func() {
			//		m.registerChannel <- workerName
			//		fmt.Println(" worker. done task")
			//	}()
			//	return
			//}else{
				time.Sleep(100 * time.Millisecond)
				workerName = <- m.registerChannel
				println("Retrying worker", workerName)
				doTaskCallRes = ServiceCall(workerName, "Worker.DoTask", doTaskArgs, nil)
				go func() { m.registerChannel <- workerName }()
			}

			//for doTaskCallRes == false {
			//	//fmt.Printf("Worker.DoTask %d Err: call failed\n", idx)
			//	time.Sleep(100 * time.Millisecond)
			//	workerName = m.registerChannel
			//	doTaskCallRes = ServiceCall(workerName, "Worker.DoTask", doTaskArgs, nil)
			//	go func() { m.registerChannel = workerName }()
			//}
			//go func() { registerChan <- workerName }()
		}(taskIdx)
		taskIdx = taskIdx + 1
	}
	wg.Wait()
	fmt.Println("done")
	fmt.Printf("Schedule: %v phase done\n", phase)
}
