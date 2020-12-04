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
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// TODO:Your code here (Part III, Part IV).
	//
	var wg sync.WaitGroup
	for i := 0; i < ntasks; i++ {
		wg.Add(1)
		go func(tasknum int) {
			defer wg.Done()
			//使用for循环处理 worker调用失败的情况
			//	当worker正常工作时，通过break跳出for循环，
			//	当worker失败时，在for循环内一直分配新的worker处理这项任务
			for {
				// 从channel中取出worker
				worker := <-registerChan
				// 构造输入方法的参数
				var argv DoTaskArgs
				argv.JobName = jobName
				argv.File = mapFiles[tasknum]
				argv.Phase = phase
				argv.TaskNumber = tasknum
				argv.NumOtherPhase = n_other
				//调用rpc中的call方法
				ok := call(worker, "Worker.DoTask", argv, new(struct{}))
				if ok {
					// work使用完毕，放回channel中
					go func() {
						registerChan <- worker
					}()
					break
				}
			}
		}(i)
	}

	wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}
