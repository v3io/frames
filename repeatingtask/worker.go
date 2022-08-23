/*
Copyright 2018 Iguazio Systems Ltd.

Licensed under the Apache License, Version 2.0 (the "License") with
an addition restriction as set forth herein. You may not use this
file except in compliance with the License. You may obtain a copy of
the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License.

In addition, you may not use the software for any purposes that are
illegal under applicable law, and the grant of the foregoing license
under the Apache 2.0 license is conditioned upon your compliance with
such restriction.
*/
package repeatingtask

import (
	"context"
	"sync/atomic"
)

type worker struct {
	pool *Pool
	ctx  context.Context
}

func newWorker(ctx context.Context, pool *Pool) (*worker, error) {
	newWorker := worker{
		pool: pool,
		ctx:  ctx,
	}

	go newWorker.handleTasks() // nolint: errcheck

	return &newWorker, nil
}

func (w *worker) handleTasks() error {
	for {
		task := <-w.pool.taskChan
		_ = w.handleTask(task)
	}
}

func (w *worker) handleTask(task *Task) error {

	// check if task errored out. if so, just return since we assume that the worker
	// that injected the last error will mark it as complete
	if len(task.ErrorsChan) > task.MaxNumErrors {
		return nil
	}

	// increment repetition count and check if we passed it. if we did, don't handle the task
	repetitionIndex := atomic.AddUint64(&task.repititionIndex, 1)
	if int(repetitionIndex) > task.NumReptitions {
		return nil
	}

	// the index the user wants to see is 0 based
	repetitionIndex--

	// call the task
	err := task.Handler(task.Cookie, int(repetitionIndex))

	// if there was an error, shove it to the error channel
	if err != nil {
		task.ErrorsChan <- &TaskError{
			Repetition: int(repetitionIndex),
			Error:      err,
		}
	}

	// increment # of times we completed
	currentNumCompletions := atomic.AddUint64(&task.numCompletions, 1)

	// signal that we're done if there were more failures than allowed or that w're simply done
	if int(currentNumCompletions) >= task.NumReptitions ||
		len(task.ErrorsChan) > task.MaxNumErrors {
		w.signalTaskComplete(task)
	}

	// return our instance of the task into the pool so that another worker can handle it
	w.pool.taskChan <- task

	return nil
}

func (w *worker) signalTaskComplete(task *Task) {

	// write to the channel, but don't block. it's possible that many workers are signaling
	// completion of the task (e.g. multiple errors exceeding threshold)
	select {
	case task.OnCompleteChan <- struct{}{}:
	default:
		return
	}
}
