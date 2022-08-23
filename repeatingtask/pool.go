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

	"github.com/nuclio/errors"
)

type Pool struct {
	ctx      context.Context
	taskChan chan *Task
	workers  []*worker
}

func NewPool(ctx context.Context, maxTasks int, numWorkers int) (*Pool, error) {
	newPool := Pool{}
	newPool.taskChan = make(chan *Task, maxTasks)

	// create workers
	for workerIdx := 0; workerIdx < numWorkers; workerIdx++ {
		newWorker, err := newWorker(ctx, &newPool)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to create worker")
		}

		newPool.workers = append(newPool.workers, newWorker)
	}

	return &newPool, nil
}

func (p *Pool) SubmitTaskAndWait(task *Task) TaskErrors {
	if err := p.SubmitTask(task); err != nil {
		return TaskErrors{
			taskErrors: []*TaskError{
				{Error: errors.Wrap(err, "Failed to submit task")},
			},
		}
	}

	return task.Wait()
}

func (p *Pool) SubmitTask(task *Task) error {

	if err := task.initialize(); err != nil {
		return errors.Wrap(err, "Failed to initialize channel")
	}

	for parallelIdx := 0; parallelIdx < task.MaxParallel; parallelIdx++ {
		select {
		case p.taskChan <- task:
		default:
			return errors.New("Failed to submit task - enlarge the pool max # of tasks")
		}
	}

	return nil
}
