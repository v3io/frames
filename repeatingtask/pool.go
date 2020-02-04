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
