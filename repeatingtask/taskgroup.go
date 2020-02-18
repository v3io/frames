package repeatingtask

import (
	"fmt"
	"sync"

	"github.com/nuclio/errors"
)

type TaskGroupErrors struct {
	taskErrors []TaskErrors
}

func (tge *TaskGroupErrors) Errors() []error {
	var errors []error

	for _, err := range tge.taskErrors {
		if err.Error() != nil {
			errors = append(errors, err.Error())
		}
	}

	return errors
}

func (tge *TaskGroupErrors) Error() error {
	taskGroupErrors := tge.Errors()

	if len(taskGroupErrors) == 0 {
		return nil
	}

	errorString := ""
	for _, err := range taskGroupErrors {
		errorString += fmt.Sprintf("%s\n", err.Error())
	}

	return errors.New(errorString)
}

type TaskGroup struct {
	tasks     []*Task
	tasksLock sync.Locker
}

func NewTaskGroup() (*TaskGroup, error) {
	return &TaskGroup{
		tasksLock: &sync.Mutex{},
	}, nil
}

func (t *TaskGroup) AddTask(task *Task) error {
	t.tasksLock.Lock()
	t.tasks = append(t.tasks, task)
	t.tasksLock.Unlock()

	return nil
}

func (t *TaskGroup) Wait() TaskGroupErrors {
	taskGroupErrors := TaskGroupErrors{}

	// iterate over tasks and read into task group errors
	for _, task := range t.tasks {

		// wait for task and add task errors
		taskGroupErrors.taskErrors = append(taskGroupErrors.taskErrors, task.Wait())
	}

	return taskGroupErrors
}
