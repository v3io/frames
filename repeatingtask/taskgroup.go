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
