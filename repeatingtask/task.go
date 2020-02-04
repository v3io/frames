package repeatingtask

import (
	"fmt"
	"sync"
	"time"

	"github.com/nuclio/errors"
)

const (
	InfiniteFailures = -1
)

type TaskError struct {
	Repetition int
	Error      error
}

type TaskErrors struct {
	taskErrors  []*TaskError
	stringValue string
}

func (te *TaskErrors) String() string {
	if te.stringValue != "" {
		return te.stringValue
	}

	errorString := ""

	for _, taskError := range te.taskErrors {
		errorString += fmt.Sprintf("%d: %s\n",
			taskError.Repetition,
			errors.GetErrorStackString(taskError.Error, 10))
	}

	te.stringValue = errorString

	return te.stringValue
}

func (te *TaskErrors) Error() error {
	if len(te.taskErrors) == 0 {
		return nil
	}

	return errors.New(te.String())
}

type Task struct {
	NumReptitions  int
	MaxParallel    int
	Handler        func(interface{}, int) error
	OnCompleteChan chan struct{}
	Timeout        time.Duration
	ErrorsChan     chan *TaskError
	MaxNumErrors   int
	Cookie         interface{}

	repititionIndex        uint64
	numInstancesInTaskChan uint64
	lock                   sync.Locker
}

func (t *Task) initialize() error {
	t.lock = &sync.Mutex{}
	t.OnCompleteChan = make(chan struct{}, 1)
	t.ErrorsChan = make(chan *TaskError, t.NumReptitions)

	return nil
}

func (t *Task) Wait() TaskErrors {
	<-t.OnCompleteChan

	// read errors
	var taskErrors TaskErrors
	done := false

	for !done {
		select {
		case taskError := <-t.ErrorsChan:
			taskErrors.taskErrors = append(taskErrors.taskErrors, taskError)
		default:
			done = true
		}
	}

	return taskErrors
}
