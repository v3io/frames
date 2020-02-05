package repeatingtask

import (
	"context"
	"github.com/pkg/errors"
	"testing"
	"time"

	"github.com/nuclio/logger"
	nucliozap "github.com/nuclio/zap"
	"github.com/stretchr/testify/suite"
)

type handlerConfig struct {
	name       string
	delay      time.Duration
	errorAfter int
}

type poolSuite struct {
	suite.Suite
	pool   *Pool
	logger logger.Logger
	ctx    context.Context
}

func (suite *poolSuite) SetupTest() {
	var err error

	suite.logger, _ = nucliozap.NewNuclioZapTest("test")
	suite.ctx = context.Background()

	suite.pool, err = NewPool(context.TODO(), 1024, 32)
	suite.Require().NoError(err)
}

func (suite *poolSuite) TestNoParallel() {
	suite.T().Skip()

	task := &Task{
		NumReptitions: 16,
		MaxParallel:   1,
		MaxNumErrors:  0,
		Handler:       suite.delayingNoConcurrentHandler,
		Cookie:        100 * time.Millisecond,
	}

	err := suite.pool.SubmitTask(task)
	suite.Require().NoError(err)

	<-task.OnCompleteChan
}

func (suite *poolSuite) TestParallel() {
	task1 := &Task{
		NumReptitions: 512,
		MaxParallel:   4,
		MaxNumErrors:  0,
		Handler:       suite.delayingErrorHandler,
		Cookie: &handlerConfig{
			name:  "task1",
			delay: 1000 * time.Millisecond,
		},
	}

	err := suite.pool.SubmitTask(task1)
	suite.Require().NoError(err)

	task2 := &Task{
		NumReptitions: 256,
		MaxParallel:   8,
		MaxNumErrors:  0,
		Handler:       suite.delayingErrorHandler,
		Cookie: &handlerConfig{
			name:  "task2",
			delay: 500 * time.Millisecond,
		},
	}

	err = suite.pool.SubmitTask(task2)
	suite.Require().NoError(err)

	task1Errors := task1.Wait()
	task2Errors := task2.Wait()

	suite.Require().NoError(task1Errors.Error())
	suite.Require().NoError(task2Errors.Error())
}

func (suite *poolSuite) TestErrors() {
	task1 := &Task{
		NumReptitions: 256,
		MaxParallel:   4,
		MaxNumErrors:  1,
		Handler:       suite.delayingErrorHandler,
		Cookie: &handlerConfig{
			name:       "task1",
			errorAfter: 20,
		},
	}

	//taskErrors := suite.pool.SubmitTaskAndWait(task1)
	//suite.Require().Error(taskErrors.Error())

	task2 := &Task{
		NumReptitions: 128,
		MaxParallel:   4,
		MaxNumErrors:  4,
		Handler:       suite.delayingErrorHandler,
		Cookie: &handlerConfig{
			name:       "task2",
			errorAfter: 50,
		},
	}

	taskGroup := TaskGroup{}

	err := suite.pool.SubmitTask(task1)
	suite.Require().NoError(err)

	err = suite.pool.SubmitTask(task2)
	suite.Require().NoError(err)

	err = taskGroup.AddTask(task1)
	suite.Require().NoError(err)
	err = taskGroup.AddTask(task2)
	suite.Require().NoError(err)

	taskGroupErrors := taskGroup.Wait()
	suite.Require().Error(taskGroupErrors.Error())
	suite.logger.DebugWith("Got error", "err", taskGroupErrors.Error())
}

func (suite *poolSuite) delayingNoConcurrentHandler(cookie interface{}, repetitionIndex int) error {
	suite.logger.DebugWith("Called", "rep", repetitionIndex)

	// TODO: test not running in parallel
	time.Sleep(cookie.(time.Duration))

	return nil
}

func (suite *poolSuite) delayingErrorHandler(cookie interface{}, repetitionIndex int) error {
	handlerConfig := cookie.(*handlerConfig)

	suite.logger.DebugWith("Called",
		"rep", repetitionIndex,
		"name", handlerConfig.name,
		"errorAfter", handlerConfig.errorAfter)

	if handlerConfig.delay != 0 {
		time.Sleep(handlerConfig.delay)
	}

	if handlerConfig.errorAfter != 0 && repetitionIndex > handlerConfig.errorAfter {
		return errors.Errorf("Error at repetition %d", repetitionIndex)
	}

	return nil
}

func TestPoolSuite(t *testing.T) {
	suite.Run(t, new(poolSuite))
}
