package framulate

import (
	"context"
	"github.com/nuclio/errors"

	"github.com/v3io/frames"
	"github.com/v3io/frames/http"
	"github.com/v3io/frames/pb"
	"github.com/v3io/frames/repeatingtask"

	"github.com/nuclio/logger"
)

type Framulate struct {
	ctx          context.Context
	logger       logger.Logger
	taskPool     *repeatingtask.Pool
	config       *Config
	framesClient frames.Client
	scenario     scenario
}

func NewFramulate(ctx context.Context, loggerInstance logger.Logger, config *Config) (*Framulate, error) {
	var err error

	newFramulate := Framulate{
		config: config,
		logger: loggerInstance,
	}

	newFramulate.taskPool, err = repeatingtask.NewPool(ctx,
		config.MaxTasks,
		config.MaxInflightRequests)

	if err != nil {
		return nil, errors.Wrap(err, "Failed to create pool")
	}

	newFramulate.scenario, err = newFramulate.createScenario(config)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create scenario")
	}

	session := pb.Session{
		Container: newFramulate.config.ContainerName,
		User:      newFramulate.config.UserName,
		Token:     newFramulate.config.AccessKey,
	}

	newFramulate.framesClient, err = http.NewClient(newFramulate.config.FramesURL,
		&session,
		newFramulate.logger)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create client")
	}

	newFramulate.logger.DebugWith("Framulate created", "config", config)

	return &newFramulate, nil
}

func (f *Framulate) Start() error {
	return f.scenario.Start()
}

func (f *Framulate) createScenario(config *Config) (scenario, error) {
	switch config.Scenario.Kind {
	case scenarioKindWriteVerify:
		return newWriteVerifyScenario(f.logger, f, config)
	default:
		return nil, errors.Errorf("Undefined scenario: %s", config.Scenario.Kind)
	}
}
