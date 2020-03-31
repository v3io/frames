package framulate

import (
	"context"
	nethttp "net/http"
	"strings"
	"time"

	"github.com/v3io/frames"
	"github.com/v3io/frames/grpc"
	"github.com/v3io/frames/http"
	"github.com/v3io/frames/pb"
	"github.com/v3io/frames/repeatingtask"

	"github.com/nuclio/errors"
	"github.com/nuclio/logger"
	_ "net/http/pprof"
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
		config.Transport.MaxInflightRequests)

	if err != nil {
		return nil, errors.Wrap(err, "Failed to create pool")
	}

	newFramulate.scenario, err = newFramulate.createScenario(config)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create scenario")
	}

	newFramulate.framesClient, err = newFramulate.createFramesClient(config)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create frames client")
	}

	go func() {
		err := nethttp.ListenAndServe(":8082", nil)
		if err != nil {
			newFramulate.logger.WarnWith("Failed to create profiling endpoint", "err", err)
		}
	}()

	newFramulate.logger.DebugWith("Framulate created", "config", config)

	return &newFramulate, nil
}

func (f *Framulate) Start() error {
	doneChan := make(chan struct{})

	// log statistics periodically
	go func() {
		for {
			select {
			case <-time.After(1 * time.Second):
				f.scenario.LogStatistics()
			case <-doneChan:
				return
			}
		}
	}()

	err := f.scenario.Start()
	doneChan <- struct{}{}

	if err == nil {

		// final output
		f.scenario.LogStatistics()
	}

	return err
}

func (f *Framulate) createFramesClient(config *Config) (frames.Client, error) {
	session := pb.Session{
		Container: config.ContainerName,
		User:      config.UserName,
		Token:     config.AccessKey,
	}

	if strings.HasPrefix(config.Transport.URL, "http") {
		f.logger.DebugWith("Creating HTTP client", "url", config.Transport.URL)

		return http.NewClient(config.Transport.URL,
			&session,
			f.logger)
	}

	f.logger.DebugWith("Creating gRPC client", "url", config.Transport.URL)

	return grpc.NewClient(config.Transport.URL,
		&session,
		f.logger)
}

func (f *Framulate) createScenario(config *Config) (scenario, error) {
	switch config.Scenario.Kind {
	case scenarioKindWriteVerify:
		return newWriteVerifyScenario(f.logger, f, config)
	default:
		return nil, errors.Errorf("Undefined scenario: %s", config.Scenario.Kind)
	}
}
