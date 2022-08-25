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
package framulate

import (
	"context"
	nethttp "net/http"
	//nolint: golint
	_ "net/http/pprof"
	"strings"
	"time"

	"github.com/nuclio/errors"
	"github.com/nuclio/logger"
	"github.com/v3io/frames"
	"github.com/v3io/frames/grpc"
	"github.com/v3io/frames/http"
	"github.com/v3io/frames/pb"
	"github.com/v3io/frames/repeatingtask"
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
