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

package api

// API Layer

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/v3io/frames"
	"github.com/v3io/frames/backends"
	// Load backends (make sure they register)
	_ "github.com/v3io/frames/backends/csv"
	_ "github.com/v3io/frames/backends/kv"
	_ "github.com/v3io/frames/backends/stream"
	_ "github.com/v3io/frames/backends/tsdb"
	"github.com/v3io/frames/backends/utils"
	v3iohttp "github.com/v3io/v3io-go/pkg/dataplane/http"
)

const (
	missingMsg = "missing parameters"
)

// API layer, implements common CRUD operations
// TODO: Call it DAL? (data access layer)
type API struct {
	logger        logger.Logger
	backends      map[string]frames.DataBackend
	config        *frames.Config
	historyServer *utils.HistoryServer
}

// New returns a new API layer struct
func New(logger logger.Logger, config *frames.Config, historyServer *utils.HistoryServer) (*API, error) {
	if logger == nil {
		var err error
		logger, err = frames.NewLogger(config.Log.Level)
		if err != nil {
			return nil, errors.Wrap(err, "can't create logger")
		}
	}

	api := &API{
		logger:        logger,
		config:        config,
		historyServer: historyServer,
	}

	if err := api.createBackends(config); err != nil {
		msg := "can't create backends"
		api.logger.ErrorWith(msg, "error", err, "config", config)
		return nil, errors.Wrap(err, "can't create backends")
	}

	return api, nil
}

// Read reads from database, emitting results to wf
func (api *API) Read(request *frames.ReadRequest, out chan frames.Frame) error {
	api.logger.DebugWith("read request", "request", request)

	backend, ok := api.backends[request.Proto.Backend]

	if !ok {
		api.logger.ErrorWith("unknown backend", "name", request.Proto.Backend)
		return fmt.Errorf("unknown backend - %q", request.Proto.Backend)
	}

	queryStartTime := time.Now()
	iter, err := backend.Read(request)
	if err != nil {
		api.logger.ErrorWith("can't query", "error", err)
		return errors.Wrap(err, "can't query")
	}

	for iter.Next() {
		out <- iter.At()
	}

	queryDuration := time.Since(queryStartTime)

	if err := iter.Err(); err != nil {
		msg := "error during iteration"
		api.logger.ErrorWith(msg, "error", err)
		return errors.Wrap(err, msg)
	}

	if api.historyServer != nil {
		api.historyServer.AddReadLog(request, queryDuration, queryStartTime)
	}
	return nil
}

// Write write data to backend, returns num_frames, num_rows, error
func (api *API) Write(request *frames.WriteRequest, in chan frames.Frame) (int, int, error) {
	if request.Backend == "" || request.Table == "" {
		api.logger.ErrorWith(missingMsg, "request", request)
		return -1, -1, fmt.Errorf(missingMsg)
	}

	api.logger.DebugWith("write request", "request", request)
	backend, ok := api.backends[request.Backend]
	if !ok {
		api.logger.ErrorWith("unknown backend", "name", request.Backend)
		return -1, -1, fmt.Errorf("unknown backend - %s", request.Backend)
	}

	ingestStartTime := time.Now()
	appender, err := backend.Write(request)
	if err != nil {
		msg := "backend Write failed"
		api.logger.ErrorWith(msg, "error", err)
		return -1, -1, errors.Wrap(err, msg)
	}
	nFrames, nRows := 0, 0
	if request.ImmidiateData != nil {
		nFrames, nRows = 1, request.ImmidiateData.Len()
	}

	for frame := range in {
		api.logger.DebugWith("frame to write", "size", frame.Len())
		if err := appender.Add(frame); err != nil {
			msg := "can't add frame"
			api.logger.ErrorWith(msg, "error", err)
			if strings.Contains(err.Error(), "Failed POST with status 401") {
				err = errors.New("unauthorized update (401), may be caused by wrong password or credentials")
			}
			return nFrames, nRows, errors.Wrap(err, msg)
		}

		nFrames++
		nRows += frame.Len()
		api.logger.DebugWith("write", "numFrames", nFrames, "numRows", nRows)
	}

	api.logger.Debug("write done")

	// TODO: Specify timeout in request?
	if nRows > 0 {
		if err := appender.WaitForComplete(time.Duration(api.config.DefaultTimeout) * time.Second); err != nil {
			msg := "can't wait for completion"
			api.logger.ErrorWith(msg, "error", err)
			return nFrames, nRows, errors.Wrap(err, msg)
		}
	} else {
		api.logger.DebugWith("write request with zero rows", "frames", nFrames, "requst", request)
	}

	ingestDuration := time.Since(ingestStartTime)
	if api.historyServer != nil {
		api.historyServer.AddWriteLog(request, ingestDuration, ingestStartTime)
	}

	return nFrames, nRows, nil
}

// Create will create a new table
func (api *API) Create(request *frames.CreateRequest) error {
	if request.Proto.Backend == "" || request.Proto.Table == "" {
		api.logger.ErrorWith(missingMsg, "request", request)
		return fmt.Errorf(missingMsg)
	}

	api.logger.DebugWith("create", "request", request)
	backend, ok := api.backends[request.Proto.Backend]
	if !ok {
		api.logger.ErrorWith("unknown backend", "name", request.Proto.Backend)
		return fmt.Errorf("unknown backend - %s", request.Proto.Backend)
	}

	createStartTime := time.Now()
	if err := backend.Create(request); err != nil {
		api.logger.ErrorWith("error creating table", "error", err, "request", request)
		return errors.Wrap(err, "error creating table")
	}

	createDuration := time.Since(createStartTime)
	if api.historyServer != nil {
		api.historyServer.AddCreateLog(request, createDuration, createStartTime)
	}

	return nil
}

// Delete deletes a table or part of it
func (api *API) Delete(request *frames.DeleteRequest) error {
	if request.Proto.Backend == "" || request.Proto.Table == "" {
		api.logger.ErrorWith(missingMsg, "request", request)
		return fmt.Errorf(missingMsg)
	}

	api.logger.DebugWith("delete", "request", request)
	backend, ok := api.backends[request.Proto.Backend]
	if !ok {
		api.logger.ErrorWith("unknown backend", "name", request.Proto.Backend)
		return fmt.Errorf("unknown backend - %s", request.Proto.Backend)
	}

	deleteStartTime := time.Now()

	if err := backend.Delete(request); err != nil {
		api.logger.ErrorWith("error deleting table", "error", err, "request", request)
		return errors.Wrap(err, "can't delete")
	}

	deleteDuration := time.Since(deleteStartTime)
	if api.historyServer != nil {
		api.historyServer.AddDeleteLog(request, deleteDuration, deleteStartTime)
	}

	return nil
}

// Exec executes a command on the backend
func (api *API) Exec(request *frames.ExecRequest) (frames.Frame, error) {
	if request.Proto.Backend == "" || request.Proto.Table == "" {
		api.logger.ErrorWith(missingMsg, "request", request)
		return nil, fmt.Errorf(missingMsg)
	}

	// TODO: This print session in clear text
	//	api.logger.DebugWith("exec", "request", request)
	backend, ok := api.backends[request.Proto.Backend]
	if !ok {
		api.logger.ErrorWith("unknown backend", "name", request.Proto.Backend)
		return nil, fmt.Errorf("unknown backend - %s", request.Proto.Backend)
	}

	executeStartTime := time.Now()
	frame, err := backend.Exec(request)
	if err != nil {
		api.logger.ErrorWith("error in exec", "error", err, "request", request)
		return nil, errors.Wrap(err, "can't exec")
	}

	executeDuration := time.Since(executeStartTime)
	if api.historyServer != nil {
		api.historyServer.AddExecuteLog(request, executeDuration, executeStartTime)
	}
	return frame, nil
}

func (api *API) History(request *frames.HistoryRequest, out chan frames.Frame) error {
	return api.historyServer.GetLogs(request, out)
}

func (api *API) createBackends(config *frames.Config) error {
	api.backends = make(map[string]frames.DataBackend)

	for _, backendConfig := range config.Backends {
		newClient := v3iohttp.NewClient(&v3iohttp.NewClientInput{DialTimeout: time.Duration(backendConfig.DialTimeoutSeconds) * time.Second, MaxConnsPerHost: math.MaxInt64})

		api.logger.InfoWith("Creating v3io context for backend",
			"backend", backendConfig.Name,
			"workers", backendConfig.V3ioGoWorkers,
			"requestChanLength", backendConfig.V3ioGoRequestChanLength,
			"maxConns", backendConfig.MaxConnections)

		newContextInput := &v3iohttp.NewContextInput{
			HTTPClient:     newClient,
			NumWorkers:     backendConfig.V3ioGoWorkers,
			RequestChanLen: backendConfig.V3ioGoRequestChanLength,
			MaxConns:       backendConfig.MaxConnections,
		}
		// create a context for the backend
		v3ioContext, err := v3iohttp.NewContext(api.logger, newContextInput)

		if err != nil {
			return errors.Wrap(err, "Failed to create v3io context for backend")
		}

		factory := backends.GetFactory(backendConfig.Type)
		if factory == nil {
			return fmt.Errorf("unknown backend - %q", backendConfig.Type)
		}

		backend, err := factory(api.logger, v3ioContext, backendConfig, config)
		if err != nil {
			return errors.Wrapf(err, "%s:%s - can't create backend", backendConfig.Name, backendConfig.Type)
		}

		api.backends[backendConfig.Name] = backend

	}

	return nil
}
