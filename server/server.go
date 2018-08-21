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

package server

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/v3io/frames"
	//"github.com/v3io/frames/backends/kv" // TODO
	"github.com/v3io/frames/backends/csv"

	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/valyala/fasthttp"
)

// Possible states
const (
	ReadyState   = "ready"
	RunningState = "running"
	ErrorState   = "error"
)

var (
	statusPath = []byte("/_/status")
)

// Server is HTTP server
type Server struct {
	address string // listen address
	server  *fasthttp.Server
	state   string

	backend frames.DataBackend
	config  *frames.V3ioConfig
	context *frames.DataContext
	logger  logger.Logger
}

// New creates a new server
func New(cfg *frames.V3ioConfig, addr string) (*Server, error) {
	ctx, err := newContext(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "Can't create context")
	}

	backend, err := csv.NewBackend(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "can't create backend")
	}

	srv := &Server{
		address: addr,
		state:   ReadyState,
		backend: backend,
		config:  cfg,
		context: ctx,
		logger:  ctx.Logger,
	}

	return srv, nil
}

// State returns the server state
func (s *Server) State() string {
	return s.state
}

// Start starts the server
func (s *Server) Start() error {
	if state := s.State(); state != ReadyState {
		s.logger.ErrorWith("Start from bad state", "state", state)
		return fmt.Errorf("bad state - %s", state)
	}

	s.server = &fasthttp.Server{
		Handler: s.handler,
	}

	go func() {
		err := s.server.ListenAndServe(s.address)
		if err != nil {
			s.logger.ErrorWith("Error running HTTP server", "error", err)
			s.state = ErrorState
		}
	}()

	s.state = RunningState
	return nil
}

func (s *Server) handler(ctx *fasthttp.RequestCtx) {
	if bytes.Compare(ctx.Path(), statusPath) == 0 {
		fmt.Fprintf(ctx, "%s\n", s.State())
		return
	}

	request := &frames.ReadRequest{}
	if err := json.Unmarshal(ctx.PostBody(), request); err != nil {
		s.logger.ErrorWith("Can't decode request", "error", err)
		ctx.Error(fmt.Sprintf("Bad request - %s", err), http.StatusBadRequest)
		return
	}

	if request.Query != "" {
		if err := s.populateQuery(request); err != nil {
			s.logger.ErrorWith("Can't populate query", "request", request, "error", err)
			ctx.Error(err.Error(), http.StatusBadRequest)
			return
		}
	}

	// TODO: Validate request

	iter, err := s.backend.Read(request)
	if err != nil {
		s.logger.ErrorWith("Can't query", "error", err)
		ctx.Error(fmt.Sprintf("Can't query - %s", err), http.StatusInternalServerError)
		return
	}

	sw := func(w *bufio.Writer) {
		enc := frames.NewEncoder(w)
		for iter.Next() {
			if err := enc.Encode(iter.At()); err != nil {
				s.logger.ErrorWith("Can't encode result", "error", err)
				return
			}

			w.Flush()
		}

		if err := iter.Err(); err != nil {
			s.logger.ErrorWith("Error during iteration", "error", err)
		}
	}
	ctx.SetBodyStreamWriter(sw)
}

func (s *Server) populateQuery(request *frames.ReadRequest) error {
	sqlQuery, err := frames.ParseSQL(request.Query)
	if err != nil {
		return errors.Wrap(err, "Bad SQL query")
	}

	if request.Table != "" {
		return fmt.Errorf("Both query AND table provided")
	}
	request.Table = sqlQuery.Table

	if request.Columns != nil {
		return fmt.Errorf("Both query AND columns provided")
	}
	request.Columns = sqlQuery.Columns

	if request.Filter != "" {
		return fmt.Errorf("Both query AND filter provided")
	}
	request.Filter = sqlQuery.Filter

	if request.GroupBy != "" {
		return fmt.Errorf("Both query AND group_by provided")
	}
	request.GroupBy = sqlQuery.GroupBy

	return nil
}

func newContext(cfg *frames.V3ioConfig) (*frames.DataContext, error) {
	logger, err := frames.NewLogger(cfg.Verbose)
	if err != nil {
		return nil, errors.Wrap(err, "Can't create logger")
	}

	container, err := frames.CreateContainer(
		logger, cfg.V3ioURL, cfg.Container, cfg.Username, cfg.Password, cfg.Workers)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create data container")
	}

	return &frames.DataContext{Container: container, Logger: logger}, nil
}
