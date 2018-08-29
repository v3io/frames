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
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/v3io/frames"
	"github.com/v3io/frames/backends/csv"
	"github.com/v3io/frames/backends/kv"

	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/valyala/fasthttp"
)

// Possible states
const (
	ReadyState   = "ready"
	RunningState = "running"
	ErrorState   = "error"
	maxBatchSize = 10000
)

var (
	statusPath = []byte("/_/status")
	configPath = []byte("/_/config")
	writePath  = []byte("/write")
	readPath   = []byte("/read")
)

// Server is HTTP server
type Server struct {
	address string // listen address
	server  *fasthttp.Server
	state   string

	config   *frames.Config
	backends map[string]frames.DataBackend
	logger   logger.Logger
}

// New creates a new server
func New(cfg *frames.Config, addr string, logger logger.Logger) (*Server, error) {
	var err error

	if logger == nil {
		logger, err = frames.NewLogger(cfg.Verbose)
		if err != nil {
			return nil, errors.Wrap(err, "can't create logger")
		}
	}

	srv := &Server{
		address: addr,
		state:   ReadyState,
		config:  cfg,
		logger:  logger,
	}

	srv.backends, err = srv.createBackends(cfg.Backends)
	if err != nil {
		return nil, errors.Wrap(err, "can't create backends")
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
		s.logger.ErrorWith("start from bad state", "state", state)
		return fmt.Errorf("bad state - %s", state)
	}

	s.server = &fasthttp.Server{
		Handler: s.handler,
	}

	go func() {
		err := s.server.ListenAndServe(s.address)
		if err != nil {
			s.logger.ErrorWith("error running HTTP server", "error", err)
			s.state = ErrorState
		}
	}()

	s.state = RunningState
	s.logger.InfoWith("server started", "address", s.address)
	return nil
}

func (s *Server) handler(ctx *fasthttp.RequestCtx) {
	// TODO: Check API key

	switch {
	case bytes.Compare(ctx.Path(), statusPath) == 0:
		fmt.Fprintf(ctx, "%s\n", s.State())
	case bytes.Compare(ctx.Path(), configPath) == 0:
		s.handleConfig(ctx)
	case bytes.Compare(ctx.Path(), writePath) == 0:
		s.handleWrite(ctx)
	case bytes.Compare(ctx.Path(), readPath) == 0:
		s.handleRead(ctx)
	default:
		ctx.Error(fmt.Sprintf("unknown path - %q", string(ctx.Path())), http.StatusNotFound)
	}
}

func (s *Server) handleRead(ctx *fasthttp.RequestCtx) {
	if !ctx.IsPost() { // ctx.PostBody() blocks on GET
		ctx.Error("unsupported method", http.StatusMethodNotAllowed)
	}

	request := &frames.ReadRequest{}
	if err := json.Unmarshal(ctx.PostBody(), request); err != nil {
		s.logger.ErrorWith("can't decode request", "error", err)
		ctx.Error(fmt.Sprintf("bad request - %s", err), http.StatusBadRequest)
		return
	}

	if request.Query != "" {
		if err := s.populateQuery(request); err != nil {
			s.logger.ErrorWith("can't populate query", "request", request, "error", err)
			ctx.Error(err.Error(), http.StatusBadRequest)
			return
		}
	}

	// TODO: Validate request

	// TODO: We'd like to have a map of name->config of backends in configuration
	backend, ok := s.backends[request.Backend]
	if !ok {
		s.logger.ErrorWith("unknown backend", "name", request.Backend)
		ctx.Error(fmt.Sprintf("unknown backend - %q", request.Backend), http.StatusBadRequest)
		return
	}

	iter, err := backend.Read(request)
	if err != nil {
		s.logger.ErrorWith("can't query", "error", err)
		ctx.Error(fmt.Sprintf("can't query - %s", err), http.StatusInternalServerError)
		return
	}

	sw := func(w *bufio.Writer) {
		enc := frames.NewEncoder(w)
		for iter.Next() {
			if err := enc.Encode(iter.At()); err != nil {
				s.logger.ErrorWith("can't encode result", "error", err)
				return
			}

			w.Flush()
		}

		if err := iter.Err(); err != nil {
			s.logger.ErrorWith("error during iteration", "error", err)
		}
	}
	ctx.SetBodyStreamWriter(sw)
}

func (s *Server) handleWrite(ctx *fasthttp.RequestCtx) {
	if !ctx.IsPost() { // ctx.PostBody() blocks on GET
		ctx.Error("unsupported method", http.StatusMethodNotAllowed)
	}

	args := ctx.QueryArgs()

	request := &frames.WriteRequest{
		Backend: string(args.Peek("backend")),
		Table:   string(args.Peek("table")),
	}

	if request.Backend == "" || request.Table == "" {
		s.logger.ErrorWith("bad write request", "request", args.String())
		ctx.Error("missing parameters", http.StatusBadRequest)
		return
	}

	backend, ok := s.backends[request.Backend]
	if !ok {
		s.logger.ErrorWith("unkown backend", "name", request.Backend)
		ctx.Error(fmt.Sprintf("unknown backend - %s", request.Backend), http.StatusBadRequest)
		return
	}

	appender, err := backend.Write(request)
	if err != nil {
		ctx.Error(err.Error(), http.StatusBadRequest)
		return
	}

	reader, writer := io.Pipe()

	nFrames, nRows := 0, 0
	go func() {
		dec := frames.NewDecoder(reader)

		for {
			frame, err := dec.Decode()

			if err != nil {
				if err == io.EOF {
					return
				}

				s.logger.ErrorWith("can't decode", "error", err)
				ctx.Error(err.Error(), http.StatusInternalServerError)
				return
			}

			if err := appender.Add(frame); err != nil {
				s.logger.ErrorWith("can't add frame", "error", err)
				ctx.Error(err.Error(), http.StatusInternalServerError)
				return
			}

			nFrames++
			nRows += frame.Len()
		}
	}()

	ctx.Request.BodyWriteTo(writer)

	// TODO: Specify timeout in request?
	if err := appender.WaitForComplete(time.Minute); err != nil {
		s.logger.ErrorWith("can't wait for completion", "error", err)
		ctx.Error(err.Error(), http.StatusInternalServerError)
	}

	response := map[string]interface{}{
		"num_frames": nFrames,
		"num_rows":   nRows,
	}

	data, err := json.Marshal(response)
	if err != nil {
		s.logger.ErrorWith("can't encode response", "error", err)
		ctx.Error(err.Error(), http.StatusInternalServerError)
		return
	}

	ctx.Write(data)
}

func (s *Server) handleConfig(ctx *fasthttp.RequestCtx) {
	enc := json.NewEncoder(ctx)
	if err := enc.Encode(s.config); err != nil {
		s.logger.ErrorWith("can't encode configuration", "error", err)
		ctx.Error(fmt.Sprintf("can't encode config - %s", err), http.StatusInternalServerError)
	}
}

func (s *Server) populateQuery(request *frames.ReadRequest) error {
	sqlQuery, err := frames.ParseSQL(request.Query)
	if err != nil {
		return errors.Wrap(err, "bad SQL query")
	}

	if request.Table != "" {
		return fmt.Errorf("both query AND table provided")
	}
	request.Table = sqlQuery.Table

	if request.Columns != nil {
		return fmt.Errorf("both query AND columns provided")
	}
	request.Columns = sqlQuery.Columns

	if request.Filter != "" {
		return fmt.Errorf("both query AND filter provided")
	}
	request.Filter = sqlQuery.Filter

	if request.GroupBy != "" {
		return fmt.Errorf("both query AND group_by provided")
	}
	request.GroupBy = sqlQuery.GroupBy

	return nil
}

func (s *Server) createBackends(configs []frames.BackendConfig) (map[string]frames.DataBackend, error) {
	backends := make(map[string]frames.DataBackend)

	for _, cfg := range configs {
		var factory func(logger.Logger, *frames.BackendConfig) (frames.DataBackend, error)

		switch strings.ToLower(cfg.Type) {
		case "csv":
			factory = csv.NewBackend
		case "kv":
			factory = kv.NewBackend
		}

		backend, err := factory(s.logger, &cfg)
		if err != nil {
			return nil, errors.Wrapf(err, "%s:%s - can't create backend", cfg.Name, cfg.Type)
		}

		backends[cfg.Name] = backend
	}

	return backends, nil
}
