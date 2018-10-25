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

	"github.com/v3io/frames"
	"github.com/v3io/frames/api"

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
	createPath = []byte("/create")
	deletePath = []byte("/delete")
	okBytes    = []byte("OK")
)

// Server is HTTP server
type Server struct {
	address string // listen address
	server  *fasthttp.Server
	state   string

	config *frames.Config
	api    *api.API
	logger logger.Logger
}

// New creates a new server
func New(config *frames.Config, addr string, logger logger.Logger) (*Server, error) {
	var err error

	if err := config.Validate(); err != nil {
		return nil, errors.Wrap(err, "bad configuration")
	}

	if logger == nil {
		logger, err = frames.NewLogger(config.Log.Level)
		if err != nil {
			return nil, errors.Wrap(err, "can't create logger")
		}
	}

	api, err := api.New(logger, config)
	if err != nil {
		return nil, errors.Wrap(err, "can't create API")
	}

	srv := &Server{
		address: addr,
		state:   ReadyState,
		config:  config,
		logger:  logger,
		api:     api,
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
		// TODO: Configuration?
		MaxRequestBodySize: 8 * (1 << 30), // 8GB
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
		s.handleStatus(ctx)
	case bytes.Compare(ctx.Path(), configPath) == 0:
		s.handleConfig(ctx)
	case bytes.Compare(ctx.Path(), writePath) == 0:
		s.handleWrite(ctx)
	case bytes.Compare(ctx.Path(), readPath) == 0:
		s.handleRead(ctx)
	case bytes.Compare(ctx.Path(), createPath) == 0:
		s.handleCreate(ctx)
	case bytes.Compare(ctx.Path(), deletePath) == 0:
		s.handleDelete(ctx)
	default:
		ctx.Error(fmt.Sprintf("unknown path - %q", string(ctx.Path())), http.StatusNotFound)
	}
}

func (s *Server) handleStatus(ctx *fasthttp.RequestCtx) {
	status := map[string]interface{}{
		"state": s.State(),
	}

	s.replyJSON(ctx, status)
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

	// TODO: Validate request
	s.logger.InfoWith("read request", "request", request)

	ch := make(chan frames.Frame)
	go func() {
		err := s.api.Read(request, ch)
		close(ch)
		if err != nil {
			// Can't set status since we already sent data
			s.logger.ErrorWith("error reading", "error", err)
		}
	}()

	ctx.SetBodyStreamWriter(func(w *bufio.Writer) {
		enc := frames.NewEncoder(w)
		for frame := range ch {
			if err := enc.Encode(frame); err != nil {
				// Can't set status since we already sent data
				s.logger.ErrorWith("can't encode result", "error", err)
			}

			if err := w.Flush(); err != nil {
				// Can't set status since we already sent data
				s.logger.ErrorWith("can't flush", "error", err)
			}
		}
	})
}

func (s *Server) handleWrite(ctx *fasthttp.RequestCtx) {
	if !ctx.IsPost() { // ctx.PostBody() blocks on GET
		ctx.Error("unsupported method", http.StatusMethodNotAllowed)
	}

	reader, writer := io.Pipe()
	go func() {
		ctx.Request.BodyWriteTo(writer)
		writer.Close()
	}()

	dec := frames.NewDecoder(reader)

	// First message is the write reqeust
	request, err := dec.DecodeWriteRequest()
	if err != nil {
		msg := "bad write request"
		s.logger.ErrorWith(msg, "error", err)
		ctx.Error(msg, http.StatusBadRequest)
		return
	}

	var (
		writeError error
		nFrames    int
		nRows      int
	)

	ch := make(chan frames.Frame)
	done := make(chan bool)
	go func() {
		nFrames, nRows, writeError = s.api.Write(request, ch)
		close(done)
	}()

	for {
		frame, err := dec.DecodeFrame()
		if err != nil {
			if err != io.EOF {
				s.logger.ErrorWith("decode error", "error", err)
				ctx.Error("decode error", http.StatusInternalServerError)
			}
			break
		}

		ch <- frame
	}

	close(ch)
	<-done

	// We can't handle writeError right after .Write since it's done in a goroutine
	if writeError != nil {
		s.logger.ErrorWith("write error", "error", writeError)
		ctx.Error("write error: "+writeError.Error(), http.StatusInternalServerError)
		return
	}

	reply := map[string]interface{}{
		"num_frames": nFrames,
		"num_rows":   nRows,
	}
	s.replyJSON(ctx, reply)
}

func (s *Server) handleCreate(ctx *fasthttp.RequestCtx) {
	request := &frames.CreateRequest{}
	if err := json.Unmarshal(ctx.PostBody(), request); err != nil {
		s.logger.ErrorWith("can't decode request", "error", err)
		ctx.Error(fmt.Sprintf("bad request - %s", err), http.StatusBadRequest)
		return
	}

	if err := s.api.Create(request); err != nil {
		ctx.Error(err.Error(), http.StatusInternalServerError)
		return
	}

	s.replyOK(ctx)
}

func (s *Server) handleDelete(ctx *fasthttp.RequestCtx) {
	request := &frames.DeleteRequest{}
	if err := json.Unmarshal(ctx.PostBody(), request); err != nil {
		s.logger.ErrorWith("can't decode request", "error", err)
		ctx.Error(fmt.Sprintf("bad request - %s", err), http.StatusBadRequest)
		return
	}

	if err := s.api.Delete(request); err != nil {
		ctx.Error("can't delete", http.StatusInternalServerError)
		return
	}

	s.replyOK(ctx)
}

func (s *Server) handleConfig(ctx *fasthttp.RequestCtx) {
	s.replyJSON(ctx, s.config)
}

func (s *Server) replyJSON(ctx *fasthttp.RequestCtx, reply interface{}) error {
	ctx.Response.Header.SetContentType("application/json")
	if err := json.NewEncoder(ctx).Encode(reply); err != nil {
		s.logger.ErrorWith("can't encode JSON", "error", err, "reply", reply)
		ctx.Error("can't encode JSON", http.StatusInternalServerError)
		return err
	}

	return nil
}

func (s *Server) replyOK(ctx *fasthttp.RequestCtx) {
	ctx.SetStatusCode(http.StatusOK)
	ctx.Write(okBytes)
}
