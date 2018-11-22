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

package http

import (
	"bufio"
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

var (
	okBytes = []byte("OK")
)

// Server is HTTP server
type Server struct {
	*frames.ServerBase

	address string // listen address
	server  *fasthttp.Server
	routes  map[string]func(*fasthttp.RequestCtx)

	config *frames.Config
	api    *api.API
	logger logger.Logger
}

// NewServer creates a new server
func NewServer(config *frames.Config, addr string, logger logger.Logger) (*Server, error) {
	var err error

	if err := config.Validate(); err != nil {
		return nil, errors.Wrap(err, "bad configuration")
	}

	if err := config.InitDefaults(); err != nil {
		return nil, errors.Wrap(err, "failed to init defaults")
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
		ServerBase: frames.NewServerBase(),

		address: addr,
		config:  config,
		logger:  logger,
		api:     api,
	}

	srv.initRoutes()

	return srv, nil
}

// Start starts the server
func (s *Server) Start() error {
	if state := s.State(); state != frames.ReadyState {
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
			s.SetError(err)
		}
	}()

	s.SetState(frames.RunningState)
	s.logger.InfoWith("server started", "address", s.address)
	return nil
}

func (s *Server) handler(ctx *fasthttp.RequestCtx) {
	fn, ok := s.routes[string(ctx.Path())]
	if !ok {
		ctx.Error(fmt.Sprintf("unknown path - %q", string(ctx.Path())), http.StatusNotFound)
		return
	}

	fn(ctx)
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
	var apiError error
	go func() {
		defer close(ch)
		apiError = s.api.Read(request, ch)
		if apiError != nil {
			s.logger.ErrorWith("error reading", "error", apiError)
		}
	}()

	ctx.SetBodyStreamWriter(func(w *bufio.Writer) {
		enc := frames.NewEncoder(w)
		for frame := range ch {
			if err := enc.Encode(frame); err != nil {
				s.logger.ErrorWith("can't encode result", "error", err)
				enc.EncodeError(err)
			}

			if err := w.Flush(); err != nil {
				s.logger.ErrorWith("can't flush", "error", err)
				enc.EncodeError(err)
			}
		}

		if apiError != nil {
			enc.EncodeError(apiError)
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

	var nFrames, nRows int
	var writeError error

	ch := make(chan frames.Frame, 1)
	done := make(chan bool)
	go func() {
		defer close(done)
		nFrames, nRows, writeError = s.api.Write(request, ch)
	}()

	for writeError == nil {
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
	if !ctx.IsPost() { // ctx.PostBody() blocks on GET
		ctx.Error("unsupported method", http.StatusMethodNotAllowed)
	}

	request := &frames.CreateRequest{}
	if err := json.Unmarshal(ctx.PostBody(), request); err != nil {
		s.logger.ErrorWith("can't decode request", "error", err)
		ctx.Error(fmt.Sprintf("bad request - %s", err), http.StatusBadRequest)
		return
	}

	s.logger.InfoWith("create", "request", request)
	if err := s.api.Create(request); err != nil {
		ctx.Error(err.Error(), http.StatusInternalServerError)
		return
	}

	s.replyOK(ctx)
}

func (s *Server) handleDelete(ctx *fasthttp.RequestCtx) {
	if !ctx.IsPost() { // ctx.PostBody() blocks on GET
		ctx.Error("unsupported method", http.StatusMethodNotAllowed)
	}

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

func (s *Server) handleGrafana(ctx *fasthttp.RequestCtx) {
	args := ctx.QueryArgs()
	request := &frames.ReadRequest{
		Backend:      string(args.Peek("backend")),
		Table:        string(args.Peek("table")),
		Query:        string(args.Peek("query")),
		Filter:       string(args.Peek("filter")),
		GroupBy:      string(args.Peek("group_by")),
		Limit:        int64(args.GetUintOrZero("limit")),
		MessageLimit: int64(args.GetUintOrZero("messge_limit")),
		Marker:       string(args.Peek("marker")),
	}

	// TODO: Validate request
	s.logger.InfoWith("grafana request", "request", request)

	ch := make(chan frames.Frame)
	var apiError error
	go func() {
		defer close(ch)
		apiError = s.api.Read(request, ch)
		if apiError != nil {
			s.logger.ErrorWith("error reading (grafana)", "error", apiError)
		}
	}()

	var frames []*JSONFrame
	for frame := range ch {
		jframe, err := frameToJSON(frame)
		if err != nil {
			s.logger.ErrorWith("can't encode frame", "error", err)
			msg := fmt.Sprintf("can't encode frame - %s", err)
			ctx.Error(msg, http.StatusInternalServerError)
			return
		}
		frames = append(frames, jframe)
	}

	if apiError != nil {
		msg := fmt.Sprintf("API error - %s", apiError)
		ctx.Error(msg, http.StatusInternalServerError)
		return
	}

	if err := json.NewEncoder(ctx).Encode(frames); err != nil {
		s.logger.ErrorWith("can't encode result", "error", err)
	}
}

func (s *Server) handleExec(ctx *fasthttp.RequestCtx) {
	if !ctx.IsPost() { // ctx.PostBody() blocks on GET
		ctx.Error("unsupported method", http.StatusMethodNotAllowed)
	}

	request := &frames.ExecRequest{}
	if err := json.Unmarshal(ctx.PostBody(), request); err != nil {
		s.logger.ErrorWith("can't decode request", "error", err)
		ctx.Error(fmt.Sprintf("bad request - %s", err), http.StatusBadRequest)
		return
	}

	if err := s.api.Exec(request); err != nil {
		ctx.Error("can't exec", http.StatusInternalServerError)
		return
	}

	s.replyOK(ctx)
}

func (s *Server) initRoutes() {
	s.routes = map[string]func(*fasthttp.RequestCtx){
		"/_/config": s.handleConfig,
		"/_/status": s.handleStatus,
		"/create":   s.handleCreate,
		"/delete":   s.handleDelete,
		"/grafana":  s.handleGrafana,
		"/read":     s.handleRead,
		"/write":    s.handleWrite,
		"/exec":     s.handleExec,
	}
}
