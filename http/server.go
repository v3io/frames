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
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"

	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/v3io/frames"
	"github.com/v3io/frames/api"
	"github.com/v3io/frames/backends/utils"
	"github.com/v3io/frames/pb"
	"github.com/valyala/fasthttp"
)

var (
	okBytes          = []byte("OK")
	basicAuthPrefix  = []byte("Basic ")
	bearerAuthPrefix = []byte("Bearer ")
)

const AccessKeyUser = "__ACCESS_KEY"

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
func NewServer(config *frames.Config, addr string, logger logger.Logger, monitoring *utils.Monitoring) (*Server, error) {
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

	api, err := api.New(logger, config, monitoring)
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
	s.logger.InfoWith("HTTP server started", "address", s.address)
	return nil
}

func (s *Server) handler(ctx *fasthttp.RequestCtx) {
	// Avoid something like a double slash causing a misroute to status due to the fact that ctx.URI() and ctx.Path()
	// translate a path like //read to /, which in turn causes the plaintext status being returned to a client that is
	// expecteing a binary response (which currently results in a Python MemoryError on the client side).
	canonicalPath := path.Clean(string(ctx.Request.Header.RequestURI()))
	fn, ok := s.routes[canonicalPath]
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

	_ = s.replyJSON(ctx, status)
}

func (s *Server) handleRead(ctx *fasthttp.RequestCtx) {
	if !ctx.IsPost() { // ctx.PostBody() blocks on GET
		ctx.Error("unsupported method", http.StatusMethodNotAllowed)
	}

	requestInner := &pb.ReadRequest{}
	if err := json.Unmarshal(ctx.PostBody(), requestInner); err != nil {
		s.logger.ErrorWith("can't decode request", "error", err)
		ctx.Error(fmt.Sprintf("bad request - %s", err), http.StatusBadRequest)
		return
	}
	request := &frames.ReadRequest{
		Proto: requestInner,
	}

	// TODO: Validate request

	if requestInner.Session != nil {
		s.httpAuth(ctx, requestInner.Session)
		request.Password = frames.InitSecretString(requestInner.Session.Password)
		request.Token = frames.InitSecretString(requestInner.Session.Token)
		requestInner.Session.Password = ""
		requestInner.Session.Token = ""
	}

	s.logger.DebugWith("read request", "request", request)

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
			iface, ok := frame.(pb.Framed)
			if !ok {
				s.logger.Error("unknown frame type")
				s.writeError(enc, fmt.Errorf("unknown frame type"))
			}

			if err := enc.Encode(iface.Proto()); err != nil {
				s.logger.ErrorWith("can't encode result", "error", err)
				s.writeError(enc, err)
			}

			if err := w.Flush(); err != nil {
				s.logger.ErrorWith("can't flush", "error", err)
				s.writeError(enc, err)
			}
		}

		if apiError != nil {
			s.writeError(enc, apiError)
		}
	})
}

func (s *Server) writeError(enc *frames.Encoder, err error) {
	msg := &pb.Frame{
		Error: err.Error(),
	}
	_ = enc.Encode(msg)
}

func (s *Server) handleWrite(ctx *fasthttp.RequestCtx) {
	if !ctx.IsPost() { // ctx.PostBody() blocks on GET
		ctx.Error("unsupported method", http.StatusMethodNotAllowed)
	}

	reader, writer := io.Pipe()
	go func() {
		_ = ctx.Request.BodyWriteTo(writer)
		_ = writer.Close()
	}()

	dec := frames.NewDecoder(reader)

	// First message is the write reqeust
	req := &pb.InitialWriteRequest{}
	if err := dec.Decode(req); err != nil {
		msg := "bad write request"
		s.logger.ErrorWith(msg, "error", err)
		ctx.Error(msg, http.StatusBadRequest)
		return
	}

	var frame frames.Frame
	if req.InitialData != nil {
		frame = frames.NewFrameFromProto(req.InitialData)
	}

	saveMode, err := frames.SaveModeFromString(req.SaveMode)
	if err != nil {
		s.logger.ErrorWith("bad write request", "error", err)
		ctx.Error(err.Error(), http.StatusBadRequest)
		return
	}

	request := &frames.WriteRequest{
		Session:       req.Session,
		Backend:       req.Backend,
		Table:         req.Table,
		ImmidiateData: frame,
		Condition:     req.Condition,
		Expression:    req.Expression,
		HaveMore:      req.More,
		SaveMode:      saveMode,
	}

	s.httpAuth(ctx, request.Session)
	request.Password = frames.InitSecretString(req.Session.Password)
	request.Token = frames.InitSecretString(req.Session.Token)
	req.Session.Password = ""
	req.Session.Token = ""

	var nFrames, nRows int
	var writeError error

	ch := make(chan frames.Frame, 1)
	done := make(chan bool)
	go func() {
		defer close(done)
		nFrames, nRows, writeError = s.api.Write(request, ch)
	}()

	for writeError == nil {
		msg := &pb.Frame{}
		err := dec.Decode(msg)
		if err != nil {
			if err != io.EOF {
				s.logger.ErrorWith("decode error", "error", err)
				ctx.Error("decode error", http.StatusInternalServerError)
			}
			break
		}

		ch <- frames.NewFrameFromProto(msg)
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
	_ = s.replyJSON(ctx, reply)
}

func (s *Server) handleCreate(ctx *fasthttp.RequestCtx) {
	if !ctx.IsPost() { // ctx.PostBody() blocks on GET
		ctx.Error("unsupported method", http.StatusMethodNotAllowed)
	}

	requestInner := &pb.CreateRequest{}
	if err := json.Unmarshal(ctx.PostBody(), requestInner); err != nil {
		s.logger.ErrorWith("can't decode request", "error", err)
		ctx.Error(fmt.Sprintf("bad request - %s", err), http.StatusBadRequest)
		return
	}
	request := &frames.CreateRequest{
		Proto: requestInner,
	}

	if requestInner.Session != nil {
		s.httpAuth(ctx, requestInner.Session)
		request.Password = frames.InitSecretString(requestInner.Session.Password)
		request.Token = frames.InitSecretString(requestInner.Session.Token)
		requestInner.Session.Password = ""
		requestInner.Session.Token = ""
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

	requestInner := &pb.DeleteRequest{}
	if err := json.Unmarshal(ctx.PostBody(), requestInner); err != nil {
		s.logger.ErrorWith("can't decode request", "error", err)
		ctx.Error(fmt.Sprintf("bad request - %s", err), http.StatusBadRequest)
		return
	}
	request := &frames.DeleteRequest{
		Proto: requestInner,
	}

	if requestInner.Session != nil {
		s.httpAuth(ctx, requestInner.Session)
		request.Password = frames.InitSecretString(requestInner.Session.Password)
		request.Token = frames.InitSecretString(requestInner.Session.Token)
		requestInner.Session.Password = ""
		requestInner.Session.Token = ""
	}

	if err := s.api.Delete(request); err != nil {
		ctx.Error(err.Error(), http.StatusInternalServerError)
		return
	}

	s.replyOK(ctx)
}

func (s *Server) handleConfig(ctx *fasthttp.RequestCtx) {
	_ = s.replyJSON(ctx, s.config)
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
	_, _ = ctx.Write(okBytes)
}

func (s *Server) handleExec(ctx *fasthttp.RequestCtx) {
	if !ctx.IsPost() { // ctx.PostBody() blocks on GET
		ctx.Error("unsupported method", http.StatusMethodNotAllowed)
	}

	requestInner := &pb.ExecRequest{}
	if err := json.Unmarshal(ctx.PostBody(), requestInner); err != nil {
		s.logger.ErrorWith("can't decode request", "error", err)
		ctx.Error(fmt.Sprintf("bad request - %s", err), http.StatusBadRequest)
		return
	}
	request := &frames.ExecRequest{
		Proto: requestInner,
	}

	s.httpAuth(ctx, request.Proto.Session)
	request.Password = frames.InitSecretString(request.Proto.Session.Password)
	request.Token = frames.InitSecretString(request.Proto.Session.Token)
	request.Proto.Session.Password = ""
	request.Proto.Session.Token = ""

	frame, err := s.api.Exec(request)
	if err != nil {
		ctx.Error("can't exec", http.StatusInternalServerError)
		return
	}

	enc := frames.NewEncoder(ctx)
	var frameData string
	if frame != nil {
		data, err := frames.MarshalFrame(frame)
		if err != nil {
			s.logger.ErrorWith("can't marshal frame", "error", err)
			s.writeError(enc, fmt.Errorf("can't marsha frame - %s", err))
		}

		frameData = base64.StdEncoding.EncodeToString(data)
	}

	ctx.SetStatusCode(http.StatusOK)
	_ = json.NewEncoder(ctx).Encode(map[string]string{
		"frame": frameData,
	})
}

func (s *Server) handleSimpleJSONQuery(ctx *fasthttp.RequestCtx) {
	s.handleSimpleJSON(ctx, "query")
}

func (s *Server) handleSimpleJSONSearch(ctx *fasthttp.RequestCtx) {
	s.handleSimpleJSON(ctx, "search")
}

func (s *Server) handleSimpleJSON(ctx *fasthttp.RequestCtx, method string) {
	req, err := simpleJSONRequestFactory(method, ctx.PostBody())
	if err != nil {
		s.logger.ErrorWith("can't initialize simplejson request", "error", err)
		ctx.Error(fmt.Sprintf("bad request - %s", err), http.StatusBadRequest)
		return
	}
	// passing session in order to easily pass token, when implemented
	request := req.GetReadRequest(nil)

	s.httpAuth(ctx, request.Proto.Session)
	request.Password = frames.InitSecretString(request.Proto.Session.Password)
	request.Token = frames.InitSecretString(request.Proto.Session.Token)
	request.Proto.Session.Password = ""
	request.Proto.Session.Token = ""

	ch := make(chan frames.Frame)
	var apiError error
	go func() {
		defer close(ch)
		apiError = s.api.Read(request, ch)
		if apiError != nil {
			s.logger.ErrorWith("error reading", "error", apiError)
		}
	}()
	if apiError != nil {
		ctx.Error(fmt.Sprintf("Error creating response - %s", apiError), http.StatusInternalServerError)
	}
	if resp, err := CreateResponse(req, ch); err != nil {
		ctx.Error(fmt.Sprintf("Error creating response - %s", err), http.StatusInternalServerError)
	} else {
		_ = s.replyJSON(ctx, resp)
	}
}

func (s *Server) handleHistory(ctx *fasthttp.RequestCtx) {
	if !ctx.IsPost() { // ctx.PostBody() blocks on GET
		ctx.Error("unsupported method", http.StatusMethodNotAllowed)
	}

	requestInner := &pb.HistoryRequest{}
	if err := json.Unmarshal(ctx.PostBody(), requestInner); err != nil {
		s.logger.ErrorWith("can't decode request", "error", err)
		ctx.Error(fmt.Sprintf("bad request - %s", err), http.StatusBadRequest)
		return
	}
	request := &frames.HistoryRequest{
		Proto: requestInner,
	}

	s.httpAuth(ctx, request.Proto.Session)
	request.Password = frames.InitSecretString(request.Proto.Session.Password)
	request.Token = frames.InitSecretString(request.Proto.Session.Token)
	request.Proto.Session.Password = ""
	request.Proto.Session.Token = ""

	frame, err := s.api.History(request)
	if err != nil {
		ctx.Error(fmt.Sprintf("failed to get usage history log, err: %v", err), http.StatusInternalServerError)
		return
	}

	enc := frames.NewEncoder(ctx)
	var frameData string
	if frame != nil {
		data, err := frames.MarshalFrame(frame)
		if err != nil {
			s.logger.ErrorWith("can't marshal frame", "error", err)
			s.writeError(enc, fmt.Errorf("can't marsha frame - %s", err))
		}

		frameData = base64.StdEncoding.EncodeToString(data)
	}

	ctx.SetStatusCode(http.StatusOK)
	_ = json.NewEncoder(ctx).Encode(map[string]string{
		"frame": frameData,
	})
}

// based on https://github.com/buaazp/fasthttprouter/tree/master/examples/auth
func (s *Server) httpAuth(ctx *fasthttp.RequestCtx, session *frames.Session) {
	auth := ctx.Request.Header.Peek("Authorization")
	if auth == nil {
		return
	}

	switch {
	case bytes.HasPrefix(auth, basicAuthPrefix):
		s.parseBasicAuth(auth, session)
	case bytes.HasPrefix(auth, bearerAuthPrefix):
		s.parseBearerAuth(auth, session)
	default:
		s.logger.WarnWith("unknown auth scheme")
	}
}

func (s *Server) parseBasicAuth(auth []byte, session *frames.Session) {
	encodedData := auth[len(basicAuthPrefix):]
	data := make([]byte, base64.StdEncoding.DecodedLen(len(encodedData)))
	n, err := base64.StdEncoding.Decode(data, encodedData)
	if err != nil {
		s.logger.WarnWith("error in basic auth, can't base64 decode", "error", err)
		return
	}
	data = data[:n]

	i := bytes.IndexByte(data, ':')
	if i < 0 {
		s.logger.Warn("error in basic auth, can't find ':'")
		return
	}

	user := string(data[:i])
	if user == AccessKeyUser {
		session.Token = string(data[i+1:])
	} else {
		session.User = user
		session.Password = string(data[i+1:])
	}
}

func (s *Server) parseBearerAuth(auth []byte, session *frames.Session) {
	session.Token = string(auth[len(bearerAuthPrefix):])
}

func (s *Server) initRoutes() {
	s.routes = map[string]func(*fasthttp.RequestCtx){
		"/_/config": s.handleConfig,
		"/_/status": s.handleStatus,
		"/create":   s.handleCreate,
		"/delete":   s.handleDelete,
		"/read":     s.handleRead,
		"/write":    s.handleWrite,
		"/exec":     s.handleExec,
		"/history":  s.handleHistory,
		"/":         s.handleStatus,
		"/query":    s.handleSimpleJSONQuery,
		"/search":   s.handleSimpleJSONSearch,
	}
}
