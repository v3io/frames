package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/v3io/frames"
	"github.com/v3io/frames/backends/kv"
	"github.com/v3io/frames/backends/mock" // TODO

	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/vmihailenco/msgpack"
)

// State is server state
type State int

// Possible states
const (
	ReadyState State = iota
	RunningState
	ErrorState
)

func (s State) String() string {
	switch s {
	case ReadyState:
		return "ready"
	case RunningState:
		return "running"
	case ErrorState:
		return "error"
	}

	return fmt.Sprintf("Unknown state - %d", s)
}

// Server is HTTP server
type Server struct {
	address string // listen address
	server  *http.Server
	state   State

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

	var backend frames.DataBackend

	if false {
		backend, err = kv.NewBackend(ctx)
		if err != nil {
			ctx.Logger.ErrorWith("Can't create backend", "error", err)
			return nil, errors.Wrap(err, "Can't create backend")
		}
	} else {
		backend = &mock.Backend{}
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
func (s *Server) State() State {
	return s.state
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/_/status" {
		fmt.Fprintf(w, "%s\n", s.State())
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		s.logger.Error("Not a flusher")
		http.Error(w, "not a flusher", http.StatusInternalServerError)
		return
	}

	defer r.Body.Close()
	request := &frames.DataReadRequest{}
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(request); err != nil {
		s.logger.ErrorWith("Can't decode request", "error", err)
		http.Error(w, fmt.Sprintf("Bad request - %s", err), http.StatusBadRequest)
		return
	}

	// TODO: Validate request

	iter, err := s.backend.ReadRequest(request)
	if err != nil {
		s.logger.ErrorWith("Can't query", "error", err)
		http.Error(w, fmt.Sprintf("Can't query - %s", err), http.StatusInternalServerError)
	}

	enc := msgpack.NewEncoder(w)
	for iter.Next() {
		if err := enc.Encode(iter.At()); err != nil {
			s.logger.ErrorWith("Can't encode result", "error", err)
			return
		}

		flusher.Flush()
	}

	if err := iter.Err(); err != nil {
		s.logger.ErrorWith("Error during iteration", "error", err)
	}
}

// Start starts the server
func (s *Server) Start() error {
	if state := s.State(); state != ReadyState {
		s.logger.ErrorWith("Start from bad state", "state", state.String())
		return fmt.Errorf("bad state - %s", state)
	}

	s.server = &http.Server{
		Addr:    s.address,
		Handler: s,
	}

	go func() {
		err := s.server.ListenAndServe()
		if err != nil {
			s.logger.ErrorWith("Error running HTTP server", "error", err)
			s.state = ErrorState
		}
	}()

	s.state = RunningState
	return nil
}

// Stop stops the server
func (s *Server) Stop(ctx context.Context) error {
	if state := s.State(); state != RunningState {
		s.logger.ErrorWith("Stop from bad state", "state", state.String())
		return fmt.Errorf("bad state - %s", state)
	}

	if ctx == nil {
		ctx = context.Background()
	}

	if err := s.server.Shutdown(ctx); err != nil {
		s.logger.ErrorWith("Error shutting down", "error", err)
		s.state = ErrorState
		return errors.Wrap(err, "Error shutting down")
	}

	s.state = ReadyState
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
