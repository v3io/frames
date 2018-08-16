package server

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/v3io/frames/pkg/backends/kv"
	"github.com/v3io/frames/pkg/common"
	"github.com/v3io/frames/pkg/utils"

	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/vmihailenco/msgpack"
)

// Server is HTTP server
type Server struct {
	backend common.DataBackend
	config  *common.V3ioConfig
	context *common.DataContext
	logger  logger.Logger
}

// NewServer creates a new server
func NewServer(cfg *common.V3ioConfig) (*Server, error) {
	ctx, err := newContext(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "Can't create context")
	}

	backend, err := kv.NewKVBackend(ctx)
	if err != nil {
		ctx.Logger.ErrorWith("Can't create backend", "error", err)
		return nil, errors.Wrap(err, "Can't create backend")
	}

	srv := &Server{
		backend: backend,
		config:  cfg,
		context: ctx,
		logger:  ctx.Logger,
	}

	return srv, nil

}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		s.logger.Error("Not a flusher")
		http.Error(w, "not a flusher", http.StatusInternalServerError)
		return
	}

	defer r.Body.Close()
	request := &common.DataReadRequest{}
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

func newContext(cfg *common.V3ioConfig) (*common.DataContext, error) {
	logger, _ := utils.NewLogger(cfg.Verbose)
	container, err := utils.CreateContainer(
		logger, cfg.V3ioUrl, cfg.Container, cfg.Username, cfg.Password, cfg.Workers)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create data container")
	}

	return &common.DataContext{Container: container, Logger: logger}, nil
}
