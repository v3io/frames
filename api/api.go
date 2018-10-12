package api

// API Layer

import (
	"fmt"
	"io"
	"time"

	"github.com/v3io/frames"
	"github.com/v3io/frames/backends"

	// Load backends (make sure they register)
	_ "github.com/v3io/frames/backends/csv"
	_ "github.com/v3io/frames/backends/kv"
	_ "github.com/v3io/frames/backends/tsdb"

	"github.com/nuclio/logger"
	"github.com/pkg/errors"
)

// WriterFlusher is interface that implements io.Reader and Flush
type WriterFlusher interface {
	io.Writer
	Flush() error
}

// API layer, implements common CRUD operations
// TODO: Call it DAL? (data access layer)
type API struct {
	logger   logger.Logger
	backends map[string]frames.DataBackend
}

// New returns a new API layer struct
func New(logger logger.Logger, config *frames.Config) (*API, error) {
	if logger == nil {
		var err error
		logger, err = frames.NewLogger(config.Verbose)
		if err != nil {
			return nil, errors.Wrap(err, "can't create logger")
		}
	}

	api := &API{
		logger: logger,
	}

	if err := api.createBackends(config.Backends); err != nil {
		msg := "can't create backends"
		api.logger.ErrorWith(msg, "error", err, "config", config)
		return nil, errors.Wrap(err, "can't create backends")
	}

	return api, nil
}

// Read reads from database, emitting results to wf
func (api *API) Read(request *frames.ReadRequest, wf WriterFlusher) error {
	if request.Query != "" {
		if err := api.populateQuery(request); err != nil {
			msg := "can't populate query"
			api.logger.ErrorWith(msg, "request", request, "error", err)
			return errors.Wrap(err, msg)
		}
	}

	api.logger.InfoWith("read request", "request", request)

	backend, ok := api.backends[request.Backend]

	if !ok {
		api.logger.ErrorWith("unknown backend", "name", request.Backend)
		return fmt.Errorf("unknown backend - %q", request.Backend)
	}

	iter, err := backend.Read(request)
	if err != nil {
		api.logger.ErrorWith("can't query", "error", err)
		return errors.Wrap(err, "can't query")
	}

	enc := frames.NewEncoder(wf)

	for iter.Next() {
		if err := enc.Encode(iter.At()); err != nil {
			msg := "can't encode result"
			api.logger.ErrorWith(msg, "error", err)
			return errors.Wrap(err, msg)
		}

		if err := wf.Flush(); err != nil {
			msg := "can't flush"
			api.logger.ErrorWith(msg, "error", err)
			return errors.Wrap(err, msg)
		}
	}

	if err := iter.Err(); err != nil {
		msg := "error during iteration"
		api.logger.ErrorWith(msg, "error", err)
		return errors.Wrap(err, msg)
	}

	return nil
}

// Write write data to backend, returns num_frames, num_rows, error
func (api *API) Write(request *frames.WriteRequest, dec *frames.Decoder) (int, int, error) {
	if request.Backend == "" || request.Table == "" {
		msg := "missing parameters"
		api.logger.ErrorWith(msg, "request", request)
		return -1, -1, fmt.Errorf(msg)
	}

	api.logger.InfoWith("write request", "request", request)
	backend, ok := api.backends[request.Backend]
	if !ok {
		api.logger.ErrorWith("unkown backend", "name", request.Backend)
		return -1, -1, fmt.Errorf("unknown backend - %s", request.Backend)
	}

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

	for {
		frame, err := dec.DecodeFrame()

		if err != nil {
			if err == io.EOF {
				break
			}

			msg := "can't decode"
			api.logger.ErrorWith(msg, "error", err)
			return nFrames, nRows, errors.Wrap(err, msg)
		}

		api.logger.DebugWith("frame to write", "size", frame.Len())
		if err := appender.Add(frame); err != nil {
			msg := "can't add frame"
			api.logger.ErrorWith(msg, "error", err)
			return nFrames, nRows, errors.Wrap(err, msg)
		}

		nFrames++
		nRows += frame.Len()
		api.logger.DebugWith("write", "numFrames", nFrames, "numRows", nRows)
	}

	api.logger.Debug("write done")

	// TODO: Specify timeout in request?
	if err := appender.WaitForComplete(3 * time.Minute); err != nil {
		msg := "can't wait for completion"
		api.logger.ErrorWith(msg, "error", err)
		return nFrames, nRows, errors.Wrap(err, msg)
	}

	return nFrames, nRows, nil
}

// Create will create a new table
func (api *API) Create(request *frames.CreateRequest) error {
	if request.Backend == "" || request.Table == "" {
		msg := "missing parameters"
		api.logger.ErrorWith(msg, "request", request)
		return fmt.Errorf(msg)
	}

	api.logger.DebugWith("create", "request", request)
	backend, ok := api.backends[request.Backend]
	if !ok {
		api.logger.ErrorWith("unkown backend", "name", request.Backend)
		return fmt.Errorf("unknown backend - %s", request.Backend)
	}

	if err := backend.Create(request); err != nil {
		api.logger.ErrorWith("error creating table", "error", err, "request", request)
		return errors.Wrap(err, "error creating table")
	}

	return nil
}

// Delete deletes a table or part of it
func (api *API) Delete(request *frames.DeleteRequest) error {
	if request.Backend == "" || request.Table == "" {
		msg := "missing parameters"
		api.logger.ErrorWith(msg, "request", request)
		return fmt.Errorf(msg)
	}

	api.logger.DebugWith("delete", "request", request)
	backend, ok := api.backends[request.Backend]
	if !ok {
		api.logger.ErrorWith("unkown backend", "name", request.Backend)
		return fmt.Errorf("unknown backend - %s", request.Backend)
	}

	if err := backend.Delete(request); err != nil {
		api.logger.ErrorWith("error deleting table", "error", err, "request", request)
		return errors.Wrap(err, "can't delete")
	}

	return nil
}

func (api *API) populateQuery(request *frames.ReadRequest) error {
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

func (api *API) createBackends(configs []*frames.BackendConfig) error {
	api.backends = make(map[string]frames.DataBackend)

	for _, cfg := range configs {
		factory := backends.GetFactory(cfg.Type)
		if factory == nil {
			return fmt.Errorf("unknown backend - %q", cfg.Type)
		}

		backend, err := factory(api.logger, cfg)
		if err != nil {
			return errors.Wrapf(err, "%s:%s - can't create backend", cfg.Name, cfg.Type)
		}

		api.backends[cfg.Name] = backend
	}

	return nil
}
