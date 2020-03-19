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

package tsdb

import (
	"fmt"
	"hash/fnv"
	"reflect"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/golang/groupcache/lru"
	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/v3io/frames"
	"github.com/v3io/frames/backends"
	"github.com/v3io/frames/v3ioutils"
	v3io "github.com/v3io/v3io-go/pkg/dataplane"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/pquerier"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/schema"
	tsdbutils "github.com/v3io/v3io-tsdb/pkg/utils"
)

// Backend is a TSDB backend
type Backend struct {
	queriers          *lru.Cache
	queriersLock      sync.Mutex
	backendConfig     *frames.BackendConfig
	framesConfig      *frames.Config
	logger            logger.Logger
	v3ioContext       v3io.Context
	inactivityTimeout time.Duration
}

// NewBackend returns a new TSDB backend
func NewBackend(logger logger.Logger, v3ioContext v3io.Context, cfg *frames.BackendConfig, framesConfig *frames.Config) (frames.DataBackend, error) {

	querierCacheSize := framesConfig.QuerierCacheSize
	if querierCacheSize == 0 {
		querierCacheSize = 64
	}

	newBackend := Backend{
		queriers:          lru.New(querierCacheSize),
		logger:            logger.GetChild("tsdb"),
		backendConfig:     cfg,
		framesConfig:      framesConfig,
		v3ioContext:       v3ioContext,
		inactivityTimeout: 0,
	}

	return &newBackend, nil
}

func (b *Backend) newConfig(session *frames.Session) *config.V3ioConfig {

	cfg := &config.V3ioConfig{
		WebAPIEndpoint:               session.Url,
		Container:                    session.Container,
		Username:                     session.User,
		Password:                     session.Password,
		AccessKey:                    session.Token,
		Workers:                      b.backendConfig.Workers,
		LogLevel:                     b.framesConfig.Log.Level,
		LoadPartitionsFromSchemaAttr: b.framesConfig.TsdbLoadPartitionsFromSchemaAttr,
	}
	return config.WithDefaults(cfg)
}

func (b *Backend) newAdapter(session *frames.Session, password string, token string, path string) (*tsdb.V3ioAdapter, error) {

	session = frames.InitSessionDefaults(session, b.framesConfig)
	containerName, newPath, err := v3ioutils.ProcessPaths(session, path, false)
	if err != nil {
		return nil, err
	}

	session.Container = containerName
	cfg := b.newConfig(session)

	container, err := v3ioutils.NewContainer(
		b.v3ioContext,
		session,
		password,
		token,
		b.logger)

	if err != nil {
		return nil, err
	}

	if b.backendConfig.Workers == 0 {
		resp, err := container.GetClusterMDSync(&v3io.GetClusterMDInput{})
		if err != nil {
			return nil, fmt.Errorf("could not detrmine num vns in cluster")
		}
		getClusterMDOutput := resp.Output.(*v3io.GetClusterMDOutput)
		b.backendConfig.Workers = getClusterMDOutput.NumberOfVNs
		cfg.Workers = getClusterMDOutput.NumberOfVNs
	}
	cfg.TablePath = newPath
	b.logger.DebugWith("TSDB configuration", "config", cfg)
	adapter, err := tsdb.NewV3ioAdapter(cfg, container, b.logger)
	if err != nil {
		return nil, err
	}

	return adapter, nil
}

// Get underlying bytes of string for read-only purposes to avoid allocating a slice.
func getBytes(str string) []byte {
	hdr := *(*reflect.StringHeader)(unsafe.Pointer(&str))
	return *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
		Data: hdr.Data,
		Len:  hdr.Len,
		Cap:  hdr.Len,
	}))
}

// GetAdapter returns an adapter
func (b *Backend) GetAdapter(session *frames.Session, password string, token string, path string) (*tsdb.V3ioAdapter, error) {
	adapter, err := b.newAdapter(session, password, token, path)
	if err != nil {
		return nil, err
	}
	return adapter, nil
}

// GetQuerier returns a querier
func (b *Backend) GetQuerier(session *frames.Session, password string, token string, path string) (*pquerier.V3ioQuerier, error) {

	h := fnv.New64()
	_, _ = h.Write(getBytes(session.Url))
	_, _ = h.Write(getBytes(session.Container))
	_, _ = h.Write(getBytes(path))
	_, _ = h.Write(getBytes(session.User))
	_, _ = h.Write(getBytes(password))
	_, _ = h.Write(getBytes(token))
	key := h.Sum64()

	b.queriersLock.Lock()
	defer b.queriersLock.Unlock()
	qry, found := b.queriers.Get(key)
	if !found {
		qry, found = b.queriers.Get(key) // Double-check locking
		if !found {
			var err error
			adapter, err := b.newAdapter(session, password, token, path)
			if err != nil {
				return nil, err
			}
			qry, err = adapter.QuerierV2()
			if err != nil {
				return nil, errors.Wrap(err, "Failed to initialize Querier")
			}
			b.queriers.Add(key, qry)
		}
	}

	return qry.(*pquerier.V3ioQuerier), nil
}

// Create creates a TSDB table
func (b *Backend) Create(request *frames.CreateRequest) error {

	rate := request.Proto.Rate
	if request.Proto.Rate == "" {
		return errors.New("Must specify 'rate' attribute to specify maximum sample rate, e.g. '1/m'")
	}

	aggregationGranularity := config.DefaultAggregationGranularity
	if request.Proto.AggregationGranularity != "" {
		aggregationGranularity = request.Proto.AggregationGranularity
	}

	defaultRollups := request.Proto.Aggregates

	session := frames.InitSessionDefaults(request.Proto.Session, b.framesConfig)
	containerName, path, err := v3ioutils.ProcessPaths(session, request.Proto.Table, false)
	if err != nil {
		return err
	}

	session.Container = containerName
	cfg := b.newConfig(session)
	cfg.Password = request.Password.Get()
	cfg.AccessKey = request.Token.Get()

	cfg.TablePath = path
	dbSchema, err := schema.NewSchema(cfg, rate, aggregationGranularity, defaultRollups, "") // TODO: support create table with cross-label aggregates

	if err != nil {
		return errors.Wrap(err, "failed to create a TSDB schema")
	}

	container, err := v3ioutils.NewContainer(
		b.v3ioContext,
		session,
		cfg.Password,
		cfg.AccessKey,
		b.logger)

	if err != nil {
		return errors.Wrap(err, "failed to create container")
	}

	err = tsdb.CreateTSDB(cfg, dbSchema, container)
	if b.ignoreCreateExists(request, err) {
		return nil
	}
	return err
}

// Delete deletes a table or part of it
func (b *Backend) Delete(request *frames.DeleteRequest) error {
	var err error

	end := time.Now().Unix() * 1000
	if request.Proto.End != "" {
		end, err = tsdbutils.Str2unixTime(request.Proto.End)
		if err != nil {
			return errors.Wrap(err, "failed to parse end time")
		}
	}

	start := end - 1000*3600 // Default start time = one hour before the end time
	if request.Proto.Start != "" {
		start, err = tsdbutils.Str2unixTime(request.Proto.Start)
		if err != nil {
			return errors.Wrap(err, "failed to parse start time")
		}
	}

	delAll := request.Proto.Start == "" && request.Proto.End == ""

	adapter, err := b.GetAdapter(request.Proto.Session, request.Password.Get(), request.Token.Get(), request.Proto.Table)
	if err != nil {
		if request.Proto.IfMissing == frames.IgnoreError && b.isSchemaNotFoundError(err) {
			return nil
		}
		return err
	}

	params := tsdb.DeleteParams{DeleteAll: delAll,
		From:    start,
		To:      end,
		Filter:  request.Proto.Filter,
		Metrics: request.Proto.Metrics}
	err = adapter.DeleteDB(params)
	return err

}

// Exec executes a command
func (b *Backend) Exec(request *frames.ExecRequest) (frames.Frame, error) {
	return nil, fmt.Errorf("TSDB backend doesn't support the 'execute' command")
}

func (b *Backend) ignoreCreateExists(request *frames.CreateRequest, err error) bool {
	if request.Proto.IfExists != frames.IgnoreError {
		return false
	}

	// TODO: Ask TSDB to return  specific error value; this is brittle
	return err == nil || strings.Contains(err.Error(), "TSDB table already exists")
}

func (b *Backend) isSchemaNotFoundError(err error) bool {
	return strings.Contains(err.Error(), "no TSDB schema file found")
}

func init() {
	if err := backends.Register("tsdb", NewBackend); err != nil {
		panic(err)
	}
}
