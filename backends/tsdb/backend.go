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
	"unsafe"

	"github.com/v3io/frames"
	"github.com/v3io/frames/backends"

	"github.com/golang/groupcache/lru"
	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/v3io/frames/v3ioutils"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/schema"
	tsdbutils "github.com/v3io/v3io-tsdb/pkg/utils"
	"github.com/valyala/fasthttp"
)

// Backend is a tsdb backend
type Backend struct {
	//map[uint64]*tsdb.V3ioAdapter
	adapters      *lru.Cache
	adaptersLock  sync.Mutex
	backendConfig *frames.BackendConfig
	framesConfig  *frames.Config
	logger        logger.Logger
	httpClient    *fasthttp.Client
}

// NewBackend return a new tsdb backend
func NewBackend(logger logger.Logger, httpClient *fasthttp.Client, cfg *frames.BackendConfig, framesConfig *frames.Config) (frames.DataBackend, error) {

	frames.InitBackendDefaults(cfg, framesConfig)
	adapterCacheSize := framesConfig.AdapterCacheSize
	if adapterCacheSize == 0 {
		adapterCacheSize = 64
	}

	newBackend := Backend{
		adapters:      lru.New(adapterCacheSize),
		logger:        logger.GetChild("tsdb"),
		backendConfig: cfg,
		framesConfig:  framesConfig,
		httpClient:    httpClient,
	}

	return &newBackend, nil
}

func (b *Backend) newConfig(session *frames.Session) *config.V3ioConfig {

	cfg := &config.V3ioConfig{
		WebApiEndpoint: session.Url,
		Container:      session.Container,
		Username:       session.User,
		Password:       session.Password,
		AccessKey:      session.Token,
		Workers:        b.backendConfig.Workers,
		LogLevel:       b.framesConfig.Log.Level,
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
		b.httpClient,
		session,
		password,
		token,
		b.logger,
		cfg.Workers,
	)

	if err != nil {
		return nil, err
	}

	cfg.TablePath = newPath
	b.logger.DebugWith("tsdb config", "config", cfg)
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

	h := fnv.New64()
	_, _ = h.Write(getBytes(session.Url))
	_, _ = h.Write(getBytes(session.Container))
	_, _ = h.Write(getBytes(path))
	_, _ = h.Write(getBytes(session.User))
	_, _ = h.Write(getBytes(password))
	_, _ = h.Write(getBytes(token))
	key := h.Sum64()

	adapter, found := b.adapters.Get(key)
	if !found {
		b.adaptersLock.Lock()
		defer b.adaptersLock.Unlock()
		adapter, found = b.adapters.Get(key) // Double-checked locking
		if !found {
			var err error
			adapter, err = b.newAdapter(session, password, token, path)
			if err != nil {
				return nil, err
			}
			b.adapters.Add(key, adapter)
		}
	}

	return adapter.(*tsdb.V3ioAdapter), nil
}

// Create creates a table
func (b *Backend) Create(request *frames.CreateRequest) error {

	attrs := request.Proto.Attributes()

	attr, ok := attrs["rate"]
	if !ok {
		return errors.New("Must specify 'rate' attribute to specify maximum sample rate, e.g. '1/m'")
	}
	rate, isStr := attr.(string)
	if !isStr {
		return errors.New("'rate' attribute must be a string, e.g. '1/m'")
	}

	aggregationGranularity := config.DefaultAggregationGranularity
	attr, ok = attrs["aggregation-granularity"]
	if ok {
		val, isStr := attr.(string)
		if !isStr {
			return errors.New("'aggregation-granularity' attribute must be a string")
		}
		aggregationGranularity = val
	}

	defaultRollups := ""
	attr, ok = attrs["aggregates"]
	if ok {
		val, isStr := attr.(string)
		if !isStr {
			return errors.New("'aggregates' attribute must be a string")
		}
		defaultRollups = val
	}

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
	dbSchema, err := schema.NewSchema(cfg, rate, aggregationGranularity, defaultRollups, "") // todo: support create table with cross label aggregates

	if err != nil {
		return errors.Wrap(err, "Failed to create a TSDB schema.")
	}

	err = tsdb.CreateTSDB(cfg, dbSchema)
	if b.ignoreCreateExists(request, err) {
		return nil
	}
	return err
}

// Delete deletes a table or part of it
func (b *Backend) Delete(request *frames.DeleteRequest) error {

	start, err := tsdbutils.Str2duration(request.Proto.Start)
	if err != nil {
		return err
	}

	end, err := tsdbutils.Str2duration(request.Proto.End)
	if err != nil {
		return err
	}

	delAll := request.Proto.Start == "" && request.Proto.End == ""

	adapter, err := b.GetAdapter(request.Proto.Session, request.Password.Get(), request.Token.Get(), request.Proto.Table)
	if err != nil {
		if request.Proto.IfMissing == frames.IgnoreError && b.isSchemaNotFoundError(err) {
			return nil
		} else {
			return err
		}
	}

	err = adapter.DeleteDB(delAll, false, start, end)
	if err == nil {
		return err
	}

	return err

}

// Exec executes a command
func (b *Backend) Exec(request *frames.ExecRequest) (frames.Frame, error) {
	return nil, fmt.Errorf("TSDB backend does not support Exec")
}

func (b *Backend) ignoreCreateExists(request *frames.CreateRequest, err error) bool {
	if request.Proto.IfExists != frames.IgnoreError {
		return false
	}

	// TODO: Ask tsdb to return specific error value, this is brittle
	return err == nil || strings.Contains(err.Error(), "A TSDB table already exists")
}

func (b *Backend) isSchemaNotFoundError(err error) bool {
	return strings.Contains(err.Error(), "No TSDB schema file found")
}

func init() {
	if err := backends.Register("tsdb", NewBackend); err != nil {
		panic(err)
	}
}
