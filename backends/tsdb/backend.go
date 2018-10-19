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
	"github.com/nuclio/logger"

	"github.com/v3io/frames"
	"github.com/v3io/frames/backends"

	"github.com/pkg/errors"
	"github.com/v3io/frames/v3ioutils"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
)

// Backend is a tsdb backend
type Backend struct {
	adapters      map[string]*tsdb.V3ioAdapter
	backendConfig *frames.BackendConfig
	framesConfig  *frames.Config
	tsdbConfig    *config.V3ioConfig
	logger        logger.Logger
	container     *v3io.Container
}

// NewBackend return a new key/value backend
func NewBackend(logger logger.Logger, cfg *frames.BackendConfig, framesConfig *frames.Config) (frames.DataBackend, error) {
	newBackend := Backend{
		adapters:      map[string]*tsdb.V3ioAdapter{},
		logger:        logger.GetChild("tsdb"),
		backendConfig: cfg,
		framesConfig:  framesConfig,
	}

	tsdbConfig := &config.V3ioConfig{
		WebApiEndpoint: cfg.V3ioURL,
		Container:      cfg.Container,
		Username:       cfg.Username,
		Password:       cfg.Password,
		Workers:        cfg.Workers,
		LogLevel:       framesConfig.Log.Level,
	}

	_, err := config.GetOrLoadFromStruct(tsdbConfig)
	if err != nil {
		// if we couldn't load the file and its not the default
		return nil, err
	}

	newBackend.tsdbConfig = tsdbConfig

	container, err := v3ioutils.CreateContainer(logger,
		cfg.V3ioURL, cfg.Container, cfg.Username, cfg.Password, cfg.Workers)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create V3IO data container")
	}
	newBackend.container = container

	/*	if cfg.Path != "" {
			adapter, err := newAdapter(cfg, cfg.Path)
			if err != nil {
				return nil, err
			}
			newBackend.adapters[cfg.Path] = adapter
		}
	*/
	return &newBackend, nil
}

func (b *Backend) newAdapter(path string) (*tsdb.V3ioAdapter, error) {

	if path == "" {
		path = b.backendConfig.Path
	}

	b.tsdbConfig.TablePath = path
	fmt.Println("conf:", b.tsdbConfig)
	adapter, err := tsdb.NewV3ioAdapter(b.tsdbConfig, b.container, b.logger)
	if err != nil {
		return nil, err
	}

	return adapter, nil
}

// GetAdapter returns an adapter
func (b *Backend) GetAdapter(path string) (*tsdb.V3ioAdapter, error) {
	// TODO: maintain adapter cache, for now create new per read/write request
	//adapter, ok := b.adapters[path]
	//if !ok {
	//	b.adapters[path] = adapter
	//}

	adapter, err := b.newAdapter(path)
	if err != nil {
		return nil, err
	}
	return adapter, nil
}

// Create creates a table
func (b *Backend) Create(request *frames.CreateRequest) error {
	return fmt.Errorf("Create not implemented")
}

// Delete deletes a table or part of it
func (b *Backend) Delete(request *frames.DeleteRequest) error {
	return fmt.Errorf("Delete not implemented")
}

func init() {
	if err := backends.Register("tsdb", NewBackend); err != nil {
		panic(err)
	}
}
