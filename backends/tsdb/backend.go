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
	"github.com/nuclio/logger"
	"github.com/v3io/frames"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
)

type Backend struct {
	adapters map[string]*tsdb.V3ioAdapter
	config   *frames.BackendConfig
	logger   logger.Logger
}

// NewBackend return a new key/value backend
func NewBackend(logger logger.Logger, cfg *frames.BackendConfig) (frames.DataBackend, error) {

	newBackend := Backend{
		adapters: map[string]*tsdb.V3ioAdapter{},
		logger:   logger,
		config:   cfg,
	}

	if cfg.Path != "" {
		adapter, err := newAdapter(cfg, cfg.Path)
		if err != nil {
			return nil, err
		}
		newBackend.adapters[cfg.Path] = adapter
	}

	return &newBackend, nil
}

func newAdapter(cfg *frames.BackendConfig, path string) (*tsdb.V3ioAdapter, error) {

	if path == "" {
		path = cfg.Path
	}

	tsdbConfig := config.V3ioConfig{
		V3ioUrl:   cfg.V3ioURL,
		Container: cfg.Container,
		Path:      path,
		Username:  cfg.Username,
		Password:  cfg.Password,
		Workers:   cfg.Workers,
	}

	adapter, err := tsdb.NewV3ioAdapter(&tsdbConfig, nil, nil)
	if err != nil {
		return nil, err
	}

	return adapter, nil
}

func (b *Backend) GetAdapter(path string) (*tsdb.V3ioAdapter, error) {
	// TODO: Expire unused adapters
	adapter, ok := b.adapters[path]
	if !ok {
		adapter, err := newAdapter(b.config, path)
		if err != nil {
			return nil, err
		}
		b.adapters[path] = adapter
	}

	return adapter, nil
}

func (b *Backend) Create(request *frames.CreateRequest) error {
	return nil
}

func (b *Backend) Delete(request *frames.DeleteRequest) error {
	return nil
}
