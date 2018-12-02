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

package backends

import (
	"strings"

	"github.com/nuclio/logger"
	"github.com/v3io/frames"
)

var (
	backendRegistry = NewRegistry(strings.ToLower)
)

// Factory is a backend factory
type Factory func(logger.Logger, *frames.BackendConfig, *frames.Config) (frames.DataBackend, error)

// Register registers a backend factory for a type
func Register(typ string, factory Factory) error {
	return backendRegistry.Register(typ, factory)
}

// GetFactory returns factory for a backend
func GetFactory(typ string) Factory {
	val := backendRegistry.Get(typ)
	if val == nil {
		return nil
	}

	f, ok := val.(Factory)
	if !ok {
		// TODO: Log
		return nil
	}
	return f
}
