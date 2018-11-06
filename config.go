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

package frames

import (
	"fmt"
	"os"
)

// Configuration environment variables
const (
	WebAPIEndpointEnvironmentVariable  = "V3IO_URL"
	WebAPIContainerEnvironmentVariable = "V3IO_CONTAINER"
	WebAPIUsernameEnvironmentVariable  = "V3IO_USERNAME"
	WebAPIPasswordEnvironmentVariable  = "V3IO_PASSWORD"
)

// LogConfig is the logging configuration
type LogConfig struct {
	Level string `json:"level,omitempty"`
}

// Config is server configuration
type Config struct {
	Log            LogConfig `json:"log"`
	DefaultLimit   int       `json:"limit,omitempty"`
	DefaultTimeout int       `json:"timeout,omitempty"`

	// default V3IO connection details
	WebAPIEndpoint string `json:"webApiEndpoint"`
	Container      string `json:"container"`
	Username       string `json:"username,omitempty"`
	Password       string `json:"password,omitempty"`
	// Number of parallel V3IO worker routines
	Workers int `json:"workers"`

	Backends []*BackendConfig `json:"backends,omitempty"`
}

// InitDefaults initializes the defaults for configuration
func (c *Config) InitDefaults() error {
	if c.DefaultTimeout == 0 {
		c.DefaultTimeout = 30
	}
	if c.WebAPIEndpoint == "" {
		c.WebAPIEndpoint = os.Getenv(WebAPIEndpointEnvironmentVariable)
	}
	if c.Container == "" {
		c.Container = os.Getenv(WebAPIContainerEnvironmentVariable)
	}
	if c.Username == "" {
		c.Username = os.Getenv(WebAPIUsernameEnvironmentVariable)
	}
	if c.Password == "" {
		c.Password = os.Getenv(WebAPIPasswordEnvironmentVariable)
	}
	if c.Workers == 0 {
		c.Workers = 8
	}
	return nil
}

// InitBackendDefaults initializes default configuration for backend
func InitBackendDefaults(cfg *BackendConfig, framesConfig *Config) {
	if cfg.URL == "" {
		cfg.URL = framesConfig.WebAPIEndpoint
	}
	if cfg.Container == "" {
		cfg.Container = framesConfig.Container
	}
	if cfg.Username == "" {
		cfg.Username = framesConfig.Username
	}
	if cfg.Password == "" {
		cfg.Password = framesConfig.Password
	}
	if cfg.Workers == 0 {
		cfg.Workers = framesConfig.Workers
	}
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if len(c.Backends) == 0 {
		return fmt.Errorf("no backends")
	}

	names := make(map[string]bool)

	for i, backend := range c.Backends {
		if backend.Name == "" {
			backend.Name = backend.Type
		}

		if backend.Type == "" {
			return fmt.Errorf("backend %q missing type", backend.Name)
		}

		if found := names[backend.Name]; found {
			return fmt.Errorf("backend %d - duplicate name %q", i, backend.Name)
		}

		names[backend.Name] = true
	}

	return nil
}

// BackendConfig is backend configuration
type BackendConfig struct {
	Type string `json:"type"` // v3io, csv, ...
	Name string `json:"name"`

	// Backend API URL and credential
	URL       string `json:"url,omitempty"`
	Container string `json:"container,omitempty"`
	Path      string `json:"path,omitempty"`
	Username  string `json:"username,omitempty"`
	Password  string `json:"password,omitempty"`
	Workers   int    `json:"workers"`

	// backend specific options
	Options map[string]interface{} `json:"options"`

	// CSV backend
	RootDir string `json:"rootdir,omitempty"`
}