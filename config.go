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

	return nil
}

// InitSessionDefaults initializes session defaults
func InitSessionDefaults(session *Session, framesConfig *Config) *Session {

	if session == nil {
		session = &Session{}
	}

	if session.Url == "" {
		session.Url = framesConfig.WebAPIEndpoint
	}
	if session.Container == "" {
		session.Container = framesConfig.Container
	}
	if session.User == "" {
		session.User = framesConfig.Username
	}
	if session.Password == "" {
		session.Password = framesConfig.Password
	}

	return session
}

// InitBackendDefaults initializes default configuration for backend
func InitBackendDefaults(cfg *BackendConfig, framesConfig *Config) {
	if cfg.Workers == 0 {
		cfg.Workers = framesConfig.Workers
		if cfg.Workers == 0 {
			cfg.Workers = 8
		}
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

// BackendConfig is default backend configuration
type BackendConfig struct {
	Type    string `json:"type"` // v3io, csv, ...
	Name    string `json:"name"`
	Workers int    `json:"workers"`
	// backend specific options
	Options map[string]interface{} `json:"options"`

	// FileSystem config
	FileSystem FSConfig `fileSystem:"rootdir,omitempty"`
}

// NewSession will create a new session. It will populate missing values from
// the environment.  Environment variables have V3IO_ prefix (e.g. V3IO_USER)
func NewSession(url, container, path, user, password, token string) *Session {
	if url == "" {
		url = os.Getenv("V3IO_URL")
	}

	if container == "" {
		container = os.Getenv("V3IO_CONTAINER")
	}

	if path == "" {
		path = os.Getenv("V3IO_PATH")
	}

	if user == "" {
		user = os.Getenv("V3IO_USER")
	}

	if password == "" {
		password = os.Getenv("V3IO_PASSWORD")
	}

	if token == "" {
		token = os.Getenv("V3IO_TOKEN")
	}

	return &Session{
		Url:       url,
		Container: container,
		Path:      path,
		User:      user,
		Password:  password,
		Token:     token,
	}
}

// FSConfig is FileSystem options
type FSConfig struct {
	Type    string `json:"type"`
	RootDir string `json:"rootDir"`
}
