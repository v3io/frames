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
	"io"
	"strings"

	"github.com/v3io/frames"
)

var (
	fsRegistry = NewRegistry(strings.ToLower)
)

// Reader is file system reader
type Reader interface {
	io.Reader
	io.Seeker
	io.Closer
}

// Writer is file system writer
type Writer interface {
	io.Writer
	io.Closer
	Sync() error
}

// FileSystem is a file system interface
type FileSystem interface {
	// Open opens a file for reading
	Open(path string) (Reader, error)
	// Create creates a new file
	Create(path string) (Writer, error)
	// Append opens a new file for appending
	Append(path string) (Writer, error)
	// Delete delets a path
	Delete(path string) error
}

// FSFactory is a FileSystem factory
type FSFactory func(*frames.FSConfig) (FileSystem, error)

// RegisterFS registers a file system
func RegisterFS(typ string, fs FSFactory) error {
	return fsRegistry.Register(typ, fs)
}

// GetFS returns FSFactory for name (nil if not found)
func GetFS(typ string) FSFactory {
	val := fsRegistry.Get(typ)
	if val == nil {
		return nil
	}

	fs, ok := val.(FSFactory)
	if !ok {
		// TODO: Log
		return nil
	}
	return fs
}
