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
	"os"
	"path"

	"github.com/v3io/frames"
)

// FileFS is file based FileSystem
type FileFS struct {
	root string
}

// NewFileFS returns a new FileFS FileSystem
func NewFileFS(cfg *frames.FSConfig) (FileSystem, error) {
	root := cfg.RootDir
	if !isDir(root) {
		if err := os.Mkdir(root, 0700); err != nil {
			return nil, err
		}
	}

	fs := FileFS{
		root: root,
	}
	return &fs, nil
}

// Open opens a new file
func (f *FileFS) Open(path string) (Reader, error) {
	path = f.pathTo(path)
	return os.Open(path)
}

// Create creates a new file
func (f *FileFS) Create(path string) (Writer, error) {
	path = f.pathTo(path)
	return os.Create(path)
}

// Append opens path to append
func (f *FileFS) Append(path string) (Writer, error) {
	path = f.pathTo(path)
	return os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
}

// Delete delets path
func (f *FileFS) Delete(path string) error {
	path = f.pathTo(path)
	return os.Remove(path)
}

func (f *FileFS) pathTo(name string) string {
	return path.Join(f.root, name)
}

func isDir(path string) bool {
	finfo, err := os.Stat(path)
	if err != nil {
		return false
	}

	return finfo.IsDir()
}

func init() {
	if err := RegisterFS("file", NewFileFS); err != nil {
		panic("can't register FileFS")
	}
}
