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
	"fmt"
	"sync"
)

// Registry is a registry of things (by name)
type Registry struct {
	lock sync.RWMutex
	m    map[string]interface{}
	norm func(string) string
}

// NewRegistry returns a new Registry
func NewRegistry(normalizeName func(string) string) *Registry {
	return &Registry{
		m:    make(map[string]interface{}),
		norm: normalizeName,
	}
}

// Register registers a value
func (r *Registry) Register(name string, value interface{}) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	name = r.norm(name)
	if _, ok := r.m[name]; ok {
		return fmt.Errorf("%q already registered", name)
	}

	r.m[name] = value
	return nil
}

// Get returns the value associated with name, nil if not found
func (r *Registry) Get(name string) interface{} {
	r.lock.RLock()
	defer r.lock.RUnlock()

	name = r.norm(name)
	return r.m[name]
}
