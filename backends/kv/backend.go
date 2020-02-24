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

package kv

import (
	"fmt"
	"strings"
	"time"

	"github.com/nuclio/logger"
	"github.com/v3io/frames"
	"github.com/v3io/frames/v3ioutils"
	"github.com/v3io/v3io-go/pkg/dataplane"
)

// Backend is NoSQL (key/value) backend
type Backend struct {
	logger            logger.Logger
	numWorkers        int
	inactivityTimeout time.Duration
	framesConfig      *frames.Config
	v3ioContext       v3io.Context
}

// NewBackend returns a new NoSQL (key/value) backend
func NewBackend(logger logger.Logger, v3ioContext v3io.Context, config *frames.BackendConfig, framesConfig *frames.Config) (frames.DataBackend, error) {

	newBackend := Backend{
		logger:            logger.GetChild("kv"),
		numWorkers:        config.Workers,
		framesConfig:      framesConfig,
		v3ioContext:       v3ioContext,
		inactivityTimeout: 0,
	}
	return &newBackend, nil
}

// Create creates a table - not required for the NoSQL backend
func (b *Backend) Create(request *frames.CreateRequest) error {
	return fmt.Errorf("'create' isn't required for the NoSQL backend; the table is created on first write")
}

// Delete deletes a table (or part of it)
func (b *Backend) Delete(request *frames.DeleteRequest) error {

	container, path, err := b.newConnection(request.Proto.Session, request.Password.Get(), request.Token.Get(), request.Proto.Table, true)
	if err != nil {
		return err
	}

	return v3ioutils.DeleteTable(b.logger, container, path, request.Proto.Filter, b.numWorkers, request.Proto.IfMissing == frames.IgnoreError)
	// TODO: delete the table directory entry if filter == ""
}

// Exec executes a command
func (b *Backend) Exec(request *frames.ExecRequest) (frames.Frame, error) {
	cmd := strings.TrimSpace(strings.ToLower(request.Proto.Command))
	switch cmd {
	case "infer", "infer_schema":
		return nil, b.inferSchema(request)
	case "update":
		return nil, b.updateItem(request)
	}
	return nil, fmt.Errorf("NoSQL backend doesn't support execute command '%s'", cmd)
}

func (b *Backend) updateItem(request *frames.ExecRequest) error {
	varKey, hasKey := request.Proto.Args["key"]
	varExpr, hasExpr := request.Proto.Args["expression"]
	if !hasExpr || !hasKey || request.Proto.Table == "" {
		return fmt.Errorf("missing a required parameter - 'table', 'expression', and/or 'key' argument")
	}

	key := varKey.GetSval()
	expr := varExpr.GetSval()

	condition := ""
	if val, ok := request.Proto.Args["condition"]; ok {
		condition = val.GetSval()
	}

	container, path, err := b.newConnection(request.Proto.Session, request.Password.Get(), request.Token.Get(), request.Proto.Table, true)
	if err != nil {
		return err
	}

	b.logger.DebugWith("update item", "path", path, "key", key, "expr", expr, "condition", condition)
	return container.UpdateItemSync(&v3io.UpdateItemInput{Path: path + key, Expression: &expr, Condition: condition})
}

func (b *Backend) newConnection(session *frames.Session, password string, token string, path string, addSlash bool) (v3io.Container, string, error) {

	session = frames.InitSessionDefaults(session, b.framesConfig)
	containerName, newPath, err := v3ioutils.ProcessPaths(session, path, addSlash)
	if err != nil {
		return nil, "", err
	}

	session.Container = containerName
	container, err := v3ioutils.NewContainer(
		b.v3ioContext,
		session,
		password,
		token,
		b.logger)

	if err == nil && b.numWorkers == 0 {
		resp, err := container.GetClusterMDSync(&v3io.GetClusterMDInput{})
		if err != nil {
			return nil, "", fmt.Errorf("could not detrmine num vns in cluster")
		}
		getClusterMDOutput := resp.Output.(*v3io.GetClusterMDOutput)
		b.numWorkers = getClusterMDOutput.NumberOfVNs
		b.logger.Info("going to use ", b.numWorkers, "workers")
	}

	return container, newPath, err
}
