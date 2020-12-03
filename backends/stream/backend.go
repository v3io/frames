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

package stream

import (
	"fmt"
	"strings"

	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/v3io/frames"
	"github.com/v3io/frames/backends"
	"github.com/v3io/frames/v3ioutils"
	v3io "github.com/v3io/v3io-go/pkg/dataplane"
)

// Backend is a streaming backend
type Backend struct {
	backendConfig *frames.BackendConfig
	framesConfig  *frames.Config
	logger        logger.Logger
	v3ioContext   v3io.Context
}

// NewBackend returns a new platform ("v3io") streaming backend
func NewBackend(logger logger.Logger, v3ioContext v3io.Context, cfg *frames.BackendConfig, framesConfig *frames.Config) (frames.DataBackend, error) {

	newBackend := Backend{
		logger:        logger.GetChild("stream"),
		backendConfig: cfg,
		framesConfig:  framesConfig,
		v3ioContext:   v3ioContext,
	}

	return &newBackend, nil
}

// Create creates a stream
func (b *Backend) Create(request *frames.CreateRequest) error {

	// TODO: Check whether Stream exists; if it already has the desired params can silently ignore, may need a -silent flag

	shards := int64(1)

	if request.Proto.Shards != 0 {
		shards = request.Proto.Shards
		if shards < 1 {
			return errors.Errorf("'shards' must be a positive integer (got %v)", shards)
		}
	}

	retention := int64(24)
	if request.Proto.RetentionHours != 0 {
		retention = request.Proto.RetentionHours
		if retention < 1 {
			return errors.Errorf("'retention_hours' must be a positive integer (got %v)", retention)
		}
	}

	container, path, err := b.newConnection(request.Proto.Session, request.Password.Get(), request.Token.Get(), request.Proto.Table, true)
	if err != nil {
		return err
	}

	err = container.CreateStreamSync(&v3io.CreateStreamInput{
		Path: path, ShardCount: int(shards), RetentionPeriodHours: int(retention)})
	if err != nil {
		b.logger.ErrorWith("CreateStream failed", "path", path, "err", err)
	}

	return nil
}

// Delete deletes a table or part of it
func (b *Backend) Delete(request *frames.DeleteRequest) error {

	err := backends.ValidateRequest("stream", request.Proto, nil)
	if err != nil {
		return err
	}

	container, path, err := b.newConnection(request.Proto.Session, request.Password.Get(), request.Token.Get(), request.Proto.Table, true)
	if err != nil {
		return err
	}

	err = container.DeleteStreamSync(&v3io.DeleteStreamInput{Path: path})
	if err != nil {
		b.logger.ErrorWith("DeleteStream failed", "path", path, "err", err)
	}

	return nil
}

// Exec executes a command
func (b *Backend) Exec(request *frames.ExecRequest) (frames.Frame, error) {
	cmd := strings.TrimSpace(strings.ToLower(request.Proto.Command))
	switch cmd {
	case "put":
		return nil, b.put(request)
	}
	return nil, fmt.Errorf("streaming backend doesn't support execute command '%s'", cmd)
}

func (b *Backend) put(request *frames.ExecRequest) error {

	varData, hasData := request.Proto.Args["data"]
	if !hasData || request.Proto.Table == "" {
		return fmt.Errorf("missing a required parameter - 'table' (stream name) and/or 'data' argument (record data)")
	}
	data := varData.GetSval()

	clientInfo := ""
	if val, ok := request.Proto.Args["client_info"]; ok {
		clientInfo = val.GetSval()
	}

	partitionKey := ""
	if val, ok := request.Proto.Args["partition_key"]; ok {
		partitionKey = val.GetSval()
	}

	container, path, err := b.newConnection(request.Proto.Session, request.Password.Get(), request.Token.Get(), request.Proto.Table, true)
	if err != nil {
		return err
	}

	b.logger.DebugWith("put record", "path", path, "len", len(data), "client", clientInfo, "partition", partitionKey)
	records := []*v3io.StreamRecord{{
		Data: []byte(data), ClientInfo: []byte(clientInfo), PartitionKey: partitionKey,
	}}
	_, err = container.PutRecordsSync(&v3io.PutRecordsInput{
		Path:    path,
		Records: records,
	})

	return err
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

	return container, newPath, err
}

func init() {
	if err := backends.Register("stream", NewBackend); err != nil {
		panic(err)
	}
}
