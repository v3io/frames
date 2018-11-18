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
	"time"

	"github.com/v3io/frames/pb"
)

// DataBackend is an interface for read/write on backend
type DataBackend interface {
	// TODO: Expose name, type, config ... ?
	Read(request *ReadRequest) (FrameIterator, error)
	Write(request *WriteRequest) (FrameAppender, error) // TODO: use Appender for write streaming
	Create(request *CreateRequest) error
	Delete(request *DeleteRequest) error
}

// FrameIterator iterates over frames
type FrameIterator interface {
	Next() bool
	Err() error
	At() Frame
}

// FrameAppender appends frames
type FrameAppender interface {
	Add(frame Frame) error
	WaitForComplete(timeout time.Duration) error
}

// ReadRequest is a read/query request
type ReadRequest = pb.ReadRequest

// JoinStruct is a join structure
type JoinStruct = pb.JoinStruct

// WriteRequest is request for writing data
// TODO: Unite with probouf (currenly the protobuf message combines both this
// and a frame message)
type WriteRequest struct {
	Session *Session `msgpack:"session"`
	Backend string   `msgpack:"backend"` // backend name
	Table   string   `msgpack:"table"`   // Table name (path)
	// Data message sent with the write request (in case of a stream multiple messages can follow)
	ImmidiateData Frame `msgpack:"intermidate,omitempty"`
	// Expression template, for update expressions generated from combining columns data with expression
	Expression string `msgpack:"expression,omitempty"`
	// Will we get more message chunks (in a stream), if not we can complete
	HaveMore bool `msgpack:"more"`
}

// CreateRequest is a table creation request
type CreateRequest = pb.CreateRequest

// DeleteRequest is a deletion request
type DeleteRequest = pb.DeleteRequest

// TableSchema is a table schema
type TableSchema = pb.TableSchema

// SchemaField represents a schema field for Avro record.
type SchemaField = pb.SchemaField

// SchemaKey is a schema key
type SchemaKey = pb.SchemaKey

// Session information
type Session = pb.Session

// Shortcut for fail/ignore
const (
	IgnoreError = pb.ErrorOptions_IGNORE
	FailOnError = pb.ErrorOptions_FAIL
)
