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
	"time"
)

// LogConfig is the logging configuration
type LogConfig struct {
	Level string `json:"level,omitempty"`
}

// Config is server configuration
type Config struct {
	Log          LogConfig `json:"log"`
	DefaultLimit int       `json:"limit,omitempty"`

	Backends []*BackendConfig `json:"backends,omitempty"`
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if len(c.Backends) == 0 {
		return fmt.Errorf("no backends")
	}

	names := make(map[string]bool)

	for i, backend := range c.Backends {
		if backend.Name == "" {
			return fmt.Errorf("backend %d missing name", i)
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

	// V3IO backend
	V3ioURL   string `json:"v3ioUrl,omitempty"`
	Container string `json:"container,omitempty"`
	Path      string `json:"path,omitempty"`
	Username  string `json:"username,omitempty"`
	Password  string `json:"password,omitempty"`
	Workers   int    `json:"workers,omitempty"` // Number of parallel V3IO worker routines

	// CSV backend
	RootDir string `json:"rootdir,omitempty"`
}

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

// ReadRequest is request for reading data
type ReadRequest struct {
	// name of the backend
	Backend string `json:"backend"`
	// schema (for describing unstructured/schemaless data)
	Schema *TableSchema `json:"schema"`
	// on the wire message format: json | msgpack | csv
	DataFormat string `json:"data_format"`
	// orgenized as rows (vs columns)
	RowLayout bool `json:"row_layout"`
	// support multi-index response
	MultiIndex bool `json:"multi_index"`

	// SQL query
	Query string `json:"query"`

	// Table name (path)
	Table string `json:"table"`
	// list of requested columns (or with aggregators  e.g. SUM(salary))
	Columns []string `json:"columns"`
	// query filter expression (Where)
	Filter string `json:"filter"`
	// group by expression
	GroupBy string `json:"group_by"` // TODO: []string? (as in SQL)
	// Enrichement with 2nd tables
	Join []*JoinStruct // struct with backend, table name, src col, dst col

	// max rows to return in total
	Limit int `json:"limit"`
	// maximum rows per message
	MaxInMessage int `json:"max_in_message"`
	// for future use, throttling
	Marker string `json:"marker"`

	// NoSQL specific fields
	// request specific DB segments (slices)
	Segments          []int    `json:"segments"`
	TotalSegment      int      `json:"total_segment"`
	ShardingKeys      []string `json:"sharding_keys"`
	SortKeyRangeStart string   `json:"sort_key_range_start"`
	SortKeyRangeEnd   string   `json:"sort_key_range_end"`

	// TSDB and Stream specific fields
	Start       string `json:"start"`
	End         string `json:"end"`
	StepRaw     string `json:"step"`        // time.Duration format
	Aggragators string `json:"aggragators"` // TSDB aggregation functions

	// Stream specific fields
	Seek     string `json:"seek"`
	ShardID  string `json:"shard"`
	Sequence int    `json:"sequence"`
}

// JoinStruct is join data
type JoinStruct struct {
	// TODO
}

// Step return the step
func (tr *ReadRequest) Step() (time.Duration, error) {
	return time.ParseDuration(tr.StepRaw)
}

// WriteRequest is request for writing data
type WriteRequest struct {
	Backend string `msgpack:"backend"` // backend name
	Table   string `msgpack:"table"`   // Table name (path)
	// Data message sent with the write request (in case of a stream multiple messages can follow)
	ImmidiateData Frame `msgpack:"intermidate,omitempty"`
	// Expression template, for update expressions generated from combining columns data with expression
	Expression string `msgpack:"expression,omitempty"`
	// Will we get more message chunks (in a stream), if not we can complete
	HaveMore bool `msgpack:"more"`
}

// CreateRequest is a table creation request
type CreateRequest struct {
	// name of the backend
	Backend string `json:"backend"`
	// Table name (path)
	Table string `json:"table"`
	// list of attributes used for creating the table
	Attributes map[string]interface{} `json:"attributes,omitempty"`
	// schema (for describing unstructured/schemaless data)
	Schema *TableSchema `json:"schema,omitempty"`
}

// DeleteRequest is a deletion request
type DeleteRequest struct {
	// name of the backend
	Backend string `json:"backend"`
	// Table name (path)
	Table string `json:"table"`
	// Filter string for selective delete
	Filter string `json:"filter,omitempty"`
	// Force delete
	Force bool `json:"force,omitempty"`
	// TSDB and Stream specific fields
	Start string `json:"start,omitempty"`
	End   string `json:"end,omitempty"`
}

// TableSchema is a table schema
type TableSchema struct {
	Type      string         `json:"type,omitempty"`
	Namespace string         `json:"namespace,omitempty"`
	Name      string         `json:"name,omitempty"`
	Doc       string         `json:"doc,omitempty"`
	Aliases   []string       `json:"aliases,omitempty"`
	Fields    []*SchemaField `json:"fields"`
	Key       *SchemaKey     `json:"key,omitempty"`
}

// SchemaField represents a schema field for Avro record.
type SchemaField struct {
	Name       string      `json:"name,omitempty"`
	Doc        string      `json:"doc,omitempty"`
	Default    interface{} `json:"default"`
	Type       string      `json:"type,omitempty"`
	Properties map[string]interface{}
}

// Property return a schema property
func (s *SchemaField) Property(key string) (interface{}, bool) {
	if s.Properties == nil {
		return nil, false
	}

	val, ok := s.Properties[key]
	return val, ok
}

// SchemaKey is a schema key
type SchemaKey struct {
	ShardingKey []string `json:"shardingKey,omitempty"`
	SortingKey  []string `json:"sortingKey,omitempty"`
}
