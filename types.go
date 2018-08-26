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

	"github.com/nuclio/logger"
	"github.com/v3io/v3io-go-http"
)

// DataContext is context for data
type DataContext struct {
	Container *v3io.Container
	Logger    logger.Logger
	Workers   int
	Extra     interface{}
}

// DataBackend is an interface for read/write on backend
type DataBackend interface {
	Read(request *ReadRequest) (FrameIterator, error)
	Write(request *WriteRequest) (FrameAppender, error) // TODO: use Appender for write streaming
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
	// nosql | tsdb | sql | stream | csv ..
	Type string `json:"type"`
	// json | msgpack | csv
	DataFormat string `json:"data_format"`
	// orgenized as rows (vs columns)
	RowLayout bool `json:"row_layout"`

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

	// max rows to return in total
	Limit int `json:"limit"`
	// maximum rows per message
	MaxInMessage int `json:"max_in_message"`
	// for future use, throttling
	Marker string `json:"marker"`

	Extra interface{} `json:"extra"`
}

// KVRead is read specific fields
type KVRead struct {
	// request specific DB segments (slices)
	Segments          []int    `json:"segments"`
	TotalSegment      int      `json:"total_segment"`
	ShardingKeys      []string `json:"sharding_keys"`
	SortKeyRangeStart string   `json:"sort_key_range_start"`
	SortKeyRangeEnd   string   `json:"sort_key_range_end"`
}

// TSDBRead is TSDB specific fields
type TSDBRead struct {
	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"end_time"`
	StepRaw   string    `json:"step"` // time.Duration format
}

// Step return the step
func (tr *TSDBRead) Step() (time.Duration, error) {
	return time.ParseDuration(tr.StepRaw)
}

// WriteRequest is request for writing data
type WriteRequest struct {
	// nosql | tsdb | sql | stream ..
	Type string
	// Table name (path)
	Table string
	// Data message sent with the write request (in case of a stream multiple messages can follow)
	ImmidiateData Frame
	// Will we get more message chunks (in a stream), if not we can complete
	HaveMore bool
}

// V3ioConfig is v3io configuration
type V3ioConfig struct {
	// V3IO Connection details: Url, Data container, relative path for this dataset, credentials
	V3ioURL   string `json:"v3ioUrl"`
	Container string `json:"container"`
	Path      string `json:"path"`
	Username  string `json:"username"`
	Password  string `json:"password"`

	// Set logging level: debug | info | warn | error (info by default)
	Verbose string `json:"verbose,omitempty"`
	// Number of parallel V3IO worker routines
	Workers      int `json:"workers"`
	DefaultLimit int `json:"limit,omitempty"`
}
