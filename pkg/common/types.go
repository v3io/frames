package common

import (
	"github.com/nuclio/logger"
	"github.com/v3io/v3io-go-http"
	"time"
)

type DataContext struct {
	Container *v3io.Container
	Logger    logger.Logger
	Workers   int
}

type DataBackend interface {
	ReadRequest(request *DataReadRequest) (MessageIterator, error)
	WriteRequest(request *DataWriteRequest) error // TODO: use Appender for write streaming
}

type MessageIterator interface {
	Next() bool
	Err() error
	At() *Message
}

type Message struct {
	// List of labels
	Labels map[string]string `msgpack:"labels,omitempty"`

	// If we send in column orientations
	Columns map[string][]interface{} `msgpack:columns,omitempty"`

	// If we send in row orientations
	Rows []map[string]interface{} `msgpack:"rows,omitempty"`
}

type DataReadRequest struct {
	// nosql | tsdb | sql | stream ..
	Type string
	// json | msgpack | csv
	DataFormat string
	// orgenized as rows (vs columns)
	RowLayout bool
	// Table name (path)
	Table string
	// list of requested columns (or with aggregators  e.g. SUM(salary))
	Columns []string
	// query filter expression (Where)
	Filter string
	// group by expression
	GroupBy string

	// max rows to return in total
	Limit int
	// maximum rows per message
	MaxInMessage int
	// for future use, throttling
	Marker string

	// KV Specific fields
	// request specific DB segments (slices)
	Segments          []int
	TotalSegment      int
	ShardingKeys      []string
	SortKeyRangeStart string
	SortKeyRangeEnd   string

	// TSDB/Col specific fields
	StartTime time.Time
	EndTime   time.Time
	Step      string // duration string
}

type DataWriteRequest struct {
	// nosql | tsdb | sql | stream ..
	Type string
	// Table name (path)
	Table string

	// Name of column(s) used as index, TODO: if more than one separate with ","
	Key string

	// List of labels
	Labels map[string]string `msgpack:"labels,omitempty"`
	// If we send in column orientations
	Columns map[string][]interface{} `msgpack:columns,omitempty"`
	// If we send in row orientations
	Rows []map[string]interface{} `msgpack:"rows,omitempty"`
}

type V3ioConfig struct {
	// V3IO Connection details: Url, Data container, relative path for this dataset, credentials
	V3ioUrl   string `json:"v3ioUrl"`
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
