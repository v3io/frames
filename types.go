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
}

// DataBackend is an interface for read/write on backend
type DataBackend interface {
	ReadRequest(request *DataReadRequest) (MessageIterator, error)
	WriteRequest(request *DataWriteRequest) (MessageAppender, error) // TODO: use Appender for write streaming
}

// MessageIterator iterates over message
type MessageIterator interface {
	Next() bool
	Err() error
	At() *Message
}

// MessageAppender appends messages
type MessageAppender interface {
	Add(message *Message) error
	WaitForComplete(timeout time.Duration) error
}

// DataReadRequest is request for reading data
type DataReadRequest struct {
	// nosql | tsdb | sql | stream ..
	Type string `json:"type"`
	// json | msgpack | csv
	DataFormat string `json:"data_format"`
	// orgenized as rows (vs columns)
	RowLayout bool `json:"row_layout"`

	// TODO: Use SQL
	// Table name (path)
	Table string `json:"table"`
	// list of requested columns (or with aggregators  e.g. SUM(salary))
	Columns []string `json:"columns"`
	// query filter expression (Where)
	Filter string `json:"filter"`
	// group by expression
	GroupBy string `json:"group_by"`

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

// DataWriteRequest is request for writing data
type DataWriteRequest struct {
	// nosql | tsdb | sql | stream ..
	Type string
	// Table name (path)
	Table string
	// Data message sent with the write request (in case of a stream multiple messages can follow)
	ImmidiateData *Message
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
