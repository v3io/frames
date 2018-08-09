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
	ReadRequest(request *DataRequest) (MessageIterator, error)
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
	// Column names
	Names []string `msgpack:"names,omitempty"`
	// Row data
	Rows [][]interface{} `msgpack:"rows,omitempty"`
}

type DataRequest struct {
	Type       string // nosql | tsdb | sql | stream ..
	DataFormat string // json | msgpack | csv
	Table      string
	Columns    []string
	Filter     string
	GroupBy    string

	Limit        int
	Marker       string
	Segment      int
	TotalSegment int

	ShardingKeys      []string
	SortKeyRangeStart string
	SortKeyRangeEnd   string

	StartTime time.Time
	EndTime   time.Time
	Step      int
}
