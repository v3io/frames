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
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/v3io/frames/pb"
)

// DType is data type
type DType pb.DType

// Possible data types
var (
	BoolType   = DType(pb.DType_BOOLEAN)
	FloatType  = DType(pb.DType_FLOAT)
	IntType    = DType(pb.DType_INTEGER)
	StringType = DType(pb.DType_STRING)
	TimeType   = DType(pb.DType_TIME)
	NullType   = DType(pb.DType_NULL)
)

type SaveMode int

func (mode SaveMode) GetNginxModeName() string {
	switch mode {
	case ErrorIfTableExists:
		return ""
	case OverwriteTable:
		return ""
	case UpdateItem:
		return "CreateOrReplaceAttributes"
	case OverwriteItem:
		return "OverWriteAttributes"
	case CreateNewItemsOnly:
		return "CreateNewItemOnly"
	default:
		return ""
	}
}

func (mode SaveMode) String() string {
	switch mode {
	case ErrorIfTableExists:
		return "errorIfTableExists"
	case OverwriteTable:
		return "overwriteTable"
	case UpdateItem:
		return "updateItem"
	case OverwriteItem:
		return "overwriteItem"
	case CreateNewItemsOnly:
		return "createNewItemsOnly"
	default:
		return ""
	}
}

const (
	ErrorIfTableExists SaveMode = iota
	OverwriteTable
	UpdateItem
	OverwriteItem
	CreateNewItemsOnly
)

// Column is a data column
type Column interface {
	Len() int                                 // Number of elements
	Name() string                             // Column name
	DType() DType                             // Data type (e.g. IntType, FloatType ...)
	Ints() ([]int64, error)                   // Data as []int64
	IntAt(i int) (int64, error)               // Int value at index i
	Floats() ([]float64, error)               // Data as []float64
	FloatAt(i int) (float64, error)           // Float value at index i
	Strings() []string                        // Data as []string
	StringAt(i int) (string, error)           // String value at index i
	Times() ([]time.Time, error)              // Data as []time.Time
	TimeAt(i int) (time.Time, error)          // time.Time value at index i
	Bools() ([]bool, error)                   // Data as []bool
	BoolAt(i int) (bool, error)               // bool value at index i
	Slice(start int, end int) (Column, error) // Slice of data
	CopyWithName(newName string) Column       // Create a copy of the current column
}

// Frame is a collection of columns
type Frame interface {
	Labels() map[string]interface{}          // Label set
	Names() []string                         // Column names
	Indices() []Column                       // Index columns
	Len() int                                // Number of rows
	Column(name string) (Column, error)      // Column by name
	Slice(start int, end int) (Frame, error) // Slice of Frame
	IterRows(includeIndex bool) RowIterator  // Iterate over rows
	IsNull(index int, colName string) bool
	NullValuesMap() []*pb.NullValuesMap
}

// RowIterator is an iterator over frame rows
type RowIterator interface {
	Next() bool                      // Advance to next row
	Row() map[string]interface{}     // Row as map of name->value
	RowNum() int                     // Current row number
	Indices() map[string]interface{} // MultiIndex as name->value
	Err() error                      // Iteration error
}

// DataBackend is an interface for read/write on backend
type DataBackend interface {
	// TODO: Expose name, type, config ... ?
	Read(request *ReadRequest) (FrameIterator, error)
	Write(request *WriteRequest) (FrameAppender, error) // TODO: use Appender for write streaming
	Create(request *CreateRequest) error
	Delete(request *DeleteRequest) error
	Exec(request *ExecRequest) (Frame, error)
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
	Close()
}

// ReadRequest is a read/query request
type ReadRequest struct {
	Proto    *pb.ReadRequest
	Password SecretString
	Token    SecretString
}

func (readRequest ReadRequest) ToMap() map[string]string {

	reqMap := make(map[string]string, 10)

	if readRequest.Proto.Query != "" {
		reqMap["query"] = readRequest.Proto.Query
	}
	if len(readRequest.Proto.Columns) > 0 {
		reqMap["columns"] = strings.Join(readRequest.Proto.Columns, ",")
	}
	if readRequest.Proto.Filter != "" {
		reqMap["filter"] = readRequest.Proto.Filter
	}
	if readRequest.Proto.GroupBy != "" {
		reqMap["groupBy"] = readRequest.Proto.GroupBy
	}
	if readRequest.Proto.Limit != 0 {
		reqMap["limit"] = fmt.Sprintf("%v", readRequest.Proto.Limit)
	}
	if readRequest.Proto.MessageLimit != 0 {
		reqMap["messageLimit"] = fmt.Sprintf("%v", readRequest.Proto.MessageLimit)
	}
	if len(readRequest.Proto.ShardingKeys) > 0 {
		reqMap["shardingkeys"] = strings.Join(readRequest.Proto.ShardingKeys, ",")
	}
	if readRequest.Proto.SortKeyRangeStart != "" {
		reqMap["sortKeyRangeStart"] = readRequest.Proto.SortKeyRangeStart
	}
	if readRequest.Proto.SortKeyRangeEnd != "" {
		reqMap["sortKeyRangeEnd"] = readRequest.Proto.SortKeyRangeEnd
	}
	if readRequest.Proto.Start != "" {
		reqMap["start"] = readRequest.Proto.Start
	}
	if readRequest.Proto.End != "" {
		reqMap["end"] = readRequest.Proto.End
	}
	if readRequest.Proto.Step != "" {
		reqMap["step"] = readRequest.Proto.Step
	}
	if readRequest.Proto.Aggregators != "" {
		reqMap["aggregators"] = readRequest.Proto.Aggregators
	}
	if readRequest.Proto.AggregationWindow != "" {
		reqMap["aggregationWindow"] = readRequest.Proto.AggregationWindow
	}
	if readRequest.Proto.Seek != "" {
		reqMap["seek"] = readRequest.Proto.Seek
	}
	if readRequest.Proto.ShardId != "" {
		reqMap["shardID"] = readRequest.Proto.ShardId
	}
	if readRequest.Proto.Sequence != 0 {
		reqMap["sequence"] = fmt.Sprintf("%v", readRequest.Proto.Sequence)
	}

	return reqMap
}

// JoinStruct is a join structure
type JoinStruct = pb.JoinStruct

// WriteRequest is request for writing data
// TODO: Unite with probouf (currenly the protobuf message combines both this
// and a frame message)
type WriteRequest struct {
	Session  *Session
	Password SecretString
	Token    SecretString
	Backend  string // backend name
	Table    string // Table name (path)
	// Data message sent with the write request (in case of a stream multiple messages can follow)
	ImmidiateData Frame
	// Expression template, for update expressions generated from combining columns data with expression
	Expression string
	// Condition template, for update conditions generated from combining columns data with expression
	Condition string
	// Columns to partition the data by
	PartitionKeys []string
	// Will we get more message chunks (in a stream), if not we can complete
	HaveMore bool
	SaveMode SaveMode
}

func (writeRequest WriteRequest) ToMap() map[string]string {

	reqMap := make(map[string]string)

	if writeRequest.Expression != "" {
		reqMap["expression"] = writeRequest.Expression
	}
	if writeRequest.Condition != "" {
		reqMap["condition"] = writeRequest.Condition
	}
	if len(writeRequest.PartitionKeys) > 0 {
		reqMap["partitionKeys"] = strings.Join(writeRequest.PartitionKeys, ",")
	}

	reqMap["saveMode"] = writeRequest.SaveMode.String()

	return reqMap
}

// CreateRequest is a table creation request
type CreateRequest struct {
	Proto    *pb.CreateRequest
	Password SecretString
	Token    SecretString
}

func (createRequest CreateRequest) ToMap() map[string]string {

	reqMap := make(map[string]string)

	if createRequest.Proto.Rate != "" {
		reqMap["rate"] = createRequest.Proto.Rate
	}
	if createRequest.Proto.Aggregates != "" {
		reqMap["aggregates"] = createRequest.Proto.Aggregates
	}
	if createRequest.Proto.AggregationGranularity != "" {
		reqMap["aggregationGranularity"] = createRequest.Proto.AggregationGranularity
	}
	if createRequest.Proto.Shards != 0 {
		reqMap["shards"] = fmt.Sprintf("%v", createRequest.Proto.Shards)
	}
	if createRequest.Proto.RetentionHours != 0 {
		reqMap["retentionHours"] = fmt.Sprintf("%v", createRequest.Proto.RetentionHours)
	}

	return reqMap
}

// DeleteRequest is a deletion request
type DeleteRequest struct {
	Proto    *pb.DeleteRequest
	Password SecretString
	Token    SecretString
}

func (deleteRequest DeleteRequest) ToMap() map[string]string {

	reqMap := make(map[string]string)

	if deleteRequest.Proto.Filter != "" {
		reqMap["filter"] = deleteRequest.Proto.Filter
	}
	if deleteRequest.Proto.Start != "" {
		reqMap["start"] = deleteRequest.Proto.Start
	}
	if deleteRequest.Proto.End != "" {
		reqMap["end"] = deleteRequest.Proto.End
	}
	if len(deleteRequest.Proto.Metrics) > 0 {
		reqMap["metrics"] = strings.Join(deleteRequest.Proto.Metrics, ",")
	}

	return reqMap
}

// HistoryRequest is a history logs request
type HistoryRequest struct {
	Proto    *pb.HistoryRequest
	Password SecretString
	Token    SecretString
}

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

// ExecRequest is execution request
type ExecRequest struct {
	Proto    *pb.ExecRequest
	Password SecretString
	Token    SecretString
}

func (executeRequest ExecRequest) ToMap() map[string]string {

	reqMap := make(map[string]string)

	if executeRequest.Proto.Command != "" {
		reqMap["command"] = executeRequest.Proto.Command
	}
	if executeRequest.Proto.Expression != "" {
		reqMap["expression"] = executeRequest.Proto.Expression
	}
	if len(executeRequest.Proto.Args) > 0 {
		for k, v := range executeRequest.Proto.Args {
			reqMap[fmt.Sprintf("args_%v", k)] = v.String()
		}
	}

	return reqMap
}

// Hides a string such as a password from both plain and json logs.
type SecretString struct {
	s *string
}

func InitSecretString(s string) SecretString {
	return SecretString{s: &s}
}

func (s SecretString) Get() string {
	return *s.s
}

func SaveModeFromString(mode string) (SaveMode, error) {
	switch mode {
	case "", "errorIfTableExists": // this is the default save mode
		return ErrorIfTableExists, nil
	case "overwriteTable":
		return OverwriteTable, nil
	case "updateItem":
		return UpdateItem, nil
	case "overwriteItem":
		return OverwriteItem, nil
	case "createNewItemsOnly":
		return CreateNewItemsOnly, nil
	default:
		return -1, errors.Errorf("no save mode named '%v'", mode)
	}
}
