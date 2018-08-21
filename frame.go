package frames

import (
	"reflect"
	"time"
)

// Type is data type
type Type reflect.Type

// Possible data types
var (
	IntType    Type = reflect.TypeOf([]int{})
	FloatType  Type = reflect.TypeOf([]float64{})
	StringType Type = reflect.TypeOf([]string{})
	TimeType   Type = reflect.TypeOf([]time.Time{})
)

// Column is a data column
type Column interface {
	Len() int                                 // Number of elements
	Name() string                             // Column name
	DType() Type                              // Data type (e.g. IntType, FloatType ...)
	Ints() ([]int, error)                     // Data as []int
	Floats() ([]float64, error)               // Data as []float64
	Strings() ([]string, error)               // Data as []string
	Times() ([]time.Time, error)              // Data as []time.Time
	Slice(start int, end int) (Column, error) // Slice of data
}

// Frame is a collection of columns
type Frame interface {
	Columns() []string                       // Column names
	Len() int                                // Number of rows
	Column(name string) (Column, error)      // Column by name
	Slice(start int, end int) (Frame, error) // Slice of Frame
}
