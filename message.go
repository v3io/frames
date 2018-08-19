package frames

import (
	"fmt"
	"time"
)

// Message sent over the wire with multiple columns and data points
type Message struct {
	// Name of column(s) used as index, TODO: if more than one separate with ","
	IndexCol string
	// List of labels
	Labels map[string]string `msgpack:"labels,omitempty"`
	// Columns of data
	Columns map[string]interface{} `msgpack:"columns,omitempty"`
	// For Writes, Will we get more message chunks (in a stream), if not we can complete
	HaveMore bool
}

// Type is data type
type Type int

// Possible data types
const (
	UnknownType Type = iota
	IntType
	FloatType
	StringType
	TimeType
)

func (t Type) String() string {
	switch t {
	case IntType:
		return "int"
	case FloatType:
		return "float"
	case StringType:
		return "string"
	case TimeType:
		return "time"
	}

	return fmt.Sprintf("Unknown Type - %d", t)
}

// ColumnType returns the column type
func (m *Message) ColumnType(name string) (Type, error) {
	col, ok := m.Columns[name]
	if !ok {
		return UnknownType, fmt.Errorf("column %q not found", name)
	}

	switch col.(type) {
	case []int:
		return IntType, nil
	case []float64:
		return FloatType, nil
	case []string:
		return StringType, nil
	case []time.Time:
		return TimeType, nil
	}

	return UnknownType, fmt.Errorf("Unknown column type - %T", col)
}

// Ints return column as []int
func (m *Message) Ints(name string) ([]int, error) {
	col, ok := m.Columns[name]
	if !ok {
		return nil, fmt.Errorf("column %q not found", name)
	}

	icol, ok := col.([]int)
	if !ok {
		return nil, fmt.Errorf("column %q is not []int (type %T)", name, col)
	}

	return icol, nil
}

// Floats return column as []float64
func (m *Message) Floats(name string) ([]float64, error) {
	col, ok := m.Columns[name]
	if !ok {
		return nil, fmt.Errorf("column %q not found", name)
	}

	fcol, ok := col.([]float64)
	if !ok {
		return nil, fmt.Errorf("column %q is not []float64 (type %T)", name, col)
	}

	return fcol, nil
}

// Strings return column as []string
func (m *Message) Strings(name string) ([]string, error) {
	col, ok := m.Columns[name]
	if !ok {
		return nil, fmt.Errorf("column %q not found", name)
	}

	scol, ok := col.([]string)
	if !ok {
		return nil, fmt.Errorf("column %q is not []string (type %T)", name, col)
	}

	return scol, nil
}

// Times return column as []time.Time
func (m *Message) Times(name string) ([]time.Time, error) {
	col, ok := m.Columns[name]
	if !ok {
		return nil, fmt.Errorf("column %q not found", name)
	}

	tcol, ok := col.([]time.Time)
	if !ok {
		return nil, fmt.Errorf("column %q is not []time.Time (type %T)", name, col)
	}

	return tcol, nil
}
