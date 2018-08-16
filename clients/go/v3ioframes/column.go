package v3ioframes

import (
	"fmt"
	"time"
)

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

func (dt Type) String() string {
	switch dt {
	case IntType:
		return "int"
	case FloatType:
		return "float"
	case StringType:
		return "string"
	case TimeType:
		return "time"
	}

	return fmt.Sprintf("Unknown Type - %d", dt)
}

// Column is column of data
type Column struct {
	Type Type
	Name string
	data interface{}
}

// Ints return []int from column
func (s *Column) Ints() ([]int, error) {
	if s.Type != IntType {
		return nil, fmt.Errorf("Type mismatch - %s", s.Type)
	}

	return s.data.([]int), nil
}

// Floats return []float64 from column
func (s *Column) Floats() ([]float64, error) {
	if s.Type != FloatType {
		return nil, fmt.Errorf("Type mismatch - %s", s.Type)
	}

	return s.data.([]float64), nil
}

// Strings return []string from column
func (s *Column) Strings() ([]string, error) {
	if s.Type != StringType {
		return nil, fmt.Errorf("Type mismatch - %s", s.Type)
	}

	return s.data.([]string), nil
}

// Times return []time.Time from column
func (s *Column) Times() ([]time.Time, error) {
	if s.Type != TimeType {
		return nil, fmt.Errorf("Type mismatch - %s", s.Type)
	}

	return s.data.([]time.Time), nil
}
