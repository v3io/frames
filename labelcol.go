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

// LabelColumn is a column with same value
type LabelColumn struct {
	name  string
	value interface{}
	size  int
}

// NewLabelColumn returns new label column
func NewLabelColumn(name string, value interface{}, size int) (*LabelColumn, error) {
	switch value.(type) {
	case int64, float64, string, time.Time, bool:
		// OK
	case int8:
		value = int64(value.(int8))
	case int16:
		value = int64(value.(int16))
	case int32:
		value = int64(value.(int32))
	case int:
		value = int64(value.(int))
	case float32:
		value = float64(value.(float32))
	default:
		return nil, fmt.Errorf("LabelColumn unspported type - %T", value)
	}

	lc := &LabelColumn{
		name:  name,
		value: value,
		size:  size,
	}

	return lc, nil
}

// Name returns the column name
func (lc *LabelColumn) Name() string {
	return lc.name
}

// Len returns the number of elements
func (lc *LabelColumn) Len() int {
	return lc.size
}

// DType returns the data type
func (lc *LabelColumn) DType() DType {
	switch lc.value.(type) {
	case string:
		return StringType
	case int64:
		return IntType
	case float64:
		return FloatType
	case time.Time:
		return TimeType
	case bool:
		return BoolType
	}

	return nil
}

// Ints returns data as []int
func (lc *LabelColumn) Ints() ([]int64, error) {
	typedVal, ok := lc.value.(int64)
	if !ok {
		return nil, fmt.Errorf("wrong type (type is %s)", lc.DType())
	}

	data := make([]int64, lc.Len())
	for i := range data {
		data[i] = typedVal
	}

	return data, nil
}

// IntAt returns int value at index i
func (lc *LabelColumn) IntAt(i int) (int64, error) {
	return intAt(lc, i)
}

// Floats returns data as []float64
func (lc *LabelColumn) Floats() ([]float64, error) {
	typedVal, ok := lc.value.(float64)
	if !ok {
		return nil, fmt.Errorf("wrong type (type is %s)", lc.DType())
	}

	data := make([]float64, lc.Len())
	for i := range data {
		data[i] = typedVal
	}

	return data, nil
}

// FloatAt returns float64 value at index i
func (lc *LabelColumn) FloatAt(i int) (float64, error) {
	return floatAt(lc, i)
}

// Strings returns data as []string
func (lc *LabelColumn) Strings() []string {
	var value string

	switch lc.value.(type) {
	case string:
		value = lc.value.(string)
	default:
		value = fmt.Sprintf("%v", lc.value)
	}

	data := make([]string, lc.Len())
	for i := range data {
		data[i] = value
	}

	return data
}

// StringAt returns string value at index i
func (lc *LabelColumn) StringAt(i int) (string, error) {
	return stringAt(lc, i)
}

// Times returns data as []time.Time
func (lc *LabelColumn) Times() ([]time.Time, error) {
	typedVal, ok := lc.value.(time.Time)
	if !ok {
		return nil, fmt.Errorf("wrong type (type is %s)", lc.DType())
	}

	data := make([]time.Time, lc.Len())
	for i := range data {
		data[i] = typedVal
	}

	return data, nil
}

// TimeAt returns time.Time value at index i
func (lc *LabelColumn) TimeAt(i int) (time.Time, error) {
	return timeAt(lc, i)
}

// Bools returns data as []bool
func (lc *LabelColumn) Bools() ([]bool, error) {
	typedVal, ok := lc.value.(bool)
	if !ok {
		return nil, fmt.Errorf("wrong type (type is %s)", lc.DType())
	}

	data := make([]bool, lc.Len())
	for i := range data {
		data[i] = typedVal
	}

	return data, nil
}

// BoolAt returns bool value at index i
func (lc *LabelColumn) BoolAt(i int) (bool, error) {
	return boolAt(lc, i)
}

// Slice returns a Column with is slice of data
func (lc *LabelColumn) Slice(start int, end int) (Column, error) {
	if err := validateSlice(start, end, lc.Len()); err != nil {
		return nil, err
	}

	return NewLabelColumn(lc.name, lc.value, end-start)
}

// Append appends a value
func (lc *LabelColumn) Append(value interface{}) error {
	if value != lc.value {
		return fmt.Errorf("value mismatch %v != %v", value, lc.value)
	}
	lc.size++
	return nil
}

// LabelColumnMessage is over-the-wire LabelColumn message
type LabelColumnMessage struct {
	Value interface{} `msgpack:"value"`
	Size  int         `msgpack:"size"`
	Name  string      `msgpack:"name"`
	DType string      `msgpack:"dtype"`
}

// Marshal marshals to native type
func (lc *LabelColumn) Marshal() (interface{}, error) {
	return &LabelColumnMessage{
		Value: lc.value,
		Size:  lc.Len(),
		Name:  lc.Name(),
		DType: lc.DType().String(),
	}, nil
}
