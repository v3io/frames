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
	"reflect"
	"time"
)

// SliceColumn is a column with slice data
type SliceColumn struct {
	name string
	data interface{}
	size int
}

// NewSliceColumn return a new SliceColumn
func NewSliceColumn(name string, data interface{}) (*SliceColumn, error) {
	var size int

	switch reflect.TypeOf(data) {
	case IntType:
		size = len(data.([]int))
	case FloatType:
		size = len(data.([]float64))
	case StringType:
		size = len(data.([]string))
	case TimeType:
		size = len(data.([]time.Time))
	case BoolType:
		size = len(data.([]bool))
	default:
		return nil, fmt.Errorf("unspported data type - %T", data)
	}

	sc := &SliceColumn{
		data: data,
		name: name,
		size: size,
	}

	return sc, nil
}

// Name returns the column name
func (sc *SliceColumn) Name() string {
	return sc.name
}

// Len returns the number of elements
func (sc *SliceColumn) Len() int {
	return sc.size
}

// DType returns the data type
func (sc *SliceColumn) DType() DType {
	return reflect.TypeOf(sc.data)
}

// Ints returns data as []int
func (sc *SliceColumn) Ints() ([]int, error) {
	typedCol, ok := sc.data.([]int)
	if !ok {
		return nil, fmt.Errorf("wrong type (type is %s)", sc.DType())
	}

	return typedCol, nil
}

// IntAt returns int value at index i (might panic)
func (sc *SliceColumn) IntAt(i int) int {
	typedCol, _ := sc.Ints()
	return typedCol[i]
}

// Floats returns data as []float64
func (sc *SliceColumn) Floats() ([]float64, error) {
	typedCol, ok := sc.data.([]float64)
	if !ok {
		return nil, fmt.Errorf("wrong type (type is %s)", sc.DType())
	}

	return typedCol, nil
}

// FloatAt returns float64 value at index i (might panic)
func (sc *SliceColumn) FloatAt(i int) float64 {
	typedCol, _ := sc.Floats()
	return typedCol[i]
}

// intToFloat convert an int col to a float64 col
func (sc *SliceColumn) intToFloat() {
	newData := make([]float64, sc.size)
	for i := 0; i < sc.size; i++ {
		newData[i] = float64(sc.IntAt(i))
	}
	sc.data = newData
}

// Strings returns data as []string
func (sc *SliceColumn) Strings() []string {
	typedCol, ok := sc.data.([]string)
	if ok {
		return typedCol
	}

	typedCol = make([]string, sc.Len())
	var getString func(i int) string

	dtype := sc.DType()
	switch dtype {
	case IntType:
		getString = func(i int) string {
			return fmt.Sprintf("%v", sc.IntAt(i))
		}
	case FloatType:
		getString = func(i int) string {
			return fmt.Sprintf("%v", sc.FloatAt(i))
		}
	case TimeType:
		getString = func(i int) string {
			return sc.TimeAt(i).Format(time.RFC3339Nano)
		}
	default:
		panic(fmt.Sprintf("unknown dtype - %v", dtype))
	}

	for i := 0; i < sc.Len(); i++ {
		typedCol[i] = getString(i)
	}

	return typedCol
}

// StringAt returns string value at index i (might panic)
func (sc *SliceColumn) StringAt(i int) string {
	return sc.Strings()[i]
}

// Times returns data as []time.Time
func (sc *SliceColumn) Times() ([]time.Time, error) {
	typedCol, ok := sc.data.([]time.Time)
	if !ok {
		return nil, fmt.Errorf("wrong type (type is %s)", sc.DType())
	}

	return typedCol, nil
}

// TimeAt returns time.Time value at index i (might panic)
func (sc *SliceColumn) TimeAt(i int) time.Time {
	typedCol, _ := sc.Times()
	return typedCol[i]
}

// Bools returns data as []bool
func (sc *SliceColumn) Bools() ([]bool, error) {
	typedCol, ok := sc.data.([]bool)
	if !ok {
		return nil, fmt.Errorf("wrong type (type is %s)", sc.DType())
	}

	return typedCol, nil
}

// BoolAt returns bool value at index i (might panic)
func (sc *SliceColumn) BoolAt(i int) bool {
	typedCol, _ := sc.Bools()
	return typedCol[i]
}

// Slice returns a Column with is slice of data
func (sc *SliceColumn) Slice(start int, end int) (Column, error) {
	if err := validateSlice(start, end, sc.Len()); err != nil {
		return nil, err
	}

	var slice interface{}
	switch sc.DType() {
	case IntType:
		typedCol, _ := sc.Ints()
		slice = typedCol[start:end]
	case FloatType:
		typedCol, _ := sc.Floats()
		slice = typedCol[start:end]
	case StringType:
		typedCol := sc.Strings()
		slice = typedCol[start:end]
	case TimeType:
		typedCol, _ := sc.Times()
		slice = typedCol[start:end]
	}

	return NewSliceColumn(sc.Name(), slice)
}

// Append appends a value
func (sc *SliceColumn) Append(value interface{}) error {
	switch sc.DType() {
	case IntType:
		typedVal, ok := value.(int)
		if !ok {
			// if the new value is a float, convert the col to floats
			floatVal, ok := value.(float64)
			if !ok {
				return fmt.Errorf("wrong type for %s(%s) - %T", sc.name, sc.DType(), value)
			}
			sc.intToFloat()
			typedCol, _ := sc.Floats()
			sc.data = append(typedCol, floatVal)
		} else {
			typedCol, _ := sc.Ints()
			sc.data = append(typedCol, typedVal)
		}

	case FloatType:
		typedVal, ok := value.(float64)
		if !ok {
			// if its an Int, convert to float
			intVal, ok := value.(int)
			if !ok {
				return fmt.Errorf("wrong type for %s(%s) - %T", sc.name, sc.DType(), value)
			}
			typedVal = float64(intVal)
		}

		typedCol, _ := sc.Floats()
		sc.data = append(typedCol, typedVal)
	case StringType:
		typedVal, ok := value.(string)
		if !ok {
			return fmt.Errorf("wrong type for %s(%s) - %T", sc.name, sc.DType(), value)
		}
		typedCol := sc.Strings()
		sc.data = append(typedCol, typedVal)
	case TimeType:
		typedVal, ok := value.(time.Time)
		if !ok {
			return fmt.Errorf("wrong type for %s(%s) - %T", sc.name, sc.DType(), value)
		}

		typedCol, _ := sc.Times()
		sc.data = append(typedCol, typedVal)
	default:
		return fmt.Errorf("unknown column type - %s for %s", sc.DType(), sc.name)
	}

	sc.size++
	return nil
}

// SliceColumnMessage is SliceColum over-the-wirte message
// We encode this way and not have single `Data interface{}` since msgpack
// then will packs []int to int8, int16 ...
type SliceColumnMessage struct {
	Name       string      `msgpack:"name"`
	DType      string      `msgpack:"dtype"`
	IntData    []int       `msgpack:"ints,omitempty"`
	FloatData  []float64   `msgpack:"floats,omitempty"`
	StringData []string    `msgpack:"strings,omitempty"`
	TimeData   []time.Time `msgpack:"times,omitempty"`
	BoolData   []bool      `msgpack:"bools,omitempty"`
	// We can't encode time in Python the way Go's msgpack works since
	// Python's msgpack won't accept -1 code
	NSTimeData []int `msgpack:"ns_times,omitempty"`
}

// Marshal marshals to native type
func (sc *SliceColumn) Marshal() (interface{}, error) {
	msg := &SliceColumnMessage{
		Name: sc.Name(),
	}

	switch sc.DType() {
	case IntType:
		msg.IntData = sc.data.([]int)
	case FloatType:
		msg.FloatData = sc.data.([]float64)
	case StringType:
		msg.StringData = sc.data.([]string)
	case TimeType:
		msg.TimeData = sc.data.([]time.Time)
	case BoolType:
		msg.BoolData = sc.data.([]bool)
	default:
		return nil, fmt.Errorf("can't marshal column of type %s", sc.DType())
	}

	msg.DType = sc.DType().String()
	return msg, nil
}
