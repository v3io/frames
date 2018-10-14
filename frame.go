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
	"reflect"
	"time"
)

// DType is data type
type DType reflect.Type

// Possible data types
var (
	IntType    DType = reflect.TypeOf([]int{})
	FloatType  DType = reflect.TypeOf([]float64{})
	StringType DType = reflect.TypeOf([]string{})
	TimeType   DType = reflect.TypeOf([]time.Time{})
)

// Column is a data column
type Column interface {
	Len() int                                 // Number of elements
	Name() string                             // Column name
	DType() DType                             // Data type (e.g. IntType, FloatType ...)
	Ints() ([]int, error)                     // Data as []int
	IntAt(i int) int                          // Int value at index i (might panic)
	Floats() ([]float64, error)               // Data as []float64
	FloatAt(i int) float64                    // Float value at index i (might panic)
	Strings() []string                        // Data as []string
	StringAt(i int) string                    // String value at index i (might panic)
	Times() ([]time.Time, error)              // Data as []time.Time
	TimeAt(i int) time.Time                   // time.Time value at index i (might panic)
	Slice(start int, end int) (Column, error) // Slice of data
	Append(value interface{}) error           // Append a value
}

// Frame is a collection of columns
type Frame interface {
	Labels() map[string]interface{}          // Label set
	Names() []string                         // Column names
	Indices() []Column                       // Index columns
	Len() int                                // Number of rows
	Column(name string) (Column, error)      // Column by name
	Slice(start int, end int) (Frame, error) // Slice of Frame
}
