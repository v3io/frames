// +build carrow

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
	"log"
	"time"

	"github.com/pkg/errors"

	"github.com/v3io/frames/carrow"
)

var (
	// HasArrow signals we have arrow
	HasArrow = true

	dtype2dtype = map[carrow.DType]DType{
		carrow.BoolType:      BoolType,
		carrow.Float64Type:   FloatType,
		carrow.Integer64Type: IntType,
		carrow.StringType:    StringType,
		carrow.TimestampType: TimeType,
	}
)

// NewArrowColumn returns a new arrow backed column
func NewArrowColumn(name string, data interface{}) (Column, error) {
	var (
		arr   *carrow.Array
		field *carrow.Field
		err   error
	)

	switch data.(type) {
	case []bool:
		field, err = carrow.NewField(name, carrow.BoolType)
		if err != nil {
			return nil, errors.Wrap(err, "bool: can't create field")
		}
		typedData := data.([]bool)
		bld := carrow.NewBoolArrayBuilder()
		if bld == nil {
			return nil, fmt.Errorf("bool: can't create builder")
		}
		for i, val := range typedData {
			if err := bld.Append(val); err != nil {
				return nil, errors.Wrapf(err, "bool: can't append %d:%#v", i, val)
			}
		}
		arr, err = bld.Finish()
		if err != nil {
			return nil, errors.Wrap(err, "bool: can't create array")
		}
	case []float64:
		field, err = carrow.NewField(name, carrow.Float64Type)
		if err != nil {
			return nil, errors.Wrap(err, "float64: can't create field")
		}
		typedData := data.([]float64)
		bld := carrow.NewFloat64ArrayBuilder()
		if bld == nil {
			return nil, fmt.Errorf("float64: can't create builder")
		}
		for i, val := range typedData {
			if err := bld.Append(val); err != nil {
				return nil, errors.Wrapf(err, "float64: can't append %d:%#v", i, val)
			}
		}
		arr, err = bld.Finish()
		if err != nil {
			return nil, errors.Wrap(err, "float64: can't create array")
		}
	case []int64:
		field, err = carrow.NewField(name, carrow.Integer64Type)
		if err != nil {
			return nil, errors.Wrap(err, "int64: can't create field")
		}
		typedData := data.([]int64)
		bld := carrow.NewInt64ArrayBuilder()
		if bld == nil {
			return nil, fmt.Errorf("int64: can't create builder")
		}
		for i, val := range typedData {
			if err := bld.Append(val); err != nil {
				return nil, errors.Wrapf(err, "int64: can't append %d:%#v", i, val)
			}
		}
		arr, err = bld.Finish()
		if err != nil {
			return nil, errors.Wrap(err, "int64: can't create array")
		}
	case []string:
		field, err = carrow.NewField(name, carrow.StringType)
		if err != nil {
			return nil, errors.Wrap(err, "string: can't create field")
		}
		typedData := data.([]string)
		bld := carrow.NewStringArrayBuilder()
		if bld == nil {
			return nil, fmt.Errorf("string: can't create builder")
		}
		for i, val := range typedData {
			if err := bld.Append(val); err != nil {
				return nil, errors.Wrapf(err, "string: can't append %d:%#v", i, val)
			}
		}
		arr, err = bld.Finish()
		if err != nil {
			return nil, errors.Wrap(err, "int64: can't create array")
		}
	case []time.Time:
		field, err = carrow.NewField(name, carrow.TimestampType)
		if err != nil {
			return nil, errors.Wrap(err, "timestamp: can't create field")
		}
		typedData := data.([]time.Time)
		bld := carrow.NewTimestampArrayBuilder()
		if bld == nil {
			return nil, fmt.Errorf("timestamp: can't create builder")
		}
		for i, val := range typedData {
			if err := bld.Append(val); err != nil {
				return nil, errors.Wrapf(err, "timestamp: can't append %d:%#v", i, val)
			}
		}
		arr, err = bld.Finish()
		if err != nil {
			return nil, errors.Wrap(err, "timestamp: can't create array")
		}
	default:
		return nil, fmt.Errorf("unkown data type - %T", data)
	}

	col, err := carrow.NewColumn(field, arr)
	if err != nil {
		return nil, errors.Wrap(err, "can't create column")
	}

	return &ArrowColumn{col}, nil
}

// ArrowColumnBuilder builds arrow based columns
type ArrowColumnBuilder struct {
	field        *carrow.Field
	boolBuilder  *carrow.BoolArrayBuilder
	floatBuilder *carrow.Float64ArrayBuilder
	intBuilder   *carrow.Int64ArrayBuilder
	strBuilder   *carrow.StringArrayBuilder
	tsBuilder    *carrow.TimestampArrayBuilder
}

// NewArrowColumnBuilder return new ArrowColumnBuilder
func NewArrowColumnBuilder(name string, dtype DType, size int) (*ArrowColumnBuilder, error) {
	var typ carrow.DType
	bld := &ArrowColumnBuilder{}
	switch dtype {
	case BoolType:
		typ = carrow.BoolType
		bld.boolBuilder = carrow.NewBoolArrayBuilder()
	case FloatType:
		typ = carrow.Float64Type
		bld.floatBuilder = carrow.NewFloat64ArrayBuilder()
	case IntType:
		typ = carrow.Integer64Type
		bld.intBuilder = carrow.NewInt64ArrayBuilder()
	case StringType:
		typ = carrow.StringType
		bld.strBuilder = carrow.NewStringArrayBuilder()
	case TimeType:
		typ = carrow.TimestampType
		bld.tsBuilder = carrow.NewTimestampArrayBuilder()
	default:
		return nil, fmt.Errorf("unsupported dtype - %s", dtype)
	}

	var err error
	bld.field, err = carrow.NewField(name, typ)
	if err != nil {
		return nil, err
	}

	return bld, nil
}

// Append appends a value
func (b *ArrowColumnBuilder) Append(value interface{}) error {
	switch b.field.DType() {
	case carrow.BoolType:
		bval, ok := value.(bool)
		if !ok {
			return typeError(value, "bool")
		}
		b.boolBuilder.Append(bval)
	case carrow.Float64Type:
		fval, err := asFloat64(value)
		if err != nil {
			return err
		}
		return b.floatBuilder.Append(fval)
	case carrow.Integer64Type:
		ival, err := asInt64(value)
		if err != nil {
			return err
		}
		return b.intBuilder.Append(ival)
	case carrow.StringType:
		sval, ok := value.(string)
		if !ok {
			return typeError(value, "string")
		}
		return b.strBuilder.Append(sval)
	case carrow.TimestampType:
		tval, ok := value.(time.Time)
		if !ok {
			return typeError(value, "time.Time")
		}
		return b.tsBuilder.Append(tval)
	}
	return fmt.Errorf("unsupported dtype - %s", b.field.DType())
}

// At return value at
func (b *ArrowColumnBuilder) At(index int) (interface{}, error) {
	return nil, fmt.Errorf("not supported")
}

// Set sets a value
func (b *ArrowColumnBuilder) Set(index int, value interface{}) error {
	return fmt.Errorf("not supported")
}

// Delete deletes a value
func (b *ArrowColumnBuilder) Delete(index int) error {
	return fmt.Errorf("not supported")
}

// Finish create the colum
func (b *ArrowColumnBuilder) Finish() Column {
	var (
		arr *carrow.Array
		err error
	)

	switch b.field.DType() {
	case carrow.BoolType:
		arr, err = b.boolBuilder.Finish()
		if err != nil {
			// TODO: Error in builder interface?
			log.Printf("bool build error: %s", err)
			return nil
		}
	case carrow.Float64Type:
		arr, err = b.floatBuilder.Finish()
		if err != nil {
			log.Printf("float build error: %s", err)
			return nil
		}
	case carrow.Integer64Type:
		arr, err = b.intBuilder.Finish()
		if err != nil {
			log.Printf("int64 build error: %s", err)
			return nil
		}
	case carrow.StringType:
		arr, err = b.strBuilder.Finish()
		if err != nil {
			log.Printf("string build error: %s", err)
			return nil
		}
	case carrow.TimestampType:
		arr, err = b.tsBuilder.Finish()
		if err != nil {
			log.Printf("time build error: %s", err)
			return nil // TODO: Error
		}
	default:
		log.Printf("unsupported dtype - %s", b.field.DType())
		return nil
	}

	col, err := carrow.NewColumn(b.field, arr)
	if err != nil {
		log.Printf("can't create column: %s", err)
		return nil
	}

	return &ArrowColumn{col}
}

func typeError(value interface{}, typ string) error {
	return fmt.Errorf("can't convert %v (%T) to %s", value, value, typ)
}

// ArrowColumn is an arrow backed column
type ArrowColumn struct {
	col *carrow.Column
}

// Len returns the lengh of the column
func (a *ArrowColumn) Len() int {
	return a.col.Len()
}

// Name returns the column name
func (a *ArrowColumn) Name() string {
	return a.col.Field().Name()
}

// DType returns the data type
func (a *ArrowColumn) DType() DType {
	return dtype2dtype[a.col.Field().DType()]
}

// Ints returns data as []int64
func (a *ArrowColumn) Ints() ([]int64, error) {
	// TODO: Find a more efficient way, also cache?
	data := make([]int64, a.Len())
	for i := 0; i < a.Len(); i++ {
		val, err := a.IntAt(i)
		if err != nil {
			return nil, err
		}
		data[i] = val
	}
	return data, nil
}

// IntAt returns int value at i
func (a *ArrowColumn) IntAt(i int) (int64, error) {
	return a.col.Int64At(i)
}

// Floats returns data as []float64
func (a *ArrowColumn) Floats() ([]float64, error) {
	// TODO: Find a more efficient way, also cache?
	data := make([]float64, a.Len())
	for i := 0; i < a.Len(); i++ {
		val, err := a.FloatAt(i)
		if err != nil {
			return nil, err
		}
		data[i] = val
	}
	return data, nil
}

// FloatAt returns float value at i
func (a *ArrowColumn) FloatAt(i int) (float64, error) {
	return a.col.Float64At(i)
}

// Strings return data as []string
func (a *ArrowColumn) Strings() []string {
	// TODO: Find a more efficient way, also cache?
	data := make([]string, a.Len())
	for i := 0; i < a.Len(); i++ {
		val, err := a.StringAt(i)
		if err != nil {
			return nil, err
		}
		data[i] = val
	}
	return data, nil
}

// StringAt returns string at i
func (a *ArrowColumn) StringAt(i int) (string, error) {
	return a.col.StringAt(i)
}

// Times return data as []time.Time
func (a *ArrowColumn) Times() ([]time.Time, error) {
	// TODO: Find a more efficient way, also cache?
	data := make([]time.Time, a.Len())
	for i := 0; i < a.Len(); i++ {
		val, err := a.TimeAt(i)
		if err != nil {
			return nil, err
		}
		data[i] = val
	}
	return data, nil
}

// TimeAt returns time value at i
func (a *ArrowColumn) TimeAt(i int) (time.Time, error) {
	return a.col.TimeAt(i)
}

// Bools returns data as []bool
func (a *ArrowColumn) Bools() ([]bool, error) {
	// TODO: Find a more efficient way, also cache?
	data := make([]bool, a.Len())
	for i := 0; i < a.Len(); i++ {
		val, err := a.TimeAt(i)
		if err != nil {
			return nil, err
		}
		data[i] = val
	}
	return data, nil
}

// BoolAt at returns bool value at i
func (a *ArrowColumn) BoolAt(i int) (bool, error) {
	return a.col.BoolAt(i)
}

// Slice returns a slice of the column
func (a *ArrowColumn) Slice(start int, end int) (Column, error) {
	length := end - start
	c, err := a.col.Slice(start, length)
	if err != nil {
		return nil, err
	}

	return &ArrowColumn{c}, nil
}

// ArrowFrame is an arrow backed frame
type ArrowFrame struct {
	table *carrow.Table
}

// ArrowFrameFromTable returns ArrowFrame from underlying carrow.Table
func ArrowFrameFromTable(table *carrow.Table) (*ArrowFrame, error) {
	if table == nil {
		return nil, errors.Errorf("nil table")
	}
	return &ArrowFrame{table}, nil
}

// Labels returns frame lables
func (a *ArrowFrame) Labels() map[string]interface{} {
	// TODO: table metadata
	return nil
}

// Names return the column names
func (a *ArrowFrame) Names() []string {
	n := a.table.NumCols()
	names := make([]string, n)
	for i := 0; i < n; i++ {
		col, err := a.table.ColByIndex(i)
		if err != nil {
			// TODO: Log or changes frames API
			return nil
		}
		names[i] = col.Field().Name()
	}

	return names
}

// Indices returns nil since arrow don't have indices
func (a *ArrowFrame) Indices() []Column {
	return nil
}

// Len returns number of rows
func (a *ArrowFrame) Len() int {
	return a.table.NumRows()
}

// Column returns a column by name
func (a *ArrowFrame) Column(name string) (Column, error) {
	col, err := a.table.ColByName(name)
	if err != nil {
		return nil, errors.Wrap(err, "col")
	}

	return &ArrowColumn{col}, nil
}

// Slice return a slice from the frame
func (a *ArrowFrame) Slice(start int, end int) (Frame, error) {
	length := end - start
	t, err := a.table.Slice(start, length)
	if err != nil {
		return nil, err
	}

	return &ArrowFrame{t}, nil
}

// IterRows returns an iterator over rows
func (a *ArrowFrame) IterRows(includeIndex bool) RowIterator {
	return newRowIterator(a, includeIndex)
}

// Table returns the underlying arrow table
func (a *ArrowFrame) Table() *carrow.Table {
	return a.table
}

// NewArrowFrame returns a new Frame
func NewArrowFrame(columns []Column) (Frame, error) {
	acols := make([]*carrow.Column, len(columns))
	for i, col := range columns {
		acol, ok := col.(*ArrowColumn)
		if !ok {
			return nil, fmt.Errorf("%d of %T - not *ArrowColumn", i, col)
		}
		acols[i] = acol.col
	}

	tbl, err := carrow.NewTableFromColumns(acols)
	if err != nil {
		return nil, errors.Wrap(err, "can't create arrow table")
	}

	return &ArrowFrame{tbl}, nil
}
