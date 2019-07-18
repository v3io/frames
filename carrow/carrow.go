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

package carrow

import (
	"fmt"
	"time"
	"unsafe"
	// "runtime"  // until we figure out crashes
)

// Make sure pkg-config knows where to find arrow & plasma, you can set
//	export PKG_CONFIG_PATH=/opt/miniconda/lib/pkgconfig

/*
#cgo pkg-config: arrow plasma
#cgo LDFLAGS: -lcarrow -L.

#include "carrow.h"
#include <stdlib.h>
*/
import "C"

// DType is a data type
type DType C.int

// Supported data types
var (
	BoolType      = DType(C.BOOL_DTYPE)
	Float64Type   = DType(C.FLOAT64_DTYPE)
	Integer64Type = DType(C.INTEGER64_DTYPE)
	StringType    = DType(C.STRING_DTYPE)
	TimestampType = DType(C.TIMESTAMP_DTYPE)
)

func (dt DType) String() string {
	switch dt {
	case BoolType:
		return "bool"
	case Float64Type:
		return "float64"
	case Integer64Type:
		return "int64"
	case StringType:
		return "string"
	case TimestampType:
		return "timestamp"
	}

	return "<unknown>"
}

// Field is a field description
type Field struct {
	ptr unsafe.Pointer
}

// NewField returns a new Field
func NewField(name string, dtype DType) (*Field, error) {
	cName := C.CString(name)
	defer func() { C.free(unsafe.Pointer(cName)) }()

	ptr := C.field_new(cName, C.int(dtype))
	if ptr == nil {
		return nil, fmt.Errorf("can't create field from %s: %s", name, dtype)
	}

	field := &Field{ptr}
	/*
		runtime.SetFinalizer(field, func(f *Field) {
			C.field_free(f.ptr)
		})
	*/
	return field, nil
}

// FieldList is a warpper around std::shared_ptr<arrow::Field>
type FieldList struct {
	ptr unsafe.Pointer
}

// NewFieldList returns a new Field List
func NewFieldList() (*FieldList, error) {
	ptr := C.fields_new()
	if ptr == nil {
		return nil, fmt.Errorf("can't create fields list")
	}

	fieldList := &FieldList{ptr}
	/*
		runtime.SetFinalizer(fieldList, func(f *FieldList) {
			C.fields_free(f.ptr)
		})
	*/
	return fieldList, nil
}

// Name returns the field name
func (f *Field) Name() string {
	return C.GoString(C.field_name(f.ptr))
}

// DType returns the field data type
func (f *Field) DType() DType {
	return DType(C.field_dtype(f.ptr))
}

// Schema is table schema
type Schema struct {
	ptr unsafe.Pointer
}

// NewSchema creates a new schema
func NewSchema(fields []*Field) (*Schema, error) {
	fieldsList, err := NewFieldList()
	if err != nil {
		return nil, fmt.Errorf("can't create schema,failed creating fields list")
	}
	cf := fieldsList.ptr

	for _, f := range fields {
		C.fields_append(cf, f.ptr)
	}
	ptr := C.schema_new(cf)
	if ptr == nil {
		return nil, fmt.Errorf("can't create schema")
	}
	schema := &Schema{ptr}
	/*
		runtime.SetFinalizer(schema, func(s *Schema) {
			C.schema_free(schema.ptr)
		})
	*/

	return schema, nil
}

type builder struct {
	ptr unsafe.Pointer
}

type result struct {
	r C.result_t
}

func (r result) Err() error {
	if r.r.err == nil {
		return nil
	}

	err := fmt.Errorf(C.GoString(r.r.err))
	C.free(unsafe.Pointer(r.r.err))
	return err
}

func (r result) Ptr() unsafe.Pointer {
	return unsafe.Pointer(C.result_ptr(r.r))
}

func (r result) Str() string {
	cp := C.result_cp(r.r)
	if cp == nil {
		return ""
	}

	return C.GoString(cp)
}

func (r result) FreeStr() string {
	cp := C.result_cp(r.r)
	if cp == nil {
		return ""
	}

	s := C.GoString(cp)
	C.free(unsafe.Pointer(cp))
	return s
}

func (r result) Int() int64 {
	return int64(C.result_i(r.r))
}

func (r result) Float() float64 {
	return float64(C.result_f(r.r))
}

// BoolArrayBuilder used for building bool Arrays
type BoolArrayBuilder struct {
	builder
}

// NewBoolArrayBuilder returns a new BoolArrayBuilder
func NewBoolArrayBuilder() *BoolArrayBuilder {
	r := result{C.array_builder_new(C.int(BoolType))}
	// TODO: Do we want to change the New function to return *type, error?
	if r.Err() != nil {
		return nil
	}
	return &BoolArrayBuilder{builder{r.Ptr()}}
}

// Finish returns array from builder
// You can't use the builder after calling Finish
func (b *builder) Finish() (*Array, error) {
	r := &result{C.array_builder_finish(b.ptr)}
	if err := r.Err(); err != nil {
		return nil, err
	}

	return &Array{r.Ptr()}, nil
}

// Append appends a bool
func (b *BoolArrayBuilder) Append(val bool) error {
	var ival int
	if val {
		ival = 1
	}
	r := result{C.array_builder_append_bool(b.ptr, C.int(ival))}
	return r.Err()
}

// Float64ArrayBuilder used for building float Arrays
type Float64ArrayBuilder struct {
	builder
}

// NewFloat64ArrayBuilder returns a new Float64ArrayBuilder
func NewFloat64ArrayBuilder() *Float64ArrayBuilder {
	r := result{C.array_builder_new(C.int(Float64Type))}
	if r.Err() != nil {
		return nil
	}
	return &Float64ArrayBuilder{builder{r.Ptr()}}
}

// Append appends an integer
func (b *Float64ArrayBuilder) Append(val float64) error {
	r := result{C.array_builder_append_float(b.ptr, C.double(val))}
	return r.Err()
}

// Int64ArrayBuilder used for building integer Arrays
type Int64ArrayBuilder struct {
	builder
}

// NewInt64ArrayBuilder returns a new Int64ArrayBuilder
func NewInt64ArrayBuilder() *Int64ArrayBuilder {
	r := result{C.array_builder_new(C.int(Integer64Type))}
	if r.Err() != nil {
		return nil
	}
	return &Int64ArrayBuilder{builder{r.Ptr()}}
}

// Append appends an integer
func (b *Int64ArrayBuilder) Append(val int64) error {
	r := result{C.array_builder_append_int(b.ptr, C.long(val))}
	return r.Err()
}

// StringArrayBuilder used for building string Arrays
type StringArrayBuilder struct {
	builder
}

// NewStringArrayBuilder returns a new StringArrayBuilder
func NewStringArrayBuilder() *StringArrayBuilder {
	r := result{C.array_builder_new(C.int(StringType))}
	if r.Err() != nil {
		return nil
	}
	return &StringArrayBuilder{builder{r.Ptr()}}
}

// Append appends a string
func (b *StringArrayBuilder) Append(val string) error {
	cStr := C.CString(val)
	defer C.free(unsafe.Pointer(cStr))
	length := C.ulong(len(val)) // len is in bytes
	r := result{C.array_builder_append_string(b.ptr, cStr, length)}
	return r.Err()
}

// TimestampArrayBuilder used for building bool Arrays
type TimestampArrayBuilder struct {
	builder
}

// NewTimestampArrayBuilder returns a new TimestampArrayBuilder
func NewTimestampArrayBuilder() *TimestampArrayBuilder {
	r := result{C.array_builder_new(C.int(TimestampType))}
	if r.Err() != nil {
		return nil
	}
	return &TimestampArrayBuilder{builder{r.Ptr()}}
}

// Append appends a timestamp
func (b *TimestampArrayBuilder) Append(val time.Time) error {
	r := result{C.array_builder_append_timestamp(b.ptr, C.long(val.UnixNano()))}
	return r.Err()
}

// Array is arrow array
type Array struct {
	ptr unsafe.Pointer
}

// Length returns the length of the array
func (a *Array) Length() int {
	i := C.array_length(a.ptr)
	return int(i)
}

// Column is an arrow colum
type Column struct {
	ptr unsafe.Pointer
}

// DType returns the Column data type
func (c *Column) DType() DType {
	return DType(C.column_dtype(c.ptr))
}

// NewColumn returns a new column
func NewColumn(field *Field, arr *Array) (*Column, error) {
	if field == nil || arr == nil {
		return nil, fmt.Errorf("nil pointer")
	}

	ptr := C.column_new(field.ptr, arr.ptr)
	c := &Column{ptr}
	if c.DType() != field.DType() {
		return nil, fmt.Errorf("column type doesn't match Field type")
	}

	return c, nil
}

// Field returns the column field
func (c *Column) Field() *Field {
	ptr := C.column_field(c.ptr)
	return &Field{ptr}
}

// Len return the column length (-1 on error)
func (c *Column) Len() int {
	return int(C.column_len(c.ptr))
}

// BoolAt returns bool value at i
func (c *Column) BoolAt(i int) (bool, error) {
	r := result{C.column_bool_at(c.ptr, C.longlong(i))}
	if err := r.Err(); err != nil {
		return false, err
	}

	return r.Int() != 0, nil
}

// Int64At returns int64 value at i
func (c *Column) Int64At(i int) (int64, error) {
	r := result{C.column_int_at(c.ptr, C.longlong(i))}
	if err := r.Err(); err != nil {
		return 0, err
	}

	return r.Int(), nil
}

// Float64At returns float64 value at i
func (c *Column) Float64At(i int) (float64, error) {
	r := result{C.column_float_at(c.ptr, C.longlong(i))}
	if err := r.Err(); err != nil {
		return 0, err
	}

	return r.Float(), nil
}

// StringAt returns string value at i
func (c *Column) StringAt(i int) (string, error) {
	r := result{C.column_string_at(c.ptr, C.longlong(i))}
	if err := r.Err(); err != nil {
		return "", err
	}

	return r.FreeStr(), nil
}

// TimeAt returns time value at i
func (c *Column) TimeAt(i int) (time.Time, error) {
	r := result{C.column_timestamp_at(c.ptr, C.longlong(i))}
	if err := r.Err(); err != nil {
		return time.Time{}, err
	}

	epochNano := r.Int()
	t := time.Unix(epochNano/1e9, epochNano%1e9)
	return t, nil
}

// Slice returns a slice from the column
func (c *Column) Slice(offset, length int) (*Column, error) {
	if offset < 0 || length < 0 || offset+length > c.Len() {
		return nil, fmt.Errorf("bad slice: [%d:%d]", offset, offset+length)
	}

	r := result{C.column_slice(c.ptr, C.int64_t(offset), C.int64_t(length))}
	if err := r.Err(); err != nil {
		return nil, err
	}

	return &Column{r.Ptr()}, nil
}

// Table is arrow table
type Table struct {
	ptr unsafe.Pointer
}

// NewTableFromColumns creates new Table from slice of columns
func NewTableFromColumns(columns []*Column) (*Table, error) {
	fields := make([]*Field, len(columns))
	cptr := C.columns_new()
	defer func() {
		// FIXME
		// C.columns_free(cptr)
	}()

	for i, col := range columns {
		fields[i] = col.Field()
		C.columns_append(cptr, col.ptr)
	}

	schema, err := NewSchema(fields)
	if err != nil {
		return nil, err
	}
	ptr := C.table_new(schema.ptr, cptr)
	return &Table{ptr}, nil
}

// NewTableFromPtr creates a new table from underlying C pointer
// You probably shouldn't use this function
func NewTableFromPtr(ptr unsafe.Pointer) *Table {
	return &Table{ptr}
}

// NumRows returns the number of rows
func (t *Table) NumRows() int {
	return int(C.table_num_rows(t.ptr))
}

// NumCols returns the number of columns
func (t *Table) NumCols() int {
	return int(C.table_num_cols(t.ptr))
}

// ColByName returns column by name
func (t *Table) ColByName(name string) (*Column, error) {
	cStr := C.CString(name)
	r := result{C.table_col_by_name(t.ptr, cStr)}
	C.free(unsafe.Pointer(cStr))

	if err := r.Err(); err != nil {
		return nil, err
	}

	return &Column{r.Ptr()}, nil
}

// ColByIndex returns column by index
func (t *Table) ColByIndex(i int) (*Column, error) {
	n := t.NumCols()
	if i < 0 || i >= n {
		return nil, fmt.Errorf("col %d out of bounds [0:%d]", i, n-1)
	}

	r := result{C.table_col_by_index(t.ptr, C.longlong(i))}
	if err := r.Err(); err != nil {
		return nil, err
	}

	return &Column{r.Ptr()}, nil
}

// Slice returns a slice from the table
func (t *Table) Slice(offset, length int) (*Table, error) {
	if offset < 0 || length < 0 || offset+length > t.NumRows() {
		return nil, fmt.Errorf("bad slice: [%d:%d]", offset, offset+length)
	}

	r := result{C.table_slice(t.ptr, C.int64_t(offset), C.int64_t(length))}
	if err := r.Err(); err != nil {
		return nil, err
	}

	return &Table{r.Ptr()}, nil
}

// Ptr returns the underlying C++ pointer
func (t *Table) Ptr() unsafe.Pointer {
	return t.ptr
}
