// +build arrow

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

package arrow

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestField(t *testing.T) {
	require := require.New(t)
	name, dtype := "field-1", Integer64Type
	field, _ := NewField(name, dtype)
	require.Equal(field.Name(), name, "field name")
	require.Equal(field.DType(), dtype, "field dtype")
}

func TestSchema(t *testing.T) {
	require := require.New(t)
	name, dtype := "field-1", Integer64Type
	field, _ := NewField(name, dtype)
	schema, _ := NewSchema([]*Field{field})
	require.Equal(field.Name(), name, "field name")
	require.Equal(field.DType(), dtype, "field dtype")
	require.NotNil(schema)
}

func TestBoolBuilder(t *testing.T) {
	require := require.New(t)
	b := NewBoolArrayBuilder()
	require.NotNil(b.ptr, "create")
	b.Append(true)
}

func TestFloatBuilder(t *testing.T) {
	require := require.New(t)
	b := NewFloat64ArrayBuilder()
	require.NotNil(b.ptr, "create")
	b.Append(7.2)
}

func TestIntBuilder(t *testing.T) {
	require := require.New(t)
	b := NewInt64ArrayBuilder()
	require.NotNil(b.ptr, "create")
	b.Append(7)
}

func TestStringBuilder(t *testing.T) {
	require := require.New(t)
	b := NewStringArrayBuilder()
	require.NotNil(b.ptr, "create")
	b.Append("hello")
}

func TestTimestampBuilder(t *testing.T) {
	require := require.New(t)
	b := NewTimestampArrayBuilder()
	require.NotNil(b.ptr, "create")
	b.Append(time.Now())
}

func TestColumnBoolGet(t *testing.T) {
	require := require.New(t)
	b := NewBoolArrayBuilder()
	require.NotNil(b.ptr, "create")

	const size = 137
	const mod = 7
	for i := 0; i < size; i++ {
		b.Append(i%mod == 0)
	}

	arr, err := b.Finish()
	require.NoError(err, "finish")

	name, dtype := "field-1", BoolType
	field, err := NewField(name, dtype)
	require.NoError(err, "field")

	col, err := NewColumn(field, arr)
	require.NoError(err, "column")
	for i := 0; i < size; i++ {
		v, err := col.BoolAt(i)
		require.NoError(err, "bool at %d - error", i)
		require.Equalf(i%mod == 0, v, "bool at %d", i)
	}
}

func TestColumnIntGet(t *testing.T) {
	require := require.New(t)
	b := NewInt64ArrayBuilder()
	require.NotNil(b.ptr, "create")

	const size = 137
	for i := int64(0); i < size; i++ {
		b.Append(i)
	}

	arr, err := b.Finish()
	require.NoError(err, "finish")

	name, dtype := "field-1", Integer64Type
	field, err := NewField(name, dtype)
	require.NoError(err, "field")

	col, err := NewColumn(field, arr)
	require.NoError(err, "column")
	for i := 0; i < size; i++ {
		v, err := col.Int64At(i)
		require.NoError(err, "int at %d - error", i)
		require.Equalf(int64(i), v, "int at %d", i)
	}
}

func TestColumnFloatGet(t *testing.T) {
	require := require.New(t)
	b := NewFloat64ArrayBuilder()
	require.NotNil(b.ptr, "create")

	const size = 137
	for i := 0; i < size; i++ {
		b.Append(float64(i))
	}

	arr, err := b.Finish()
	require.NoError(err, "finish")

	name, dtype := "field-1", Float64Type
	field, err := NewField(name, dtype)
	require.NoError(err, "field")

	col, err := NewColumn(field, arr)
	require.NoError(err, "column")
	for i := 0; i < size; i++ {
		v, err := col.Float64At(i)
		require.NoError(err, "float at %d - error", i)
		require.Equalf(float64(i), v, "float at %d", i)
	}
}

func TestColumnStringGet(t *testing.T) {
	require := require.New(t)
	b := NewStringArrayBuilder()
	require.NotNil(b.ptr, "create")

	ival := func(i int) string {
		return fmt.Sprintf("value %d", i)
	}

	const size = 137
	for i := 0; i < size; i++ {
		b.Append(ival(i))
	}

	arr, err := b.Finish()
	require.NoError(err, "finish")

	name, dtype := "field-1", StringType
	field, err := NewField(name, dtype)
	require.NoError(err, "field")

	col, err := NewColumn(field, arr)
	require.NoError(err, "column")
	for i := 0; i < size; i++ {
		v, err := col.StringAt(i)
		require.NoError(err, "string at %d - error", i)
		require.Equalf(ival(i), v, "string at %d", i)
	}
}

func TestColumnTimeGet(t *testing.T) {
	require := require.New(t)
	b := NewTimestampArrayBuilder()
	require.NotNil(b.ptr, "create")

	start := time.Now()

	tval := func(i int) time.Time {
		return start.Add(time.Duration(i) * 17 * time.Millisecond)
	}

	const size = 137
	for i := 0; i < size; i++ {
		b.Append(tval(i))
	}

	arr, err := b.Finish()
	require.NoError(err, "finish")

	name, dtype := "field-1", TimestampType
	field, err := NewField(name, dtype)
	require.NoError(err, "field")

	col, err := NewColumn(field, arr)
	require.NoError(err, "column")
	for i := 0; i < size; i++ {
		v, err := col.TimeAt(i)
		require.NoError(err, "time at %d - error", i)
		// Use .Equal to ignore monotinic clock
		require.True(v.Equal(tval(i)), "time at %d", i)
	}
}

func TestColumnSlice(t *testing.T) {
	require := require.New(t)
	b := NewInt64ArrayBuilder()
	require.NotNil(b.ptr, "create")

	const size = 137
	for i := int64(0); i < size; i++ {
		b.Append(i)
	}

	arr, err := b.Finish()
	require.NoError(err, "finish")

	name, dtype := "field-1", Integer64Type
	field, err := NewField(name, dtype)
	require.NoError(err, "field")

	col, err := NewColumn(field, arr)
	require.NoError(err, "column")

	offset, n := 29, 44
	s, err := col.Slice(offset, n)
	require.NoError(err, "slice")
	require.Equal(n, s.Len(), "slice len")
	for i := 0; i < n; i++ {
		v, err := s.Int64At(i)
		require.NoError(err, "int at %d - error", i)
		require.Equalf(int64(i+offset), v, "int at %d", i)
	}
}
