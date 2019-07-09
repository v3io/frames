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
