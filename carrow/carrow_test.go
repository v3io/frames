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