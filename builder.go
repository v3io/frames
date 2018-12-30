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
	"github.com/v3io/frames/pb"
	"time"
)

// ColumnBuilder is interface for building columns
type ColumnBuilder interface {
	Append(value interface{}) error
	Set(index int, value interface{}) error
	Finish() (Column, error)
}

// NewSliceColumnBuilder return a builder for SliceColumn
func NewSliceColumnBuilder(name string, dtype DType, size int) ColumnBuilder {
	msg := &pb.Column{
		Kind:  pb.Column_SLICE,
		Name:  name,
		Dtype: pb.DType(dtype),
	}

	if size > 0 {
		msg.Size = int64(size)
	}
	// TODO: pre alloate array. Note that for strings we probably don't want to
	// do this since we'll allocate strings twice - zero value then real value

	return &sliceColumBuilder{msg: msg}
}

type sliceColumBuilder struct {
	msg *pb.Column
}

func (b *sliceColumBuilder) Append(value interface{}) error {
	var err error
	switch b.msg.Dtype {
	case pb.DType_INTEGER:
		err = b.appendInt(value)
	case pb.DType_FLOAT:
		err = b.appendFloat(value)
	case pb.DType_STRING:
		err = b.appendString(value)
	case pb.DType_TIME:
		err = b.appendTime(value)
	case pb.DType_BOOLEAN:
		err = b.appendBool(value)
	default:
		return fmt.Errorf("unknown dtype - %s", b.msg.Dtype)
	}

	if err == nil {
		b.msg.Size++
	}
	return err
}

func (b *sliceColumBuilder) appendInt(value interface{}) error {
	switch value.(type) {
	case int64:
		b.msg.Ints = append(b.msg.Ints, value.(int64))
		return nil
	case int:
		b.msg.Ints = append(b.msg.Ints, int64(value.(int)))
		return nil
	case int8:
		b.msg.Ints = append(b.msg.Ints, int64(value.(int8)))
		return nil
	case int16:
		b.msg.Ints = append(b.msg.Ints, int64(value.(int16)))
		return nil
	case int32:
		b.msg.Ints = append(b.msg.Ints, int64(value.(int32)))
		return nil
	}

	return b.typeError(value)
}

func (b *sliceColumBuilder) appendTime(value interface{}) error {
	switch value.(type) {
	case time.Time:
		b.msg.Times = append(b.msg.Times, value.(time.Time).UnixNano())
		return nil
	case int64:
		b.msg.Times = append(b.msg.Times, value.(int64))
		return nil
	}

	return b.typeError(value)
}

func (b *sliceColumBuilder) typeError(value interface{}) error {
	return fmt.Errorf("unsupported type for %s column - %T", b.msg.Dtype, value)
}

func (b *sliceColumBuilder) appendFloat(value interface{}) error {
	switch value.(type) {
	case float64:
		b.msg.Floats = append(b.msg.Floats, value.(float64))
		return nil
	case float32:
		b.msg.Floats = append(b.msg.Floats, float64(value.(float32)))
		return nil
	}

	return b.typeError(value)
}

func (b *sliceColumBuilder) appendString(value interface{}) error {
	s, ok := value.(string)
	if !ok {
		return b.typeError(value)
	}

	b.msg.Strings = append(b.msg.Strings, s)
	return nil
}

func (b *sliceColumBuilder) appendBool(value interface{}) error {
	bval, ok := value.(bool)
	if !ok {
		return b.typeError(value)
	}

	b.msg.Bools = append(b.msg.Bools, bval)
	return nil
}

func (b *sliceColumBuilder) Set(index int, value interface{}) error {
	b.resize(index+1)
}

func (b *sliceColumBuilder) resize(size int) {
	switch b.msg.Dtype {
	case pb.DType_INTEGER:
	case pb.DType_FLOAT:
		err = b.appendFloat(value)
	case pb.DType_STRING:
		err = b.appendString(value)
	case pb.DType_TIME:
		err = b.appendTime(value)
	case pb.DType_BOOLEAN:
		err = b.appendBool(value)
}

func (b *sliceColumBuilder) Finish() (Column, error) {
}
