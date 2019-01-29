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
	"math"
	"sort"
	"sync"
	"time"

	"github.com/v3io/frames/pb"
)

// ColumnBuilder is interface for building columns
type ColumnBuilder interface {
	Append(value interface{}) error
	At(index int) (interface{}, error)
	Set(index int, value interface{}) error
	Delete(index int) error
	Finish() Column
}

// NewSliceColumnBuilder return a builder for SliceColumn
func NewSliceColumnBuilder(name string, dtype DType, size int) ColumnBuilder {
	msg := &pb.Column{
		Kind:  pb.Column_SLICE,
		Name:  name,
		Dtype: pb.DType(dtype),
		Size:  int64(size),
	}

	// TODO: pre alloate array. Note that for strings we probably don't want to
	// do this since we'll allocate strings twice - zero value then real value
	return &sliceColumBuilder{
		msg:          msg,
		originalSize: int64(size),
	}
}

type sliceColumBuilder struct {
	msg          *pb.Column
	deleted      map[int]bool // use map so deleting twice will work
	once         sync.Once
	originalSize int64
}

func (b *sliceColumBuilder) At(index int) (interface{}, error) {
	return valueAt(b.msg, index)
}

func (b *sliceColumBuilder) Append(value interface{}) error {
	return b.Set(int(b.msg.Size), value)
}

func (b *sliceColumBuilder) Set(index int, value interface{}) error {
	b.resize(index + 1)
	switch b.msg.Dtype {
	case pb.DType_INTEGER:
		return b.setInt(index, value)
	case pb.DType_FLOAT:
		return b.setFloat(index, value)
	case pb.DType_STRING:
		return b.setString(index, value)
	case pb.DType_TIME:
		return b.setTime(index, value)
	case pb.DType_BOOLEAN:
		return b.setBool(index, value)
	default:
		return fmt.Errorf("unknown dtype - %s", b.msg.Dtype)
	}
}

func (b *sliceColumBuilder) Delete(index int) error {
	if index < 0 || int64(index) >= b.msg.Size {
		return fmt.Errorf("index out of bounds: [0:%d]", b.msg.Size-1)
	}

	b.once.Do(func() {
		b.deleted = make(map[int]bool)
	})

	b.deleted[index] = true
	return nil
}

func (b *sliceColumBuilder) setInt(index int, value interface{}) error {
	switch value.(type) {
	case int64:
		b.msg.Ints[index] = value.(int64)
		return nil
	case int:
		b.msg.Ints[index] = int64(value.(int))
		return nil
	case int8:
		b.msg.Ints[index] = int64(value.(int8))
		return nil
	case int16:
		b.msg.Ints[index] = int64(value.(int16))
		return nil
	case int32:
		b.msg.Ints[index] = int64(value.(int32))
		return nil
	}

	return b.typeError(value)
}

func (b *sliceColumBuilder) setTime(index int, value interface{}) error {
	switch value.(type) {
	case time.Time:
		b.msg.Times[index] = value.(time.Time).UnixNano()
		return nil
	case int64:
		b.msg.Times[index] = value.(int64)
		return nil
	}

	return b.typeError(value)
}

func (b *sliceColumBuilder) typeError(value interface{}) error {
	return fmt.Errorf("unsupported type for %s slice column - %T", b.msg.Dtype, value)
}

func (b *sliceColumBuilder) setFloat(index int, value interface{}) error {
	switch value.(type) {
	case float64:
		b.msg.Floats[index] = value.(float64)
		return nil
	case float32:
		b.msg.Floats[index] = float64(value.(float32))
		return nil
	}

	return b.typeError(value)
}

func (b *sliceColumBuilder) setString(index int, value interface{}) error {
	s, ok := value.(string)
	if !ok {
		return b.typeError(value)
	}

	b.msg.Strings[index] = s
	return nil
}

func (b *sliceColumBuilder) setBool(index int, value interface{}) error {
	bval, ok := value.(bool)
	if !ok {
		return b.typeError(value)
	}

	b.msg.Bools[index] = bval
	return nil
}

func (b *sliceColumBuilder) resize(size int) {
	b.msg.Size = int64(size)
	switch b.msg.Dtype {
	case pb.DType_INTEGER:
		b.msg.Ints = resizeInt64(b.msg.Ints, size)
	case pb.DType_FLOAT:
		currentSize := cap(b.msg.Floats)
		if currentSize >= size {
			b.msg.Floats = b.msg.Floats[:size]
			return
		}
		floats := make([]float64, size)
		copy(floats, b.msg.Floats)
		b.msg.Floats = floats
		b.fillDefaultValues(currentSize, size)
	case pb.DType_STRING:
		if cap(b.msg.Strings) >= size {
			b.msg.Strings = b.msg.Strings[:size]
			return
		}
		strings := make([]string, size)
		copy(strings, b.msg.Strings)
		b.msg.Strings = strings
	case pb.DType_TIME:
		b.msg.Times = resizeInt64(b.msg.Times, size)
	case pb.DType_BOOLEAN:
		if cap(b.msg.Bools) >= size {
			b.msg.Bools = b.msg.Bools[:size]
			return
		}
		bools := make([]bool, size)
		copy(bools, b.msg.Bools)
		b.msg.Bools = bools
	}
}

func resizeInt64(buf []int64, size int) []int64 {
	if cap(buf) >= size {
		return buf[:size]
	}
	ints := make([]int64, size)
	copy(ints, buf)
	return ints
}

// TODO: Return error
func (b *sliceColumBuilder) Finish() Column {
	b.removeDeleted()
	b.fillMissingRows()
	return &colImpl{msg: b.msg}
}

func (b *sliceColumBuilder) removeDeleted() {
	// TODO
}

func (b *sliceColumBuilder) fillMissingRows() {
	currentSize, _ := b.getActualCapacity()
	if int64(currentSize) >= b.originalSize {
		return
	}

	b.resize(int(b.originalSize))
	b.fillDefaultValues(currentSize, int(b.originalSize))
}

func (b *sliceColumBuilder) fillDefaultValues(from, to int) {
	switch b.msg.Dtype {
	case pb.DType_FLOAT:
		for i := from; i < to; i++ {
			b.setFloat(int(i), math.NaN())
		}
	}
}

func (b *sliceColumBuilder) getActualCapacity() (int, error) {
	switch b.msg.Dtype {
	case pb.DType_INTEGER:
		return cap(b.msg.Ints), nil
	case pb.DType_FLOAT:
		return cap(b.msg.Floats), nil
	case pb.DType_STRING:
		return cap(b.msg.Strings), nil
	case pb.DType_TIME:
		return cap(b.msg.Times), nil
	case pb.DType_BOOLEAN:
		return cap(b.msg.Bools), nil
	}

	return 0, fmt.Errorf("not supported type %v", b.msg.Dtype)
}

// NewLabelColumnBuilder return a builder for LabelColumn
func NewLabelColumnBuilder(name string, dtype DType, size int) ColumnBuilder {
	msg := &pb.Column{
		Kind:  pb.Column_LABEL,
		Name:  name,
		Dtype: pb.DType(dtype),
		Size:  int64(size),
	}

	switch dtype {
	case IntType:
		msg.Ints = make([]int64, 1)
	case FloatType:
		msg.Floats = make([]float64, 1)
	case StringType:
		msg.Strings = make([]string, 1)
	case TimeType:
		msg.Times = make([]int64, 1)
	case BoolType:
		msg.Bools = make([]bool, 1)
	}

	return &labelColumBuilder{msg: msg, empty: true}
}

type labelColumBuilder struct {
	msg     *pb.Column
	empty   bool
	deleted map[int]bool
	once    sync.Once
}

func (b *labelColumBuilder) At(index int) (interface{}, error) {
	return valueAt(b.msg, index)
}

func (b *labelColumBuilder) Finish() Column {
	b.msg.Size -= int64(len(b.deleted))
	return &colImpl{msg: b.msg}
}

func (b *labelColumBuilder) Append(value interface{}) error {
	return b.Set(int(b.msg.Size), value)
}

func (b *labelColumBuilder) Set(index int, value interface{}) error {
	var err error
	switch b.msg.Dtype {
	case pb.DType_INTEGER:
		err = b.setInt(index, value)
	case pb.DType_FLOAT:
		err = b.setFloat(index, value)
	case pb.DType_STRING:
		err = b.setString(index, value)
	case pb.DType_TIME:
		err = b.setTime(index, value)
	case pb.DType_BOOLEAN:
		err = b.setBool(index, value)
	default:
		return fmt.Errorf("unknown dtype - %s", b.msg.Dtype)
	}

	if err == nil {
		newSize := int64(index + 1)
		if b.msg.Size < newSize {
			b.msg.Size = newSize
		}
	}

	return err
}

func (b *labelColumBuilder) Delete(index int) error {
	// TODO: Check error
	b.once.Do(func() {
		b.deleted = make(map[int]bool)
	})

	b.deleted[index] = true
	return nil
}

func (b *labelColumBuilder) setInt(index int, value interface{}) error {
	var ival int64
	switch value.(type) {
	case int64:
		ival = value.(int64)
	case int:
		ival = int64(value.(int))
	case int8:
		ival = int64(value.(int8))
	case int16:
		ival = int64(value.(int16))
	case int32:
		ival = int64(value.(int32))
	default:
		return b.typeError(value)
	}

	if b.empty {
		b.msg.Ints[0] = ival
		b.empty = false
	} else {
		if b.msg.Ints[0] != ival {
			return b.valueError(b.msg.Ints[0], ival)
		}
	}

	return nil
}

func (b *labelColumBuilder) setFloat(index int, value interface{}) error {
	var fval float64
	switch value.(type) {
	case float64:
		fval = value.(float64)
	case float32:
		fval = float64(value.(float32))
	default:
		return b.typeError(value)
	}

	if b.empty {
		b.msg.Floats[0] = fval
		b.empty = false
	} else {
		if b.msg.Floats[0] != fval {
			return b.valueError(b.msg.Floats[0], fval)
		}
	}

	return nil
}

func (b *labelColumBuilder) setString(index int, value interface{}) error {
	s, ok := value.(string)
	if !ok {
		return b.typeError(value)
	}

	if b.empty {
		b.msg.Strings[0] = s
		b.empty = false
	} else {
		if b.msg.Strings[0] != s {
			return b.valueError(b.msg.Strings[0], s)
		}
	}

	return nil
}

func (b *labelColumBuilder) setBool(index int, value interface{}) error {
	bval, ok := value.(bool)
	if !ok {
		return b.typeError(value)
	}

	if b.empty {
		b.msg.Bools[0] = bval
		b.empty = false
	} else {
		if b.msg.Bools[0] != bval {
			return b.valueError(b.msg.Bools[0], bval)
		}
	}

	return nil
}

func (b *labelColumBuilder) setTime(index int, value interface{}) error {
	var t int64
	switch value.(type) {
	case time.Time:
		t = value.(time.Time).UnixNano()
	case int64:
		t = value.(int64)
	default:
		return b.typeError(value)
	}

	if b.empty {
		b.msg.Times[0] = t
		b.empty = false
	} else {
		if b.msg.Times[0] != t {
			return b.valueError(b.msg.Times[0], t)
		}
	}

	return nil

}

func (b *labelColumBuilder) typeError(value interface{}) error {
	return fmt.Errorf("unsupported type for %s label column - %T", b.msg.Dtype, value)
}

func (b *labelColumBuilder) valueError(current, value interface{}) error {
	return fmt.Errorf("differnt value int %s label column - %v != %v", b.msg.Dtype, value, current)
}

func valueAt(msg *pb.Column, index int) (interface{}, error) {
	if int64(index) >= msg.Size {
		return nil, fmt.Errorf("index out of bounds %d > %d", index, msg.Size-1)
	}

	if msg.Kind == pb.Column_LABEL {
		index = 0
	}

	switch msg.Dtype {
	case pb.DType_INTEGER:
		if len(msg.Ints) < index+1 {
			return nil, nil
		}
		return msg.Ints[index], nil
	case pb.DType_FLOAT:
		if len(msg.Floats) < index+1 {
			return nil, nil
		}
		return msg.Floats[index], nil
	case pb.DType_STRING:
		if len(msg.Strings) < index+1 {
			return nil, nil
		}
		return msg.Strings[index], nil
	case pb.DType_TIME:
		if len(msg.Times) < index+1 {
			return nil, nil
		}
		sec := msg.Times[index] / 1e9
		nsec := msg.Times[index] % 1e9
		return time.Unix(sec, nsec), nil
	case pb.DType_BOOLEAN:
		if len(msg.Bools) < index+1 {
			return nil, nil
		}
		return msg.Bools[index], nil
	}

	return nil, nil
}

func map2slice(m map[int]bool) []int {
	s := make([]int, len(m))
	i := 0
	for v := range m {
		s[i] = v
		i++
	}
	return s
}

// FIXME:
func shrink(values []int, deleted map[int]bool) []int {
	if len(deleted) == 0 {
		return nil
	}

	delIdxs := map2slice(deleted)
	sort.Ints(delIdxs)
	out := make([]int, len(values)-len(deleted))
	start := 0
	cp := 0
	for _, end := range delIdxs {
		if end == start || end == start+1 {
			start = end
			continue
		}
		copy(out[cp:], values[start:end])
		cp += end - start
		start = end + 1
	}

	if start < len(values) {
		copy(out[cp:], values[start:])
	}

	return out
}
