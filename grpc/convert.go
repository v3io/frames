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

package grpc

import (
	"fmt"
	"strconv"
	"time"

	"github.com/v3io/frames"
	"github.com/v3io/frames/pb"
)

// TODO: Find a way for frames to use the .proto
var (
	dtypes = map[pb.DType]frames.DType{
		pb.DType_INTEGER: frames.IntType,
		pb.DType_FLOAT:   frames.FloatType,
		pb.DType_STRING:  frames.StringType,
		pb.DType_TIME:    frames.TimeType,
		pb.DType_BOOLEAN: frames.BoolType,
	}
)

type colImpl struct {
	// We can embed column since the field names and the method names are the same
	// (e.g. Name string and Name() string)
	msg   *pb.Column
	times []time.Time
}

func (c *colImpl) Len() int {
	if c.msg.Kind == pb.Column_LABEL {
		return int(c.msg.Size)
	}

	// Slice column
	switch c.msg.Dtype {
	case pb.DType_INTEGER:
		return len(c.msg.Ints)
	case pb.DType_FLOAT:
		return len(c.msg.Floats)
	case pb.DType_STRING:
		return len(c.msg.Strings)
	case pb.DType_TIME:
		return len(c.msg.Times)
	case pb.DType_BOOLEAN:
		return len(c.msg.Bools)
	}

	// TODO: panic?
	return -1
}

func (c *colImpl) Name() string {
	return c.msg.Name
}

func (c *colImpl) DType() frames.DType {
	return dtypes[c.msg.Dtype]
}

func (c *colImpl) Ints() ([]int64, error) {
	if err := c.checkDType(pb.DType_INTEGER); err != nil {
		return nil, err
	}

	var data []int64
	if c.msg.Kind == pb.Column_SLICE {
		data = c.msg.Ints
	} else {
		data = make([]int64, c.msg.Size)
		for i := int64(0); i < c.msg.Size; i++ {
			data[i] = c.msg.Ints[0]
		}
	}

	return data, nil
}

func (c *colImpl) IntAt(i int) (int64, error) {
	if err := c.validateAt(pb.DType_INTEGER, i); err != nil {
		return 0, err
	}

	if c.msg.Kind == pb.Column_LABEL {
		i = 0
	}
	return c.msg.Ints[i], nil
}

func (c *colImpl) Floats() ([]float64, error) {
	if err := c.checkDType(pb.DType_FLOAT); err != nil {
		return nil, err
	}

	var data []float64
	if c.msg.Kind == pb.Column_SLICE {
		data = c.msg.Floats
	} else {
		data = make([]float64, c.msg.Size)
		for i := int64(0); i < c.msg.Size; i++ {
			data[i] = c.msg.Floats[0]
		}
	}

	return data, nil
}

func (c *colImpl) FloatAt(i int) (float64, error) {
	if err := c.validateAt(pb.DType_FLOAT, i); err != nil {
		return 0.0, err
	}

	if c.msg.Kind == pb.Column_LABEL {
		i = 0
	}
	return c.msg.Floats[i], nil
}

func (c *colImpl) Strings() []string {
	if c.msg.Dtype == pb.DType_STRING && c.msg.Kind == pb.Column_SLICE {
		return c.msg.Strings
	}

	data := make([]string, c.Len())
	for i := 0; i < len(data); i++ {
		data[i], _ = c.StringAt(i)
	}

	return data
}

func (c *colImpl) StringAt(i int) (string, error) {
	if err := c.checkInbounds(i); err != nil {
		return "", err
	}

	dtype := c.msg.Dtype
	switch dtype {
	case pb.DType_INTEGER:
		val, err := c.IntAt(i)
		if err != nil {
			return "", err
		}

		return strconv.FormatInt(val, 10), nil
	case pb.DType_FLOAT:
		val, err := c.FloatAt(i)
		if err != nil {
			return "", err
		}
		return strconv.FormatFloat(val, 'f', -1, 64), nil
	case pb.DType_STRING:
		if c.msg.Kind == pb.Column_LABEL {
			i = 0
		}
		return c.msg.Strings[i], nil
	case pb.DType_TIME:
		val, err := c.TimeAt(i)
		if err != nil {
			return "", err
		}
		return val.Format(time.RFC3339Nano), nil
	case pb.DType_BOOLEAN:
		val, err := c.BoolAt(i)
		if err != nil {
			return "", err
		}

		s := "false"
		if val {
			s = "true"
		}
		return s, nil
	}

	return "", fmt.Errorf("unknown dtype - %d (%s)", dtype, dtypeName(dtype))
}

func (c *colImpl) Times() ([]time.Time, error) {
	if err := c.checkDType(pb.DType_TIME); err != nil {
		return nil, err
	}

	// TODO: sync.Once?
	if c.times == nil {
		times := make([]time.Time, c.Len())
		for i := 0; i < c.Len(); i++ {
			c.times[i] = pb.NSToTime(c.msg.Times[i])
		}

		c.times = times
	}

	return c.times, nil
}

func (c *colImpl) TimeAt(i int) (time.Time, error) {
	if err := c.validateAt(pb.DType_TIME, i); err != nil {
		return time.Time{}, err
	}

	ns := c.msg.Times[i]
	return pb.NSToTime(ns), nil
}

func (c *colImpl) Bools() ([]bool, error) {
	if err := c.checkDType(pb.DType_BOOLEAN); err != nil {
		return nil, err
	}

	return c.msg.Bools, nil
}

func (c *colImpl) BoolAt(i int) (bool, error) {
	if err := c.validateAt(pb.DType_BOOLEAN, i); err != nil {
		return false, err
	}

	return c.msg.Bools[i], nil
}

func (c *colImpl) Slice(start int, end int) (frames.Column, error) {
	if start > end {
		return nil, fmt.Errorf("start %d bigger than end %d", start, end)
	}

	if err := c.checkInbounds(start); err != nil {
		return nil, err
	}

	if end != 0 {
		if err := c.checkInbounds(end - 1); err != nil {
			return nil, err
		}
	}

	msg := &pb.Column{
		Kind:  c.msg.Kind,
		Dtype: c.msg.Dtype,
		Name:  c.msg.Name,
	}

	if c.msg.Kind == pb.Column_LABEL {
		msg.Size = int64(start - end)
	}

	switch c.msg.Dtype {
	case pb.DType_INTEGER:
		data := c.msg.Ints
		if c.msg.Kind == pb.Column_SLICE {
			data = data[start:end]
		}
		msg.Ints = data
	case pb.DType_FLOAT:
		data := c.msg.Floats
		if c.msg.Kind == pb.Column_SLICE {
			data = data[start:end]
		}
		msg.Floats = data
	case pb.DType_STRING:
		data := c.msg.Strings
		if c.msg.Kind == pb.Column_SLICE {
			data = data[start:end]
		}
		msg.Strings = data
	case pb.DType_TIME:
		data := c.msg.Times
		if c.msg.Kind == pb.Column_SLICE {
			data = data[start:end]
		}
		msg.Times = data
	case pb.DType_BOOLEAN:
		data := c.msg.Bools
		if c.msg.Kind == pb.Column_SLICE {
			data = data[start:end]
		}
		msg.Bools = data
	}

	col := &colImpl{
		msg: msg,
	}
	return col, nil
}

func (c *colImpl) Append(value interface{}) error {
	if c.msg.Kind == pb.Column_LABEL {
		return c.appendLabel(value)
	}

	return c.appendSlice(value)
}

func (c *colImpl) appendSlice(value interface{}) error {
	switch c.msg.Dtype {
	case pb.DType_INTEGER:
		v, ok := value.(int64)
		if !ok {
			return fmt.Errorf("wrong type for int64 - %T", value)
		}
		c.msg.Ints = append(c.msg.Ints, v)
		return nil
	case pb.DType_FLOAT:
		v, ok := value.(float64)
		if !ok {
			return fmt.Errorf("wrong type for float64 - %T", value)
		}
		c.msg.Floats = append(c.msg.Floats, v)
		return nil
	case pb.DType_STRING:
		v, ok := value.(string)
		if !ok {
			return fmt.Errorf("wrong type for string - %T", value)
		}
		c.msg.Strings = append(c.msg.Strings, v)
		return nil
	case pb.DType_TIME:
		v, ok := value.(time.Time)
		if !ok {
			return fmt.Errorf("wrong type for time.Time - %T", value)
		}
		c.msg.Times = append(c.msg.Times, v.UnixNano())
		return nil
	case pb.DType_BOOLEAN:
		v, ok := value.(bool)
		if !ok {
			return fmt.Errorf("wrong type for bool - %T", value)
		}
		c.msg.Bools = append(c.msg.Bools, v)
		return nil
	}

	return fmt.Errorf("unknown dtype - %s", dtypeName(c.msg.Dtype))
}

func (c *colImpl) appendLabel(value interface{}) error {
	if !c.sameLabelValue(value) {
		return fmt.Errorf("append - wrong type or value mismatch - %v", value)
	}

	c.msg.Size++
	return nil
}

func (c *colImpl) sameLabelValue(value interface{}) bool {
	switch c.msg.Dtype {
	case pb.DType_INTEGER:
		v, ok := value.(int64)
		if !ok {
			return false
		}
		return v == c.msg.Ints[0]
	case pb.DType_FLOAT:
		v, ok := value.(float64)
		if !ok {
			return false
		}
		return v == c.msg.Floats[0]
	case pb.DType_STRING:
		v, ok := value.(string)
		if !ok {
			return false
		}
		return v == c.msg.Strings[0]
	case pb.DType_TIME:
		v, ok := value.(time.Time)
		if !ok {
			return false
		}
		return v.UnixNano() == c.msg.Times[0]
	case pb.DType_BOOLEAN:
		v, ok := value.(bool)
		if !ok {
			return false
		}
		return v == c.msg.Bools[0]
	}

	return false
}

func (c *colImpl) validateAt(dtype pb.DType, i int) error {
	if err := c.checkDType(dtype); err != nil {
		return err
	}

	return c.checkInbounds(i)
}

func (c *colImpl) checkInbounds(i int) error {
	if i > 0 && i < c.Len() {
		return nil
	}
	return fmt.Errorf("index %d out of bounds [0:%d]", i, c.Len())
}

func (c *colImpl) checkDType(dtype pb.DType) error {
	if c.msg.Dtype != dtype {
		return fmt.Errorf("wrong dtype")
	}

	return nil
}

func asFrame(msg *pb.Frame) (frames.Frame, error) {
	labels := pb.AsGoMap(msg.Labels)

	columns := asCols(msg.Columns)
	indices := asCols(msg.Indices)
	return frames.NewFrame(columns, indices, labels)
}

func asCols(cols []*pb.Column) []frames.Column {
	fcols := make([]frames.Column, len(cols))
	for i, col := range cols {
		fcols[i] = &colImpl{
			msg: col,
		}
	}

	return fcols
}

func dtypeName(dtype pb.DType) string {
	return pb.DType_name[int32(dtype)]
}
