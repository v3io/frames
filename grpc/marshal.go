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

	"github.com/v3io/frames"
)

var (
	intType    = frames.IntType.String()
	floatType  = frames.FloatType.String()
	stringType = frames.StringType.String()
	timeType   = frames.TimeType.String()
	boolType   = frames.BoolType.String()
)

func decodeReadRequest(request *ReadRequest) *frames.ReadRequest {
	return &frames.ReadRequest{
		Backend:      request.Backend,
		DataFormat:   request.DataFormat,
		RowLayout:    request.RowLayout,
		MultiIndex:   request.MultiIndex,
		Query:        request.Query,
		Table:        request.Table,
		Columns:      request.Columns,
		MaxInMessage: int(request.MessageLimit),
	}
}

func frameMessage(frame frames.Frame) (*Frame, error) {
	names := frame.Names()
	columns := make([]*Column, len(names))
	for i, name := range names {
		col, err := frame.Column(name)
		if err != nil {
			return nil, err
		}

		pbCol, err := colToPB(col)
		if err != nil {
			return nil, err
		}
		columns[i] = pbCol
	}

	indices := make([]*Column, len(frame.Indices()))
	for i, col := range frame.Indices() {
		pbCol, err := colToPB(col)
		if err != nil {
			return nil, err
		}
		indices[i] = pbCol
	}

	pbFrame := &Frame{
		Columns: columns,
		Indices: indices,
	}
	return pbFrame, nil
}

func colToPB(column frames.Column) (*Column, error) {
	dtype, err := dtypeToPB(column.DType())
	if err != nil {
		return nil, err
	}

	msg := &Column{
		Name:  column.Name(),
		Dtype: dtype,
	}

	switch column.(type) {
	case *frames.SliceColumn:
		if err := fillSlice(dtype, msg, column); err != nil {
			return nil, err
		}
	case *frames.LabelColumn:
		if err := fillLabel(dtype, msg, column); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown column type - %T", column)
	}

	return msg, nil
}

func fillSlice(dtype DType, msg *Column, column frames.Column) error {
	msg.Kind = Column_SLICE
	switch dtype {
	case DType_BOOLEAN:
		vals, err := column.Bools()
		if err != nil {
			return err
		}

		msg.Bools = vals
	case DType_FLOAT:
		vals, err := column.Floats()
		if err != nil {
			return err
		}

		msg.Floats = vals
	case DType_INTEGER:
		vals, err := column.Ints()
		if err != nil {
			return err
		}

		msg.Ints = vals
	case DType_STRING:
		msg.Strings = column.Strings()
	case DType_TIME:
		vals, err := column.Times()
		if err != nil {
			return err
		}

		times := make([]int64, len(vals))
		for i, val := range vals {
			times[i] = val.UnixNano()
		}

		msg.Times = times
	default:
		return fmt.Errorf("unknown dtype - %s", column.DType())
	}

	return nil
}

func fillLabel(dtype DType, msg *Column, column frames.Column) error {
	msg.Kind = Column_LABEL
	msg.Size = int64(column.Len())

	switch dtype {
	case DType_BOOLEAN:
		val, err := column.BoolAt(0)
		if err != nil {
			return err
		}

		msg.Bools = []bool{val}
	case DType_FLOAT:
		val, err := column.FloatAt(0)
		if err != nil {
			return err
		}

		msg.Floats = []float64{val}
	case DType_INTEGER:
		val, err := column.IntAt(0)
		if err != nil {
			return err
		}

		msg.Ints = []int64{val}
	case DType_STRING:
		val, err := column.StringAt(0)
		if err != nil {
			return err
		}

		msg.Strings = []string{val}
	case DType_TIME:
		val, err := column.TimeAt(0)
		if err != nil {
			return err
		}

		msg.Times = []int64{val.UnixNano()}
	default:
		return fmt.Errorf("unknown dtype - %s", column.DType())
	}

	return nil
}

func dtypeToPB(dtype frames.DType) (DType, error) {
	switch dtype {
	case frames.BoolType:
		return DType_BOOLEAN, nil
	case frames.FloatType:
		return DType_FLOAT, nil
	case frames.IntType:
		return DType_INTEGER, nil
	case frames.StringType:
		return DType_STRING, nil
	case frames.TimeType:
		return DType_TIME, nil
	}

	return DType(-1), fmt.Errorf("unknown dtype - %d", dtype)
}
