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
	"time"

	"github.com/pkg/errors"
)

// MapFrame is a frame based on map
type MapFrame struct {
	byIndex  map[int]Column
	indexCol Column
}

// NewMapFrame returns a new MapFrame
func NewMapFrame(columns []Column, indexColumn Column) (*MapFrame, error) {
	if err := checkEqualLen(columns, indexColumn); err != nil {
		return nil, err
	}

	byIndex := make(map[int]Column)
	for i, col := range columns {
		byIndex[i] = col
	}

	frame := &MapFrame{
		byIndex:  byIndex,
		indexCol: indexColumn,
	}

	return frame, nil
}

// NewMapFrameFromMap returns a new MapFrame from a map
func NewMapFrameFromMap(data map[string]interface{}) (*MapFrame, error) {
	var (
		columns = make([]Column, len(data))
		i       = 0
		col     Column
		err     error
	)

	for name, values := range data {
		switch values.(type) {
		case []int:
			col, err = NewSliceColumn(name, values.([]int))
			if err != nil {
				return nil, errors.Wrap(err, "can't create int column")
			}
		case []float64:
			col, err = NewSliceColumn(name, values.([]float64))
			if err != nil {
				return nil, errors.Wrap(err, "can't create float column")
			}
		case []string:
			col, err = NewSliceColumn(name, values.([]string))
			if err != nil {
				return nil, errors.Wrap(err, "can't create string column")
			}
		case []time.Time:
			col, err = NewSliceColumn(name, values.([]time.Time))
			if err != nil {
				return nil, errors.Wrap(err, "can't create time column")
			}
		default:
			return nil, fmt.Errorf("unsupported data type - %T", values)
		}

		columns[i] = col
		i++
	}

	return NewMapFrame(columns, nil)
}

// Columns returns the column names
func (mf *MapFrame) Columns() []string {
	names := make([]string, len(mf.byIndex))

	for i := 0; i < len(mf.byIndex); i++ {
		names[i] = mf.byIndex[i].Name() // TODO: Check if exists?
	}

	return names
}

// IndexColumn returns the index column, nil if there's none
func (mf *MapFrame) IndexColumn() Column {
	return mf.indexCol
}

// Len is the number of rows
func (mf *MapFrame) Len() int {
	for _, col := range mf.byIndex {
		return col.Len()
	}

	return 0
}

// Column gets a column by name
func (mf *MapFrame) Column(name string) (Column, error) {
	// TODO: We can speed it up by calculating once, but then we'll use more memory
	for _, col := range mf.byIndex {
		if col.Name() == name {
			return col, nil
		}
	}

	return nil, fmt.Errorf("column %q not found", name)
}

// Slice return a new Frame with is slice of the original
func (mf *MapFrame) Slice(start int, end int) (Frame, error) {
	if err := validateSlice(start, end, mf.Len()); err != nil {
		return nil, err
	}

	frameSlice := make([]Column, len(mf.byIndex))
	for i, col := range mf.byIndex {
		slice, err := col.Slice(start, end)
		if err != nil {
			return nil, errors.Wrapf(err, "can't get slice from %q", col.Name())
		}

		frameSlice[i] = slice
	}

	return NewMapFrame(frameSlice, mf.IndexColumn())
}

// MapFrameMessage is over-the-wire frame data
type MapFrameMessage struct {
	Columns       []string                       `msgpack:"columns"`
	SliceIndexCol *SliceColumnMessage            `msgpack:"slice_index,omitempty"`
	LabelIndexCol *LabelColumnMessage            `msgpack:"label_index,omitempty"`
	SliceCols     map[string]*SliceColumnMessage `msgpack:"slice_cols,omitempty"`
	LabelCols     map[string]*LabelColumnMessage `msgpack:"label_cols,omitempty"`
}

// Marshal marshals to native type
func (mf *MapFrame) Marshal() (interface{}, error) {
	msg := &MapFrameMessage{
		Columns:   mf.Columns(),
		LabelCols: make(map[string]*LabelColumnMessage),
		SliceCols: make(map[string]*SliceColumnMessage),
	}

	for _, col := range mf.byIndex {
		colMsg, err := mf.marshalColumn(col)
		if err != nil {
			return nil, err
		}

		switch colMsg.(type) {
		case *SliceColumnMessage:
			msg.SliceCols[col.Name()] = colMsg.(*SliceColumnMessage)
		case *LabelColumnMessage:
			msg.LabelCols[col.Name()] = colMsg.(*LabelColumnMessage)
		default:
			return nil, fmt.Errorf("unknown marshaled message type - %T", colMsg)
		}
	}

	if iCol := mf.IndexColumn(); iCol != nil {
		colMsg, err := mf.marshalColumn(iCol)
		if err != nil {
			return nil, err
		}

		switch colMsg.(type) {
		case *SliceColumnMessage:
			msg.SliceIndexCol = colMsg.(*SliceColumnMessage)
		case *LabelColumnMessage:
			msg.LabelIndexCol = colMsg.(*LabelColumnMessage)
		default:
			return nil, fmt.Errorf("unknown marshaled message type - %T", colMsg)
		}
	}

	return msg, nil
}

func (mf *MapFrame) marshalColumn(col Column) (interface{}, error) {
	marshaler, ok := col.(Marshaler)
	if !ok {
		return nil, fmt.Errorf("column %q is not Marshaler", col.Name())
	}

	msg, err := marshaler.Marshal()
	if err != nil {
		return nil, errors.Wrapf(err, "can't marshal %q", col.Name())
	}

	return msg, nil
}

func validateSlice(start int, end int, size int) error {
	if start < 0 || end < 0 {
		return fmt.Errorf("negative indexing not supported")
	}

	if end < start {
		return fmt.Errorf("end < start")
	}

	if start >= size {
		return fmt.Errorf("start out of bounds")
	}

	if end >= size {
		return fmt.Errorf("end out of bounds")
	}

	return nil
}

func checkEqualLen(columns []Column, indexCol Column) error {
	size := -1
	for _, col := range columns {
		if size == -1 { // first column
			size = col.Len()
			continue
		}

		if colSize := col.Len(); colSize != size {
			return fmt.Errorf("%q column size mismatch (%d != %d)", col.Name(), colSize, size)
		}
	}

	if indexCol != nil && size != -1 && indexCol.Len() != size {
		return fmt.Errorf("index column size mismatch (%d != %d)", indexCol.Len(), size)
	}

	return nil
}
