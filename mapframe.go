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

	"github.com/pkg/errors"
)

const (
	mapFrameTag = "mapFrame"
)

// MapFrame is a frame based on map
type MapFrame struct {
	byIndex map[int]Column
}

// NewMapFrame returns a new MapFrame
func NewMapFrame(columns []Column) (*MapFrame, error) {
	if err := checkEqualLen(columns); err != nil {
		return nil, err
	}

	byIndex := make(map[int]Column)
	for i, col := range columns {
		byIndex[i] = col
	}

	return &MapFrame{byIndex}, nil
}

// Columns returns the column names
func (mf *MapFrame) Columns() []string {
	names := make([]string, len(mf.byIndex))

	for i := 0; i < len(mf.byIndex); i++ {
		names[i] = mf.byIndex[i].Name() // TODO: Check if exists?
	}

	return names
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

	return NewMapFrame(frameSlice)
}

// Marshal marshals to native type
func (mf *MapFrame) Marshal() (map[string]interface{}, error) {
	data := map[string]interface{}{
		"tag": mapFrameTag,
	}

	columns := make([]map[string]interface{}, len(mf.byIndex))
	for i := range columns {
		col := mf.byIndex[i]
		marshaler, ok := col.(Marshaler)
		if !ok {
			return nil, fmt.Errorf("column %q is not a Marshaler", col.Name())
		}

		colData, err := marshaler.Marshal()
		if err != nil {
			return nil, errors.Wrapf(err, "can't marshal column %q", col.Name())
		}

		columns[i] = colData
	}

	data["columns"] = columns
	return data, nil
}

func validateSlice(start int, end int, size int) error {
	if start < 0 || end < 0 {
		return fmt.Errorf("negative indexing not supported")
	}

	if end < start {
		return fmt.Errorf("end <= start")
	}

	if start >= size {
		return fmt.Errorf("start out of bounds")
	}

	if end >= size {
		return fmt.Errorf("end out of bounds")
	}

	return nil
}

func checkEqualLen(columns []Column) error {
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

	return nil
}
