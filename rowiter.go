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
	"sync"
)

type rowIterator struct {
	columns   []Column
	err       error
	frame     Frame
	index     interface{}
	indexName string
	indices   map[string]interface{}
	once      sync.Once
	row       map[string]interface{}
	rowNum    int
}

func newRowIterator(frame Frame) *rowIterator {
	return &rowIterator{
		frame: frame,
	}
}

func (it *rowIterator) init() {
	it.columns, it.err = it.frameColumns(it.frame)
	if it.err != nil {
		return
	}

	if len(it.frame.Indices()) == 1 {
		for _, col := range it.frame.Indices() {
			it.indexName = col.Name()
		}
	}
}

func (it *rowIterator) Next() bool {
	it.once.Do(it.init)

	if it.err != nil || it.rowNum >= it.frame.Len() {
		return false
	}

	row, err := it.getRow(it.rowNum, it.columns)
	if err != nil {
		it.err = err
		return false
	}

	indices, err := it.getRow(it.rowNum, it.frame.Indices())
	if err != nil {
		it.err = err
		return false
	}

	it.row = row
	it.indices = indices
	if it.indexName != "" {
		it.index = indices[it.indexName]
	} else {
		it.index = nil
	}

	it.rowNum++
	return true
}

func (it *rowIterator) Err() error {
	return it.err
}

func (it *rowIterator) Row(includeIndex bool) map[string]interface{} {
	if !includeIndex {
		return it.row
	}

	// TODO: Do we want to keep a copy of this?
	allCols := make(map[string]interface{})
	for key, value := range it.row {
		allCols[key] = value
	}

	for key, value := range it.indices {
		allCols[key] = value
	}

	return allCols
}

func (it *rowIterator) Index() interface{} {
	return it.index
}

func (it *rowIterator) Indices() map[string]interface{} {
	return it.indices
}

func (it *rowIterator) RowNum() int {
	return it.rowNum - 1
}

func (it *rowIterator) frameColumns(frame Frame) ([]Column, error) {
	names := frame.Names()
	columns := make([]Column, len(names))
	for i, name := range names {
		col, err := frame.Column(name)
		if err != nil {
			return nil, err
		}
		columns[i] = col
	}

	return columns, nil
}

func (it *rowIterator) getRow(rowNum int, columns []Column) (map[string]interface{}, error) {
	row := make(map[string]interface{})
	for _, col := range columns {
		var value interface{}
		var err error
		switch col.DType() {
		case IntType:
			value, err = col.IntAt(it.rowNum)
		case FloatType:
			value, err = col.FloatAt(it.rowNum)
		case StringType:
			value, err = col.StringAt(it.rowNum)
		case TimeType:
			value, err = col.TimeAt(it.rowNum)
		case BoolType:
			value, err = col.BoolAt(it.rowNum)
		default:
			err = fmt.Errorf("%s:%d - unknown dtype - %s", col.Name(), it.rowNum, col.DType())
		}

		if err != nil {
			return nil, err
		}

		// TODO: tmp bug fix (when index name is "")
		name := col.Name()
		if name == "" {
			name = "__name__"
		}
		row[name] = value
	}

	return row, nil
}
