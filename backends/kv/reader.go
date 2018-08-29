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

package kv

import (
	"fmt"
	"strings"

	"github.com/v3io/v3io-go-http"

	"github.com/v3io/frames"
	"github.com/v3io/frames/backends/utils"
	"github.com/v3io/frames/v3ioutils"
)

// Backend is key/value backend
type Backend struct {
	ctx *frames.DataContext
}

// NewBackend return a new key/value backend
func NewBackend(ctx *frames.DataContext) (frames.DataBackend, error) {
	newBackend := Backend{ctx: ctx}
	return &newBackend, nil
}

// Read does a read request
func (kv *Backend) Read(request *frames.ReadRequest) (frames.FrameIterator, error) {
	kvRequest, ok := request.Extra.(frames.KVRead)
	if !ok {
		return nil, fmt.Errorf("not a KV request")
	}

	tablePath := request.Table
	if !strings.HasSuffix(tablePath, "/") {
		tablePath += "/"
	}

	if request.MaxInMessage == 0 {
		request.MaxInMessage = 256 // TODO: More?
	}

	input := v3io.GetItemsInput{Path: tablePath, Filter: request.Filter, AttributeNames: request.Columns}
	fmt.Println(input, request)
	iter, err := v3ioutils.NewAsyncItemsCursor(kv.ctx.Container, &input, kv.ctx.Workers, kvRequest.ShardingKeys)
	if err != nil {
		return nil, err
	}

	newKVIter := Iterator{ctx: kv.ctx, request: request, iter: iter}
	return &newKVIter, nil
}

// Iterator is key/value iterator
type Iterator struct {
	ctx       *frames.DataContext
	request   *frames.ReadRequest
	iter      *v3ioutils.AsyncItemsCursor
	err       error
	currFrame frames.Frame
}

// Next advances the iterator to next frame
func (ki *Iterator) Next() bool {
	var columns []frames.Column
	var byName map[string]frames.Column

	rowNum := 0
	for ; rowNum < ki.request.MaxInMessage && ki.iter.Next(); rowNum++ {
		row := ki.iter.GetFields()
		for name, field := range row {
			col, ok := byName[name]
			if !ok {
				data, err := utils.NewColumn(field, rowNum-1)
				if err != nil {
					ki.err = err
					return false
				}

				col, err := frames.NewSliceColumn(name, data)
				columns = append(columns, col)
				byName[name] = col
			}

			if err := col.Append(field); err != nil {
				ki.err = err
				return false
			}
		}

		// fill columns with nil if there was no value
		for name, col := range byName {
			if _, ok := row[name]; ok {
				continue
			}

			var err error
			err = utils.AppendNil(col)
			if err != nil {
				ki.err = err
				return false
			}
		}
	}

	if ki.iter.Err() != nil {
		ki.err = ki.iter.Err()
		return false
	}

	if rowNum == 0 {
		return false
	}

	var err error
	ki.currFrame, err = frames.NewMapFrame(columns)
	if err != nil {
		ki.err = err
		return false
	}

	return true
}

// Err return the last error
func (ki *Iterator) Err() error {
	return ki.err
}

// At return the current frames
func (ki *Iterator) At() frames.Frame {
	return ki.currFrame
}
