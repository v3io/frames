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
	"encoding/json"
	"fmt"

	"github.com/v3io/frames"
	"github.com/v3io/frames/backends"
	"github.com/v3io/frames/backends/utils"
	"github.com/v3io/frames/v3ioutils"
	"github.com/v3io/v3io-go/pkg/dataplane"
)

const (
	indexColKey = "__name"
)

// Read does a read request
func (kv *Backend) Read(request *frames.ReadRequest) (frames.FrameIterator, error) {

	if request.Proto.MessageLimit == 0 {
		request.Proto.MessageLimit = 256 // TODO: More?
	}

	columns := request.Proto.Columns
	if len(columns) < 1 || columns[0] == "" {
		columns = []string{"*"}
	}

	container, tablePath, err := kv.newConnection(request.Proto.Session, request.Password.Get(), request.Token.Get(), request.Proto.Table, true)
	if err != nil {
		return nil, err
	}

	partitions, err := kv.getPartitions(tablePath, container, "")
	if err != nil {
		return nil, err
	}

	input := v3io.GetItemsInput{Path: "", Filter: request.Proto.Filter, AttributeNames: columns}
	kv.logger.DebugWith("read input", "input", input, "request", request)

	iter, err := v3ioutils.NewAsyncItemsCursor(
		container, &input, kv.numWorkers, request.Proto.ShardingKeys, kv.logger, 0, partitions)
	if err != nil {
		return nil, err
	}

	// Get Schema
	schemaInput := &v3io.GetObjectInput{Path: tablePath + ".#schema"}
	resp, err := container.GetObjectSync(schemaInput)
	if err != nil {
		return nil, err
	}
	schema := &v3ioutils.OldV3ioSchema{}
	if err := json.Unmarshal(resp.HTTPResponse.Body(), schema); err != nil {
		return nil, err
	}

	newKVIter := Iterator{request: request, iter: iter, keyColumnName: schema.Key, shouldDuplicateIndex: containsString(columns, schema.Key)}
	return &newKVIter, nil
}

// Iterator is key/value iterator
type Iterator struct {
	request              *frames.ReadRequest
	iter                 *v3ioutils.AsyncItemsCursor
	err                  error
	currFrame            frames.Frame
	keyColumnName        string
	shouldDuplicateIndex bool
}

// Next advances the iterator to next frame
func (ki *Iterator) Next() bool {
	var columns []frames.Column
	byName := map[string]frames.Column{}

	rowNum := 0
	numOfSchemaFiles := 0

	for ; rowNum < int(ki.request.Proto.MessageLimit) && ki.iter.Next(); rowNum++ {
		row := ki.iter.GetFields()

		// Skip table schema object
		rowIndex, ok := row[indexColKey]
		if (ok && rowIndex == ".#schema") || len(row) == 0 {
			numOfSchemaFiles++
			continue
		}
		// Indicates whether the key column exists as an attribute in addition to the object name (__name)
		_, hasKeyColumnAttribute := row[ki.keyColumnName]

		for name, field := range row {
			colName := name
			if colName == indexColKey { // convert `__name` attribute name to the key column
				if hasKeyColumnAttribute {
					continue
				}
				colName = ki.keyColumnName
			}

			col, ok := byName[colName]
			if !ok {
				data, err := utils.NewColumn(field, rowNum-numOfSchemaFiles)
				if err != nil {
					ki.err = err
					return false
				}

				col, err = frames.NewSliceColumn(colName, data)
				if err != nil {
					ki.err = err
					return false
				}
				columns = append(columns, col)
				byName[colName] = col
			}

			if err := utils.AppendColumn(col, field); err != nil {
				ki.err = err
				return false
			}
		}

		// fill columns with nil if there was no value
		for name, col := range byName {
			if name == ki.keyColumnName && !hasKeyColumnAttribute {
				name = indexColKey
			}
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

	var indices []frames.Column

	// If the only column that was requested is the key-column don't set it as an index.
	// Otherwise, set the key column (if requested) to be the index or not depending on the `ResetIndex` value.
	if !ki.request.Proto.ResetIndex && (len(columns) > 1 || columns[0].Name() != ki.keyColumnName) {
		indexCol, ok := byName[ki.keyColumnName]
		if ok {
			delete(byName, ki.keyColumnName)

			// If a user requested specific columns containing the index, duplicate the index column
			// to be an index and a column
			if ki.shouldDuplicateIndex {
				dupIndex := indexCol.CopyWithName(fmt.Sprintf("_%v", ki.keyColumnName))
				indices = []frames.Column{dupIndex}
			} else {
				indices = []frames.Column{indexCol}
				columns = utils.RemoveColumn(ki.keyColumnName, columns)
			}
		}
	}

	var err error
	ki.currFrame, err = frames.NewFrame(columns, indices, nil)
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

func init() {
	if err := backends.Register("kv", NewBackend); err != nil {
		panic(err)
	}
}

func (kv *Backend) getPartitions(path string, container v3io.Container, marker string) ([]string, error) {
	var partitions []string
	input := &v3io.GetContainerContentsInput{Path: path, DirectoriesOnly: true, Marker: marker}
	res, err := container.GetContainerContentsSync(input)
	if err != nil {
		return nil, err
	}
	out := res.Output.(*v3io.GetContainerContentsOutput)
	if len(out.CommonPrefixes) > 0 {
		for _, partition := range out.CommonPrefixes {
			parts, err := kv.getPartitions(partition.Prefix, container, "")
			if err != nil {
				return nil, err
			}
			partitions = append(partitions, parts...)
		}
	} else if input.Marker == "" { // Add a partition to the list if the folder does not have other directories
		partitions = append(partitions, path)
	}

	if out.NextMarker != "" {
		parts, err := kv.getPartitions(path, container, out.NextMarker)
		if err != nil {
			return nil, err
		}
		partitions = append(partitions, parts...)
	}

	return partitions, nil
}

func containsString(s []string, subString string) bool {
	for _, str := range s {
		if str == subString {
			return true
		}
	}

	return false
}
