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

	"github.com/v3io/frames"
	"github.com/v3io/frames/backends"
	"github.com/v3io/frames/backends/utils"
	"github.com/v3io/frames/pb"
	"github.com/v3io/frames/v3ioutils"
	"github.com/v3io/v3io-go/pkg/dataplane"
)

const (
	indexColKey = "__name"
)

var systemAttrs = []string{"__gid", "__mode", "__mtime_nsecs", "__mtime_secs", "__size", "__uid", "__ctime_nsecs", "__ctime_secs"}

// Read sends a read request
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

	partitions, err := kv.getPartitions(tablePath, container)
	if err != nil {
		return nil, err
	}

	// Create a new platform (v3io) connection with specific RequestChannel length
	container, tablePath, err = kv.newConnection(request.Proto.Session,
		request.Password.Get(),
		request.Token.Get(),
		request.Proto.Table,
		true)

	if err != nil {
		return nil, err
	}

	input := v3io.GetItemsInput{Filter: request.Proto.Filter, AttributeNames: columns, SortKeyRangeStart: request.Proto.SortKeyRangeStart, SortKeyRangeEnd: request.Proto.SortKeyRangeEnd}
	kv.logger.DebugWith("read input", "input", input, "request", request)

	iter, err := v3ioutils.NewAsyncItemsCursor(
		container, &input, kv.numWorkers, request.Proto.ShardingKeys, kv.logger, 0, partitions,
		request.Proto.SortKeyRangeStart, request.Proto.SortKeyRangeEnd)
	if err != nil {
		return nil, err
	}

	schemaInterface, err := v3ioutils.GetSchema(tablePath, container)
	if err != nil {
		return nil, err
	}
	schemaObj := schemaInterface.(*v3ioutils.OldV3ioSchema)

	shouldDuplicateSorting := schemaObj.SortingKey != "" && containsString(columns, schemaObj.SortingKey)
	newKVIter := Iterator{request: request, iter: iter, schema: schemaObj, shouldDuplicateIndex: containsString(columns, schemaObj.Key), shouldDuplicateSorting: shouldDuplicateSorting}
	return &newKVIter, nil
}

// Iterator is key/value iterator
type Iterator struct {
	request                *frames.ReadRequest
	iter                   *v3ioutils.AsyncItemsCursor
	err                    error
	currFrame              frames.Frame
	shouldDuplicateIndex   bool
	schema                 *v3ioutils.OldV3ioSchema
	shouldDuplicateSorting bool
}

// Next advances the iterator to next frame
func (ki *Iterator) Next() bool {
	var columns []frames.Column
	byName := map[string]frames.Column{}

	rowNum := 0
	numOfSchemaFiles := 0
	var nullColumns []*pb.NullValuesMap
	hasAnyNulls := false

	columnNamesToReturn := ki.request.Proto.Columns
	specificColumnsRequested := len(columnNamesToReturn) != 0

	// Create columns
	for _, field := range ki.schema.Fields {
		if specificColumnsRequested && !containsString(ki.request.Proto.Columns, field.Name) {
			continue
		} else if !specificColumnsRequested {
			columnNamesToReturn = append(columnNamesToReturn, field.Name)
		}

		f, err := ki.schema.GetField(field.Name)
		if err != nil {
			ki.err = err
			return false
		}
		data, err := utils.NewColumnFromType(f.Type, 0)
		if err != nil {
			ki.err = err
			return false
		}

		col, err := frames.NewSliceColumn(field.Name, data)
		if err != nil {
			ki.err = err
			return false
		}
		columns = append(columns, col)
		byName[field.Name] = col
	}

	indexKeyRequested := false
	if specificColumnsRequested && len(columnNamesToReturn) != len(columns) {
		if containsString(ki.request.Proto.Columns, indexColKey) {
			indexKeyRequested = true
			sysCol, err := frames.NewSliceColumn(indexColKey, make([]string, 0))
			if err != nil {
				ki.err = err
				return false
			}
			columns = append(columns, sysCol)
			byName[indexColKey] = sysCol
		}
		// If still not all columns found
		if len(columnNamesToReturn) != len(columns) {
			for _, attr := range systemAttrs {
				if containsString(ki.request.Proto.Columns, attr) {
					sysCol, err := frames.NewSliceColumn(attr, make([]int64, 0))
					if err != nil {
						ki.err = err
						return false
					}
					columns = append(columns, sysCol)
					byName[attr] = sysCol
				}
			}
		}
	}

	if specificColumnsRequested && len(columns) != len(ki.request.Proto.Columns) {
		// Requested a column that doesn't exist
		for _, reqCol := range ki.request.Proto.Columns {
			found := false
			for _, col := range columns {
				if reqCol == col.Name() {
					found = true
					break
				}
			}
			if !found {
				ki.err = fmt.Errorf("column '%v' doesn't exist", reqCol)
				return false
			}
		}
	}
	for ; rowNum < int(ki.request.Proto.MessageLimit) && ki.iter.Next(); rowNum++ {
		row := ki.iter.GetFields()

		// Skip table schema object
		rowIndex, ok := row[indexColKey]
		if (ok && rowIndex == ".#schema") || len(row) == 0 {
			numOfSchemaFiles++
			continue
		}
		// Indicates whether the key column exists as an attribute in addition to the object name (__name)
		_, hasKeyColumnAttribute := row[ki.schema.Key]

		for name, field := range row {
			colName := name
			if colName == indexColKey && !indexKeyRequested { // convert `__name` attribute name to the key column
				if hasKeyColumnAttribute {
					continue
				} else {
					colName = ki.schema.Key
				}
			}

			col, ok := byName[colName]
			if !ok {
				ki.err = fmt.Errorf("column '%v' for item with key: '%v' doesn't exist in the schema file. "+
					"Your data structure was probably changed; try re-inferring the schema for the table",
					colName, rowIndex)
				return false
			}

			if err := utils.AppendColumn(col, field); err != nil {
				ki.err = err
				return false
			}
		}

		// Fill columns with nil if there was no value
		var currentNullMask pb.NullValuesMap
		currentNullMask.NullColumns = make(map[string]bool)
		for _, fieldName := range columnNamesToReturn {
			name := fieldName
			if name == ki.schema.Key && !hasKeyColumnAttribute {
				name = indexColKey
			}
			if _, ok := row[name]; ok {
				continue
			}

			var err error
			err = utils.AppendNil(byName[name])
			if err != nil {
				ki.err = err
				return false
			}
			currentNullMask.NullColumns[name] = true
			hasAnyNulls = true
		}
		nullColumns = append(nullColumns, &currentNullMask)
	}

	if ki.iter.Err() != nil {
		ki.err = ki.iter.Err()
		return false
	}

	if rowNum == 0 {
		return false
	}

	var indices []frames.Column

	// If the only column that was requested is the key column, don't set it as an index.
	// Otherwise, set the key column (if requested) to be the index or not depending on the `ResetIndex` value.
	if len(columns) > 0 && !ki.request.Proto.ResetIndex {
		if len(columns) > 1 || columns[0].Name() != ki.schema.Key {
			ki.handleIndices(ki.schema.Key, byName, ki.shouldDuplicateIndex, &indices, &columns)
			delete(byName, ki.schema.Key)
		}
		if ki.schema.SortingKey != "" && (len(columns) > 1 || columns[0].Name() != ki.schema.SortingKey) {
			ki.handleIndices(ki.schema.SortingKey, byName, ki.shouldDuplicateSorting, &indices, &columns)
			delete(byName, ki.schema.SortingKey)
		}
	}

	if !hasAnyNulls {
		nullColumns = nil
	}
	var err error
	ki.currFrame, err = frames.NewFrameWithNullValues(columns, indices, nil, nullColumns)
	if err != nil {
		ki.err = err
		return false
	}

	return true
}

func (ki *Iterator) handleIndices(index string, data map[string]frames.Column, shouldDup bool, indices *[]frames.Column, columns *[]frames.Column) {
	col, ok := data[index]
	if ok {
		// If a user requested specific columns containing the index, duplicate
		// the index column to be an index and a column
		if shouldDup {
			dupIndex := col.CopyWithName(fmt.Sprintf("_%v", index))
			*indices = append(*indices, dupIndex)
		} else {
			*indices = append(*indices, col)
			*columns = utils.RemoveColumn(index, *columns)
		}
	}
}

// Err returns the last error
func (ki *Iterator) Err() error {
	return ki.err
}

// At returns the current frames
func (ki *Iterator) At() frames.Frame {
	return ki.currFrame
}

func init() {
	if err := backends.Register("kv", NewBackend); err != nil {
		panic(err)
	}
}

func (kv *Backend) getPartitions(path string, container v3io.Container) ([]string, error) {
	var partitions []string
	var done bool
	var marker string
	for !done {
		input := &v3io.GetContainerContentsInput{Path: path, DirectoriesOnly: true, Marker: marker}
		res, err := container.GetContainerContentsSync(input)
		if err != nil {
			return nil, err
		}
		if res != nil {
			res.Release() // Releasing underlying fasthttp response
		}
		out := res.Output.(*v3io.GetContainerContentsOutput)
		if len(out.CommonPrefixes) > 0 {
			for _, partition := range out.CommonPrefixes {
				parts, err := kv.getPartitions(partition.Prefix, container)
				if err != nil {
					return nil, err
				}
				partitions = append(partitions, parts...)
			}
			// Add a partition to the list if this is a leaf folder and it's the first time we query the folder
		} else if marker == "" {
			partitions = append(partitions, path)
		}
		marker = out.NextMarker
		done = !out.IsTruncated
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
