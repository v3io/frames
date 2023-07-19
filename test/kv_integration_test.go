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

package test

import (
	"fmt"
	"github.com/v3io/frames/pb"
	"github.com/v3io/frames/v3ioutils"
	"strings"
	"time"

	"github.com/nuclio/logger"
	"github.com/stretchr/testify/suite"
	"github.com/v3io/frames"
	v3io "github.com/v3io/v3io-go/pkg/dataplane"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/tsdbtest"
)

type KvTestSuite struct {
	suite.Suite
	tablePath      string
	suiteTimestamp int64
	basicQueryTime int64
	client         frames.Client
	backendName    string
	v3ioContainer  v3io.Container
	internalLogger logger.Logger
}

func GetKvTestsConstructorFunc() SuiteCreateFunc {
	return func(client frames.Client, v3ioContainer v3io.Container, internalLogger logger.Logger) suite.TestingSuite {
		return &KvTestSuite{client: client,
			backendName:    "kv",
			v3ioContainer:  v3ioContainer,
			internalLogger: internalLogger}
	}
}

func (kvSuite *KvTestSuite) toMillis(date string) int64 {
	time, err := tsdbtest.DateStringToMillis(date)
	kvSuite.Require().NoError(err)
	return time
}

func (kvSuite *KvTestSuite) SetupSuite() {
	kvSuite.Require().NotNil(kvSuite.client, "client not set")
}

func (kvSuite *KvTestSuite) generateRandomSampleFrameWithEmptyIndices(size int, indexNames []string, emptyIndices map[string]bool, columnNames []string) frames.Frame {
	kvSuite.Require().True(len(indexNames) <= 2, "KV API supports no more than two indices")
	var indexColumns []frames.Column

	emptyIndexValues := make([]string, size)
	uniqueIndexValues := make([]string, size)
	for i := 0; i < size; i++ {
		emptyIndexValues[i] = ""
		uniqueIndexValues[i] = fmt.Sprintf("%d", i)
	}

	for _, indexName := range indexNames {
		var indexColumn frames.Column
		var err error
		if ok := emptyIndices[indexName]; ok {
			indexColumn, err = frames.NewSliceColumn(indexName, emptyIndexValues)
		} else {
			indexColumn, err = frames.NewSliceColumn(indexName, uniqueIndexValues)
		}
		kvSuite.Require().NoError(err)

		indexColumns = append(indexColumns, indexColumn)
	}

	columns := make([]frames.Column, len(columnNames))
	for i, name := range columnNames {
		columns[i] = FloatCol(kvSuite.T(), name, size)
	}

	frame, err := frames.NewFrame(columns, indexColumns, nil)
	kvSuite.Require().NoError(err)

	return frame
}

func (kvSuite *KvTestSuite) generateRandomSampleFrame(size int, indexName string, columnNames []string) frames.Frame {
	var icol frames.Column

	index := make([]string, size)
	for i := 0; i < size; i++ {
		index[i] = fmt.Sprintf("%v%v", i, time.Now().Nanosecond())
	}

	icol, err := frames.NewSliceColumn(indexName, index)
	kvSuite.Require().NoError(err)

	columns := make([]frames.Column, len(columnNames))
	for i, name := range columnNames {
		columns[i] = FloatCol(kvSuite.T(), name, size)
	}

	frame, err := frames.NewFrame(columns, []frames.Column{icol}, nil)
	kvSuite.Require().NoError(err)

	return frame
}

func (kvSuite *KvTestSuite) generateSequentialSampleFrame(size int, indexName string, columnNames []string) frames.Frame {
	var icol frames.Column

	index := make([]int, size)
	for i := 0; i < size; i++ {
		index[i] = i
	}

	icol, err := frames.NewSliceColumn(indexName, index)
	kvSuite.Require().NoError(err)

	columns := make([]frames.Column, len(columnNames))
	for i, name := range columnNames {
		columns[i] = FloatCol(kvSuite.T(), name, size)
	}

	frame, err := frames.NewFrame(columns, []frames.Column{icol}, nil)
	kvSuite.Require().NoError(err)

	return frame
}

//func (kvSuite *KvTestSuite) TestWriteToExistingFolderWithoutSchema() {
//	table := fmt.Sprintf("frames_ci/TestWriteToExistingFolderWithoutSchema%d", time.Now().UnixNano())
//
//	columnNames := []string{"n1", "n2"}
//	frame := kvSuite.generateSequentialSampleFrame(3, "idx", columnNames)
//	wreq := &frames.WriteRequest{
//		Backend:  kvSuite.backendName,
//		Table:    table,
//		SaveMode: frames.ErrorIfTableExists,
//	}
//
//	appender, err := kvSuite.client.Write(wreq)
//	kvSuite.Require().NoError(err)
//
//	err = appender.Add(frame)
//	kvSuite.Require().NoError(err)
//
//	err = appender.WaitForComplete(time.Second)
//	kvSuite.Require().NoError(err)
//
//	//delete schema
//	schemaInput := &v3io.DeleteObjectInput{Path: table + "/.#schema"}
//	err = kvSuite.v3ioContainer.DeleteObjectSync(schemaInput)
//	kvSuite.Require().NoError(err)
//	//write again
//	appender, err = kvSuite.client.Write(wreq)
//	kvSuite.Require().NoError(err, "failed to write frame")
//
//	err = appender.Add(frame)
//	kvSuite.Require().NoError(err, "failed to add frame")
//
//	err = appender.WaitForComplete(time.Second)
//	kvSuite.Require().Error(err, "writing to an existing folder without a schema should fail")
//
//}

func (kvSuite *KvTestSuite) generateSequentialSampleFrameWithTypes(size int, indexName string, columnNames map[string]string) frames.Frame {
	var icol frames.Column

	index := make([]int, size)
	for i := 0; i < size; i++ {
		index[i] = i
	}

	icol, err := frames.NewSliceColumn(indexName, index)
	kvSuite.Require().NoError(err)

	columns := make([]frames.Column, len(columnNames))
	i := 0
	for columnName, columnType := range columnNames {
		switch columnType {
		case "string":
			columns[i] = StringCol(kvSuite.T(), columnName, size)
		case "float":
			columns[i] = FloatCol(kvSuite.T(), columnName, size)
		case "bool":
			columns[i] = BoolCol(kvSuite.T(), columnName, size)
		case "time":
			columns[i] = TimeCol(kvSuite.T(), columnName, size)
		case "int":
			columns[i] = IntCol(kvSuite.T(), columnName, size)
		default:
			kvSuite.T().Fatalf("type %v not supported", columnType)
		}

		i++
	}

	frame, err := frames.NewFrame(columns, []frames.Column{icol}, nil)
	kvSuite.Require().NoError(err)

	return frame
}

//func (kvSuite *KvTestSuite) TestAll() {
//	table := fmt.Sprintf("frames_ci/kv_test_all%d", time.Now().UnixNano())
//
//	kvSuite.T().Log("write")
//	frame := kvSuite.generateRandomSampleFrame(5, "idx", []string{"n1", "n2", "n3"})
//	wreq := &frames.WriteRequest{
//		Backend: kvSuite.backendName,
//		Table:   table,
//	}
//
//	appender, err := kvSuite.client.Write(wreq)
//	kvSuite.Require().NoError(err)
//
//	err = appender.Add(frame)
//	kvSuite.Require().NoError(err)
//
//	err = appender.WaitForComplete(10 * time.Second)
//	kvSuite.Require().NoError(err)
//
//	time.Sleep(3 * time.Second) // Let DB sync
//
//	kvSuite.T().Log("read")
//	rreq := &pb.ReadRequest{
//		Backend: kvSuite.backendName,
//		Table:   table,
//	}
//
//	it, err := kvSuite.client.Read(rreq)
//	kvSuite.Require().NoError(err)
//
//	for it.Next() {
//		// TODO: More checks
//		fr := it.At()
//		kvSuite.Require().Contains([]int{fr.Len(), fr.Len() - 1}, frame.Len(), "wrong length")
//	}
//
//	kvSuite.Require().NoError(it.Err())
//
//	kvSuite.T().Log("delete")
//	dreq := &pb.DeleteRequest{
//		Backend: kvSuite.backendName,
//		Table:   table,
//	}
//
//	err = kvSuite.client.Delete(dreq)
//	kvSuite.Require().NoError(err)
//}
//
//func (kvSuite *KvTestSuite) TestRangeScan() {
//	table := fmt.Sprintf("frames_ci/kv_range_scan%d", time.Now().UnixNano())
//
//	index := []string{"mike", "joe", "mike", "jim", "mike"}
//	icol, err := frames.NewSliceColumn("key", index)
//	kvSuite.Require().NoError(err)
//	sorting := []string{"aa", "cc", "bb", "aa", "dd"}
//	sortcol, err := frames.NewSliceColumn("sorting", sorting)
//	kvSuite.Require().NoError(err)
//
//	columns := []frames.Column{
//		StringCol(kvSuite.T(), "n1", len(index)),
//	}
//
//	frame, err := frames.NewFrame(columns, []frames.Column{icol, sortcol}, nil)
//	kvSuite.Require().NoError(err)
//
//	wreq := &frames.WriteRequest{
//		Backend: kvSuite.backendName,
//		Table:   table,
//	}
//
//	appender, err := kvSuite.client.Write(wreq)
//	kvSuite.Require().NoError(err)
//
//	err = appender.Add(frame)
//	kvSuite.Require().NoError(err)
//
//	err = appender.WaitForComplete(10 * time.Second)
//	kvSuite.Require().NoError(err)
//
//	//check schema
//	schemaInput := &v3io.GetObjectInput{Path: table + "/.#schema"}
//	resp, err := kvSuite.v3ioContainer.GetObjectSync(schemaInput)
//	kvSuite.Require().NoError(err)
//	schema := &v3ioutils.OldV3ioSchema{}
//	err = json.Unmarshal(resp.HTTPResponse.Body(), schema)
//	kvSuite.Require().NoError(err)
//	kvSuite.Require().Equal("key", schema.Key)
//	kvSuite.Require().Equal("sorting", schema.SortingKey)
//	kvSuite.Require().Len(schema.Fields, 3, "wrong number of columns in schema")
//	////
//	rreq := &pb.ReadRequest{
//		Backend:           kvSuite.backendName,
//		Table:             table,
//		ShardingKeys:      []string{"mike", "joe"},
//		SortKeyRangeStart: "bb",
//	}
//
//	it, err := kvSuite.client.Read(rreq)
//	kvSuite.Require().NoError(err)
//	for it.Next() {
//		frame = it.At()
//		if len(frame.Indices()) != 2 {
//			kvSuite.T().Fatal("wrong number of indices, expected 2, got ", len(frame.Indices()))
//		}
//		indexCol := frame.Indices()[0]
//		sortcol := frame.Indices()[1]
//		if frame.Len() != 3 {
//			kvSuite.T().Fatal("got different number of results, expected 3, got ", frame.Len())
//		}
//		for i := 0; i < frame.Len(); i++ {
//			currentKey, _ := indexCol.StringAt(i)
//			sorting, _ := sortcol.StringAt(i)
//			if !((currentKey == "mike" || currentKey == "joe") && sorting >= "bb") {
//				kvSuite.T().Fatal("key name does not match, expected mike.bb, joe.cc, or mike.dd, got ", frame)
//			}
//		}
//	}
//	////
//	rreq = &pb.ReadRequest{
//		Backend:           kvSuite.backendName,
//		Table:             table,
//		ShardingKeys:      []string{"mike"},
//		SortKeyRangeStart: "aa",
//		SortKeyRangeEnd:   "cc",
//	}
//
//	it, err = kvSuite.client.Read(rreq)
//	kvSuite.Require().NoError(err)
//
//	for it.Next() {
//		frame = it.At()
//		kvSuite.Require().Len(frame.Indices(), 2, "wrong number of indices")
//		indexCol := frame.Indices()[0]
//		sortcol := frame.Indices()[1]
//		kvSuite.Require().Equal(2, frame.Len(), "got different number of results")
//		for i := 0; i < frame.Len(); i++ {
//			currentKey, _ := indexCol.StringAt(i)
//			sorting, _ := sortcol.StringAt(i)
//			kvSuite.Require().Equal("mike", currentKey)
//			kvSuite.Require().Contains([]string{"aa", "bb"}, sorting)
//		}
//	}
//}
//
//func (kvSuite *KvTestSuite) TestNullValuesWrite() {
//	table := fmt.Sprintf("frames_ci/kv_test_nulls%d", time.Now().UnixNano())
//
//	index := []string{"mike", "joe", "jim"}
//	icol, err := frames.NewSliceColumn("idx", index)
//	kvSuite.Require().NoError(err)
//
//	columns := []frames.Column{
//		FloatCol(kvSuite.T(), "n1", len(index)),
//		StringCol(kvSuite.T(), "n2", len(index)),
//		BoolCol(kvSuite.T(), "n3", len(index)),
//		TimeCol(kvSuite.T(), "n4", len(index)),
//	}
//
//	nullValues := initializeNullColumns(len(index))
//	nullValues[0].NullColumns["n1"] = true
//
//	nullValues[1].NullColumns["n2"] = true
//	nullValues[1].NullColumns["n3"] = true
//	nullValues[1].NullColumns["n4"] = true
//
//	frame, err := frames.NewFrameWithNullValues(columns, []frames.Column{icol}, nil, nullValues)
//	kvSuite.Require().NoError(err)
//
//	kvSuite.T().Log("write")
//	wreq := &frames.WriteRequest{
//		Backend: kvSuite.backendName,
//		Table:   table,
//	}
//
//	appender, err := kvSuite.client.Write(wreq)
//	kvSuite.Require().NoError(err)
//
//	err = appender.Add(frame)
//	kvSuite.Require().NoError(err)
//
//	err = appender.WaitForComplete(10 * time.Second)
//	kvSuite.Require().NoError(err)
//
//	input := v3io.GetItemsInput{AttributeNames: []string{"__name", "n1", "n2", "n3", "n4"}}
//
//	iter, err := v3ioutils.NewAsyncItemsCursor(
//		kvSuite.v3ioContainer, &input, 1,
//		nil, kvSuite.internalLogger,
//		0, []string{table + "/"},
//		"", "")
//
//	for iter.Next() {
//		currentRow := iter.GetItem()
//
//		key, _ := currentRow.GetFieldString("__name")
//		switch key {
//		case ".#schema":
//			continue
//		case "mike":
//			kvSuite.Require().Nil(currentRow.GetField("n1"),
//				"item %v - key n1 supposed to be null but got %v", key, currentRow.GetField("n1"))
//
//			kvSuite.Require().NotNil(currentRow.GetField("n2"),
//				"item %v - key n2 supposed to be null but got %v", key, currentRow.GetField("n2"))
//			kvSuite.Require().NotNil(currentRow.GetField("n3"),
//				"item %v - key n3 supposed to be null but got %v", key, currentRow.GetField("n3"))
//			kvSuite.Require().NotNil(currentRow.GetField("n4"),
//				"item %v - key n4 supposed to be null but got %v", key, currentRow.GetField("n4"))
//		case "joe":
//			kvSuite.Require().NotNil(currentRow.GetField("n1"),
//				"item %v - key n1 supposed to be null but got %v", key, currentRow.GetField("n1"))
//
//			kvSuite.Require().Nil(currentRow.GetField("n2"),
//				"item %v - key n2 supposed to be null but got %v", key, currentRow.GetField("n2"))
//			kvSuite.Require().Nil(currentRow.GetField("n3"),
//				"item %v - key n3 supposed to be null but got %v", key, currentRow.GetField("n3"))
//			kvSuite.Require().Nil(currentRow.GetField("n4"),
//				"item %v - key n4 supposed to be null but got %v", key, currentRow.GetField("n4"))
//		case "jim":
//			kvSuite.Require().NotNil(currentRow.GetField("n1"),
//				"item %v - key n1 supposed to be null but got %v", key, currentRow.GetField("n1"))
//			kvSuite.Require().NotNil(currentRow.GetField("n2"),
//				"item %v - key n2 supposed to be null but got %v", key, currentRow.GetField("n2"))
//			kvSuite.Require().NotNil(currentRow.GetField("n3"),
//				"item %v - key n3 supposed to be null but got %v", key, currentRow.GetField("n3"))
//			kvSuite.Require().NotNil(currentRow.GetField("n4"),
//				"item %v - key n4 supposed to be null but got %v", key, currentRow.GetField("n4"))
//		default:
//			kvSuite.T().Fatalf("got an unexpected key '%v'", key)
//		}
//	}
//
//	kvSuite.Require().NoError(iter.Err(), "error querying items got")
//}
//
//// IG-19426
//func (kvSuite *KvTestSuite) TestNullColumnWrite() {
//	table := fmt.Sprintf("frames_ci/kv_test_nulls%d", time.Now().UnixNano())
//
//	index := []string{"mike", "joe", "jim"}
//	indexCol := &pb.Column{
//		Kind:    pb.Column_SLICE,
//		Name:    "idx",
//		Dtype:   pb.DType_STRING,
//		Strings: index,
//	}
//
//	columns := []*pb.Column{
//		{
//			Kind:  pb.Column_SLICE,
//			Name:  "n1",
//			Dtype: pb.DType_NULL,
//		},
//		{
//			Kind:  pb.Column_SLICE,
//			Name:  "n2",
//			Dtype: pb.DType_INTEGER,
//			Ints:  []int64{1, 2, 3},
//		},
//	}
//
//	nullValues := initializeNullColumns(len(index))
//	nullValues[0].NullColumns["n1"] = true
//	nullValues[1].NullColumns["n1"] = true
//	nullValues[2].NullColumns["n1"] = true
//
//	frame := frames.NewFrameFromProto(&pb.Frame{
//		Columns:    columns,
//		Indices:    []*pb.Column{indexCol},
//		NullValues: nullValues,
//	})
//
//	wreq := &frames.WriteRequest{
//		Backend: kvSuite.backendName,
//		Table:   table,
//	}
//
//	appender, err := kvSuite.client.Write(wreq)
//	kvSuite.Require().NoError(err)
//
//	err = appender.Add(frame)
//	kvSuite.Require().NoError(err)
//
//	err = appender.WaitForComplete(10 * time.Second)
//	kvSuite.Require().NoError(err)
//
//	input := v3io.GetItemsInput{AttributeNames: []string{"__name", "n1", "n2"}}
//
//	iter, err := v3ioutils.NewAsyncItemsCursor(
//		kvSuite.v3ioContainer, &input, 1,
//		nil, kvSuite.internalLogger,
//		0, []string{table + "/"},
//		"", "")
//
//	var numRows int
//	for iter.Next() {
//		currentRow := iter.GetItem()
//
//		key, _ := currentRow.GetFieldString("__name")
//
//		if key != ".#schema" {
//			numRows++
//		}
//
//		switch key {
//		case ".#schema":
//			continue
//		case "mike":
//			kvSuite.Require().Nil(currentRow.GetField("n1"),
//				"item %v - key n1 supposed to be null but got %v", key, currentRow.GetField("n1"))
//
//			kvSuite.Require().NotNil(currentRow.GetField("n2"),
//				"item %v - key n2 supposed to be null but got %v", key, currentRow.GetField("n2"))
//		case "joe":
//			kvSuite.Require().Nil(currentRow.GetField("n1"),
//				"item %v - key n1 supposed to be null but got %v", key, currentRow.GetField("n1"))
//
//			kvSuite.Require().NotNil(currentRow.GetField("n2"),
//				"item %v - key n2 supposed to be null but got %v", key, currentRow.GetField("n2"))
//		case "jim":
//			kvSuite.Require().Nil(currentRow.GetField("n1"),
//				"item %v - key n1 supposed to be null but got %v", key, currentRow.GetField("n1"))
//			kvSuite.Require().NotNil(currentRow.GetField("n2"),
//				"item %v - key n2 supposed to be null but got %v", key, currentRow.GetField("n2"))
//		default:
//			kvSuite.T().Fatalf("got an unexpected key '%v'", key)
//		}
//	}
//
//	kvSuite.Require().Equal(3, numRows)
//
//	kvSuite.Require().NoError(iter.Err(), "error querying items got")
//}
//
//func (kvSuite *KvTestSuite) TestNullValuesRead() {
//	table := fmt.Sprintf("frames_ci/kv_test_nulls_read%d", time.Now().UnixNano())
//
//	data := make(map[string]map[string]interface{})
//	data["mike"] = map[string]interface{}{"idx": "mike", "n2": "dsad", "n3": true, "n4": time.Now()}
//	data["joe"] = map[string]interface{}{"idx": "joe", "n1": 12.2}
//	data["jim"] = map[string]interface{}{"idx": "jim", "n1": 66.6, "n2": "XXX", "n3": true, "n4": time.Now()}
//
//	input := &v3io.PutItemsInput{Path: table, Items: data}
//	res, err := kvSuite.v3ioContainer.PutItemsSync(input)
//	kvSuite.Require().NoError(err, "error putting test data")
//	kvSuite.Require().NoError(res.Error, "error putting test data")
//
//	oldSchema := v3ioutils.NewSchema("idx", "")
//	schema := v3ioutils.NewSchema("idx", "")
//	_ = schema.AddField("idx", "", false)
//	_ = schema.AddField("n1", 12.6, true)
//	_ = schema.AddField("n2", "", true)
//	_ = schema.AddField("n3", true, true)
//	_ = schema.AddField("n4", time.Now(), true)
//	err = oldSchema.UpdateSchema(kvSuite.v3ioContainer, table+"/", schema)
//	kvSuite.Require().NoError(err, "failed to create schema")
//
//	rreq := &pb.ReadRequest{
//		Backend: kvSuite.backendName,
//		Table:   table,
//	}
//
//	it, err := kvSuite.client.Read(rreq)
//	kvSuite.Require().NoError(err)
//
//	for it.Next() {
//		// TODO: More checks
//		frame := it.At()
//		if frame.Len() != len(data) {
//			kvSuite.T().Fatalf("wrong length: %d != %d", frame.Len(), frame.Len())
//		}
//
//		indexCol := frame.Indices()[0]
//		for i := 0; i < frame.Len(); i++ {
//			currentKey, _ := indexCol.StringAt(i)
//			for _, columnName := range frame.Names() {
//				// Checking that the desired Null values are indeed null
//				if currentKey == "mike" && columnName == "n1" {
//					kvSuite.Require().True(frame.IsNull(i, columnName), "key %v and column %v expected to be null but is not", currentKey, columnName)
//				} else if currentKey == "joe" &&
//					(columnName == "n2" || columnName == "n3" || columnName == "n4") {
//					kvSuite.Require().True(frame.IsNull(i, columnName), "key %v and column %v expected to be null but is not", currentKey, columnName)
//				} else {
//					kvSuite.Require().False(frame.IsNull(i, columnName), "key %v and column %v expected to have value but got Null", currentKey, columnName)
//				}
//			}
//		}
//	}
//
//	kvSuite.Require().NoError(it.Err())
//}
//
//func (kvSuite *KvTestSuite) TestRequestSpecificColumns() {
//	table := fmt.Sprintf("frames_ci/TestRequestSpecificColumns%d", time.Now().UnixNano())
//
//	frame := kvSuite.generateRandomSampleFrame(6, "idx", []string{"n1", "n2", "n3"})
//	wreq := &frames.WriteRequest{
//		Backend: kvSuite.backendName,
//		Table:   table,
//	}
//
//	appender, err := kvSuite.client.Write(wreq)
//	kvSuite.Require().NoError(err)
//
//	err = appender.Add(frame)
//	kvSuite.Require().NoError(err)
//
//	err = appender.WaitForComplete(10 * time.Second)
//	kvSuite.Require().NoError(err)
//
//	time.Sleep(3 * time.Second) // Let DB sync
//
//	requestedColumns := []string{"n1", "n2"}
//	rreq := &pb.ReadRequest{
//		Backend: kvSuite.backendName,
//		Table:   table,
//		Columns: requestedColumns,
//	}
//
//	it, err := kvSuite.client.Read(rreq)
//	kvSuite.Require().NoError(err)
//
//	for it.Next() {
//		fr := it.At()
//		kvSuite.Require().Contains([]int{fr.Len(), fr.Len() - 1}, frame.Len(), "wrong length")
//		kvSuite.Require().EqualValues(requestedColumns, fr.Names(), "got other columns than requested")
//	}
//
//	err = it.Err()
//	kvSuite.Require().NoError(err)
//}
//
//func (kvSuite *KvTestSuite) TestRequestSpecificColumnsWithKey() {
//	table := fmt.Sprintf("frames_ci/TestRequestSpecificColumnsWithKey%d", time.Now().UnixNano())
//
//	frame := kvSuite.generateRandomSampleFrame(6, "idx", []string{"n1", "n2", "n3"})
//	wreq := &frames.WriteRequest{
//		Backend: kvSuite.backendName,
//		Table:   table,
//	}
//
//	appender, err := kvSuite.client.Write(wreq)
//	kvSuite.Require().NoError(err)
//
//	err = appender.Add(frame)
//	kvSuite.Require().NoError(err)
//
//	err = appender.WaitForComplete(10 * time.Second)
//	kvSuite.Require().NoError(err)
//
//	time.Sleep(3 * time.Second) // Let DB sync
//
//	requestedColumns := []string{"idx", "n1", "n2"}
//	rreq := &pb.ReadRequest{
//		Backend: kvSuite.backendName,
//		Table:   table,
//		Columns: requestedColumns,
//	}
//
//	it, err := kvSuite.client.Read(rreq)
//	kvSuite.Require().NoError(err)
//
//	for it.Next() {
//		fr := it.At()
//		kvSuite.Require().Contains([]int{fr.Len(), fr.Len() - 1}, frame.Len(), "wrong length")
//		kvSuite.Require().EqualValues(requestedColumns, fr.Names(), "got other columns than requested")
//		kvSuite.Require().Equal("_idx", fr.Indices()[0].Name(), "got wrong index name")
//	}
//
//	err = it.Err()
//	kvSuite.Require().NoError(err)
//}
//
//func (kvSuite *KvTestSuite) TestDeleteWithFilter() {
//	table := fmt.Sprintf("frames_ci/kv_delete_filter%d", time.Now().UnixNano())
//
//	frame := kvSuite.generateRandomSampleFrame(5, "idx", []string{"n1", "n2"})
//	wreq := &frames.WriteRequest{
//		Backend: kvSuite.backendName,
//		Table:   table,
//	}
//
//	appender, err := kvSuite.client.Write(wreq)
//	kvSuite.Require().NoError(err)
//
//	err = appender.Add(frame)
//	kvSuite.Require().NoError(err)
//
//	err = appender.WaitForComplete(10 * time.Second)
//	kvSuite.Require().NoError(err)
//
//	kvSuite.T().Log("delete")
//	dreq := &pb.DeleteRequest{
//		Backend: kvSuite.backendName,
//		Table:   table,
//		Filter:  "__mtime_secs > 0",
//	}
//
//	err = kvSuite.client.Delete(dreq)
//	kvSuite.Require().NoError(err)
//
//	// check only schema is left
//	kvSuite.T().Log("read")
//	rreq := &pb.ReadRequest{
//		Backend: kvSuite.backendName,
//		Table:   table,
//	}
//
//	it, err := kvSuite.client.Read(rreq)
//	kvSuite.Require().NoError(err)
//
//	for it.Next() {
//		frame := it.At()
//		kvSuite.Require().Equal(frame.Len(), 0, "wrong length: %d != %d")
//	}
//	//make sure schema is not deleted
//	schemaInput := &v3io.GetObjectInput{Path: table + "/.#schema"}
//	_, err = kvSuite.v3ioContainer.GetObjectSync(schemaInput)
//	kvSuite.Require().NoError(err, "schema is not found ")
//}
//
//func (kvSuite *KvTestSuite) TestRequestSystemAttrs() {
//	table := fmt.Sprintf("frames_ci/TestRequestSystemAttrs%d", time.Now().UnixNano())
//
//	frame := kvSuite.generateRandomSampleFrame(5, "idx", []string{"n1", "n2"})
//	wreq := &frames.WriteRequest{
//		Backend: kvSuite.backendName,
//		Table:   table,
//	}
//
//	appender, err := kvSuite.client.Write(wreq)
//	kvSuite.Require().NoError(err)
//
//	err = appender.Add(frame)
//	kvSuite.Require().NoError(err)
//
//	err = appender.WaitForComplete(10 * time.Second)
//	kvSuite.Require().NoError(err)
//
//	time.Sleep(3 * time.Second) // Let DB sync
//
//	requestedColumns := []string{"idx", "n1", "__name", "__gid", "__mode", "__mtime_nsecs", "__mtime_secs", "__size", "__uid", "__ctime_nsecs", "__ctime_secs", "__atime_secs", "__atime_nsecs", "__obj_type", "__collection_id"}
//	rreq := &pb.ReadRequest{
//		Backend: kvSuite.backendName,
//		Table:   table,
//		Columns: requestedColumns,
//	}
//
//	it, err := kvSuite.client.Read(rreq)
//	kvSuite.Require().NoError(err)
//
//	for it.Next() {
//		fr := it.At()
//		kvSuite.Require().EqualValues(requestedColumns, fr.Names(), "got other columns than requested")
//	}
//
//	kvSuite.Require().NoError(it.Err())
//}
//func (kvSuite *KvTestSuite) TestNonExistingColumns() {
//	table := fmt.Sprintf("frames_ci/TestNonExistingColumns%d", time.Now().UnixNano())
//
//	frame := kvSuite.generateRandomSampleFrame(5, "idx", []string{"n1", "n2"})
//	wreq := &frames.WriteRequest{
//		Backend: kvSuite.backendName,
//		Table:   table,
//	}
//
//	appender, err := kvSuite.client.Write(wreq)
//	kvSuite.Require().NoError(err)
//
//	err = appender.Add(frame)
//	kvSuite.Require().NoError(err)
//
//	err = appender.WaitForComplete(10 * time.Second)
//	kvSuite.Require().NoError(err)
//
//	kvSuite.T().Log("read")
//	rreq := &pb.ReadRequest{
//		Backend: kvSuite.backendName,
//		Table:   table,
//		Columns: []string{"n1", "kk"},
//	}
//
//	it, _ := kvSuite.client.Read(rreq)
//	it.Next()
//	kvSuite.Require().Error(it.Err(), "error was expected when reading a non existing column")
//}
//
//func (kvSuite *KvTestSuite) TestUpdateItemNoKey() {
//	table := fmt.Sprintf("frames_ci/TestUpdateItemNoKey_%d", time.Now().UnixNano())
//	requireCtx := kvSuite.Require()
//
//	columnNames := []string{"col_1", "col_2"}
//	indexNames := []string{"key", "sorting_key"}
//
//	frame := kvSuite.generateRandomSampleFrameWithEmptyIndices(
//		3,
//		indexNames,
//		map[string]bool{indexNames[0]: true},
//		columnNames)
//
//	writeRequest := &frames.WriteRequest{
//		Backend:  kvSuite.backendName,
//		Table:    table,
//		SaveMode: frames.UpdateItem,
//	}
//
//	appender, err := kvSuite.client.Write(writeRequest)
//	requireCtx.NoError(err, "failed to create appender")
//
//	err = appender.Add(frame)
//	requireCtx.NoError(err, "failed to write frame")
//
//	err = appender.WaitForComplete(time.Second)
//	requireCtx.Error(err, "empty key error is expected")
//	requireCtx.True(strings.HasSuffix(err.Error(), fmt.Sprintf("invalid input. key %q should not be empty", indexNames[0])))
//}
//
//func (kvSuite *KvTestSuite) TestOverwriteItemNoKey() {
//	table := fmt.Sprintf("frames_ci/TestOverwriteItemNoKey_%d", time.Now().UnixNano())
//	requireCtx := kvSuite.Require()
//
//	columnNames := []string{"col1", "col_2"}
//	indexNames := []string{"key", "sorting_key"}
//
//	frame := kvSuite.generateRandomSampleFrameWithEmptyIndices(
//		3,
//		indexNames,
//		map[string]bool{indexNames[0]: true},
//		columnNames)
//
//	writeRequest := &frames.WriteRequest{
//		Backend:  kvSuite.backendName,
//		Table:    table,
//		SaveMode: frames.OverwriteItem,
//	}
//
//	appender, err := kvSuite.client.Write(writeRequest)
//	requireCtx.NoError(err, "failed to create appender")
//
//	err = appender.Add(frame)
//	requireCtx.NoError(err, "failed to write frame")
//
//	err = appender.WaitForComplete(time.Second)
//	requireCtx.Error(err, "empty key error is expected")
//	requireCtx.True(strings.HasSuffix(err.Error(), fmt.Sprintf("invalid input. key %q should not be empty", indexNames[0])))
//}
//
//func (kvSuite *KvTestSuite) TestUpdateItemNoSortingKey() {
//	table := fmt.Sprintf("frames_ci/TestUpdateItemNoSortingKey_%d", time.Now().UnixNano())
//	requireCtx := kvSuite.Require()
//
//	columnNames := []string{"col_1", "col_2"}
//	indexNames := []string{"key", "sorting_key"}
//
//	frame := kvSuite.generateRandomSampleFrameWithEmptyIndices(
//		3,
//		indexNames,
//		map[string]bool{indexNames[1]: true},
//		columnNames)
//
//	writeRequest := &frames.WriteRequest{
//		Backend:  kvSuite.backendName,
//		Table:    table,
//		SaveMode: frames.UpdateItem,
//	}
//
//	appender, err := kvSuite.client.Write(writeRequest)
//	requireCtx.NoError(err, "failed to create appender")
//
//	err = appender.Add(frame)
//	requireCtx.NoError(err, "failed to write frame")
//
//	err = appender.WaitForComplete(time.Second)
//	requireCtx.Error(err, "empty key error is expected")
//	requireCtx.True(strings.HasSuffix(err.Error(), fmt.Sprintf("invalid input. sorting key %q should not be empty", indexNames[1])))
//}

func (kvSuite *KvTestSuite) TestOverwriteItemNoSortingKey() {
	table := fmt.Sprintf("frames_ci/TestOverwriteItemNoSortingKey_%d", time.Now().UnixNano())
	requireCtx := kvSuite.Require()

	columnNames := []string{"col1", "col_2"}
	indexNames := []string{"key", "sorting_key"}

	frame := kvSuite.generateRandomSampleFrameWithEmptyIndices(
		3,
		indexNames,
		map[string]bool{indexNames[1]: true},
		columnNames)

	writeRequest := &frames.WriteRequest{
		Backend:  kvSuite.backendName,
		Table:    table,
		SaveMode: frames.OverwriteItem,
	}

	appender, err := kvSuite.client.Write(writeRequest)
	requireCtx.NoError(err, "failed to create appender")

	err = appender.Add(frame)
	requireCtx.NoError(err, "failed to write frame")

	err = appender.WaitForComplete(10 * time.Second)
	requireCtx.Error(err, "empty key error is expected")
	expectedSuffix := fmt.Sprintf("invalid input. sorting key %q should not be empty", indexNames[1])
	requireCtx.True(strings.HasSuffix(err.Error(), expectedSuffix))
}

func (kvSuite *KvTestSuite) TestUpdateExpressionWithNullValues() {
	table := fmt.Sprintf("frames_ci/kv_test_update_with_nulls_%d", time.Now().UnixNano())

	index := []string{"mike", "joe", "jim", "nil"}
	icol, err := frames.NewSliceColumn("idx", index)
	kvSuite.Require().NoError(err)

	columns := []frames.Column{
		FloatCol(kvSuite.T(), "n1", len(index)),
		StringCol(kvSuite.T(), "n2", len(index)),
		BoolCol(kvSuite.T(), "n3", len(index)),
		TimeCol(kvSuite.T(), "n4", len(index)),
	}

	frame, err := frames.NewFrame(columns, []frames.Column{icol}, nil)
	kvSuite.Require().NoError(err)

	kvSuite.T().Log("write: prepare")
	wreq := &frames.WriteRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	appender, err := kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame)
	kvSuite.Require().NoError(err)

	err = appender.WaitForComplete(10 * time.Second)
	kvSuite.Require().NoError(err)

	// Second Phase: Update existing rows with null values
	kvSuite.T().Log("write: update with null values")
	wreq = &frames.WriteRequest{
		Backend:  kvSuite.backendName,
		Table:    table,
		SaveMode: frames.UpdateItem,
	}

	appender, err = kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	// Update with Null values
	nullValues := initializeNullColumns(len(index))
	nullValues[0].NullColumns["n1"] = true

	nullValues[1].NullColumns["n2"] = true
	nullValues[1].NullColumns["n3"] = true
	nullValues[1].NullColumns["n4"] = true

	nullValues[3].NullColumns["n1"] = true
	nullValues[3].NullColumns["n2"] = true
	nullValues[3].NullColumns["n3"] = true
	nullValues[3].NullColumns["n4"] = true

	frame, err = frames.NewFrameWithNullValues(columns, []frames.Column{icol}, nil, nullValues)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame)
	kvSuite.Require().NoError(err)

	err = appender.WaitForComplete(10 * time.Second)
	kvSuite.Require().NoError(err)

	input := v3io.GetItemsInput{AttributeNames: []string{"__name", "n1", "n2", "n3", "n4"}}

	iter, err := v3ioutils.NewAsyncItemsCursor(
		kvSuite.v3ioContainer, &input, 1,
		nil, kvSuite.internalLogger,
		0, []string{table + "/"},
		"", "")

	for iter.Next() {
		currentRow := iter.GetItem()

		key, _ := currentRow.GetFieldString("__name")
		switch key {
		case ".#schema":
			continue
		case "mike":
			kvSuite.Require().Nil(currentRow.GetField("n1"),
				"item %v - key n1 supposed to be null but got %v", key, currentRow.GetField("n1"))
			kvSuite.Require().NotNil(currentRow.GetField("n2"),
				"item %v - key n2 should not be nil. actual value: %v", key, currentRow.GetField("n2"))
			kvSuite.Require().NotNil(currentRow.GetField("n3"),
				"item %v - key n3 should not be nil. actual value: %v", key, currentRow.GetField("n3"))
			kvSuite.Require().NotNil(currentRow.GetField("n4"),
				"item %v - key n4 should not be nil. actual value: %v", key, currentRow.GetField("n4"))
		case "joe":
			kvSuite.Require().NotNil(currentRow.GetField("n1"),
				"item %v - key n1 should not be nil. actual value: %v", key, currentRow.GetField("n1"))
			kvSuite.Require().Nil(currentRow.GetField("n2"),
				"item %v - key n2 supposed to be null but got %v", key, currentRow.GetField("n2"))
			kvSuite.Require().Nil(currentRow.GetField("n3"),
				"item %v - key n3 supposed to be null but got %v", key, currentRow.GetField("n3"))
			kvSuite.Require().Nil(currentRow.GetField("n4"),
				"item %v - key n4 supposed to be null but got %v", key, currentRow.GetField("n4"))
		case "jim":
			kvSuite.Require().NotNil(currentRow.GetField("n1"),
				"item %v - key n1 should not be nil. actual value: %v", key, currentRow.GetField("n1"))
			kvSuite.Require().NotNil(currentRow.GetField("n2"),
				"item %v - key n2 should not be nil. actual value: %v", key, currentRow.GetField("n2"))
			kvSuite.Require().NotNil(currentRow.GetField("n3"),
				"item %v - key n3 should not be nil. actual value: %v", key, currentRow.GetField("n3"))
			kvSuite.Require().NotNil(currentRow.GetField("n4"),
				"item %v - key n4 should not be nil. actual value: %v", key, currentRow.GetField("n4"))
		case "nil":
			kvSuite.Require().Nil(currentRow.GetField("n1"),
				"item %v - key n1 supposed to be null but got %v", key, currentRow.GetField("n1"))
			kvSuite.Require().Nil(currentRow.GetField("n2"),
				"item %v - key n2 supposed to be null but got %v", key, currentRow.GetField("n2"))
			kvSuite.Require().Nil(currentRow.GetField("n3"),
				"item %v - key n3 supposed to be null but got %v", key, currentRow.GetField("n3"))
			kvSuite.Require().Nil(currentRow.GetField("n4"),
				"item %v - key n4 supposed to be null but got %v", key, currentRow.GetField("n4"))
		default:
			kvSuite.T().Fatalf("got an unexpected key '%v'", key)
		}
	}

	kvSuite.Require().NoError(iter.Err())
}

func (kvSuite *KvTestSuite) TestWritePartitionedTable() {
	table := fmt.Sprintf("frames_ci/TestWritePartitionedTable%d", time.Now().UnixNano())

	frame := kvSuite.generateRandomSampleFrame(5, "idx", []string{"n1", "n2", "n3"})
	wreq := &frames.WriteRequest{
		Backend:       kvSuite.backendName,
		Table:         table,
		PartitionKeys: []string{"n2"},
	}

	appender, err := kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame)
	kvSuite.Require().NoError(err)

	err = appender.WaitForComplete(10 * time.Second)
	kvSuite.Require().NoError(err)

	iter := frame.IterRows(true)

	// 1. Verify all items were saved in the correct path
	for iter.Next() {
		currentRow := iter.Row()
		subPath := fmt.Sprintf("n2=%v/%v", currentRow["n2"], currentRow["idx"])
		_, err := kvSuite.v3ioContainer.GetItemSync(&v3io.GetItemInput{AttributeNames: []string{"__name"},
			Path: fmt.Sprintf("%v/%v", table, subPath)})
		kvSuite.Require().NoError(err, "item %v was not found", subPath)
	}

	// 2. Verify we can read the partitioned kv table
	rreq := &pb.ReadRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	it, err := kvSuite.client.Read(rreq)
	kvSuite.Require().NoError(err)

	for it.Next() {
		// TODO: More checks
		fr := it.At()
		validateFramesAreEqual(kvSuite.Suite, fr, frame)
	}

	kvSuite.Require().NoError(it.Err())
}

func (kvSuite *KvTestSuite) TestWritePartitionedTableWithMultiplePartitions() {
	table := fmt.Sprintf("frames_ci/TestWritePartitionedTable%d", time.Now().UnixNano())

	frame := kvSuite.generateRandomSampleFrame(5, "idx", []string{"n1", "n2", "n3", "n4"})
	wreq := &frames.WriteRequest{
		Backend:       kvSuite.backendName,
		Table:         table,
		PartitionKeys: []string{"n2", "n3", "n4"},
	}

	appender, err := kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame)
	kvSuite.Require().NoError(err)

	err = appender.WaitForComplete(10 * time.Second)
	kvSuite.Require().NoError(err)

	iter := frame.IterRows(true)

	// 1. Verify all items were saved in the correct path
	for iter.Next() {
		currentRow := iter.Row()
		subPath := fmt.Sprintf("n2=%v/n3=%v/n4=%v/%v", currentRow["n2"], currentRow["n3"], currentRow["n4"], currentRow["idx"])
		_, err := kvSuite.v3ioContainer.GetItemSync(&v3io.GetItemInput{AttributeNames: []string{"__name"},
			Path: fmt.Sprintf("%v/%v", table, subPath)})
		kvSuite.Require().NoError(err, "item %v was not found", subPath)
	}

	// 2. Verify we can read the partitioned kv table
	rreq := &pb.ReadRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	it, err := kvSuite.client.Read(rreq)
	kvSuite.Require().NoError(err)

	for it.Next() {
		// TODO: More checks
		fr := it.At()
		validateFramesAreEqual(kvSuite.Suite, fr, frame)
	}

	kvSuite.Require().NoError(it.Err())
}

func (kvSuite *KvTestSuite) TestWritePartitionedBadPartitionColumns() {
	table := fmt.Sprintf("frames_ci/TestWritePartitionedTable%d", time.Now().UnixNano())

	frame := kvSuite.generateRandomSampleFrame(5, "idx", []string{"n1", "n2", "n3"})
	wreq := &frames.WriteRequest{
		Backend:       kvSuite.backendName,
		Table:         table,
		PartitionKeys: []string{"idonotexist"},
	}

	appender, err := kvSuite.client.Write(wreq)

	kvSuite.Require().NoError(err)

	err = appender.Add(frame)
	kvSuite.Require().NoError(err)

	err = appender.WaitForComplete(10 * time.Second)
	kvSuite.Require().Error(err, "expected error for table %v, but finished successfully", table)
}

func (kvSuite *KvTestSuite) TestWritePartitionedTableWithNullValues() {
	table := fmt.Sprintf("frames_ci/TestWritePartitionedTableWithNullValues%d", time.Now().UnixNano())

	index := []string{"mike", "joe"}
	icol, err := frames.NewSliceColumn("idx", index)
	kvSuite.Require().NoError(err)

	n1 := []float64{1.0, 0.0}
	n1Col, err := frames.NewSliceColumn("n1", n1)
	kvSuite.Require().NoError(err)

	n2 := []string{"", "tal"}
	n2Col, err := frames.NewSliceColumn("n2", n2)
	kvSuite.Require().NoError(err)

	n3 := []bool{false, true}
	n3Col, err := frames.NewSliceColumn("n3", n3)
	kvSuite.Require().NoError(err)

	columns := []frames.Column{n1Col, n2Col, n3Col}

	nullValues := initializeNullColumns(len(index))
	nullValues[0].NullColumns["n2"] = true
	nullValues[0].NullColumns["n3"] = true

	nullValues[1].NullColumns["n1"] = true

	frame, err := frames.NewFrameWithNullValues(columns, []frames.Column{icol}, nil, nullValues)
	kvSuite.Require().NoError(err)

	wreq := &frames.WriteRequest{
		Backend:       kvSuite.backendName,
		Table:         table,
		PartitionKeys: []string{"n1", "n2", "n3"},
	}

	appender, err := kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame)
	kvSuite.Require().NoError(err)

	err = appender.WaitForComplete(10 * time.Second)
	kvSuite.Require().NoError(err)

	expected := map[string]string{
		"mike": "n1=1/n2=null/n3=null/mike",
		"joe":  "n1=null/n2=tal/n3=true/joe",
	}

	// Verify all items were saved in the correct path
	for _, subPath := range expected {
		_, err := kvSuite.v3ioContainer.GetItemSync(&v3io.GetItemInput{AttributeNames: []string{"__name"},
			Path: fmt.Sprintf("%v/%v", table, subPath)})
		kvSuite.Require().NoError(err, "item %v was not found", subPath)
	}
}

func (kvSuite *KvTestSuite) TestUpdateExpressionWithCondition() {
	table := fmt.Sprintf("frames_ci/kv_test_update_with_condition_%d", time.Now().UnixNano())

	index := []string{"mike", "joe", "jim", "nil"}
	icol, err := frames.NewSliceColumn("idx", index)
	kvSuite.Require().NoError(err)

	//boolCol
	bools := []bool{true, true, false, false}
	boolCol, err := frames.NewSliceColumn("bools", bools)
	if err != nil {
		kvSuite.Require().NoError(err)
	}

	numbers := []int{1, 1, 1, 1}
	numbersCol, err := frames.NewSliceColumn("nums", numbers)
	if err != nil {
		kvSuite.Require().NoError(err)
	}

	columns := []frames.Column{numbersCol, boolCol}

	frame, err := frames.NewFrame(columns, []frames.Column{icol}, nil)
	kvSuite.Require().NoError(err)

	kvSuite.T().Log("write: prepare")
	wreq := &frames.WriteRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	appender, err := kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame)
	kvSuite.Require().NoError(err)

	err = appender.WaitForComplete(10 * time.Second)
	kvSuite.Require().NoError(err)

	// Second Phase: Update existing rows with condition
	kvSuite.T().Log("write: update with condition")
	wreq = &frames.WriteRequest{
		Backend:   kvSuite.backendName,
		Table:     table,
		SaveMode:  frames.UpdateItem,
		Condition: "bools==True",
	}

	appender, err = kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	newNumbers := []int{9, 9, 9, 9}
	newNumbersCol, err := frames.NewSliceColumn("nums", newNumbers)
	if err != nil {
		kvSuite.Require().NoError(err)
	}

	newColumns := []frames.Column{newNumbersCol}

	frame, err = frames.NewFrame(newColumns, []frames.Column{icol}, nil)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame)
	kvSuite.Require().NoError(err)

	err = appender.WaitForComplete(10 * time.Second)
	kvSuite.Require().NoError(err)

	input := v3io.GetItemsInput{AttributeNames: []string{"__name", "nums", "bools"}}

	iter, err := v3ioutils.NewAsyncItemsCursor(
		kvSuite.v3ioContainer, &input, 1,
		nil, kvSuite.internalLogger,
		0, []string{table + "/"},
		"", "")

	for iter.Next() {
		currentRow := iter.GetItem()
		key, _ := currentRow.GetFieldString("__name")
		if key == ".#schema" {
			continue
		}
		num, _ := currentRow.GetFieldInt("nums")
		b := currentRow.GetField("bools").(bool)

		if b {
			kvSuite.Require().Equal(9, num)
		} else {
			kvSuite.Require().Equal(1, num)
		}
	}

	kvSuite.Require().NoError(iter.Err())
}
