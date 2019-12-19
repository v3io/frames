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
	"encoding/json"
	"fmt"
	"time"

	"github.com/nuclio/logger"
	"github.com/stretchr/testify/suite"
	"github.com/v3io/frames"
	"github.com/v3io/frames/pb"
	"github.com/v3io/frames/v3ioutils"
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
	kvSuite.NoError(err)
	return time
}

func (kvSuite *KvTestSuite) SetupSuite() {
	if kvSuite.client == nil {
		kvSuite.FailNow("client not set")
	}
}

func (kvSuite *KvTestSuite) generateRandomSampleFrame(size int, indexName string, columnNames []string) frames.Frame {
	var icol frames.Column

	index := make([]string, size)
	for i := 0; i < size; i++ {
		index[i] = fmt.Sprintf("%v%v", i, time.Now().Nanosecond())
	}

	icol, err := frames.NewSliceColumn(indexName, index)
	if err != nil {
		kvSuite.T().Fatal(err)
	}

	columns := make([]frames.Column, len(columnNames))
	for i, name := range columnNames {
		columns[i] = floatCol(kvSuite.T(), name, size)
	}

	frame, err := frames.NewFrame(columns, []frames.Column{icol}, nil)
	if err != nil {
		kvSuite.T().Fatal(err)
	}

	return frame
}

func (kvSuite *KvTestSuite) generateSequentialSampleFrame(size int, indexName string, columnNames []string) frames.Frame {
	var icol frames.Column

	index := make([]int, size)
	for i := 0; i < size; i++ {
		index[i] = i
	}

	icol, err := frames.NewSliceColumn(indexName, index)
	if err != nil {
		kvSuite.T().Fatal(err)
	}

	columns := make([]frames.Column, len(columnNames))
	for i, name := range columnNames {
		columns[i] = floatCol(kvSuite.T(), name, size)
	}

	frame, err := frames.NewFrame(columns, []frames.Column{icol}, nil)
	if err != nil {
		kvSuite.T().Fatal(err)
	}

	return frame
}

func (kvSuite *KvTestSuite) generateSequentialSampleFrameWithTypes(size int, indexName string, columnNames map[string]string) frames.Frame {
	var icol frames.Column

	index := make([]int, size)
	for i := 0; i < size; i++ {
		index[i] = i
	}

	icol, err := frames.NewSliceColumn(indexName, index)
	if err != nil {
		kvSuite.T().Fatal(err)
	}

	columns := make([]frames.Column, len(columnNames))
	i := 0
	for columnName, columnType := range columnNames {
		switch columnType {
		case "string":
			columns[i] = stringCol(kvSuite.T(), columnName, size)
		case "float":
			columns[i] = floatCol(kvSuite.T(), columnName, size)
		case "bool":
			columns[i] = boolCol(kvSuite.T(), columnName, size)
		case "time":
			columns[i] = timeCol(kvSuite.T(), columnName, size)
		default:
			kvSuite.T().Fatalf("type %v not supported", columnType)
		}

		i++
	}

	frame, err := frames.NewFrame(columns, []frames.Column{icol}, nil)
	if err != nil {
		kvSuite.T().Fatal(err)
	}

	return frame
}

func (kvSuite *KvTestSuite) TestAll() {
	table := fmt.Sprintf("kv_test_all%d", time.Now().UnixNano())

	kvSuite.T().Log("write")
	frame := kvSuite.generateRandomSampleFrame(5, "idx", []string{"n1", "n2", "n3"})
	wreq := &frames.WriteRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	appender, err := kvSuite.client.Write(wreq)
	if err != nil {
		kvSuite.T().Fatal(err)
	}

	if err := appender.Add(frame); err != nil {
		kvSuite.T().Fatal(err)
	}

	if err := appender.WaitForComplete(3 * time.Second); err != nil {
		kvSuite.T().Fatal(err)
	}

	time.Sleep(3 * time.Second) // Let DB sync

	kvSuite.T().Log("read")
	rreq := &pb.ReadRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	it, err := kvSuite.client.Read(rreq)
	if err != nil {
		kvSuite.T().Fatal(err)
	}

	for it.Next() {
		// TODO: More checks
		fr := it.At()
		if !(fr.Len() == frame.Len() || fr.Len()-1 == frame.Len()) {
			kvSuite.T().Fatalf("wrong length: %d != %d", fr.Len(), frame.Len())
		}
	}

	if err := it.Err(); err != nil {
		kvSuite.T().Fatal(err)
	}

	kvSuite.T().Log("delete")
	dreq := &pb.DeleteRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	if err := kvSuite.client.Delete(dreq); err != nil {
		kvSuite.T().Fatal(err)
	}
}

func (kvSuite *KvTestSuite) TestRangeScan() {
	table := fmt.Sprintf("kv_range_scan%d", time.Now().UnixNano())

	index := []string{"mike", "joe", "mike", "jim", "mike"}
	icol, err := frames.NewSliceColumn("key", index)
	if err != nil {
		kvSuite.T().Fatal(err)
	}
	sorting := []string{"aa", "cc", "bb", "aa", "dd"}
	sortcol, err := frames.NewSliceColumn("sorting", sorting)
	if err != nil {
		kvSuite.T().Fatal(err)
	}

	columns := []frames.Column{
		stringCol(kvSuite.T(), "n1", len(index)),
	}

	frame, err := frames.NewFrame(columns, []frames.Column{icol, sortcol}, nil)
	if err != nil {
		kvSuite.T().Fatal(err)
	}

	wreq := &frames.WriteRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	appender, err := kvSuite.client.Write(wreq)
	if err != nil {
		kvSuite.T().Fatal(err)
	}

	if err := appender.Add(frame); err != nil {
		kvSuite.T().Fatal(err)
	}

	if err := appender.WaitForComplete(3 * time.Second); err != nil {
		kvSuite.T().Fatal(err)
	}

	//check schema
	schemaInput := &v3io.GetObjectInput{Path: table + "/.#schema"}
	resp, err := kvSuite.v3ioContainer.GetObjectSync(schemaInput)
	if err != nil {
		kvSuite.T().Fatal(err.Error())
	}
	schema := &v3ioutils.OldV3ioSchema{}
	if err := json.Unmarshal(resp.HTTPResponse.Body(), schema); err != nil {
		kvSuite.T().Fatal(err)
	}
	if schema.Key != "key" {
		kvSuite.T().Fatal("wrong key in schema, expected 'key', got ", schema.Key)
	}
	if schema.SortingKey != "sorting" {
		kvSuite.T().Fatal("wrong sorting key in schema, expected 'sorting', got ", schema.SortingKey)
	}
	if len(schema.Fields) != 3 {
		kvSuite.T().Fatal("wrong number of columns in schema, expected 3, got ", len(schema.Fields))
	}
	////
	rreq := &pb.ReadRequest{
		Backend:           kvSuite.backendName,
		Table:             table,
		ShardingKeys:      []string{"mike", "joe"},
		SortKeyRangeStart: "bb",
	}

	it, err := kvSuite.client.Read(rreq)
	if err != nil {
		kvSuite.T().Fatal(err)
	}
	for it.Next() {
		frame = it.At()
		if len(frame.Indices()) != 2 {
			kvSuite.T().Fatal("wrong number of indices, expected 2, got ", len(frame.Indices()))
		}
		indexCol := frame.Indices()[0]
		sortcol := frame.Indices()[1]
		if frame.Len() != 3 {
			kvSuite.T().Fatal("got different number of results, expected 3, got ", frame.Len())
		}
		for i := 0; i < frame.Len(); i++ {
			currentKey, _ := indexCol.StringAt(i)
			sorting, _ := sortcol.StringAt(i)
			if !((currentKey == "mike" || currentKey == "joe") && sorting >= "bb") {
				kvSuite.T().Fatal("key name does not match, expected mike.bb, joe.cc, or mike.dd, got ", frame)
			}
		}
	}
	////
	rreq = &pb.ReadRequest{
		Backend:           kvSuite.backendName,
		Table:             table,
		ShardingKeys:      []string{"mike"},
		SortKeyRangeStart: "aa",
		SortKeyRangeEnd:   "cc",
	}

	it, err = kvSuite.client.Read(rreq)
	if err != nil {
		kvSuite.T().Fatal(err)
	}

	for it.Next() {
		frame = it.At()
		if len(frame.Indices()) != 2 {
			kvSuite.T().Fatal("wrong number of indices, expected 2, got ", len(frame.Indices()))
		}
		indexCol := frame.Indices()[0]
		sortcol := frame.Indices()[1]
		if frame.Len() != 2 {
			kvSuite.T().Fatal("got different number of results, expected 2, got ", frame.Len())
		}
		for i := 0; i < frame.Len(); i++ {
			currentKey, _ := indexCol.StringAt(i)
			sorting, _ := sortcol.StringAt(i)
			if !(currentKey == "mike" && (sorting == "aa" || sorting == "bb")) {
				kvSuite.T().Fatal("key name does not match, expected mike.aa or mike.bb, got ", frame)
			}
		}
	}
}

func (kvSuite *KvTestSuite) TestNullValuesWrite() {
	table := fmt.Sprintf("kv_test_nulls%d", time.Now().UnixNano())

	index := []string{"mike", "joe", "jim"}
	icol, err := frames.NewSliceColumn("idx", index)
	if err != nil {
		kvSuite.T().Fatal(err)
	}

	columns := []frames.Column{
		floatCol(kvSuite.T(), "n1", len(index)),
		stringCol(kvSuite.T(), "n2", len(index)),
		boolCol(kvSuite.T(), "n3", len(index)),
		timeCol(kvSuite.T(), "n4", len(index)),
	}

	nullValues := initializeNullColumns(len(index))
	nullValues[0].NullColumns["n1"] = true

	nullValues[1].NullColumns["n2"] = true
	nullValues[1].NullColumns["n3"] = true
	nullValues[1].NullColumns["n4"] = true

	frame, err := frames.NewFrameWithNullValues(columns, []frames.Column{icol}, nil, nullValues)
	if err != nil {
		kvSuite.T().Fatal(err)
	}

	kvSuite.T().Log("write")
	wreq := &frames.WriteRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	appender, err := kvSuite.client.Write(wreq)
	if err != nil {
		kvSuite.T().Fatal(err)
	}

	if err := appender.Add(frame); err != nil {
		kvSuite.T().Fatal(err)
	}

	if err := appender.WaitForComplete(3 * time.Second); err != nil {
		kvSuite.T().Fatal(err)
	}

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
				"item %v - key n2 supposed to be null but got %v", key, currentRow.GetField("n2"))
			kvSuite.Require().NotNil(currentRow.GetField("n3"),
				"item %v - key n3 supposed to be null but got %v", key, currentRow.GetField("n3"))
			kvSuite.Require().NotNil(currentRow.GetField("n4"),
				"item %v - key n4 supposed to be null but got %v", key, currentRow.GetField("n4"))
		case "joe":
			kvSuite.Require().NotNil(currentRow.GetField("n1"),
				"item %v - key n1 supposed to be null but got %v", key, currentRow.GetField("n1"))

			kvSuite.Require().Nil(currentRow.GetField("n2"),
				"item %v - key n2 supposed to be null but got %v", key, currentRow.GetField("n2"))
			kvSuite.Require().Nil(currentRow.GetField("n3"),
				"item %v - key n3 supposed to be null but got %v", key, currentRow.GetField("n3"))
			kvSuite.Require().Nil(currentRow.GetField("n4"),
				"item %v - key n4 supposed to be null but got %v", key, currentRow.GetField("n4"))
		case "jim":
			kvSuite.Require().NotNil(currentRow.GetField("n1"),
				"item %v - key n1 supposed to be null but got %v", key, currentRow.GetField("n1"))
			kvSuite.Require().NotNil(currentRow.GetField("n2"),
				"item %v - key n2 supposed to be null but got %v", key, currentRow.GetField("n2"))
			kvSuite.Require().NotNil(currentRow.GetField("n3"),
				"item %v - key n3 supposed to be null but got %v", key, currentRow.GetField("n3"))
			kvSuite.Require().NotNil(currentRow.GetField("n4"),
				"item %v - key n4 supposed to be null but got %v", key, currentRow.GetField("n4"))
		default:
			kvSuite.T().Fatalf("got an unexpected key '%v'", key)
		}
	}

	if iter.Err() != nil {
		kvSuite.T().Fatalf("error querying items got: %v", iter.Err())
	}
}

func (kvSuite *KvTestSuite) TestNullValuesRead() {
	table := fmt.Sprintf("kv_test_nulls_read%d", time.Now().UnixNano())

	data := make(map[string]map[string]interface{})
	data["mike"] = map[string]interface{}{"idx": "mike", "n2": "dsad", "n3": true, "n4": time.Now()}
	data["joe"] = map[string]interface{}{"idx": "joe", "n1": 12.2}
	data["jim"] = map[string]interface{}{"idx": "jim", "n1": 66.6, "n2": "XXX", "n3": true, "n4": time.Now()}

	input := &v3io.PutItemsInput{Path: table, Items: data}
	res, err := kvSuite.v3ioContainer.PutItemsSync(input)
	if err != nil {
		kvSuite.T().Fatalf("error putting test data, err: %v", err)
	}
	if res.Error != nil {
		kvSuite.T().Fatalf("error putting test data, err: %v", res.Error)
	}

	oldSchema := v3ioutils.NewSchema("idx", "")
	schema := v3ioutils.NewSchema("idx", "")
	_ = schema.AddField("idx", "", false)
	_ = schema.AddField("n1", 12.6, true)
	_ = schema.AddField("n2", "", true)
	_ = schema.AddField("n3", true, true)
	_ = schema.AddField("n4", time.Now(), true)
	err = oldSchema.UpdateSchema(kvSuite.v3ioContainer, table+"/", schema)
	if err != nil {
		kvSuite.T().Fatalf("failed to create schema, err: %v", err)
	}

	rreq := &pb.ReadRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	it, err := kvSuite.client.Read(rreq)
	if err != nil {
		kvSuite.T().Fatal(err)
	}

	for it.Next() {
		// TODO: More checks
		frame := it.At()
		if frame.Len() != len(data) {
			kvSuite.T().Fatalf("wrong length: %d != %d", frame.Len(), frame.Len())
		}

		indexCol := frame.Indices()[0]
		for i := 0; i < frame.Len(); i++ {
			currentKey, _ := indexCol.StringAt(i)
			for _, columnName := range frame.Names() {
				// Checking that the desired Null values are indeed null
				if currentKey == "mike" && columnName == "n1" {
					kvSuite.Require().True(frame.IsNull(i, columnName), "key %v and column %v expected to be null but is not", currentKey, columnName)
				} else if currentKey == "joe" &&
					(columnName == "n2" || columnName == "n3" || columnName == "n4") {
					kvSuite.Require().True(frame.IsNull(i, columnName), "key %v and column %v expected to be null but is not", currentKey, columnName)
				} else {
					kvSuite.Require().False(frame.IsNull(i, columnName), "key %v and column %v expected to have value but got Null", currentKey, columnName)
				}
			}
		}
	}

	if err := it.Err(); err != nil {
		kvSuite.T().Fatal(err)
	}
}

func (kvSuite *KvTestSuite) TestRequestSpecificColumns() {
	table := fmt.Sprintf("TestRequestSpecificColumns%d", time.Now().UnixNano())

	frame := kvSuite.generateRandomSampleFrame(6, "idx", []string{"n1", "n2", "n3"})
	wreq := &frames.WriteRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	appender, err := kvSuite.client.Write(wreq)
	kvSuite.NoError(err)

	err = appender.Add(frame)
	kvSuite.NoError(err)

	err = appender.WaitForComplete(3 * time.Second)
	kvSuite.NoError(err)

	time.Sleep(3 * time.Second) // Let DB sync

	requestedColumns := []string{"n1", "n2"}
	rreq := &pb.ReadRequest{
		Backend: kvSuite.backendName,
		Table:   table,
		Columns: requestedColumns,
	}

	it, err := kvSuite.client.Read(rreq)
	kvSuite.NoError(err)

	for it.Next() {
		fr := it.At()
		if !(fr.Len() == frame.Len() || fr.Len()-1 == frame.Len()) {
			kvSuite.T().Fatalf("wrong length: %d != %d", fr.Len(), frame.Len())
		}
		kvSuite.Require().EqualValues(requestedColumns, fr.Names(), "got other columns than requested")
	}

	err = it.Err()
	kvSuite.NoError(err)
}

func (kvSuite *KvTestSuite) TestRequestSpecificColumnsWithKey() {
	table := fmt.Sprintf("TestRequestSpecificColumnsWithKey%d", time.Now().UnixNano())

	frame := kvSuite.generateRandomSampleFrame(6, "idx", []string{"n1", "n2", "n3"})
	wreq := &frames.WriteRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	appender, err := kvSuite.client.Write(wreq)
	kvSuite.NoError(err)

	err = appender.Add(frame)
	kvSuite.NoError(err)

	err = appender.WaitForComplete(3 * time.Second)
	kvSuite.NoError(err)

	time.Sleep(3 * time.Second) // Let DB sync

	requestedColumns := []string{"idx", "n1", "n2"}
	rreq := &pb.ReadRequest{
		Backend: kvSuite.backendName,
		Table:   table,
		Columns: requestedColumns,
	}

	it, err := kvSuite.client.Read(rreq)
	kvSuite.NoError(err)

	for it.Next() {
		fr := it.At()
		if !(fr.Len() == frame.Len() || fr.Len()-1 == frame.Len()) {
			kvSuite.T().Fatalf("wrong length: %d != %d", fr.Len(), frame.Len())
		}
		kvSuite.Require().EqualValues(requestedColumns, fr.Names(), "got other columns than requested")
		kvSuite.Require().Equal("_idx", fr.Indices()[0].Name(), "got wrong index name")
	}

	err = it.Err()
	kvSuite.NoError(err)
}

func (kvSuite *KvTestSuite) TestDeleteWithFilter() {
	table := fmt.Sprintf("kv_delete_filter%d", time.Now().UnixNano())

	frame := kvSuite.generateRandomSampleFrame(5, "idx", []string{"n1", "n2"})
	wreq := &frames.WriteRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	appender, err := kvSuite.client.Write(wreq)
	if err != nil {
		kvSuite.T().Fatal(err)
	}

	if err := appender.Add(frame); err != nil {
		kvSuite.T().Fatal(err)
	}

	if err := appender.WaitForComplete(3 * time.Second); err != nil {
		kvSuite.T().Fatal(err)
	}
	kvSuite.T().Log("delete")
	dreq := &pb.DeleteRequest{
		Backend: kvSuite.backendName,
		Table:   table,
		Filter:  "__mtime_secs > 0",
	}

	if err := kvSuite.client.Delete(dreq); err != nil {
		kvSuite.T().Fatal(err)
	}

	// check only schema is left
	kvSuite.T().Log("read")
	rreq := &pb.ReadRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	it, err := kvSuite.client.Read(rreq)
	kvSuite.NoError(err)

	for it.Next() {
		frame := it.At()
		kvSuite.Require().Equal(frame.Len(), 0, "wrong length: %d != %d")
	}
	//make sure schema is not deleted
	schemaInput := &v3io.GetObjectInput{Path: table + "/.#schema"}
	_, err = kvSuite.v3ioContainer.GetObjectSync(schemaInput)
	kvSuite.NoError(err, "schema is not found ")
}
