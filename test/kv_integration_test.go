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
		kvSuite.v3ioContainer, &input, 1, nil, kvSuite.internalLogger, 0, []string{table + "/"})

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

	oldSchema := v3ioutils.NewSchema("idx")
	schema := v3ioutils.NewSchema("idx")
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

func (kvSuite *KvTestSuite) TestSaveModeErrorIfExistsTableExists() {
	table := fmt.Sprintf("TestSaveModeErrorIfExistsTableExists%d", time.Now().UnixNano())

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
	err = appender.WaitForComplete(time.Second)
	kvSuite.NoError(err, "error while saving")
	// Save a frame to the same path
	wreq = &frames.WriteRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	appender, err = kvSuite.client.Write(wreq)
	if err != nil {
		kvSuite.T().Fatal(err)
	}

	err = appender.Add(frame)
	kvSuite.NoError(err, "error while saving")

	err = appender.WaitForComplete(time.Second)
	if err == nil {
		kvSuite.T().Fatal("expected an error but finished successfully")
	}
}

func (kvSuite *KvTestSuite) TestSaveModeOverwriteTableExists() {
	table := fmt.Sprintf("TestSaveModeOverwriteTableExists%d", time.Now().UnixNano())

	frame := kvSuite.generateRandomSampleFrame(5, "idx", []string{"n1", "n2"})
	wreq := &frames.WriteRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	appender, err := kvSuite.client.Write(wreq)
	if err != nil {
		kvSuite.T().Fatal(err)
	}

	err = appender.Add(frame)
	kvSuite.NoError(err, "failed to write frame")
	if err := appender.WaitForComplete(time.Second); err != nil {
		kvSuite.T().Fatal(err)
	}

	// Save a frame to the same path
	newColumns := []string{"n3", "n4"}
	frame2 := kvSuite.generateRandomSampleFrame(5, "idx", newColumns)
	wreq = &frames.WriteRequest{
		Backend:  kvSuite.backendName,
		Table:    table,
		SaveMode: frames.Overwrite,
	}

	appender, err = kvSuite.client.Write(wreq)
	if err != nil {
		kvSuite.T().Fatal(err)
	}

	err = appender.Add(frame2)
	kvSuite.NoError(err, "failed to write frame")

	if err := appender.WaitForComplete(time.Second); err != nil {
		kvSuite.T().Fatal(err)
	}

	rreq := &pb.ReadRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}
	iter, err := kvSuite.client.Read(rreq)
	kvSuite.NoError(err, "error reading from kv")
	for iter.Next() {
		kvSuite.EqualValues(newColumns, iter.At().Names(), "expected column names do not match actual")
	}

	kvSuite.NoError(iter.Err(), "error reading from kv")
}

func (kvSuite *KvTestSuite) TestSaveModeOverwriteTableDoesntExists() {
	table := fmt.Sprintf("TestSaveModeOverwriteTableDoesntExists%d", time.Now().UnixNano())

	newColumns := []string{"n3", "n4"}
	frame := kvSuite.generateRandomSampleFrame(5, "idx", newColumns)
	wreq := &frames.WriteRequest{
		Backend:  kvSuite.backendName,
		Table:    table,
		SaveMode: frames.Overwrite,
	}

	appender, err := kvSuite.client.Write(wreq)
	if err != nil {
		kvSuite.T().Fatal(err)
	}

	err = appender.Add(frame)
	kvSuite.NoError(err, "failed to write frame")

	if err := appender.WaitForComplete(time.Second); err != nil {
		kvSuite.T().Fatal(err)
	}

	rreq := &pb.ReadRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}
	iter, err := kvSuite.client.Read(rreq)
	kvSuite.NoError(err, "error reading from kv")
	for iter.Next() {
		kvSuite.EqualValues(newColumns, iter.At().Names(), "expected column names do not match actual")
	}

	kvSuite.NoError(iter.Err(), "error reading from kv")
}

func (kvSuite *KvTestSuite) TestSaveModeAppendNewRow() {
	table := fmt.Sprintf("TestSaveModeAppendNewRow%d", time.Now().UnixNano())

	columnNames := []string{"n1", "n2"}
	frame := kvSuite.generateRandomSampleFrame(3, "idx", columnNames)
	wreq := &frames.WriteRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	appender, err := kvSuite.client.Write(wreq)
	if err != nil {
		kvSuite.T().Fatal(err)
	}

	err = appender.Add(frame)
	kvSuite.NoError(err, "failed to write frame")

	if err := appender.WaitForComplete(time.Second); err != nil {
		kvSuite.T().Fatal(err)
	}

	// Save a frame to the same path
	frame2 := kvSuite.generateRandomSampleFrame(2, "idx", columnNames)

	wreq = &frames.WriteRequest{
		Backend:  kvSuite.backendName,
		Table:    table,
		SaveMode: frames.Append,
	}

	appender, err = kvSuite.client.Write(wreq)
	if err != nil {
		kvSuite.T().Fatal(err)
	}

	err = appender.Add(frame2)
	kvSuite.NoError(err, "failed to write frame")

	if err := appender.WaitForComplete(time.Second); err != nil {
		kvSuite.T().Fatal(err)
	}

	rreq := &pb.ReadRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}
	iter, err := kvSuite.client.Read(rreq)
	kvSuite.NoError(err, "error reading from kv")
	for iter.Next() {
		currentFrame := iter.At()
		kvSuite.Require().EqualValues(columnNames, currentFrame.Names(), "expected column names do not match actual")
		kvSuite.Require().Equal(5, currentFrame.Len(), "frame is not in the expected length")
	}

	kvSuite.NoError(iter.Err(), "error reading from kv")
}

func (kvSuite *KvTestSuite) TestSaveModeAppendNewAttribute() {
	table := fmt.Sprintf("TestSaveModeAppendNewAttribute%d", time.Now().UnixNano())

	columnNames := []string{"n1", "n2"}
	frame := kvSuite.generateSequentialSampleFrame(3, "idx", columnNames)
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

	if err := appender.WaitForComplete(time.Second); err != nil {
		kvSuite.T().Fatal(err)
	}

	// Save a frame to the same path
	newColumnNames := []string{"n3"}
	frame2 := kvSuite.generateSequentialSampleFrame(3, "idx", newColumnNames)

	wreq = &frames.WriteRequest{
		Backend:  kvSuite.backendName,
		Table:    table,
		SaveMode: frames.Append,
	}

	appender, err = kvSuite.client.Write(wreq)
	if err != nil {
		kvSuite.T().Fatal(err)
	}

	err = appender.Add(frame2)
	kvSuite.NoError(err, "failed to write frame")

	if err := appender.WaitForComplete(time.Second); err != nil {
		kvSuite.T().Fatal(err)
	}

	rreq := &pb.ReadRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}
	iter, err := kvSuite.client.Read(rreq)
	kvSuite.NoError(err, "error reading from kv")
	allColumns := append(columnNames, newColumnNames...)
	for iter.Next() {
		currentFrame := iter.At()
		kvSuite.Require().EqualValues(allColumns, currentFrame.Names(), "expected column names do not match actual")
		kvSuite.Require().Equal(3, currentFrame.Len(), "frame is not in the expected length")
	}

	kvSuite.NoError(iter.Err(), "error reading from kv")
}

func (kvSuite *KvTestSuite) TestSaveModeAppendSameAttributeDifferentValues() {
	table := fmt.Sprintf("TestSaveModeAppendNewAttribute%d", time.Now().UnixNano())

	columnNames := []string{"n1", "n2"}
	frame := kvSuite.generateSequentialSampleFrame(3, "idx", columnNames)
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

	if err := appender.WaitForComplete(time.Second); err != nil {
		kvSuite.T().Fatal(err)
	}

	// Save a frame to the same path
	frame2 := kvSuite.generateSequentialSampleFrame(3, "idx", columnNames)
	wreq = &frames.WriteRequest{
		Backend:  kvSuite.backendName,
		Table:    table,
		SaveMode: frames.Append,
	}

	appender, err = kvSuite.client.Write(wreq)
	if err != nil {
		kvSuite.T().Fatal(err)
	}

	err = appender.Add(frame2)
	kvSuite.NoError(err, "failed to write frame")

	if err := appender.WaitForComplete(time.Second); err != nil {
		kvSuite.T().Fatal(err)
	}

	rreq := &pb.ReadRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}
	iter, err := kvSuite.client.Read(rreq)
	kvSuite.NoError(err, "error reading from kv")

	for iter.Next() {
		currentFrame := iter.At()
		validateFramesAreEqual(kvSuite.Suite, currentFrame, frame)
	}

	kvSuite.NoError(iter.Err(), "error reading from kv")
}

func (kvSuite *KvTestSuite) TestSaveModeAppendChangeColumnType() {
	table := fmt.Sprintf("TestSaveModeAppendChangeColumnType%d", time.Now().UnixNano())

	columnNames := []string{"n1", "n2"}
	frame := kvSuite.generateSequentialSampleFrame(3, "idx", columnNames)
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

	if err := appender.WaitForComplete(time.Second); err != nil {
		kvSuite.T().Fatal(err)
	}

	// Save a frame to the same path with different types
	frame2 := kvSuite.generateSequentialSampleFrameWithTypes(3, "idx", map[string]string{"n1": "float", "n2": "string"})

	wreq = &frames.WriteRequest{
		Backend:  kvSuite.backendName,
		Table:    table,
		SaveMode: frames.Append,
	}

	appender, err = kvSuite.client.Write(wreq)
	if err != nil {
		kvSuite.T().Fatal(err)
	}

	err = appender.Add(frame2)
	kvSuite.NoError(err, "failed to write frame")

	if err := appender.WaitForComplete(time.Second); err == nil {
		kvSuite.T().Fatalf("expected to fail, but completed succesfully")
	}
}

func (kvSuite *KvTestSuite) TestSaveModeAppendChangeIndexName() {
	table := fmt.Sprintf("TestSaveModeAppendChangeColumnType%d", time.Now().UnixNano())

	columnNames := []string{"n1", "n2"}
	frame := kvSuite.generateSequentialSampleFrame(3, "idx", columnNames)
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

	if err := appender.WaitForComplete(time.Second); err != nil {
		kvSuite.T().Fatal(err)
	}

	// Save a frame to the same path with different types
	frame2 := kvSuite.generateSequentialSampleFrame(3, "kuku", columnNames)

	fmt.Printf("original index %v, new index %v", frame.Indices()[0].Name(), frame2.Indices()[0].Name())
	wreq = &frames.WriteRequest{
		Backend:  kvSuite.backendName,
		Table:    table,
		SaveMode: frames.Append,
	}

	appender, err = kvSuite.client.Write(wreq)
	if err != nil {
		kvSuite.T().Fatal(err)
	}

	err = appender.Add(frame2)
	kvSuite.NoError(err, "failed to write frame")

	if err := appender.WaitForComplete(time.Second); err == nil {
		kvSuite.T().Fatalf("expected to fail, but completed succesfully")
	}
}

func (kvSuite *KvTestSuite) TestSaveModeAppendUpdateExpressionNewAttributes() {
	table := fmt.Sprintf("TestSaveModeAppendNewAttribute%d", time.Now().UnixNano())

	columnNames := []string{"n1", "n2"}
	frame := kvSuite.generateSequentialSampleFrame(3, "idx", columnNames)
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

	if err := appender.WaitForComplete(time.Second); err != nil {
		kvSuite.T().Fatal(err)
	}

	// Save a frame to the same path
	frame2 := kvSuite.generateSequentialSampleFrame(3, "idx", columnNames)
	wreq = &frames.WriteRequest{
		Backend:  kvSuite.backendName,
		Table:    table,
		SaveMode: frames.Append,
		Expression: "col3={n1}+{n2}",
	}

	appender, err = kvSuite.client.Write(wreq)
	if err != nil {
		kvSuite.T().Fatal(err)
	}

	err = appender.Add(frame2)
	kvSuite.NoError(err, "failed to write frame")

	if err := appender.WaitForComplete(time.Second); err != nil {
		kvSuite.T().Fatal(err)
	}

	rreq := &pb.ReadRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}
	iter, err := kvSuite.client.Read(rreq)
	kvSuite.NoError(err, "error reading from kv")

	for iter.Next() {
		currentFrame := iter.At()
		validateFramesAreEqual(kvSuite.Suite, currentFrame, frame)
	}

	kvSuite.NoError(iter.Err(), "error reading from kv")
}
func (kvSuite *KvTestSuite) TestSaveModeAppendUpdateExpressionChangeAttributeValue() {

}

func (kvSuite *KvTestSuite) TestSaveModeReplaceNewRow() {
	table := fmt.Sprintf("TestSaveModeAppendNewRow%d", time.Now().UnixNano())

	columnNames := []string{"n1", "n2"}
	frame := kvSuite.generateRandomSampleFrame(3, "idx", columnNames)
	wreq := &frames.WriteRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	appender, err := kvSuite.client.Write(wreq)
	if err != nil {
		kvSuite.T().Fatal(err)
	}

	err = appender.Add(frame)
	kvSuite.NoError(err, "failed to write frame")

	if err := appender.WaitForComplete(time.Second); err != nil {
		kvSuite.T().Fatal(err)
	}

	// Save a frame to the same path
	frame2 := kvSuite.generateRandomSampleFrame(2, "idx", columnNames)

	wreq = &frames.WriteRequest{
		Backend:  kvSuite.backendName,
		Table:    table,
		SaveMode: frames.Replace,
	}

	appender, err = kvSuite.client.Write(wreq)
	if err != nil {
		kvSuite.T().Fatal(err)
	}

	err = appender.Add(frame2)
	kvSuite.NoError(err, "failed to write frame")

	if err := appender.WaitForComplete(time.Second); err != nil {
		kvSuite.T().Fatal(err)
	}

	rreq := &pb.ReadRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}
	iter, err := kvSuite.client.Read(rreq)
	kvSuite.NoError(err, "error reading from kv")
	for iter.Next() {
		currentFrame := iter.At()
		kvSuite.Require().EqualValues(columnNames, currentFrame.Names(), "expected column names do not match actual")
		kvSuite.Require().Equal(5, currentFrame.Len(), "frame is not in the expected length")
	}

	kvSuite.NoError(iter.Err(), "error reading from kv")
}

func (kvSuite *KvTestSuite) TestSaveModeReplaceNewAttribute() {
	table := fmt.Sprintf("TestSaveModeAppendNewAttribute%d", time.Now().UnixNano())

	columnNames := []string{"n1", "n2"}
	frame := kvSuite.generateSequentialSampleFrame(3, "idx", columnNames)
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

	if err := appender.WaitForComplete(time.Second); err != nil {
		kvSuite.T().Fatal(err)
	}

	// Save a frame to the same path
	newColumnNames := []string{"n3"}
	frame2 := kvSuite.generateSequentialSampleFrame(3, "idx", newColumnNames)

	wreq = &frames.WriteRequest{
		Backend:  kvSuite.backendName,
		Table:    table,
		SaveMode: frames.Replace,
	}

	appender, err = kvSuite.client.Write(wreq)
	if err != nil {
		kvSuite.T().Fatal(err)
	}

	err = appender.Add(frame2)
	kvSuite.NoError(err, "failed to write frame")

	if err := appender.WaitForComplete(time.Second); err != nil {
		kvSuite.T().Fatal(err)
	}

	rreq := &pb.ReadRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}
	iter, err := kvSuite.client.Read(rreq)
	kvSuite.NoError(err, "error reading from kv")
	allColumns := append(columnNames, newColumnNames...)
	for iter.Next() {
		currentFrame := iter.At()
		kvSuite.Require().EqualValues(allColumns, currentFrame.Names(), "expected column names do not match actual")
		kvSuite.Require().Equal(3, currentFrame.Len(), "frame is not in the expected length")
	}

	kvSuite.NoError(iter.Err(), "error reading from kv")
}

func (kvSuite *KvTestSuite) TestSaveModeReplaceSameAttributeDifferentValues() {
	table := fmt.Sprintf("TestSaveModeAppendNewAttribute%d", time.Now().UnixNano())

	columnNames := []string{"n1", "n2"}
	frame := kvSuite.generateSequentialSampleFrame(3, "idx", columnNames)
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

	if err := appender.WaitForComplete(time.Second); err != nil {
		kvSuite.T().Fatal(err)
	}

	// Save a frame to the same path
	frame2 := kvSuite.generateSequentialSampleFrame(3, "idx", columnNames)

	wreq = &frames.WriteRequest{
		Backend:  kvSuite.backendName,
		Table:    table,
		SaveMode: frames.Replace,
	}

	appender, err = kvSuite.client.Write(wreq)
	if err != nil {
		kvSuite.T().Fatal(err)
	}

	err = appender.Add(frame2)
	kvSuite.NoError(err, "failed to write frame")

	if err := appender.WaitForComplete(time.Second); err != nil {
		kvSuite.T().Fatal(err)
	}

	rreq := &pb.ReadRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}
	iter, err := kvSuite.client.Read(rreq)
	kvSuite.NoError(err, "error reading from kv")

	for iter.Next() {
		currentFrame := iter.At()
		validateFramesAreEqual(kvSuite.Suite, currentFrame, frame2)
	}

	kvSuite.NoError(iter.Err(), "error reading from kv")
}

func (kvSuite *KvTestSuite) TestSaveModeReplaceChangeColumnType() {
	table := fmt.Sprintf("TestSaveModeReplaceChangeColumnType%d", time.Now().UnixNano())

	columnNames := []string{"n1", "n2"}
	frame := kvSuite.generateSequentialSampleFrame(3, "idx", columnNames)
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

	if err := appender.WaitForComplete(time.Second); err != nil {
		kvSuite.T().Fatal(err)
	}

	// Save a frame to the same path with different types
	frame2 := kvSuite.generateSequentialSampleFrameWithTypes(3, "idx", map[string]string{"n1": "float", "n2": "string"})

	wreq = &frames.WriteRequest{
		Backend:  kvSuite.backendName,
		Table:    table,
		SaveMode: frames.Replace,
	}

	appender, err = kvSuite.client.Write(wreq)
	if err != nil {
		kvSuite.T().Fatal(err)
	}

	err = appender.Add(frame2)
	kvSuite.NoError(err, "failed to write frame")

	if err := appender.WaitForComplete(time.Second); err == nil {
		kvSuite.T().Fatalf("expected to fail, but completed succesfully")
	}
}

func (kvSuite *KvTestSuite) TestSaveModeReplaceChangeIndexName() {
	table := fmt.Sprintf("TestSaveModeReplaceChangeIndexName%d", time.Now().UnixNano())

	columnNames := []string{"n1", "n2"}
	frame := kvSuite.generateSequentialSampleFrame(3, "idx", columnNames)
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

	if err := appender.WaitForComplete(time.Second); err != nil {
		kvSuite.T().Fatal(err)
	}

	// Save a frame to the same path with different types
	frame2 := kvSuite.generateSequentialSampleFrame(3, "kuku", columnNames)

	fmt.Printf("original index %v, new index %v", frame.Indices()[0].Name(), frame2.Indices()[0].Name())
	wreq = &frames.WriteRequest{
		Backend:  kvSuite.backendName,
		Table:    table,
		SaveMode: frames.Replace,
	}

	appender, err = kvSuite.client.Write(wreq)
	if err != nil {
		kvSuite.T().Fatal(err)
	}

	err = appender.Add(frame2)
	kvSuite.NoError(err, "failed to write frame")

	if err := appender.WaitForComplete(time.Second); err == nil {
		kvSuite.T().Fatalf("expected to fail, but completed succesfully")
	}
}


func (kvSuite *KvTestSuite) TestSaveModeReplaceUpdateExpressionNewAttributes() {}
func (kvSuite *KvTestSuite) TestSaveModeReplaceUpdateExpressionChangeAttributeValue() {}