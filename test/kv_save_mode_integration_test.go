package test

import (
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/v3io/frames"
	"github.com/v3io/frames/pb"
	"github.com/v3io/frames/v3ioutils"
	v3io "github.com/v3io/v3io-go/pkg/dataplane"
)

// === SaveMode - errorIfTableExists

func (kvSuite *KvTestSuite) TestSaveModeErrorIfExistsTableExists() {
	table := fmt.Sprintf("TestSaveModeErrorIfExistsTableExists%d", time.Now().UnixNano())

	frame := kvSuite.generateRandomSampleFrame(5, "idx", []string{"n1", "n2"})

	wreq := &frames.WriteRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	appender, err := kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame)
	kvSuite.Require().NoError(err)
	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err, "error while saving")
	// Save a frame to the same path
	wreq = &frames.WriteRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	appender, err = kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame)
	kvSuite.Require().NoError(err, "error while saving")

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().Error(err, "expected an error but finished successfully")
}

// === SaveMode - OverwriteTable

func (kvSuite *KvTestSuite) TestSaveModeOverwriteTableExists() {
	table := fmt.Sprintf("TestSaveModeOverwriteTableExists%d", time.Now().UnixNano())

	frame := kvSuite.generateRandomSampleFrame(5, "idx", []string{"n1", "n2"})
	wreq := &frames.WriteRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	appender, err := kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame)
	kvSuite.Require().NoError(err, "failed to write frame")
	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err)

	// Save a frame to the same path
	newColumns := []string{"n3", "n4"}
	frame2 := kvSuite.generateRandomSampleFrame(5, "idx", newColumns)
	wreq = &frames.WriteRequest{
		Backend:  kvSuite.backendName,
		Table:    table,
		SaveMode: frames.OverwriteTable,
	}

	appender, err = kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame2)
	kvSuite.Require().NoError(err, "failed to write frame")

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err)

	rreq := &pb.ReadRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}
	iter, err := kvSuite.client.Read(rreq)
	kvSuite.Require().NoError(err, "error reading from kv")
	for iter.Next() {
		kvSuite.EqualValues(newColumns, iter.At().Names(), "expected column names do not match actual")
	}

	kvSuite.Require().NoError(iter.Err(), "error reading from kv")
}

func (kvSuite *KvTestSuite) TestSaveModeOverwriteTableDoesntExists() {
	table := fmt.Sprintf("TestSaveModeOverwriteTableDoesntExists%d", time.Now().UnixNano())

	newColumns := []string{"n3", "n4"}
	frame := kvSuite.generateRandomSampleFrame(5, "idx", newColumns)
	wreq := &frames.WriteRequest{
		Backend:  kvSuite.backendName,
		Table:    table,
		SaveMode: frames.OverwriteTable,
	}

	appender, err := kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame)
	kvSuite.Require().NoError(err, "failed to write frame")

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err)

	rreq := &pb.ReadRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}
	iter, err := kvSuite.client.Read(rreq)
	kvSuite.Require().NoError(err, "error reading from kv")
	for iter.Next() {
		kvSuite.EqualValues(newColumns, iter.At().Names(), "expected column names do not match actual")
	}

	kvSuite.Require().NoError(iter.Err(), "error reading from kv")
}

// === SaveMode - UpdateItem

func (kvSuite *KvTestSuite) TestSaveModeUpdateItemNewRow() {
	table := fmt.Sprintf("TestSaveModeUpdateItemNewRow%d", time.Now().UnixNano())

	columnNames := []string{"n1", "n2"}
	frame := kvSuite.generateRandomSampleFrame(3, "idx", columnNames)
	wreq := &frames.WriteRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	appender, err := kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame)
	kvSuite.Require().NoError(err, "failed to write frame")

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err)

	// Save a frame to the same path
	frame2 := kvSuite.generateRandomSampleFrame(2, "idx", columnNames)

	wreq = &frames.WriteRequest{
		Backend:  kvSuite.backendName,
		Table:    table,
		SaveMode: frames.UpdateItem,
	}

	appender, err = kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame2)
	kvSuite.Require().NoError(err, "failed to write frame")

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err)

	rreq := &pb.ReadRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}
	iter, err := kvSuite.client.Read(rreq)
	kvSuite.Require().NoError(err, "error reading from kv")
	for iter.Next() {
		currentFrame := iter.At()
		kvSuite.Require().EqualValues(columnNames, currentFrame.Names(), "expected column names do not match actual")
		kvSuite.Require().Equal(5, currentFrame.Len(), "frame is not in the expected length")
	}

	kvSuite.Require().NoError(iter.Err(), "error reading from kv")
}

func (kvSuite *KvTestSuite) TestSaveModeUpdateItemNewAttribute() {
	table := fmt.Sprintf("TestSaveModeUpdateItemNewAttribute%d", time.Now().UnixNano())

	columnNames := []string{"n1", "n2"}
	frame := kvSuite.generateSequentialSampleFrame(3, "idx", columnNames)
	wreq := &frames.WriteRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	appender, err := kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame)
	kvSuite.Require().NoError(err)

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err)

	// Save a frame to the same path
	newColumnNames := []string{"n3"}
	frame2 := kvSuite.generateSequentialSampleFrame(3, "idx", newColumnNames)

	wreq = &frames.WriteRequest{
		Backend:  kvSuite.backendName,
		Table:    table,
		SaveMode: frames.UpdateItem,
	}

	appender, err = kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame2)
	kvSuite.Require().NoError(err, "failed to write frame")

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err)

	rreq := &pb.ReadRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}
	iter, err := kvSuite.client.Read(rreq)
	kvSuite.Require().NoError(err, "error reading from kv")
	allColumns := append(columnNames, newColumnNames...)
	for iter.Next() {
		currentFrame := iter.At()
		kvSuite.Require().EqualValues(allColumns, currentFrame.Names(), "expected column names do not match actual")
		kvSuite.Require().Equal(3, currentFrame.Len(), "frame is not in the expected length")
	}

	kvSuite.Require().NoError(iter.Err(), "error reading from kv")
}

func (kvSuite *KvTestSuite) TestSaveModeUpdateItemSameAttributeDifferentValues() {
	table := fmt.Sprintf("TestSaveModeUpdateItemSameAttributeDifferentValues%d", time.Now().UnixNano())

	columnNames := []string{"n1", "n2"}
	frame := kvSuite.generateSequentialSampleFrame(3, "idx", columnNames)
	wreq := &frames.WriteRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	appender, err := kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame)
	kvSuite.Require().NoError(err)

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err)

	// Save a frame to the same path
	frame2 := kvSuite.generateSequentialSampleFrame(3, "idx", columnNames)

	wreq = &frames.WriteRequest{
		Backend:  kvSuite.backendName,
		Table:    table,
		SaveMode: frames.UpdateItem,
	}

	appender, err = kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame2)
	kvSuite.Require().NoError(err, "failed to write frame")

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err)

	rreq := &pb.ReadRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}
	iter, err := kvSuite.client.Read(rreq)
	kvSuite.Require().NoError(err, "error reading from kv")

	for iter.Next() {
		currentFrame := iter.At()
		validateFramesAreEqual(kvSuite.Suite, currentFrame, frame2)
	}

	kvSuite.Require().NoError(iter.Err(), "error reading from kv")
}

func (kvSuite *KvTestSuite) TestSaveModeUpdateItemChangeColumnType() {
	table := fmt.Sprintf("TestSaveModeUpdateItemChangeColumnType%d", time.Now().UnixNano())

	columnNames := []string{"n1", "n2"}
	frame := kvSuite.generateSequentialSampleFrame(3, "idx", columnNames)
	wreq := &frames.WriteRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	appender, err := kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame)
	kvSuite.Require().NoError(err)

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err)

	// Save a frame to the same path with different types
	frame2 := kvSuite.generateSequentialSampleFrameWithTypes(3, "idx", map[string]string{"n1": "float", "n2": "string"})

	wreq = &frames.WriteRequest{
		Backend:  kvSuite.backendName,
		Table:    table,
		SaveMode: frames.UpdateItem,
	}

	appender, err = kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame2)
	kvSuite.Require().NoError(err, "failed to write frame")

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().Error(err, "expected to fail, but completed succesfully")
}

func (kvSuite *KvTestSuite) TestSaveModeUpdateItemChangeNumricColumnType() {
	table := fmt.Sprintf("TestSaveModeUpdateItemChangeNumricColumnType%d", time.Now().UnixNano())

	frame := kvSuite.generateSequentialSampleFrameWithTypes(3, "idx", map[string]string{"n1": "float", "n2": "int"})
	wreq := &frames.WriteRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	appender, err := kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame)
	kvSuite.Require().NoError(err)

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err)

	// Save a frame to the same path with different types
	frame2 := kvSuite.generateSequentialSampleFrameWithTypes(2, "idx", map[string]string{"n1": "int", "n2": "float"})
	wreq = &frames.WriteRequest{
		Backend:  kvSuite.backendName,
		Table:    table,
		SaveMode: frames.UpdateItem,
	}

	appender, err = kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame2)
	kvSuite.Require().NoError(err, "failed to write frame")

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err, "failed to WaitForComplete")

	schemaInput := &v3io.GetObjectInput{Path: table + "/.#schema"}
	resp, err := kvSuite.v3ioContainer.GetObjectSync(schemaInput)
	if err != nil {
		kvSuite.T().Fatal(err.Error())
	}
	schema := &v3ioutils.OldV3ioSchema{}
	if err := json.Unmarshal(resp.HTTPResponse.Body(), schema); err != nil {
		kvSuite.T().Fatal(err)
	}

	for _, field := range schema.Fields {
		if field.Name != "idx" && field.Type != "double" {
			kvSuite.T().Fatal("expected type float, got ", field.Type)
		}
	}

}

func (kvSuite *KvTestSuite) TestSaveModeUpdateItemChangeIndexName() {
	table := fmt.Sprintf("TestSaveModeUpdateItemChangeIndexName%d", time.Now().UnixNano())

	columnNames := []string{"n1", "n2"}
	frame := kvSuite.generateSequentialSampleFrame(3, "idx", columnNames)
	wreq := &frames.WriteRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	appender, err := kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame)
	kvSuite.Require().NoError(err)

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err)

	// Save a frame to the same path with different types
	frame2 := kvSuite.generateSequentialSampleFrame(3, "kuku", columnNames)

	wreq = &frames.WriteRequest{
		Backend:  kvSuite.backendName,
		Table:    table,
		SaveMode: frames.UpdateItem,
	}

	appender, err = kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame2)
	kvSuite.Require().NoError(err, "failed to write frame")

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().Error(err, "expected to fail, but completed succesfully")
}

func (kvSuite *KvTestSuite) TestSaveModeUpdateItemUpdateExpressionNewAttributes() {
	table := fmt.Sprintf("TestSaveModeUpdateItemUpdateExpressionNewAttributes%d", time.Now().UnixNano())

	columnNames := []string{"n1", "n2"}
	frame := kvSuite.generateSequentialSampleFrame(3, "idx", columnNames)
	wreq := &frames.WriteRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	appender, err := kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame)
	kvSuite.Require().NoError(err)

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err)

	// Save a frame to the same path
	frame2 := kvSuite.generateSequentialSampleFrame(3, "idx", columnNames)
	wreq = &frames.WriteRequest{
		Backend:    kvSuite.backendName,
		Table:      table,
		SaveMode:   frames.UpdateItem,
		Expression: "n3=n1+n2",
	}

	appender, err = kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame2)
	kvSuite.Require().NoError(err, "failed to write frame")

	err = appender.WaitForComplete(time.Second)

	// TODO: once schema inferring will be fix varify that schema was changed.
	input := v3io.GetItemsInput{AttributeNames: []string{"*"}}

	iter, err := v3ioutils.NewAsyncItemsCursor(
		kvSuite.v3ioContainer, &input,
		1, nil,
		kvSuite.internalLogger, 0, []string{table + "/"},
		"", "")

	for iter.Next() {
		currentRow := iter.GetItem()

		key, _ := currentRow.GetFieldString("__name")
		switch key {
		case ".#schema":
			continue
		default:
			n1, ok := currentRow["n1"]
			kvSuite.Require().True(ok, "item %v don't have column 'n1'", key)

			n2, ok := currentRow["n2"]
			kvSuite.Require().True(ok, "item %v don't have column 'n1'", key)
			n3, ok := currentRow["n3"]
			kvSuite.Require().True(ok, "item %v don't have column 'n1'", key)

			kvSuite.Require().Equal(n1.(float64)+n2.(float64), n3.(float64),
				"column 'n3' wasnt set correctly for item '%v'", key)
		}
	}

	kvSuite.Require().NoError(iter.Err(), "error querying items")
}

func (kvSuite *KvTestSuite) TestSaveModeUpdateItemUpdateExpressionChangeAttributeValue() {
	table := fmt.Sprintf("TestSaveModeUpdateItemUpdateExpressionChangeAttributeValue%d", time.Now().UnixNano())

	columnNames := []string{"n1", "n2"}
	frame := kvSuite.generateSequentialSampleFrame(3, "idx", columnNames)
	wreq := &frames.WriteRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	appender, err := kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame)
	kvSuite.Require().NoError(err)

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err)

	// Save a frame to the same path
	frame2 := kvSuite.generateSequentialSampleFrame(3, "idx", columnNames)
	wreq = &frames.WriteRequest{
		Backend:    kvSuite.backendName,
		Table:      table,
		SaveMode:   frames.UpdateItem,
		Expression: "n1=666;n2=12",
	}

	appender, err = kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame2)
	kvSuite.Require().NoError(err, "failed to write frame")

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err)

	// TODO: once schema inferring will be fix varify that schema was changed.
	input := v3io.GetItemsInput{AttributeNames: []string{"*"}}

	iter, err := v3ioutils.NewAsyncItemsCursor(
		kvSuite.v3ioContainer, &input,
		1, nil,
		kvSuite.internalLogger, 0, []string{table + "/"},
		"", "")

	for iter.Next() {
		currentRow := iter.GetItem()

		key, _ := currentRow.GetFieldString("__name")
		switch key {
		case ".#schema":
			continue
		default:
			kvSuite.Require().Equal(666, currentRow["n1"],
				"column 'n1' not equal for item '%v'", key)
			kvSuite.Require().Equal(12, currentRow["n2"],
				"column 'n2' not equal for item '%v'", key)
		}
	}

	if iter.Err() != nil {
		kvSuite.T().Fatalf("error querying items got: %v", iter.Err())
	}
}

// === SaveMode - CreateNewItemsOnly

func (kvSuite *KvTestSuite) TestSaveModeCreateNewItemsOnlyNewRow() {
	table := fmt.Sprintf("TestSaveModeCreateNewItemsOnlyNewRow%d", time.Now().UnixNano())

	columnNames := []string{"n1", "n2"}
	frame := kvSuite.generateRandomSampleFrame(3, "idx", columnNames)
	wreq := &frames.WriteRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	appender, err := kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame)
	kvSuite.Require().NoError(err, "failed to write frame")

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err)

	// Save a frame to the same path
	frame2 := kvSuite.generateRandomSampleFrame(2, "idx", columnNames)

	wreq = &frames.WriteRequest{
		Backend:  kvSuite.backendName,
		Table:    table,
		SaveMode: frames.CreateNewItemsOnly,
	}

	appender, err = kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame2)
	kvSuite.Require().NoError(err, "failed to write frame")

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err)

	rreq := &pb.ReadRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}
	iter, err := kvSuite.client.Read(rreq)
	kvSuite.Require().NoError(err, "error reading from kv")
	for iter.Next() {
		currentFrame := iter.At()
		kvSuite.Require().EqualValues(columnNames, currentFrame.Names(), "expected column names do not match actual")
		kvSuite.Require().Equal(5, currentFrame.Len(), "frame is not in the expected length")
	}

	kvSuite.Require().NoError(iter.Err(), "error reading from kv")
}

func (kvSuite *KvTestSuite) TestSaveModeCreateNewItemsOnlyNewAttribute() {
	table := fmt.Sprintf("TestSaveModeCreateNewItemsOnlyNewAttribute%d", time.Now().UnixNano())

	columnNames := []string{"n1", "n2"}
	frame := kvSuite.generateSequentialSampleFrame(3, "idx", columnNames)
	wreq := &frames.WriteRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	appender, err := kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame)
	kvSuite.Require().NoError(err)

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err)

	// Save a frame to the same path
	newColumnNames := []string{"n3"}
	frame2 := kvSuite.generateSequentialSampleFrame(3, "idx", newColumnNames)

	wreq = &frames.WriteRequest{
		Backend:  kvSuite.backendName,
		Table:    table,
		SaveMode: frames.CreateNewItemsOnly,
	}

	appender, err = kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame2)
	kvSuite.Require().NoError(err, "failed to write frame")

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err)

	rreq := &pb.ReadRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}
	iter, err := kvSuite.client.Read(rreq)
	kvSuite.Require().NoError(err, "error reading from kv")

	allColumns := append(columnNames, newColumnNames...)
	for iter.Next() {
		currentFrame := iter.At()
		kvSuite.Require().EqualValues(allColumns, currentFrame.Names(), "column names mismatch")
		data := FrameToDataMap(currentFrame)
		expectedFrameData := FrameToDataMap(frame)
		kvSuite.Require().EqualValues(len(expectedFrameData), len(data), "result length don't match expected")
		fmt.Println("====== got frame =", data)
		for key, expectedRow := range expectedFrameData {
			actualCurrentRow, ok := data[key]
			kvSuite.Require().True(ok, "missing key '%v' from reponse", key)

			kvSuite.Require().Equal(expectedRow["n1"], actualCurrentRow["n1"],
				"'n1' column value for item '%v' mismatch", key)
			kvSuite.Require().Equal(expectedRow["n2"], actualCurrentRow["n2"],
				"'n2' column value for item '%v' mismatch", key)
			kvSuite.Require().True(math.IsNaN(actualCurrentRow["n3"].(float64)),
				"'n3' column for item '%v' expected to be NaN but got '%v'", key, actualCurrentRow["n3"])
		}
	}

	kvSuite.Require().NoError(iter.Err(), "error reading from kv")
}

func (kvSuite *KvTestSuite) TestSaveModeCreateNewItemsOnlySameAttributeDifferentValues() {
	table := fmt.Sprintf("TestSaveModeCreateNewItemsOnlySameAttributeDifferentValues%d", time.Now().UnixNano())

	columnNames := []string{"n1", "n2"}
	frame := kvSuite.generateSequentialSampleFrame(3, "idx", columnNames)
	wreq := &frames.WriteRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	appender, err := kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame)
	kvSuite.Require().NoError(err)

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err)

	// Save a frame to the same path
	frame2 := kvSuite.generateSequentialSampleFrame(3, "idx", columnNames)
	wreq = &frames.WriteRequest{
		Backend:  kvSuite.backendName,
		Table:    table,
		SaveMode: frames.CreateNewItemsOnly,
	}

	appender, err = kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame2)
	kvSuite.Require().NoError(err, "failed to write frame")

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err)

	rreq := &pb.ReadRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}
	iter, err := kvSuite.client.Read(rreq)
	kvSuite.Require().NoError(err, "error reading from kv")

	for iter.Next() {
		currentFrame := iter.At()
		validateFramesAreEqual(kvSuite.Suite, currentFrame, frame)
	}

	kvSuite.Require().NoError(iter.Err(), "error reading from kv")
}

func (kvSuite *KvTestSuite) TestSaveModeCreateNewItemsOnlyChangeColumnType() {
	table := fmt.Sprintf("TestSaveModeCreateNewItemsOnlyChangeColumnType%d", time.Now().UnixNano())

	columnNames := []string{"n1", "n2"}
	frame := kvSuite.generateSequentialSampleFrame(3, "idx", columnNames)
	wreq := &frames.WriteRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	appender, err := kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame)
	kvSuite.Require().NoError(err)

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err)

	// Save a frame to the same path with different types
	frame2 := kvSuite.generateSequentialSampleFrameWithTypes(3, "idx", map[string]string{"n1": "float", "n2": "string"})

	wreq = &frames.WriteRequest{
		Backend:  kvSuite.backendName,
		Table:    table,
		SaveMode: frames.CreateNewItemsOnly,
	}

	appender, err = kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame2)
	kvSuite.Require().NoError(err, "failed to write frame")

	if err := appender.WaitForComplete(time.Second); err == nil {
		kvSuite.T().Fatalf("expected to fail, but completed succesfully")
	}
}

func (kvSuite *KvTestSuite) TestSaveModeCreateNewItemsOnlyChangeIndexName() {
	table := fmt.Sprintf("TestSaveModeCreateNewItemsOnlyChangeIndexName%d", time.Now().UnixNano())

	columnNames := []string{"n1", "n2"}
	frame := kvSuite.generateSequentialSampleFrame(3, "idx", columnNames)
	wreq := &frames.WriteRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	appender, err := kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame)
	kvSuite.Require().NoError(err)

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err)

	// Save a frame to the same path with different types
	frame2 := kvSuite.generateSequentialSampleFrame(3, "kuku", columnNames)

	wreq = &frames.WriteRequest{
		Backend:  kvSuite.backendName,
		Table:    table,
		SaveMode: frames.CreateNewItemsOnly,
	}

	appender, err = kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame2)
	kvSuite.Require().NoError(err, "failed to write frame")

	if err := appender.WaitForComplete(time.Second); err == nil {
		kvSuite.T().Fatalf("expected to fail, but completed succesfully")
	}
}

func (kvSuite *KvTestSuite) TestSaveModeCreateNewItemsOnlyUpdateExpressionNewAttributes() {
	table := fmt.Sprintf("TestSaveModeCreateNewItemsOnlyUpdateExpressionNewAttributes%d", time.Now().UnixNano())

	columnNames := []string{"n1", "n2"}
	frame := kvSuite.generateSequentialSampleFrame(3, "idx", columnNames)
	wreq := &frames.WriteRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	appender, err := kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame)
	kvSuite.Require().NoError(err)

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err)

	// Save a frame to the same path
	frame2 := kvSuite.generateSequentialSampleFrame(3, "idx", columnNames)
	wreq = &frames.WriteRequest{
		Backend:    kvSuite.backendName,
		Table:      table,
		SaveMode:   frames.CreateNewItemsOnly,
		Expression: "n3=n1+n2",
	}

	appender, err = kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame2)
	kvSuite.Require().NoError(err, "failed to write frame")

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err)

	// TODO: once schema inferring will be fix varify that schema was changed.
	input := v3io.GetItemsInput{AttributeNames: []string{"*"}}

	iter, err := v3ioutils.NewAsyncItemsCursor(
		kvSuite.v3ioContainer, &input,
		1, nil,
		kvSuite.internalLogger, 0, []string{table + "/"},
		"", "")

	for iter.Next() {
		currentRow := iter.GetItem()

		key, _ := currentRow.GetFieldString("__name")
		switch key {
		case ".#schema":
			continue
		default:
			_, ok := currentRow["n1"]
			kvSuite.Require().True(ok, "item %v don't have column 'n1'", key)
			_, ok = currentRow["n2"]
			kvSuite.Require().True(ok, "item %v don't have column 'n2'", key)

			_, ok = currentRow["n3"]
			kvSuite.Require().False(ok, "item %v expected to no have column 'n3' but it exists", key)
		}
	}

	if iter.Err() != nil {
		kvSuite.T().Fatalf("error querying items got: %v", iter.Err())
	}
}

func (kvSuite *KvTestSuite) TestSaveModeCreateNewItemsOnlyUpdateExpressionChangeAttributeValue() {
	table := fmt.Sprintf("TestSaveModeCreateNewItemsOnlyUpdateExpressionChangeAttributeValue%d", time.Now().UnixNano())

	columnNames := []string{"n1", "n2"}
	frame := kvSuite.generateSequentialSampleFrame(3, "idx", columnNames)
	wreq := &frames.WriteRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	appender, err := kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame)
	kvSuite.Require().NoError(err)

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err)

	// Save a frame to the same path
	frame2 := kvSuite.generateSequentialSampleFrame(3, "idx", columnNames)
	wreq = &frames.WriteRequest{
		Backend:    kvSuite.backendName,
		Table:      table,
		SaveMode:   frames.CreateNewItemsOnly,
		Expression: "n1=666;n2=12",
	}

	appender, err = kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame2)
	kvSuite.Require().NoError(err, "failed to write frame")

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err)

	rreq := &pb.ReadRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}
	iter, err := kvSuite.client.Read(rreq)
	kvSuite.Require().NoError(err, "error reading from kv")

	for iter.Next() {
		currentFrame := iter.At()
		validateFramesAreEqual(kvSuite.Suite, currentFrame, frame)
	}

	kvSuite.Require().NoError(iter.Err(), "error reading from kv")
}

// === SaveMode - OverwriteItem

func (kvSuite *KvTestSuite) TestSaveModeOverwriteItemNewRow() {
	table := fmt.Sprintf("TestSaveModeOverwriteItemNewRow%d", time.Now().UnixNano())

	columnNames := []string{"n1", "n2"}
	frame := kvSuite.generateRandomSampleFrame(3, "idx", columnNames)
	wreq := &frames.WriteRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	appender, err := kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame)
	kvSuite.Require().NoError(err, "failed to write frame")

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err)

	// Save a frame to the same path
	frame2 := kvSuite.generateRandomSampleFrame(2, "idx", columnNames)

	wreq = &frames.WriteRequest{
		Backend:  kvSuite.backendName,
		Table:    table,
		SaveMode: frames.OverwriteItem,
	}

	appender, err = kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame2)
	kvSuite.Require().NoError(err, "failed to write frame")

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err)

	rreq := &pb.ReadRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}
	iter, err := kvSuite.client.Read(rreq)
	kvSuite.Require().NoError(err, "error reading from kv")
	for iter.Next() {
		currentFrame := iter.At()
		kvSuite.Require().EqualValues(columnNames, currentFrame.Names(), "expected column names do not match actual")
		kvSuite.Require().Equal(5, currentFrame.Len(), "frame is not in the expected length")
	}

	kvSuite.Require().NoError(iter.Err(), "error reading from kv")
}

func (kvSuite *KvTestSuite) TestSaveModeOverwriteItemNewAttribute() {
	table := fmt.Sprintf("TestSaveModeOverwriteItemNewAttribute%d", time.Now().UnixNano())

	columnNames := []string{"n1", "n2"}
	frame := kvSuite.generateSequentialSampleFrame(3, "idx", columnNames)
	wreq := &frames.WriteRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	appender, err := kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame)
	kvSuite.Require().NoError(err)

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err)

	// Save a frame to the same path
	newColumnNames := []string{"n3"}
	frame2 := kvSuite.generateSequentialSampleFrame(3, "idx", newColumnNames)

	wreq = &frames.WriteRequest{
		Backend:  kvSuite.backendName,
		Table:    table,
		SaveMode: frames.OverwriteItem,
	}

	appender, err = kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame2)
	kvSuite.Require().NoError(err, "failed to write frame")

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err)

	rreq := &pb.ReadRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}
	iter, err := kvSuite.client.Read(rreq)
	kvSuite.Require().NoError(err, "error reading from kv")
	for iter.Next() {
		currentFrame := iter.At()
		data := FrameToDataMap(currentFrame)
		indexCol := frame2.Indices()[0]
		n3Col, _ := frame2.Column("n3")
		kvSuite.Require().Equal(frame2.Len(), len(data), "frame length is not as expected")

		for i := 0; i < frame2.Len(); i++ {
			currentKey, _ := indexCol.IntAt(i)
			currentRow, ok := data[fmt.Sprintf("%v", currentKey)]
			kvSuite.Require().True(ok, "response does not contain expected row with key: '%v'", currentKey)
			kvSuite.Require().True(math.IsNaN(currentRow["n1"].(float64)),
				"'n1' column for item '%v' was expected to be NaN but got '%v'", currentKey, currentRow["n1"])
			kvSuite.Require().True(math.IsNaN(currentRow["n2"].(float64)),
				"'n2' column for item '%v' was expected to be NaN but got '%v'", currentKey, currentRow["n2"])

			currentN3Value, _ := n3Col.FloatAt(i)
			kvSuite.Require().Equal(currentN3Value, currentRow["n3"].(float64),
				"'n3' column is not equal for item '%v'", currentKey)
		}
	}

	kvSuite.Require().NoError(iter.Err(), "error reading from kv")
}

func (kvSuite *KvTestSuite) TestSaveModeOverwriteItemSameAttributeDifferentValues() {
	table := fmt.Sprintf("TestSaveModeOverwriteItemSameAttributeDifferentValues%d", time.Now().UnixNano())

	columnNames := []string{"n1", "n2"}
	frame := kvSuite.generateSequentialSampleFrame(3, "idx", columnNames)
	wreq := &frames.WriteRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	appender, err := kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame)
	kvSuite.Require().NoError(err)

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err)

	// Save a frame to the same path
	frame2 := kvSuite.generateSequentialSampleFrame(3, "idx", columnNames)
	wreq = &frames.WriteRequest{
		Backend:  kvSuite.backendName,
		Table:    table,
		SaveMode: frames.OverwriteItem,
	}

	appender, err = kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame2)
	kvSuite.Require().NoError(err, "failed to write frame")

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err)

	rreq := &pb.ReadRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}
	iter, err := kvSuite.client.Read(rreq)
	kvSuite.Require().NoError(err, "error reading from kv")

	for iter.Next() {
		currentFrame := iter.At()
		validateFramesAreEqual(kvSuite.Suite, currentFrame, frame2)
	}

	kvSuite.Require().NoError(iter.Err(), "error reading from kv")
}

func (kvSuite *KvTestSuite) TestSaveModeOverwriteItemChangeColumnType() {
	table := fmt.Sprintf("TestSaveModeOverwriteItemChangeColumnType%d", time.Now().UnixNano())

	columnNames := []string{"n1", "n2"}
	frame := kvSuite.generateSequentialSampleFrame(3, "idx", columnNames)
	wreq := &frames.WriteRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	appender, err := kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame)
	kvSuite.Require().NoError(err)

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err)

	// Save a frame to the same path with different types
	frame2 := kvSuite.generateSequentialSampleFrameWithTypes(3, "idx", map[string]string{"n1": "float", "n2": "string"})

	wreq = &frames.WriteRequest{
		Backend:  kvSuite.backendName,
		Table:    table,
		SaveMode: frames.OverwriteItem,
	}

	appender, err = kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame2)
	kvSuite.Require().NoError(err, "failed to write frame")

	if err := appender.WaitForComplete(time.Second); err == nil {
		kvSuite.T().Fatalf("expected to fail, but completed succesfully")
	}
}

func (kvSuite *KvTestSuite) TestSaveModeOverwriteItemChangeIndexName() {
	table := fmt.Sprintf("TestSaveModeOverwriteItemChangeIndexName%d", time.Now().UnixNano())

	columnNames := []string{"n1", "n2"}
	frame := kvSuite.generateSequentialSampleFrame(3, "idx", columnNames)
	wreq := &frames.WriteRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	appender, err := kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame)
	kvSuite.Require().NoError(err)

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err)

	// Save a frame to the same path with different types
	frame2 := kvSuite.generateSequentialSampleFrame(3, "kuku", columnNames)

	wreq = &frames.WriteRequest{
		Backend:  kvSuite.backendName,
		Table:    table,
		SaveMode: frames.OverwriteItem,
	}

	appender, err = kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame2)
	kvSuite.Require().NoError(err, "failed to write frame")

	if err := appender.WaitForComplete(time.Second); err == nil {
		kvSuite.T().Fatalf("expected to fail, but completed succesfully")
	}
}

func (kvSuite *KvTestSuite) TestSaveModeOverwriteItemUpdateExpressionNewAttributes() {
	table := fmt.Sprintf("TestSaveModeOverwriteItemUpdateExpressionNewAttributes%d", time.Now().UnixNano())

	columnNames := []string{"n1", "n2"}
	frame := kvSuite.generateSequentialSampleFrame(3, "idx", columnNames)
	wreq := &frames.WriteRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	appender, err := kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame)
	kvSuite.Require().NoError(err)

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err)

	// Save a frame to the same path
	frame2 := kvSuite.generateSequentialSampleFrame(3, "idx", columnNames)
	wreq = &frames.WriteRequest{
		Backend:    kvSuite.backendName,
		Table:      table,
		SaveMode:   frames.OverwriteItem,
		Expression: "n3=n1+n2",
	}

	appender, err = kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame2)
	kvSuite.Require().NoError(err, "failed to write frame")

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err)

	// TODO: once schema inferring will be fix varify that schema was changed.
	input := v3io.GetItemsInput{AttributeNames: []string{"*"}}

	iter, err := v3ioutils.NewAsyncItemsCursor(
		kvSuite.v3ioContainer, &input,
		1, nil,
		kvSuite.internalLogger, 0, []string{table + "/"},
		"", "")

	for iter.Next() {
		currentRow := iter.GetItem()

		key, _ := currentRow.GetFieldString("__name")
		switch key {
		case ".#schema":
			continue
		default:
			// n1 and n2 should not exist
			_, ok := currentRow["n1"]
			kvSuite.Require().False(ok, "item %v expected to not have column 'n1' but it exists", key)
			_, ok = currentRow["n2"]
			kvSuite.Require().False(ok, "item %v expected to not have column 'n2' but it exists", key)

			_, ok = currentRow["n3"]
			kvSuite.Require().True(ok, "item %v don't have column 'n3'", key)
		}
	}

	if iter.Err() != nil {
		kvSuite.T().Fatalf("error querying items got: %v", iter.Err())
	}
}

func (kvSuite *KvTestSuite) TestSaveModeOverwriteItemUpdateExpressionChangeAttributeValue() {
	table := fmt.Sprintf("TestSaveModeOverwriteItemUpdateExpressionChangeAttributeValue%d", time.Now().UnixNano())

	columnNames := []string{"n1", "n2"}
	frame := kvSuite.generateSequentialSampleFrame(3, "idx", columnNames)
	wreq := &frames.WriteRequest{
		Backend: kvSuite.backendName,
		Table:   table,
	}

	appender, err := kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame)
	kvSuite.Require().NoError(err)

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err)

	// Save a frame to the same path
	frame2 := kvSuite.generateSequentialSampleFrame(3, "idx", columnNames)
	wreq = &frames.WriteRequest{
		Backend:    kvSuite.backendName,
		Table:      table,
		SaveMode:   frames.OverwriteItem,
		Expression: "n1=666;n2=12",
	}

	appender, err = kvSuite.client.Write(wreq)
	kvSuite.Require().NoError(err)

	err = appender.Add(frame2)
	kvSuite.Require().NoError(err, "failed to write frame")

	err = appender.WaitForComplete(time.Second)
	kvSuite.Require().NoError(err)

	input := v3io.GetItemsInput{AttributeNames: []string{"*"}}

	iter, err := v3ioutils.NewAsyncItemsCursor(
		kvSuite.v3ioContainer, &input,
		1, nil,
		kvSuite.internalLogger, 0, []string{table + "/"},
		"", "")

	for iter.Next() {
		currentRow := iter.GetItem()

		key, _ := currentRow.GetFieldString("__name")
		switch key {
		case ".#schema":
			continue
		default:
			// n1 and n2 should not exist
			n1, ok := currentRow["n1"]
			kvSuite.Require().True(ok, "item %v don't have column 'n1'", key)
			kvSuite.Require().Equal(666, n1, "item %v has wrong value for column'n1'", key)
			n2, ok := currentRow["n2"]
			kvSuite.Require().True(ok, "item %v don't have column 'n2'", key)
			kvSuite.Require().Equal(12, n2, "item %v has wrong value for column'n2'", key)

			_, ok = currentRow["n3"]
			kvSuite.Require().False(ok, "item %v expected to not have column 'n3' but it exists", key)
		}
	}
	kvSuite.Require().NoError(iter.Err(), "error querying items")
}
