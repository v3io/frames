package test

import (
	"fmt"
	"testing"
	"time"

	"github.com/nuclio/logger"
	"github.com/stretchr/testify/suite"
	"github.com/v3io/frames"
	"github.com/v3io/frames/pb"
	v3io "github.com/v3io/v3io-go/pkg/dataplane"
)

// Very high upper bound to catch all samples
const veryHighTimestamp = "157728271500000"

type TsdbTestSuite struct {
	suite.Suite
	tablePath      string
	suiteTimestamp int64
	basicQueryTime int64
	client         frames.Client
	backendName    string
	v3ioContainer  v3io.Container
	internalLogger logger.Logger
}

func GetTsdbTestsConstructorFunc() SuiteCreateFunc {
	return func(client frames.Client, v3ioContainer v3io.Container, internalLogger logger.Logger) suite.TestingSuite {
		return &TsdbTestSuite{client: client,
			backendName:    "tsdb",
			v3ioContainer:  v3ioContainer,
			internalLogger: internalLogger}
	}
}

func (tsdbSuite *TsdbTestSuite) generateSampleFrame(t testing.TB, end time.Time) frames.Frame {
	size := 60
	times := make([]time.Time, size)
	for i := range times {
		times[i] = end.Add(-time.Duration(size - i) * time.Second * 300)
	}

	index, err := frames.NewSliceColumn("time", times)
	if err != nil {
		t.Fatal(err)
	}

	columns := []frames.Column{
		FloatCol(t, "cpu", index.Len()),
		FloatCol(t, "mem", index.Len()),
		FloatCol(t, "disk", index.Len()),
	}

	frame, err := frames.NewFrame(columns, []frames.Column{index}, nil)
	if err != nil {
		t.Fatal(err)
	}

	return frame
}

func (tsdbSuite *TsdbTestSuite) generateSampleFrameWithStringMetric(t testing.TB, anchorTime time.Time) frames.Frame {
	size := 60 // TODO
	times := make([]time.Time, size)
	end := anchorTime.Truncate(time.Hour)
	for i := range times {
		times[i] = end.Add(-time.Duration(size - i) * time.Second * 300)
	}

	index, err := frames.NewSliceColumn("time", times)
	if err != nil {
		t.Fatal(err)
	}

	columns := []frames.Column{
		StringCol(t, "host", index.Len()),
	}

	frame, err := frames.NewFrame(columns, []frames.Column{index}, nil)
	if err != nil {
		t.Fatal(err)
	}

	return frame
}

func (tsdbSuite *TsdbTestSuite) SetupSuite() {
	if tsdbSuite.client == nil {
		tsdbSuite.FailNow("client not set")
	}
}

func (tsdbSuite *TsdbTestSuite) TestAll() {
	table := fmt.Sprintf("tsdb_test_all%d", time.Now().UnixNano())

	tsdbSuite.T().Log("create")
	req := &pb.CreateRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
		Rate:    "1/m",
	}
	if err := tsdbSuite.client.Create(req); err != nil {
		tsdbSuite.T().Fatal(err)
	}

	tsdbSuite.T().Log("write")
	anchorTime, _ := time.Parse(time.RFC3339, "2019-12-12T05:00:00Z")
	frame := tsdbSuite.generateSampleFrame(tsdbSuite.T(), anchorTime)
	wreq := &frames.WriteRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
	}

	appender, err := tsdbSuite.client.Write(wreq)
	if err != nil {
		tsdbSuite.T().Fatal(err)
	}

	tsdbSuite.T().Logf("saving frame to '%v', length: %v", table, frame.Len())
	if err := appender.Add(frame); err != nil {
		tsdbSuite.T().Fatal(err)
	}

	if err := appender.WaitForComplete(3 * time.Second); err != nil {
		tsdbSuite.T().Fatal(err)
	}

	time.Sleep(3 * time.Second) // Let DB sync

	tsdbSuite.T().Log("read")
	rreq := &pb.ReadRequest{
		Backend:      tsdbSuite.backendName,
		Table:        table,
		Start:        "0",
		End:          veryHighTimestamp,
		MessageLimit: 10,
	}

	it, err := tsdbSuite.client.Read(rreq)
	if err != nil {
		tsdbSuite.T().Fatal(err)
	}

	resultCount := 0
	for it.Next() {
		fr := it.At()
		resultCount += fr.Len()
	}
	// TODO: More checks
	if !(resultCount == frame.Len() || resultCount-1 == frame.Len()) {
		tsdbSuite.T().Fatalf("wrong length: %d != %d", resultCount, frame.Len())
	}

	if err := it.Err(); err != nil {
		tsdbSuite.T().Fatal(err)
	}

	tsdbSuite.T().Log("delete")
	dreq := &pb.DeleteRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
	}

	if err := tsdbSuite.client.Delete(dreq); err != nil {
		tsdbSuite.T().Fatal(err)
	}

}

func (tsdbSuite *TsdbTestSuite) TestRegressionIG14560() {
	table := fmt.Sprintf("tsdb_test_all%d", time.Now().UnixNano())

	tsdbSuite.T().Log("create")
	req := &pb.CreateRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
		Rate:    "1/m",
	}
	err := tsdbSuite.client.Create(req)
	tsdbSuite.Require().NoError(err)

	tsdbSuite.T().Log("write")
	times := []time.Time{
		time.Unix(1559668665, 0),
		time.Unix(1559668705, 0),
		time.Unix(1559668755, 0),
		time.Unix(1559668965, 0),
		time.Unix(1559669265, 0),
		time.Unix(1559669965, 0),
	}

	cpu := []float64{12.4, 30.1, 18.2, 234.2, 23.11, 91.2}

	cpuCol, err := frames.NewSliceColumn("cpu", cpu)
	tsdbSuite.Require().NoError(err)

	index, err := frames.NewSliceColumn("time", times)
	tsdbSuite.Require().NoError(err)

	columns := []frames.Column{cpuCol}
	frame, err := frames.NewFrame(columns, []frames.Column{index}, nil)
	tsdbSuite.Require().NoError(err)

	wreq := &frames.WriteRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
	}

	appender, err := tsdbSuite.client.Write(wreq)
	if err != nil {
		tsdbSuite.T().Fatal(err)
	}

	tsdbSuite.T().Logf("saving frame to '%v', length: %v", table, frame.Len())
	if err := appender.Add(frame); err != nil {
		tsdbSuite.T().Fatal(err)
	}

	if err := appender.WaitForComplete(3 * time.Second); err != nil {
		tsdbSuite.T().Fatal(err)
	}

	time.Sleep(3 * time.Second) // Let DB sync

	tsdbSuite.T().Log("read")
	rreq := &pb.ReadRequest{
		Backend:      tsdbSuite.backendName,
		Table:        table,
		Start:        "1530349527000",
		End:          "1559293527000",
		MessageLimit: 10,
	}

	it, err := tsdbSuite.client.Read(rreq)
	if err != nil {
		tsdbSuite.T().Fatal(err)
	}

	resultCount := 0
	for it.Next() {
		fr := it.At()
		resultCount += fr.Len()
	}
	tsdbSuite.Require().Equal(0, resultCount)
	tsdbSuite.Require().NoError(it.Err())

	tsdbSuite.T().Log("delete")
	dreq := &pb.DeleteRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
	}

	err = tsdbSuite.client.Delete(dreq)
	tsdbSuite.Require().NoError(err)
}

func (tsdbSuite *TsdbTestSuite) TestAllStringMetric() {
	table := fmt.Sprintf("tsdb_test_all%d", time.Now().UnixNano())

	tsdbSuite.T().Log("create")
	req := &pb.CreateRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
		Rate:    "1/m",
	}
	if err := tsdbSuite.client.Create(req); err != nil {
		tsdbSuite.T().Fatal(err)
	}

	tsdbSuite.T().Log("write")
	anchorTime, _ := time.Parse(time.RFC3339, "2019-12-12T05:00:00Z")
	frame := tsdbSuite.generateSampleFrameWithStringMetric(tsdbSuite.T(), anchorTime)
	wreq := &frames.WriteRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
	}

	appender, err := tsdbSuite.client.Write(wreq)
	if err != nil {
		tsdbSuite.T().Fatal(err)
	}

	tsdbSuite.T().Logf("saving frame to '%v', length: %v", table, frame.Len())
	if err := appender.Add(frame); err != nil {
		tsdbSuite.T().Fatal(err)
	}

	if err := appender.WaitForComplete(3 * time.Second); err != nil {
		tsdbSuite.T().Fatal(err)
	}

	time.Sleep(3 * time.Second) // Let DB sync

	tsdbSuite.T().Log("read")
	rreq := &pb.ReadRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
		Start:   "0",
		End:     veryHighTimestamp,
	}

	it, err := tsdbSuite.client.Read(rreq)
	if err != nil {
		tsdbSuite.T().Fatal(err)
	}

	for it.Next() {
		// TODO: More checks
		fr := it.At()
		if !(fr.Len() == frame.Len() || fr.Len()-1 == frame.Len()) {
			tsdbSuite.T().Fatalf("wrong length: %d != %d", fr.Len(), frame.Len())
		}
	}

	if err := it.Err(); err != nil {
		tsdbSuite.T().Fatal(err)
	}

	tsdbSuite.T().Log("delete")
	dreq := &pb.DeleteRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
	}

	if err := tsdbSuite.client.Delete(dreq); err != nil {
		tsdbSuite.T().Fatal(err)
	}

}

func (tsdbSuite *TsdbTestSuite) TestDeleteWithTimestamp() {
	table := fmt.Sprintf("TestDeleteWithTimestamp%d", time.Now().UnixNano())

	tsdbSuite.T().Log("create")
	req := &pb.CreateRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
		Rate:    "1/s",
	}
	if err := tsdbSuite.client.Create(req); err != nil {
		tsdbSuite.T().Fatal(err)
	}

	tsdbSuite.T().Log("write")
	end, _ := time.Parse(time.RFC3339, "2019-12-12T05:00:00Z")

	frame := tsdbSuite.generateSampleFrame(tsdbSuite.T(), end)
	wreq := &frames.WriteRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
	}

	appender, err := tsdbSuite.client.Write(wreq)
	if err != nil {
		tsdbSuite.T().Fatal(err)
	}

	if err := appender.Add(frame); err != nil {
		tsdbSuite.T().Fatal(err)
	}

	if err := appender.WaitForComplete(3 * time.Second); err != nil {
		tsdbSuite.T().Fatal(err)
	}

	tsdbSuite.T().Log("delete")
	dreq := &pb.DeleteRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
		Start:   "1576069387000",
		End:     "1576414987000",
	}

	if err := tsdbSuite.client.Delete(dreq); err != nil {
		tsdbSuite.T().Fatal(err)
	}

	rreq := &pb.ReadRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
		Start:   "0",
		End:     veryHighTimestamp,
	}

	it, err := tsdbSuite.client.Read(rreq)
	if err != nil {
		tsdbSuite.T().Fatal(err)
	}

	tsdbSuite.Require().False(it.Next(), "expecting no results")

	if err := it.Err(); err != nil {
		tsdbSuite.T().Fatal(err)
	}
}

func (tsdbSuite *TsdbTestSuite) TestDeleteWithRelativeTime() {
	table := fmt.Sprintf("TestDeleteWithRelativeTime%d", time.Now().UnixNano())

	tsdbSuite.T().Log("create")
	req := &pb.CreateRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
		Rate:    "1/s",
	}
	if err := tsdbSuite.client.Create(req); err != nil {
		tsdbSuite.T().Fatal(err)
	}

	tsdbSuite.T().Log("write")
	end, _ := time.Parse(time.RFC3339, "2019-12-12T05:00:00Z")

	frame := tsdbSuite.generateSampleFrame(tsdbSuite.T(), end)
	wreq := &frames.WriteRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
	}

	appender, err := tsdbSuite.client.Write(wreq)
	if err != nil {
		tsdbSuite.T().Fatal(err)
	}

	if err := appender.Add(frame); err != nil {
		tsdbSuite.T().Fatal(err)
	}

	if err := appender.WaitForComplete(3 * time.Second); err != nil {
		tsdbSuite.T().Fatal(err)
	}

	tsdbSuite.T().Log("delete")
	dreq := &pb.DeleteRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
		Start:   "0",
		End:     "now-1h",
	}

	if err := tsdbSuite.client.Delete(dreq); err != nil {
		tsdbSuite.T().Fatal(err)
	}

	rreq := &pb.ReadRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
		Start:   "0",
		End:     veryHighTimestamp,
	}

	it, err := tsdbSuite.client.Read(rreq)
	if err != nil {
		tsdbSuite.T().Fatal(err)
	}

	tsdbSuite.Require().False(it.Next(), "expecting no results")

	if err := it.Err(); err != nil {
		tsdbSuite.T().Fatal(err)
	}
}

func (tsdbSuite *TsdbTestSuite) TestDeleteWithRFC3339Time() {
	table := fmt.Sprintf("TestDeleteWithRFC3339Time%d", time.Now().UnixNano())

	tsdbSuite.T().Log("create")
	req := &pb.CreateRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
		Rate:    "1/s",
	}
	if err := tsdbSuite.client.Create(req); err != nil {
		tsdbSuite.T().Fatal(err)
	}

	tsdbSuite.T().Log("write")
	end, _ := time.Parse(time.RFC3339, "2019-12-12T05:00:00Z")

	frame := tsdbSuite.generateSampleFrame(tsdbSuite.T(), end)
	wreq := &frames.WriteRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
	}

	appender, err := tsdbSuite.client.Write(wreq)
	if err != nil {
		tsdbSuite.T().Fatal(err)
	}

	if err := appender.Add(frame); err != nil {
		tsdbSuite.T().Fatal(err)
	}

	if err := appender.WaitForComplete(3 * time.Second); err != nil {
		tsdbSuite.T().Fatal(err)
	}

	tsdbSuite.T().Log("delete")
	dreq := &pb.DeleteRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
		Start:   "2019-12-11T05:00:00Z",
		End:     "2019-12-15T05:00:00Z",
	}

	if err := tsdbSuite.client.Delete(dreq); err != nil {
		tsdbSuite.T().Fatal(err)
	}

	rreq := &pb.ReadRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
		Start:   "0",
		End:     veryHighTimestamp,
	}

	it, err := tsdbSuite.client.Read(rreq)
	if err != nil {
		tsdbSuite.T().Fatal(err)
	}

	tsdbSuite.Require().False(it.Next(), "expecting no results")

	if err := it.Err(); err != nil {
		tsdbSuite.T().Fatal(err)
	}
}

func (tsdbSuite *TsdbTestSuite) TestDeleteAll() {
	table := fmt.Sprintf("TestDeleteAll%d", time.Now().UnixNano())

	tsdbSuite.T().Log("create")
	req := &pb.CreateRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
		Rate:    "1/s",
	}
	if err := tsdbSuite.client.Create(req); err != nil {
		tsdbSuite.T().Fatal(err)
	}

	tsdbSuite.T().Log("write")
	end, _ := time.Parse(time.RFC3339, "2019-12-12T05:00:00Z")

	frame := tsdbSuite.generateSampleFrame(tsdbSuite.T(), end)
	wreq := &frames.WriteRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
	}

	appender, err := tsdbSuite.client.Write(wreq)
	if err != nil {
		tsdbSuite.T().Fatal(err)
	}

	if err := appender.Add(frame); err != nil {
		tsdbSuite.T().Fatal(err)
	}

	if err := appender.WaitForComplete(3 * time.Second); err != nil {
		tsdbSuite.T().Fatal(err)
	}

	tsdbSuite.T().Log("delete")
	dreq := &pb.DeleteRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
		Start:   "",
		End:     "",
	}

	if err := tsdbSuite.client.Delete(dreq); err != nil {
		tsdbSuite.T().Fatal(err)
	}

	input := v3io.GetItemInput{Path: table}
	_, err = tsdbSuite.v3ioContainer.GetItemSync(&input)

	tsdbSuite.Require().Error(err, "expected error but finished successfully")
}

func (tsdbSuite *TsdbTestSuite) TestDeleteAllSamplesButNotTable() {
	table := fmt.Sprintf("TestDeleteAllSamplesButNotTable%d", time.Now().UnixNano())

	tsdbSuite.T().Log("create")
	req := &pb.CreateRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
		Rate:    "1/s",
	}
	if err := tsdbSuite.client.Create(req); err != nil {
		tsdbSuite.T().Fatal(err)
	}

	tsdbSuite.T().Log("write")
	end, _ := time.Parse(time.RFC3339, "2019-12-12T05:00:00Z")

	frame := tsdbSuite.generateSampleFrame(tsdbSuite.T(), end)
	wreq := &frames.WriteRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
	}

	appender, err := tsdbSuite.client.Write(wreq)
	if err != nil {
		tsdbSuite.T().Fatal(err)
	}

	if err := appender.Add(frame); err != nil {
		tsdbSuite.T().Fatal(err)
	}

	if err := appender.WaitForComplete(3 * time.Second); err != nil {
		tsdbSuite.T().Fatal(err)
	}

	tsdbSuite.T().Log("delete")
	dreq := &pb.DeleteRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
		Start:   "",
		End:     "",
		Filter:  "1==1",
	}

	if err := tsdbSuite.client.Delete(dreq); err != nil {
		tsdbSuite.T().Fatal(err)
	}

	// Verify the entire table was not deleted
	input := v3io.GetItemInput{Path: fmt.Sprintf("%v/.schema", table), AttributeNames: []string{"__name"}}
	_, err = tsdbSuite.v3ioContainer.GetItemSync(&input)

	tsdbSuite.Require().NoError(err, "entire tsdb table got deleted")

	// Verify no data is returned
	rreq := &pb.ReadRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
		Start:   "0",
		End:     veryHighTimestamp,
	}

	it, err := tsdbSuite.client.Read(rreq)
	if err != nil {
		tsdbSuite.T().Fatal(err)
	}

	tsdbSuite.Require().True(it.Next(), "expecting nothing to be deleted")

	if err := it.Err(); err != nil {
		tsdbSuite.T().Fatal(err)
	}
}
