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
		times[i] = end.Add(-time.Duration(size-i) * time.Second * 300)
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
		times[i] = end.Add(-time.Duration(size-i) * time.Second * 300)
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
	tsdbSuite.Require().NotNil(tsdbSuite.client, "client not set")
}

func (tsdbSuite *TsdbTestSuite) TestAll() {
	table := fmt.Sprintf("frames_ci/tsdb_test_all%d", time.Now().UnixNano())

	tsdbSuite.T().Log("create")
	req := &pb.CreateRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
		Rate:    "1/m",
	}
	err := tsdbSuite.client.Create(req)
	tsdbSuite.Require().NoError(err)

	tsdbSuite.T().Log("write")
	anchorTime, _ := time.Parse(time.RFC3339, "2019-12-12T05:00:00Z")
	frame := tsdbSuite.generateSampleFrame(tsdbSuite.T(), anchorTime)
	wreq := &frames.WriteRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
	}

	appender, err := tsdbSuite.client.Write(wreq)
	tsdbSuite.Require().NoError(err)

	tsdbSuite.T().Logf("saving frame to '%v', length: %v", table, frame.Len())
	err = appender.Add(frame)
	tsdbSuite.Require().NoError(err)

	err = appender.WaitForComplete(3 * time.Second)
	tsdbSuite.Require().NoError(err)

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
	tsdbSuite.Require().NoError(err)

	resultCount := 0
	for it.Next() {
		fr := it.At()
		resultCount += fr.Len()
	}
	// TODO: More checks
	tsdbSuite.Require().Contains([]int{resultCount, resultCount}, frame.Len(), "wrong length")
	tsdbSuite.Require().NoError(it.Err())

	tsdbSuite.T().Log("delete")
	dreq := &pb.DeleteRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
	}

	err = tsdbSuite.client.Delete(dreq)
	tsdbSuite.Require().NoError(err)
}

func (tsdbSuite *TsdbTestSuite) TestRegressionIG14560() {
	table := fmt.Sprintf("frames_ci/tsdb_test_all%d", time.Now().UnixNano())

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
	tsdbSuite.Require().NoError(err)

	tsdbSuite.T().Logf("saving frame to '%v', length: %v", table, frame.Len())
	err = appender.Add(frame)
	tsdbSuite.Require().NoError(err)

	err = appender.WaitForComplete(3 * time.Second)
	tsdbSuite.Require().NoError(err)

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
	tsdbSuite.Require().NoError(err)

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
	table := fmt.Sprintf("frames_ci/tsdb_test_all%d", time.Now().UnixNano())

	tsdbSuite.T().Log("create")
	req := &pb.CreateRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
		Rate:    "1/m",
	}
	err := tsdbSuite.client.Create(req)
	tsdbSuite.Require().NoError(err)

	tsdbSuite.T().Log("write")
	anchorTime, _ := time.Parse(time.RFC3339, "2019-12-12T05:00:00Z")
	frame := tsdbSuite.generateSampleFrameWithStringMetric(tsdbSuite.T(), anchorTime)
	wreq := &frames.WriteRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
	}

	appender, err := tsdbSuite.client.Write(wreq)
	tsdbSuite.Require().NoError(err)

	tsdbSuite.T().Logf("saving frame to '%v', length: %v", table, frame.Len())
	err = appender.Add(frame)
	tsdbSuite.Require().NoError(err)

	err = appender.WaitForComplete(3 * time.Second)
	tsdbSuite.Require().NoError(err)

	time.Sleep(3 * time.Second) // Let DB sync

	tsdbSuite.T().Log("read")
	rreq := &pb.ReadRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
		Start:   "0",
		End:     veryHighTimestamp,
	}

	it, err := tsdbSuite.client.Read(rreq)
	tsdbSuite.Require().NoError(err)

	for it.Next() {
		// TODO: More checks
		fr := it.At()
		tsdbSuite.Require().Contains([]int{fr.Len(), fr.Len() - 1}, frame.Len(), "wrong length")
	}

	tsdbSuite.Require().NoError(it.Err())

	tsdbSuite.T().Log("delete")
	dreq := &pb.DeleteRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
	}

	err = tsdbSuite.client.Delete(dreq)
	tsdbSuite.Require().NoError(err)
}

func (tsdbSuite *TsdbTestSuite) TestDeleteWithTimestamp() {
	table := fmt.Sprintf("frames_ci/TestDeleteWithTimestamp%d", time.Now().UnixNano())

	tsdbSuite.T().Log("create")
	req := &pb.CreateRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
		Rate:    "1/s",
	}
	err := tsdbSuite.client.Create(req)
	tsdbSuite.Require().NoError(err)

	tsdbSuite.T().Log("write")
	end, _ := time.Parse(time.RFC3339, "2019-12-12T05:00:00Z")

	frame := tsdbSuite.generateSampleFrame(tsdbSuite.T(), end)
	wreq := &frames.WriteRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
	}

	appender, err := tsdbSuite.client.Write(wreq)
	tsdbSuite.Require().NoError(err)

	err = appender.Add(frame)
	tsdbSuite.Require().NoError(err)

	err = appender.WaitForComplete(3 * time.Second)
	tsdbSuite.Require().NoError(err)

	tsdbSuite.T().Log("delete")
	dreq := &pb.DeleteRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
		Start:   "1576069387000",
		End:     "1576414987000",
	}

	err = tsdbSuite.client.Delete(dreq)
	tsdbSuite.Require().NoError(err)

	rreq := &pb.ReadRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
		Start:   "0",
		End:     veryHighTimestamp,
	}

	it, err := tsdbSuite.client.Read(rreq)
	tsdbSuite.Require().NoError(err)

	tsdbSuite.Require().False(it.Next(), "expecting no results")
	tsdbSuite.Require().NoError(it.Err())
}

func (tsdbSuite *TsdbTestSuite) TestDeleteWithRelativeTime() {
	table := fmt.Sprintf("frames_ci/TestDeleteWithRelativeTime%d", time.Now().UnixNano())

	tsdbSuite.T().Log("create")
	req := &pb.CreateRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
		Rate:    "1/s",
	}
	err := tsdbSuite.client.Create(req)
	tsdbSuite.Require().NoError(err)

	tsdbSuite.T().Log("write")
	end, _ := time.Parse(time.RFC3339, "2019-12-12T05:00:00Z")

	frame := tsdbSuite.generateSampleFrame(tsdbSuite.T(), end)
	wreq := &frames.WriteRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
	}

	appender, err := tsdbSuite.client.Write(wreq)
	tsdbSuite.Require().NoError(err)

	err = appender.Add(frame)
	tsdbSuite.Require().NoError(err)

	err = appender.WaitForComplete(3 * time.Second)
	tsdbSuite.Require().NoError(err)

	tsdbSuite.T().Log("delete")
	dreq := &pb.DeleteRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
		Start:   "0",
		End:     "now-1h",
	}

	err = tsdbSuite.client.Delete(dreq)
	tsdbSuite.Require().NoError(err)

	rreq := &pb.ReadRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
		Start:   "0",
		End:     veryHighTimestamp,
	}

	it, err := tsdbSuite.client.Read(rreq)
	tsdbSuite.Require().NoError(err)
	tsdbSuite.Require().False(it.Next(), "expecting no results")
	tsdbSuite.Require().NoError(it.Err())
}

func (tsdbSuite *TsdbTestSuite) TestDeleteWithRFC3339Time() {
	table := fmt.Sprintf("frames_ci/TestDeleteWithRFC3339Time%d", time.Now().UnixNano())

	tsdbSuite.T().Log("create")
	req := &pb.CreateRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
		Rate:    "1/s",
	}
	err := tsdbSuite.client.Create(req)
	tsdbSuite.Require().NoError(err)

	tsdbSuite.T().Log("write")
	end, _ := time.Parse(time.RFC3339, "2019-12-12T05:00:00Z")

	frame := tsdbSuite.generateSampleFrame(tsdbSuite.T(), end)
	wreq := &frames.WriteRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
	}

	appender, err := tsdbSuite.client.Write(wreq)
	tsdbSuite.Require().NoError(err)

	err = appender.Add(frame)
	tsdbSuite.Require().NoError(err)

	err = appender.WaitForComplete(3 * time.Second)
	tsdbSuite.Require().NoError(err)

	tsdbSuite.T().Log("delete")
	dreq := &pb.DeleteRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
		Start:   "2019-12-11T05:00:00Z",
		End:     "2019-12-15T05:00:00Z",
	}

	err = tsdbSuite.client.Delete(dreq)
	tsdbSuite.Require().NoError(err)

	rreq := &pb.ReadRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
		Start:   "0",
		End:     veryHighTimestamp,
	}

	it, err := tsdbSuite.client.Read(rreq)
	tsdbSuite.Require().NoError(err)

	tsdbSuite.Require().False(it.Next(), "expecting no results")

	tsdbSuite.Require().NoError(it.Err())
}

func (tsdbSuite *TsdbTestSuite) TestDeleteAll() {
	table := fmt.Sprintf("frames_ci/TestDeleteAll%d", time.Now().UnixNano())

	tsdbSuite.T().Log("create")
	req := &pb.CreateRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
		Rate:    "1/s",
	}
	err := tsdbSuite.client.Create(req)
	tsdbSuite.Require().NoError(err)

	tsdbSuite.T().Log("write")
	end, _ := time.Parse(time.RFC3339, "2019-12-12T05:00:00Z")

	frame := tsdbSuite.generateSampleFrame(tsdbSuite.T(), end)
	wreq := &frames.WriteRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
	}

	appender, err := tsdbSuite.client.Write(wreq)
	tsdbSuite.Require().NoError(err)

	err = appender.Add(frame)
	tsdbSuite.Require().NoError(err)

	err = appender.WaitForComplete(3 * time.Second)
	tsdbSuite.Require().NoError(err)

	tsdbSuite.T().Log("delete")
	dreq := &pb.DeleteRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
		Start:   "",
		End:     "",
	}

	err = tsdbSuite.client.Delete(dreq)
	tsdbSuite.Require().NoError(err)

	input := v3io.GetItemInput{Path: table}
	_, err = tsdbSuite.v3ioContainer.GetItemSync(&input)

	tsdbSuite.Require().Error(err, "expected error but finished successfully")
}

func (tsdbSuite *TsdbTestSuite) TestDeleteAllSamplesButNotTable() {
	table := fmt.Sprintf("frames_ci/TestDeleteAllSamplesButNotTable%d", time.Now().UnixNano())

	tsdbSuite.T().Log("create")
	req := &pb.CreateRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
		Rate:    "1/s",
	}
	err := tsdbSuite.client.Create(req)
	tsdbSuite.Require().NoError(err)

	tsdbSuite.T().Log("write")
	end, _ := time.Parse(time.RFC3339, "2019-12-12T05:00:00Z")

	frame := tsdbSuite.generateSampleFrame(tsdbSuite.T(), end)
	wreq := &frames.WriteRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
	}

	appender, err := tsdbSuite.client.Write(wreq)
	tsdbSuite.Require().NoError(err)

	err = appender.Add(frame)
	tsdbSuite.Require().NoError(err)

	err = appender.WaitForComplete(3 * time.Second)
	tsdbSuite.Require().NoError(err)

	tsdbSuite.T().Log("delete")
	dreq := &pb.DeleteRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
		Start:   "",
		End:     "",
		Filter:  "1==1",
	}

	err = tsdbSuite.client.Delete(dreq)
	tsdbSuite.Require().NoError(err)

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
	tsdbSuite.Require().NoError(err)

	tsdbSuite.Require().True(it.Next(), "expecting nothing to be deleted")

	tsdbSuite.Require().NoError(it.Err())
}
