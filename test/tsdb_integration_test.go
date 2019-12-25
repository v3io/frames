package test

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/nuclio/logger"
	"github.com/stretchr/testify/suite"
	"github.com/v3io/frames"
	"github.com/v3io/frames/pb"
	v3io "github.com/v3io/v3io-go/pkg/dataplane"
)

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

func (tsdbSuite *TsdbTestSuite) generateSampleFrame(t testing.TB, anchorTime time.Time) frames.Frame {
	size := 60 // TODO
	times := make([]time.Time, size)
	end := anchorTime.Truncate(time.Hour)
	for i := range times {
		times[i] = end.Add(-time.Duration(size-i) * time.Second * 300)
	}

	index, err := frames.NewSliceColumn("idx", times)
	if err != nil {
		t.Fatal(err)
	}
	labelSlice := make([]string, size)
	for i := 0; i < size; i++ {
		labelSlice[i] = strconv.FormatBool(i > size/2)
	}
	labelCol, err := frames.NewSliceColumn("somelabel", labelSlice)
	if err != nil {
		t.Fatal(err)
	}

	columns := []frames.Column{
		floatCol(t, "cpu", index.Len()),
		floatCol(t, "mem", index.Len()),
		floatCol(t, "disk", index.Len()),
	}

	frame, err := frames.NewFrame(columns, []frames.Column{index, labelCol}, nil)
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

	index, err := frames.NewSliceColumn("idx", times)
	if err != nil {
		t.Fatal(err)
	}

	columns := []frames.Column{
		stringCol(t, "host", index.Len()),
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
	}
	req.SetAttribute("rate", "1/m")
	if err := tsdbSuite.client.Create(req); err != nil {
		tsdbSuite.T().Fatal(err)
	}

	tsdbSuite.T().Log("write")
	anchorTime := time.Unix(157728271, 0)
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
		End:          "157728271500000", // Very high upper bound to catch all samples
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

func (tsdbSuite *TsdbTestSuite) NoTestAllStringMetric() {
	table := fmt.Sprintf("tsdb_test_all%d", time.Now().UnixNano())

	tsdbSuite.T().Log("create")
	req := &pb.CreateRequest{
		Backend: tsdbSuite.backendName,
		Table:   table,
	}
	req.SetAttribute("rate", "1/m")
	if err := tsdbSuite.client.Create(req); err != nil {
		tsdbSuite.T().Fatal(err)
	}

	tsdbSuite.T().Log("write")
	anchorTime := time.Unix(1577282715, 0)
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
		End:     "157728271500000", // Very high upper bound to catch all samples
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
