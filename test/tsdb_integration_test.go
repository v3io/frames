package test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"github.com/v3io/frames"
	"github.com/v3io/frames/pb"
)

type TsdbTestSuite struct {
	suite.Suite
	tablePath      string
	suiteTimestamp int64
	basicQueryTime int64
	client         frames.Client
	backendName    string
}

func GetTsdbTestsConstructorFunc() SuiteCreateFunc {
	return func(client frames.Client) suite.TestingSuite { return &TsdbTestSuite{client: client, backendName: "tsdb"} }
}

func (tsdbSuite *TsdbTestSuite) generateSampleFrame(t testing.TB) frames.Frame {
	size := 60 // TODO
	times := make([]time.Time, size)
	end := time.Now().Truncate(time.Hour)
	for i := range times {
		times[i] = end.Add(-time.Duration(size - i) * time.Second * 300)
	}

	index, err := frames.NewSliceColumn("idx", times)
	if err != nil {
		t.Fatal(err)
	}

	columns := []frames.Column{
		floatCol(t, "cpu", index.Len()),
		floatCol(t, "mem", index.Len()),
		floatCol(t, "disk", index.Len()),
	}

	frame, err := frames.NewFrame(columns, []frames.Column{index}, nil)
	if err != nil {
		t.Fatal(err)
	}

	return frame
}

func (tsdbSuite *TsdbTestSuite) generateSampleFrameWithStringMetric(t testing.TB) frames.Frame {
	size := 60 // TODO
	times := make([]time.Time, size)
	end := time.Now().Truncate(time.Hour)
	for i := range times {
		times[i] = end.Add(-time.Duration(size - i) * time.Second * 300)
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
	frame := tsdbSuite.generateSampleFrame(tsdbSuite.T())
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
		Start:   "now-7h",
		End:     "now",
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

func (tsdbSuite *TsdbTestSuite) TestAllStringMetric() {
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
	frame := tsdbSuite.generateSampleFrameWithStringMetric(tsdbSuite.T())
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
		Start:   "now-7h",
		End:     "now",
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