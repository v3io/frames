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

type StreamTestSuite struct {
	suite.Suite
	tablePath      string
	suiteTimestamp int64
	basicQueryTime int64
	client         frames.Client
	backendName    string
	v3ioContainer  v3io.Container
	internalLogger logger.Logger
}

func GetStreamTestsConstructorFunc() SuiteCreateFunc {
	return func(client frames.Client, v3ioContainer v3io.Container, internalLogger logger.Logger) suite.TestingSuite {
		return &StreamTestSuite{client: client,
			backendName:    "stream",
			v3ioContainer:  v3ioContainer,
			internalLogger: internalLogger}
	}
}

func (streamSuite *StreamTestSuite) generateSampleFrame(t testing.TB) frames.Frame {
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

func (streamSuite *StreamTestSuite) SetupSuite() {
	if streamSuite.client == nil {
		streamSuite.FailNow("client not set")
	}
}

func (streamSuite *StreamTestSuite) TestAll() {
	table := fmt.Sprintf("stream_test_all%d", time.Now().UnixNano())

	streamSuite.T().Log("create")
	req := &pb.CreateRequest{
		Backend: streamSuite.backendName,
		Table:   table,
	}
	req.SetAttribute("retention_hours", 48)
	req.SetAttribute("shards", 1)

	if err := streamSuite.client.Create(req); err != nil {
		streamSuite.T().Fatal(err)
	}

	streamSuite.T().Log("write")
	frame := streamSuite.generateSampleFrame(streamSuite.T())
	wreq := &frames.WriteRequest{
		Backend: streamSuite.backendName,
		Table:   table,
	}

	appender, err := streamSuite.client.Write(wreq)
	if err != nil {
		streamSuite.T().Fatal(err)
	}

	if err := appender.Add(frame); err != nil {
		streamSuite.T().Fatal(err)
	}

	if err := appender.WaitForComplete(3 * time.Second); err != nil {
		streamSuite.T().Fatal(err)
	}

	time.Sleep(3 * time.Second) // Let DB sync

	streamSuite.T().Log("read")
	rreq := &pb.ReadRequest{
		Backend: streamSuite.backendName,
		Table:   table,
		Seek:    "earliest",
		ShardId: "0",
	}

	it, err := streamSuite.client.Read(rreq)
	if err != nil {
		streamSuite.T().Fatal(err)
	}

	for it.Next() {
		// TODO: More checks
		fr := it.At()
		if !(fr.Len() == frame.Len() || fr.Len()-1 == frame.Len()) {
			streamSuite.T().Fatalf("wrong length: %d != %d", fr.Len(), frame.Len())
		}
	}

	if err := it.Err(); err != nil {
		streamSuite.T().Fatal(err)
	}

	streamSuite.T().Log("delete")
	dreq := &pb.DeleteRequest{
		Backend: streamSuite.backendName,
		Table:   table,
	}

	if err := streamSuite.client.Delete(dreq); err != nil {
		streamSuite.T().Fatal(err)
	}

}
