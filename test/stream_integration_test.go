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
		times[i] = end.Add(-time.Duration(size-i) * time.Second * 300)
	}

	index, err := frames.NewSliceColumn("idx", times)
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

func (streamSuite *StreamTestSuite) SetupSuite() {
	streamSuite.Require().NotNil(streamSuite.client, "client not set")
}

func (streamSuite *StreamTestSuite) TestAll() {
	table := fmt.Sprintf("frames_ci/stream_test_all%d", time.Now().UnixNano())

	streamSuite.T().Log("create")
	req := &pb.CreateRequest{
		Backend:        streamSuite.backendName,
		Table:          table,
		RetentionHours: 48,
		Shards:         1,
	}

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
	streamSuite.Require().NoError(err)

	err = appender.Add(frame)
	streamSuite.Require().NoError(err)

	err = appender.WaitForComplete(3 * time.Second)
	streamSuite.Require().NoError(err)

	time.Sleep(3 * time.Second) // Let DB sync

	streamSuite.T().Log("read")
	rreq := &pb.ReadRequest{
		Backend: streamSuite.backendName,
		Table:   table,
		Seek:    "earliest",
		ShardId: "0",
	}

	it, err := streamSuite.client.Read(rreq)
	streamSuite.Require().NoError(err)

	for it.Next() {
		// TODO: More checks
		fr := it.At()
		streamSuite.Require().Contains([]int{fr.Len(), fr.Len() - 1}, frame.Len(), "wrong length")
	}

	streamSuite.Require().NoError(it.Err())

	streamSuite.T().Log("delete")
	dreq := &pb.DeleteRequest{
		Backend: streamSuite.backendName,
		Table:   table,
	}

	err = streamSuite.client.Delete(dreq)
	streamSuite.Require().NoError(err)
}
