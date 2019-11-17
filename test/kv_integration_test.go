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

	"github.com/stretchr/testify/suite"
	"github.com/v3io/frames"
	"github.com/v3io/frames/pb"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/tsdbtest"
)

type KvTestSuite struct {
	suite.Suite
	tablePath      string
	suiteTimestamp int64
	basicQueryTime int64
	client         frames.Client
	backendName    string
}

func GetKvTestsConstructorFunc() SuiteCreateFunc {
	return func(client frames.Client) suite.TestingSuite { return &KvTestSuite{client: client, backendName: "kv"} }
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

func (kvSuite *KvTestSuite) generateSampleFrame(t testing.TB) frames.Frame {
	var icol frames.Column

	index := []string{"mike", "joe", "jim", "rose", "emily", "dan"}
	icol, err := frames.NewSliceColumn("idx", index)
	if err != nil {
		t.Fatal(err)
	}

	columns := []frames.Column{
		floatCol(t, "n1", len(index)),
		floatCol(t, "n2", len(index)),
		floatCol(t, "n3", len(index)),
	}

	frame, err := frames.NewFrame(columns, []frames.Column{icol}, nil)
	if err != nil {
		t.Fatal(err)
	}

	return frame
}

func (kvSuite *KvTestSuite) TestAll() {
	table := fmt.Sprintf("kv_test_all%d", time.Now().UnixNano())

	kvSuite.T().Log("write")
	frame := kvSuite.generateSampleFrame(kvSuite.T())
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
