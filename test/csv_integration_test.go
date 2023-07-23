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
	"math/rand"
	"testing"
	"time"

	"github.com/nuclio/logger"
	"github.com/stretchr/testify/suite"
	"github.com/v3io/frames"
	"github.com/v3io/frames/pb"
	v3io "github.com/v3io/v3io-go/pkg/dataplane"
)

var (
	random = rand.New(rand.NewSource(time.Now().Unix()))
	size   = 200
)

type CsvTestSuite struct {
	suite.Suite
	tablePath      string
	suiteTimestamp int64
	basicQueryTime int64
	client         frames.Client
	backendName    string
}

func GetCsvTestsConstructorFunc() SuiteCreateFunc {
	return func(client frames.Client, _ v3io.Container, _ logger.Logger) suite.TestingSuite {
		return &CsvTestSuite{client: client, backendName: "csv"}
	}
}

func (csvSuite *CsvTestSuite) SetupSuite() {
	csvSuite.Require().NotNil(csvSuite.client, "client not set")
}

func (csvSuite *CsvTestSuite) generateSampleFrame(t testing.TB) frames.Frame {
	var (
		columns []frames.Column
		col     frames.Column
		err     error
	)

	bools := make([]bool, size)
	for i := range bools {
		if random.Float64() < 0.5 {
			bools[i] = true
		}
	}
	col, err = frames.NewSliceColumn("bools", bools)
	csvSuite.Require().NoError(err)
	columns = append(columns, col)

	col = FloatCol(t, "floats", size)
	columns = append(columns, col)

	ints := make([]int64, size)
	for i := range ints {
		ints[i] = random.Int63()
	}
	col, err = frames.NewSliceColumn("ints", ints)
	csvSuite.Require().NoError(err)
	columns = append(columns, col)

	col = StringCol(t, "strings", size)
	columns = append(columns, col)

	times := make([]time.Time, size)
	for i := range times {
		times[i] = time.Now().Add(time.Duration(i) * time.Second)
	}
	col, err = frames.NewSliceColumn("times", times)
	csvSuite.Require().NoError(err)
	columns = append(columns, col)

	frame, err := frames.NewFrame(columns, nil, nil)
	csvSuite.Require().NoError(err)

	return frame
}

func (csvSuite *CsvTestSuite) TestAll() {
	table := fmt.Sprintf("csv_test_all%d", time.Now().UnixNano())

	csvSuite.T().Log("write")
	frame := csvSuite.generateSampleFrame(csvSuite.T())
	wreq := &frames.WriteRequest{
		Backend: csvSuite.backendName,
		Table:   table,
	}

	appender, err := csvSuite.client.Write(wreq)
	csvSuite.Require().NoError(err)

	err = appender.Add(frame)
	csvSuite.Require().NoError(err)

	err = appender.WaitForComplete(10 * time.Second)
	csvSuite.Require().NoError(err)

	time.Sleep(3 * time.Second) // Let DB sync

	csvSuite.T().Log("read")
	rreq := &pb.ReadRequest{
		Backend: csvSuite.backendName,
		Table:   table,
	}

	it, err := csvSuite.client.Read(rreq)
	csvSuite.Require().NoError(err)

	for it.Next() {
		// TODO: More checks
		fr := it.At()
		csvSuite.Require().Contains([]int{fr.Len(), fr.Len() - 1}, frame.Len(), "wrong length")
	}

	csvSuite.Require().NoError(it.Err())

	csvSuite.T().Log("delete")
	dreq := &pb.DeleteRequest{
		Backend: csvSuite.backendName,
		Table:   table,
	}

	err = csvSuite.client.Delete(dreq)
	csvSuite.Require().NoError(err)
}
