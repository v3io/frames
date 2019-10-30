package test

import (
	"fmt"
	"github.com/stretchr/testify/suite"
	"github.com/v3io/frames"
	"github.com/v3io/frames/pb"
	"math/rand"
	"testing"
	"time"
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
	return func(client frames.Client) suite.TestingSuite { return &CsvTestSuite{client: client, backendName: "csv"} }
}

func (csvSuite *CsvTestSuite) SetupSuite() {
	if csvSuite.client == nil {
		csvSuite.FailNow("client not set")
	}
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
	if err != nil {
		t.Fatal(err)
	}
	columns = append(columns, col)

	col = floatCol(t, "floats", size)
	columns = append(columns, col)

	ints := make([]int64, size)
	for i := range ints {
		ints[i] = random.Int63()
	}
	col, err = frames.NewSliceColumn("ints", ints)
	if err != nil {
		t.Fatal(err)
	}
	columns = append(columns, col)

	col = stringCol(t, "strings", size)
	columns = append(columns, col)

	times := make([]time.Time, size)
	for i := range times {
		times[i] = time.Now().Add(time.Duration(i) * time.Second)
	}
	col, err = frames.NewSliceColumn("times", times)
	if err != nil {
		t.Fatal(err)
	}
	columns = append(columns, col)

	frame, err := frames.NewFrame(columns, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	return frame
}

func (csvSuite *CsvTestSuite) TestAll() {
	table := fmt.Sprintf("kv_test_all%d", time.Now().UnixNano())

	csvSuite.T().Log("write")
	frame := csvSuite.generateSampleFrame(csvSuite.T())
	wreq := &frames.WriteRequest{
		Backend: csvSuite.backendName,
		Table:   table,
	}

	appender, err := csvSuite.client.Write(wreq)
	if err != nil {
		csvSuite.T().Fatal(err)
	}

	if err := appender.Add(frame); err != nil {
		csvSuite.T().Fatal(err)
	}

	if err := appender.WaitForComplete(3 * time.Second); err != nil {
		csvSuite.T().Fatal(err)
	}

	time.Sleep(3 * time.Second) // Let DB sync

	csvSuite.T().Log("read")
	rreq := &pb.ReadRequest{
		Backend: csvSuite.backendName,
		Table:   table,
	}

	it, err := csvSuite.client.Read(rreq)
	if err != nil {
		csvSuite.T().Fatal(err)
	}

	for it.Next() {
		// TODO: More checks
		fr := it.At()
		if !(fr.Len() == frame.Len() || fr.Len()-1 == frame.Len()) {
			csvSuite.T().Fatalf("wrong length: %d != %d", fr.Len(), frame.Len())
		}
	}

	if err := it.Err(); err != nil {
		csvSuite.T().Fatal(err)
	}

	csvSuite.T().Log("delete")
	dreq := &pb.DeleteRequest{
		Backend: csvSuite.backendName,
		Table:   table,
	}

	if err := csvSuite.client.Delete(dreq); err != nil {
		csvSuite.T().Fatal(err)
	}

}
