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

package csv

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/pkg/errors"

	"github.com/v3io/frames"
	"github.com/v3io/frames/backends/utils"
)

// Backend is CSV backend
type Backend struct{}

// NewBackend returns a new CSV backend
func NewBackend(ctx *frames.DataContext) (*Backend, error) {
	return &Backend{}, nil
}

// FrameIterator iterates over CSV
type FrameIterator struct {
	path        string
	reader      *csv.Reader
	frame       frames.Frame
	err         error
	columnNames []string
	nRows       int
	limit       int
	frameLimit  int
}

// Read handles reading
func (b *Backend) Read(request *frames.ReadRequest) (frames.FrameIterator, error) {
	file, err := os.Open(request.Table)
	if err != nil {
		return nil, err
	}

	reader := csv.NewReader(file)
	columns, err := reader.Read()
	if err != nil {
		return nil, errors.Wrap(err, "can't read header (columns)")
	}

	it := &FrameIterator{
		path:        request.Table,
		reader:      reader,
		columnNames: columns,
		limit:       request.Limit,
		frameLimit:  request.MaxInMessage,
	}

	return it, nil
}

// Next reads the next frame, return true of succeeded
func (it *FrameIterator) Next() bool {
	rows, err := it.readNextRows()
	if err != nil {
		it.err = err
		return false
	}

	if len(rows) == 0 {
		return false
	}

	it.frame, err = it.buildFrame(rows)
	if err != nil {
		it.err = err
		return false
	}

	return true
}

// At return the current Frame
func (it *FrameIterator) At() frames.Frame {
	return it.frame
}

// Err returns the last error
func (it *FrameIterator) Err() error {
	return it.err
}

func (it *FrameIterator) readNextRows() ([][]string, error) {
	var rows [][]string
	for r := 0; it.inLimits(r); r, it.nRows = r+1, it.nRows+1 {
		row, err := it.reader.Read()
		if err != nil {
			if err == io.EOF {
				return rows, nil
			}

			return nil, err
		}

		if len(row) != len(it.columnNames) {
			return nil, fmt.Errorf("%s:%d num columns don't match headers (%d != %d)", it.path, it.nRows, len(row), len(it.columnNames))
		}

		rows = append(rows, row)
	}

	return rows, nil
}

func (it *FrameIterator) inLimits(frameRow int) bool {
	if it.limit > 0 && it.nRows >= it.limit {
		return false
	}

	if it.frameLimit > 0 && frameRow >= it.frameLimit {
		return false
	}

	return true
}

func (it *FrameIterator) buildFrame(rows [][]string) (frames.Frame, error) {
	columns := make([]frames.Column, len(it.columnNames))
	for c, colName := range it.columnNames {
		var (
			val0 = it.parseValue(rows[0][c])
			col  frames.Column
			data interface{}
			err  error
		)

		switch val0.(type) {
		case int:
			typedData := make([]int, len(rows))
			typedData[0] = val0.(int)
			for r, row := range rows[1:] {
				val, ok := it.parseValue(row[c]).(int)
				if !ok {
					return nil, fmt.Errorf("type mismatch in row %d, col %d", it.nRows, c)
				}

				typedData[r+1] = val // +1 since we start in first row
			}
			data = typedData
		case float64:
			typedData := make([]float64, len(rows))
			typedData[0] = val0.(float64)
			for r, row := range rows[1:] {
				val, ok := it.parseValue(row[c]).(float64)
				if !ok {
					return nil, fmt.Errorf("type mismatch in row %d, col %d", it.nRows, c)
				}

				typedData[r+1] = val // +1 since we start in first row
			}
			data = typedData
		case string:
			typedData := make([]string, len(rows))
			typedData[0] = val0.(string)
			for r, row := range rows[1:] {
				typedData[r+1] = row[c] // +1 since we start in first row
			}
			data = typedData
		case time.Time:
			typedData := make([]time.Time, len(rows))
			typedData[0] = val0.(time.Time)
			for r, row := range rows[1:] {
				val, ok := it.parseValue(row[c]).(time.Time)
				if !ok {
					return nil, fmt.Errorf("type mismatch in row %d, col %d", it.nRows, c)
				}

				typedData[r+1] = val // +1 since we start in first row
			}
			data = typedData
		}

		col, err = frames.NewSliceColumn(colName, data)
		if err != nil {
			return nil, errors.Wrapf(err, "can't build column %s", colName)
		}

		columns[c] = col
	}

	return frames.NewMapFrame(columns)
}

func (it *FrameIterator) parseValue(value string) interface{} {
	// Time
	t, err := time.Parse(time.RFC3339, value)
	if err == nil {
		return t
	}

	t, err = time.Parse(time.RFC3339Nano, value)
	if err == nil {
		return t
	}

	// Date
	t, err = time.Parse("2006-01-02", value)
	if err == nil {
		return t
	}

	// Int
	i, err := strconv.Atoi(value)
	if err == nil {
		return i
	}

	f, err := strconv.ParseFloat(value, 64)
	if err == nil {
		return f
	}

	// Leave as string
	return value
}

// Write handles writing
func (b *Backend) Write(request *frames.WriteRequest) (frames.FrameAppender, error) {
	file, err := os.Create(request.Table)
	if err != nil {
		return nil, err
	}

	ca := &csvAppender{
		writer: csv.NewWriter(file),
	}

	return ca, nil

}

type csvAppender struct {
	writer        *csv.Writer
	headerWritten bool
}

func (ca *csvAppender) Add(frame frames.Frame) error {
	names := frame.Columns()
	if !ca.headerWritten {
		if err := ca.writer.Write(names); err != nil {
			return errors.Wrap(err, "can't write header")
		}
		ca.headerWritten = true
	}

	for r := 0; r < frame.Len(); r++ {
		record := make([]string, len(names))
		for c, name := range names {
			col, err := frame.Column(name)
			if err != nil {
				return errors.Wrap(err, "can't get column")
			}

			val, err := utils.ColAt(col, r)
			if err != nil {
				return errors.Wrapf(err, "%s:%d can't get value", name, r)
			}

			record[c] = fmt.Sprintf("%v", val)
		}

		if err := ca.writer.Write(record); err != nil {
			return errors.Wrap(err, "can't write record")
		}
	}

	return nil
}

// WaitForComplete wait for write completion
func (ca *csvAppender) WaitForComplete(timeout time.Duration) error {
	ca.writer.Flush()
	return ca.writer.Error()
}
