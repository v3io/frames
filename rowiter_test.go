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

package frames

import (
	"fmt"
	"testing"
	"time"
)

func TestRowIterator(t *testing.T) {
	frame, err := makeFrame()
	if err != nil {
		t.Fatalf("can't create frame - %s", err)
	}

	it := frame.IterRows(false)
	for rowNum := 0; it.Next(); rowNum++ {
		if it.RowNum() != rowNum {
			t.Fatalf("rowNum mismatch %d != %d", rowNum, it.RowNum())
		}

		row := it.Row()
		if row == nil {
			t.Fatalf("empty row")
		}

		for name, val := range row {
			col, err := frame.Column(name)
			if err != nil {
				t.Fatalf("can't get column %q", name)
			}
			switch col.DType() {
			case IntType:
				cval, err := col.IntAt(rowNum)
				if err != nil {
					t.Fatalf("can't get int at %d", rowNum)
				}

				if cval != val {
					t.Fatalf("%s:%d bad value %v != %v", name, rowNum, val, cval)
				}
			case FloatType:
				cval, err := col.FloatAt(rowNum)
				if err != nil {
					t.Fatalf("can't get float at %d", rowNum)
				}

				if cval != val {
					t.Fatalf("%s:%d bad value %v != %v", name, rowNum, val, cval)
				}
			case StringType:
				cval, err := col.StringAt(rowNum)
				if err != nil {
					t.Fatalf("can't get string at %d", rowNum)
				}

				if cval != val {
					t.Fatalf("%s:%d bad value %v != %v", name, rowNum, val, cval)
				}
			case TimeType:
				cval, err := col.TimeAt(rowNum)
				if err != nil {
					t.Fatalf("can't get time at %d", rowNum)
				}

				if cval != val {
					t.Fatalf("%s:%d bad value %v != %v", name, rowNum, val, cval)
				}
			case BoolType:
				cval, err := col.BoolAt(rowNum)
				if err != nil {
					t.Fatalf("can't get bool at %d", rowNum)
				}

				if cval != val {
					t.Fatalf("%s:%d bad value %v != %v", name, rowNum, val, cval)
				}
			}
		}
	}

	if err := it.Err(); err != nil {
		t.Fatalf("iteration error - %s", err)
	}
}

func TestRowIteratorIndex(t *testing.T) {
	t.Skip("TODO")
}

func TestRowIteratorIndices(t *testing.T) {
	t.Skip("TODO")
}

func TestRowAll(t *testing.T) {
	frame, err := makeFrame()
	if err != nil {
		t.Fatal(err)
	}

	it := frame.IterRows(true)
	if ok := it.Next(); !ok {
		t.Fatalf("can't advance iterator")
	}

	if err := it.Err(); err != nil {
		t.Fatalf("error in next - %s", err)
	}

	row := it.Row()
	if len(row) != len(frame.Names())+len(frame.Indices()) {
		t.Fatalf("bad row - %+v\n", row)
	}
}

func TestNoName(t *testing.T) {
	col1, err := NewSliceColumn("ints", []int{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}

	col2, err := NewSliceColumn("", []string{"a", "b", "c"})
	if err != nil {
		t.Fatal(err)
	}

	col3, err := NewSliceColumn("floats", []float64{1.0, 2.0, 3.0})
	if err != nil {
		t.Fatal(err)
	}

	cols := []Column{col1, col2, col3}
	frame, err := NewFrame(cols, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	it := frame.IterRows(false)
	it.Next()
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}

	row := it.Row()
	if len(row) != len(cols) {
		t.Fatalf("wrong number of columns - %d != %d", len(row), len(cols))
	}

	for _, col := range cols {
		name := col.Name()
		if name != "" {
			if _, ok := row[name]; !ok {
				t.Fatalf("can't find column %q", col.Name())
			}
		}
	}

	for name := range row {
		if name == "" {
			t.Fatalf("empty column name")
		}
	}
}

// TODO: Unite with http/end2end_test.go
func makeFrame() (Frame, error) {
	size := 1027
	now := time.Now()
	idata := make([]int, size)
	fdata := make([]float64, size)
	sdata := make([]string, size)
	tdata := make([]time.Time, size)
	bdata := make([]bool, size)

	for i := 0; i < size; i++ {
		idata[i] = i
		fdata[i] = float64(i)
		sdata[i] = fmt.Sprintf("val%d", i)
		tdata[i] = now.Add(time.Duration(i) * time.Second)
		bdata[i] = i%2 == 0
	}

	columns := map[string]interface{}{
		"ints":    idata,
		"floats":  fdata,
		"strings": sdata,
		"times":   tdata,
		"bools":   bdata,
	}
	return NewFrameFromMap(columns, nil)
}
