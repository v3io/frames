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
	"testing"
)

func TestRowIterator(t *testing.T) {
	frame, err := makeFrame()
	if err != nil {
		t.Fatalf("can't create frame - %s", err)
	}

	it := frame.IterRows()
	for rowNum := 0; it.Next(); rowNum++ {
		if it.RowNum() != rowNum {
			t.Fatalf("rowNum mismatch %d != %d", rowNum, it.RowNum())
		}

		row := it.Row(false)
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

	it := frame.IterRows()
	if ok := it.Next(); !ok {
		t.Fatalf("can't advance iterator")
	}

	if err := it.Err(); err != nil {
		t.Fatalf("error in next - %s", err)
	}

	row := it.Row(true)
	if len(row) != len(frame.Names())+len(frame.Indices()) {
		t.Fatalf("bad row - %+v\n", row)
	}
}
