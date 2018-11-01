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
	"testing/quick"
	"time"
)

var (
	currentTest *testing.T
)

func TestSliceColMatchInterface(t *testing.T) {
	var col Column = &SliceColumn{} // Will fail if doesn't match interface

	col.Len() // Make compiler happy
}

func TestSliceColAPI(t *testing.T) {
	name, values := "sc_col", []string{"bugs", "daffy", "taz", "tweety"}
	col, err := NewSliceColumn(name, values)

	if err != nil {
		t.Fatalf("error creating column - %s", err)
	}

	if col.Name() != name {
		t.Fatalf("name mismatch (%q != %q)", col.Name(), name)
	}

	if col.Len() != len(values) {
		t.Fatalf("length mismatch (%d != %d)", col.Len(), len(values))
	}

	if col.DType() != StringType {
		t.Fatalf("type mismatch (%s != %s)", col.DType(), StringType)
	}

	svals := col.Strings()
	if len(svals) != len(values) {
		t.Fatalf("length mismatch (%d != %d)", len(svals), len(values))
	}

	for i, v := range svals {
		if v != values[i] {
			t.Fatalf("%d: bad value - %v\n", i, v)
		}
	}

	for i := 0; i < col.Len(); i++ {
		v, _ := col.StringAt(i)
		if v != values[i] {
			t.Fatalf("%d: bad value - %v\n", i, v)
		}
	}
}

func TestSliceColBadType(t *testing.T) {
	_, err := NewSliceColumn("col7", []int8{1})
	if err == nil {
		t.Fatalf("created a column from int8")
	}
}

var quickSliceColDTypes = []DType{
	IntType,
	FloatType,
	StringType,
	TimeType,
	BoolType,
}

func quickSliceCol(name string, dtypeIdx int, size int, ivals []int64, fvals []float64, svals []string, tvals []int64, bvals []bool) bool {

	t := currentTest

	if dtypeIdx < 0 {
		dtypeIdx = -dtypeIdx
	}

	if size < 0 {
		size = -size
	}

	size = size % 10000 // Don't want to blow up memory

	var col Column
	var err error
	dtype := quickSliceColDTypes[dtypeIdx%len(quickSliceColDTypes)]

	switch dtype {
	case IntType:
		if len(ivals) == 0 {
			return true
		}

		data := make([]int64, size)
		for i := 0; i < size; i++ {
			data[i] = ivals[i%len(ivals)]
		}

		col, err = NewSliceColumn(name, data)
	case FloatType:
		if len(fvals) == 0 {
			return true
		}

		data := make([]float64, size)
		for i := 0; i < size; i++ {
			data[i] = fvals[i%len(fvals)]
		}

		col, err = NewSliceColumn(name, data)
	case StringType:
		if len(svals) == 0 {
			return true
		}

		data := make([]string, size)
		for i := 0; i < size; i++ {
			data[i] = svals[i%len(svals)]
		}

		col, err = NewSliceColumn(name, data)
	case TimeType:
		if len(tvals) == 0 {
			return true
		}

		data := make([]time.Time, size)
		for i := 0; i < size; i++ {
			// tval is a int64 since testing/quick can't generate time.TIme
			tval := tvals[i%len(tvals)]
			data[i] = time.Unix(tval, tval%10000)
		}

		col, err = NewSliceColumn(name, data)
	case BoolType:
		if len(bvals) == 0 {
			return true
		}
		data := make([]bool, size)
		for i := 0; i < size; i++ {
			data[i] = (i % 2) == 0
		}
		col, err = NewSliceColumn(name, data)
	}

	if err != nil {
		t.Logf("can't create slice - %s", err)
		return false
	}

	if col.DType() != dtype {
		t.Logf("wrong dtype %s != %s", col.DType(), dtype)
		return false
	}

	if col.Len() != size {
		t.Logf("wrong length %d != %d", col.Len(), size)
		return false
	}

	return true
}

func TestSliceColQuick(t *testing.T) {
	currentTest = t
	defer func() {
		currentTest = nil
	}()

	if err := quick.Check(quickSliceCol, nil); err != nil {
		t.Fatal(err)
	}
}
