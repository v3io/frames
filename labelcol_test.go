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

func TestLabelColMatchInterface(t *testing.T) {
	var col Column = &LabelColumn{} // Will fail if doesn't match interface

	col.Len() // Make compiler happy
}

func TestLabelColAPI(t *testing.T) {
	name, value, size := "col1", 12.3, 33
	col, err := NewLabelColumn(name, value, size)

	if err != nil {
		t.Fatalf("error creating column - %s", err)
	}

	if col.Name() != name {
		t.Fatalf("name mismatch (%q != %q)", col.Name(), name)
	}

	if col.Len() != size {
		t.Fatalf("length mismatch (%d != %d)", col.Len(), size)
	}

	if col.DType() != FloatType {
		t.Fatalf("type mismatch (%s != %s)", col.DType(), FloatType)
	}

	values, err := col.Floats()
	if err != nil {
		t.Fatalf("error converting to []float64 - %s", err)
	}

	if len(values) != size {
		t.Fatalf("length mismatch (%d != %d)", len(values), size)
	}

	for i, v := range values {
		if v != value {
			t.Fatalf("%d: bad value - %v\n", i, v)
		}
	}

	for i := 0; i < col.Len(); i++ {
		v := col.FloatAt(i)
		if v != value {
			t.Fatalf("%d: bad value - %v\n", i, v)
		}
	}
}

func TestLabelColBadType(t *testing.T) {
	var val struct{}
	_, err := NewLabelColumn("col7", val, 10)
	if err == nil {
		t.Fatalf("created a column from unknown type")
	}
}

var quickLabelColDTypes = []DType{
	IntType,
	FloatType,
	StringType,
	TimeType,
	BoolType,
}

func quickLabelCol(name string, dtypeIdx int, size int, ival int, fval float64, sval string, tval int64, bval bool) bool {
	if size < 0 {
		size = -size
	}

	if dtypeIdx < 0 {
		dtypeIdx = -dtypeIdx
	}

	var col Column
	var err error
	dtype := quickLabelColDTypes[dtypeIdx%len(quickLabelColDTypes)]

	switch dtype {
	case IntType:
		col, err = NewLabelColumn(name, ival, size)
	case FloatType:
		col, err = NewLabelColumn(name, fval, size)
	case StringType:
		col, err = NewLabelColumn(name, sval, size)
	case TimeType:
		// tval is a int64 since testing/quick can't generate time.TIme
		t := time.Unix(tval, tval%10000)
		col, err = NewLabelColumn(name, t, size)
	case BoolType:
		col, err = NewLabelColumn(name, bval, size)
	}

	if err != nil {
		return false
	}

	if col.DType() != dtype {
		return false
	}

	if col.Len() != size {
		return false
	}

	return true
}

func TestLabelColQuick(t *testing.T) {
	if err := quick.Check(quickLabelCol, nil); err != nil {
		t.Fatal(err)
	}
}
