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

func TestMatchInterface(t *testing.T) {
	var col Column = &SliceColumn{} // Will fail if doesn't match interface

	col.Len() // Make compiler happy
}

func TestAPI(t *testing.T) {
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

	svals, err := col.Strings()
	if err != nil {
		t.Fatalf("error converting to []string - %s", err)
	}

	if len(svals) != len(values) {
		t.Fatalf("length mismatch (%d != %d)", len(svals), len(values))
	}

	for i, v := range svals {
		if v != values[i] {
			t.Fatalf("%d: bad value - %v\n", i, v)
		}
	}

	for i := 0; i < col.Len(); i++ {
		v := col.StringAt(i)
		if v != values[i] {
			t.Fatalf("%d: bad value - %v\n", i, v)
		}
	}
}

func TestBadType(t *testing.T) {
	_, err := NewSliceColumn("col7", []int8{1})
	if err == nil {
		t.Fatalf("created a column from int8")
	}
}
