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

func TestSliceBuilder(t *testing.T) {
	name := "intCol"
	dtype := IntType
	size := 10
	b := NewSliceColumnBuilder(name, dtype, size/3)
	for i := 0; i < size; i++ {
		if err := b.Set(i, i); err != nil {
			t.Fatal(err)
		}
	}

	col := b.Finish()
	if col.Len() != size {
		t.Fatalf("bad size %d != %d", col.Len(), size)
	}

	if col.DType() != dtype {
		t.Fatalf("bad dtype %d != %d", col.DType(), dtype)
	}

	vals, err := col.Ints()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("vals: %v", vals)

	for i, val := range vals {
		if int64(i) != val {
			t.Fatalf("%d: %d != %d", i, val, i)
		}
	}
}

func TestSliceBuilderEmpty(t *testing.T) {
	name := "fCol"
	dtype := FloatType
	b := NewSliceColumnBuilder(name, dtype, 0)
	size := 0
	for i := 0.7; i < 3.1; i += 0.62 {
		if err := b.Append(i); err != nil {
			t.Fatal(err)
		}
		size++

	}

	col := b.Finish()
	if col.Len() != size {
		t.Fatalf("wrong len - %d != %d", col.Len(), size)
	}
}

func TestLabelBuilder(t *testing.T) {
	name := "bCol"
	dtype := BoolType
	size := 10
	b := NewLabelColumnBuilder(name, dtype, size/3)
	val := true
	for i := 0; i < size; i++ {
		if err := b.Set(i, val); err != nil {
			t.Fatal(err)
		}
	}

	col := b.Finish()
	if col.Len() != size {
		t.Fatalf("bad size %d != %d", col.Len(), size)
	}

	err := b.Set(0, false)
	if err == nil {
		t.Fatal("changed value in label column")
	}
}

func TestBuilderDelete(t *testing.T) {
	size := 10
	b := NewSliceColumnBuilder("a", IntType, size)
	for i := 0; i < size; i++ {
		b.Set(i, i)
	}

	deleted := map[int]bool{
		1: true,
		3: true,
		9: true,
	}

	for v := range deleted {
		if err := b.Delete(v); err != nil {
			t.Fatalf("can't delete %d - %s", v, err)
		}
	}

	col := b.Finish()
	if cs := col.Len(); cs != size-len(deleted) {
		t.Fatalf("wrong size %d != %d", col.Len(), size-len(deleted))
	}

	for i := 0; i < col.Len(); i++ {
		v, err := col.IntAt(i)
		if err != nil {
			t.Fatal(err)
		}

		if deleted[int(v)] {
			t.Fatalf("found deleted value - %d", v)
		}

	}
}

func TestBuilderDeleteFirst(t *testing.T) {
	b := NewSliceColumnBuilder("a", IntType, 1)
	b.Set(0, 1)

	if err := b.Delete(0); err != nil {
		if err != nil {
			t.Fatal(err)
		}
	}

	col := b.Finish()
	if col.Len() != 0 {
		t.Fatalf("bad size: %d != 0", col.Len())
	}
}

func TestBuilderAppend(t *testing.T) {
	b := NewSliceColumnBuilder("a", IntType, 1)
	size := 5
	for i := 0; i < size; i++ {
		if err := b.Append(i); err != nil {
			t.Fatal(err)
		}
	}

	col := b.Finish()
	if col.Len() != size {
		t.Fatalf("bad size: %d != %d", col.Len(), size)
	}
}
