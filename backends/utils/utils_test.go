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

package utils

import (
	"reflect"
	"testing"
)

func TestAppendValue(t *testing.T) {
	data := []int{1, 2, 3}
	out, err := AppendValue(data, 4)
	if err != nil {
		t.Fatal(err)
	}

	expected := []int{1, 2, 3, 4}
	if !reflect.DeepEqual(out, expected) {
		t.Fatalf("bad append %v != %v", out, expected)
	}

	_, err = AppendValue(data, "4")
	if err == nil {
		t.Fatal("no error on type mismatch")
	}

	_, err = AppendValue([]bool{true}, false)
	if err == nil {
		t.Fatal("no error on unknown mismatch")
	}
}

func TestNewColumn(t *testing.T) {
	i := 7
	out, err := NewColumn(i, 3)
	if err != nil {
		t.Fatal(err)
	}

	expected := []int{0, 0, 0}
	if !reflect.DeepEqual(out, expected) {
		t.Fatalf("bad new column %v != %v", out, expected)
	}

	_, err = NewColumn(false, 2)
	if err == nil {
		t.Fatal("no error on unknown type")
	}
}

func TestAppendNil(t *testing.T) {
	t.Skip()
	/*
		data := []int{1, 2, 3}
		out, err := AppendNil(data)
		if err != nil {
			t.Fatal(err)
		}

		expected := []int{1, 2, 3, 0}
		if !reflect.DeepEqual(out, expected) {
			t.Fatalf("bad append %v != %v", out, expected)
		}

		_, err = AppendNil([]bool{true})
		if err == nil {
			t.Fatal("no error on unknown mismatch")
		}
	*/

}
