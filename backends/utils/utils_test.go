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

}
