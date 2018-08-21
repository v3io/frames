package frames

import (
	"bytes"
	"testing"
	"time"
)

func TestMarshal(t *testing.T) {
	frame := createFrame(t)

	marshaler, ok := frame.(Marshaler)
	if !ok {
		t.Fatalf("frame is not Marshaler")
	}

	out, err := marshaler.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	msg, ok := out.(*MapFrameMessage)
	if !ok {
		t.Fatalf("wrong message type - %T", msg)
	}
}

func TestRoundTrip(t *testing.T) {
	frame := createFrame(t)

	var buf bytes.Buffer
	enc := NewEncoder(&buf)
	if err := enc.Encode(frame); err != nil {
		t.Fatal(err)
	}

	dec := NewDecoder(&buf)
	_, err := dec.Decode()
	if err != nil {
		t.Fatal(err)
	}

}

func createFrame(t *testing.T) Frame {
	var (
		columns []Column
		col     Column
		err     error
	)

	col, err = NewSliceColumn("icol", []int{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}

	columns = append(columns, col)
	col, err = NewSliceColumn("fcol", []float64{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}

	columns = append(columns, col)
	col, err = NewSliceColumn("scol", []string{"1", "2", "3"})
	if err != nil {
		t.Fatal(err)
	}

	columns = append(columns, col)
	col, err = NewSliceColumn("tcol", []time.Time{time.Now(), time.Now(), time.Now()})
	if err != nil {
		t.Fatal(err)
	}

	col, err = NewLabelColumn("lcol", "srv", 3)
	if err != nil {
		t.Fatal(err)
	}

	columns = append(columns, col)
	frame, err := NewMapFrame(columns)
	if err != nil {
		t.Fatal(err)
	}

	return frame
}
