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
	"io"
	"time"

	"github.com/pkg/errors"
	"github.com/vmihailenco/msgpack"
)

// Marshaler is interface for writing native data
type Marshaler interface {
	Marshal() (interface{}, error) // Marshal to native type
}

// Message sent over the wire with multiple columns and data points
type Message struct {
	Frame map[string]interface{}
	// For Writes, Will we get more message chunks (in a stream), if not we can complete
	HaveMore bool
}

// Encoder encodes frames
type Encoder struct {
	encoder *msgpack.Encoder
}

// NewEncoder returns a new message encoder
func NewEncoder(writer io.Writer) *Encoder {
	return &Encoder{
		encoder: msgpack.NewEncoder(writer),
	}
}

// Encode encodes a frame
func (e *Encoder) Encode(frame Frame) error {
	marshaler, ok := frame.(Marshaler)
	if !ok {
		return fmt.Errorf("frame does not support marshalling")
	}

	data, err := marshaler.Marshal()
	if err != nil {
		return errors.Wrap(err, "can't marshal frame")
	}

	if err := e.encoder.Encode(data); err != nil {
		return errors.Wrap(err, "can't encode data")
	}

	return nil
}

// Decoder decodes message
type Decoder struct {
	decoder *msgpack.Decoder
}

// NewDecoder returns a new decoder
func NewDecoder(reader io.Reader) *Decoder {
	return &Decoder{
		decoder: msgpack.NewDecoder(reader),
	}
}

// Decode encodes a frame
func (d *Decoder) Decode() (Frame, error) {
	msg := &MapFrameMessage{}
	if err := d.decoder.Decode(msg); err != nil {
		return nil, err
	}

	columns := make([]Column, len(msg.Columns))
	for i, name := range msg.Columns {
		sliceColMsg, ok := msg.SliceCols[name]
		if ok {
			if sliceColMsg.Name != name {
				return nil, fmt.Errorf("%d: column name mismatch %q != %q", i, name, sliceColMsg.Name)
			}

			col, err := d.decodeSliceCol(sliceColMsg)
			if err != nil {
				return nil, err
			}

			columns[i] = col
			continue
		}

		labelColMsg, ok := msg.LabelCols[name]
		if ok {
			if labelColMsg.Name != name {
				return nil, fmt.Errorf("%d: column name mismatch %q != %q", i, name, labelColMsg.Name)
			}

			col, err := d.decodeLabelCol(labelColMsg)
			if err != nil {
				return nil, err
			}

			columns[i] = col
			continue
		}

		return nil, fmt.Errorf("column %q not found", name)
	}

	var indexCol Column
	var err error
	switch {
	case msg.SliceIndexCol != nil:
		indexCol, err = d.decodeSliceCol(msg.SliceIndexCol)
		if err != nil {
			return nil, err
		}
	case msg.LabelIndexCol != nil:
		indexCol, err = d.decodeLabelCol(msg.LabelIndexCol)
		if err != nil {
			return nil, err
		}
	}

	return NewFrame(columns, indexCol)
}

func (d *Decoder) decodeLabelCol(colMsg *LabelColumnMessage) (Column, error) {
	value := colMsg.Value
	if colMsg.DType == TimeType.String() {
		ival, ok := value.(int)
		if !ok {
			return nil, fmt.Errorf("%s bad type for ns time - %T", colMsg.Name, value)
		}

		value = d.timeFromNS(ival)
	}

	col, err := NewLabelColumn(colMsg.Name, value, colMsg.Size)
	if err != nil {
		return nil, errors.Wrapf(err, "can't create column %q from %+v", colMsg.Name, colMsg)
	}

	return col, nil
}

func (d *Decoder) decodeSliceCol(colMsg *SliceColumnMessage) (Column, error) {
	var col Column
	var err error

	// TODO: Check only one is not null?
	switch {
	case colMsg.IntData != nil:
		col, err = NewSliceColumn(colMsg.Name, colMsg.IntData)
	case colMsg.FloatData != nil:
		col, err = NewSliceColumn(colMsg.Name, colMsg.FloatData)
	case colMsg.StringData != nil:
		col, err = NewSliceColumn(colMsg.Name, colMsg.StringData)
	case colMsg.TimeData != nil:
		col, err = NewSliceColumn(colMsg.Name, colMsg.TimeData)
	case colMsg.NSTimeData != nil:
		data := make([]time.Time, len(colMsg.NSTimeData))
		for i, val := range colMsg.NSTimeData {
			data[i] = d.timeFromNS(val)
		}
		col, err = NewSliceColumn(colMsg.Name, data)
	default:
		return nil, fmt.Errorf("no data in column %q", colMsg.Name)
	}

	if err != nil {
		return nil, errors.Wrapf(err, "can't create column %q from %+v", colMsg.Name, colMsg)
	}

	return col, nil
}

func (d *Decoder) timeFromNS(value int) time.Time {
	sec := int64(value / 1e9)
	nsec := int64(value % 1e9)
	return time.Unix(sec, nsec)
}
