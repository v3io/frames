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

			var col Column
			var err error

			// TODO: Check only one is not null?
			switch {
			case sliceColMsg.IntData != nil:
				col, err = NewSliceColumn(name, sliceColMsg.IntData)
			case sliceColMsg.FloatData != nil:
				col, err = NewSliceColumn(name, sliceColMsg.FloatData)
			case sliceColMsg.StringData != nil:
				col, err = NewSliceColumn(name, sliceColMsg.StringData)
			case sliceColMsg.TimeData != nil:
				col, err = NewSliceColumn(name, sliceColMsg.TimeData)
			case sliceColMsg.NSTimeData != nil:
				data := make([]time.Time, len(sliceColMsg.NSTimeData))
				for i, val := range sliceColMsg.NSTimeData {
					data[i] = d.timeFromNS(val)
				}
				col, err = NewSliceColumn(name, data)
			default:
				return nil, fmt.Errorf("no data in column %q", name)
			}

			if err != nil {
				return nil, errors.Wrapf(err, "can't create column %q from %+v", name, sliceColMsg)
			}

			columns[i] = col
			continue
		}

		labelColMsg, ok := msg.LabelCols[name]
		if ok {
			if labelColMsg.Name != name {
				return nil, fmt.Errorf("%d: column name mismatch %q != %q", i, name, labelColMsg.Name)
			}

			value := labelColMsg.Value
			if labelColMsg.DType == TimeType.String() {
				ival, ok := value.(int)
				if !ok {
					return nil, fmt.Errorf("%s:%d bad type for ns time - %T", name, i, value)
				}

				value = d.timeFromNS(ival)
			}

			col, err := NewLabelColumn(name, value, labelColMsg.Size)
			if err != nil {
				return nil, errors.Wrapf(err, "can't create column %q from %+v", name, labelColMsg)
			}

			columns[i] = col
			continue
		}

		return nil, fmt.Errorf("column %q not found", name)
	}

	return NewMapFrame(columns)
}

func (d *Decoder) timeFromNS(value int) time.Time {
	sec := int64(value / 1e9)
	nsec := int64(value % 1e9)
	return time.Unix(sec, nsec)
}
