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

	"github.com/pkg/errors"
	"github.com/vmihailenco/msgpack"
)

// Marshaler is interface for writing native data
type Marshaler interface {
	Marshal() (map[string]interface{}, error) // Marshal to native type
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
	data := make(map[string]interface{})
	if err := d.decoder.Decode(&data); err != nil {
		return nil, err
	}

	tag, ok := data["tag"].(string)
	if !ok {
		return nil, fmt.Errorf("can't find tag in message")
	}

	if tag != mapFrameTag {
		return nil, fmt.Errorf("bad frame tag - %q", tag)
	}

	iCols, ok := data["columns"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("bad type for columns - %T", data["columns"])
	}

	columns := make([]Column, len(iCols))
	for i, icol := range iCols {
		colData, ok := icol.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("%d: bad type for column - %T", i, icol)
		}

		tag, ok := colData["tag"].(string)
		if !ok {
			return nil, fmt.Errorf("%d: no tag for column (%v)", i, icol)
		}

		name, ok := colData["name"].(string)
		if !ok {
			return nil, fmt.Errorf("%d: no name for column (%v)", i, icol)
		}

		switch tag {
		case sliceColumnTag:
			data, ok := colData["data"]
			if !ok {
				return nil, fmt.Errorf("%d: no data for column (%v)", i, icol)
			}

			col, err := NewSliceColumn(name, data)
			if err != nil {
				return nil, errors.Wrapf(err, "%d can't create column from %v", i, icol)
			}

			columns[i] = col
		case labelColumnTag:
			value, ok := colData["value"]
			if !ok {
				return nil, fmt.Errorf("%d: no value for column (%v)", i, icol)
			}

			size, ok := colData["size"].(int)
			if !ok {
				return nil, fmt.Errorf("%d: no size for column (%v)", i, icol)
			}

			col, err := NewLabelColumn(name, value, size)
			if err != nil {
				return nil, errors.Wrapf(err, "%d can't create column from %v", i, icol)
			}

			columns[i] = col
		default:
			return nil, fmt.Errorf("%d: unknown column tag - %q", i, tag)
		}
	}

	return NewMapFrame(columns)
}
