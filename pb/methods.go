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

package pb

import (
	"encoding/json"
	"fmt"
	"regexp"
	"time"
)

var (
	intRe = regexp.MustCompile("^[0-9]+$")
)

// GoValue return value as interface{}
func (v *Value) GoValue() interface{} {
	switch v.GetValue().(type) {
	case *Value_Ival:
		return v.GetIval()
	case *Value_Fval:
		return v.GetFval()
	case *Value_Sval:
		return v.GetSval()
	case *Value_Tval:
		return NSToTime(v.GetTval())
	case *Value_Bval:
		return v.GetBval()
	}

	panic(fmt.Sprintf("unknown type - %T", v.GetValue()))
}

// Attributes return the attibutes
// TODO: Calculate once (how to add field in generate protobuf code?)
func (r *CreateRequest) Attributes() map[string]interface{} {
	return AsGoMap(r.AttributeMap)
}

// NSToTime returns time from epoch nanoseconds
func NSToTime(ns int64) time.Time {
	return time.Unix(ns/1000, ns%1000)
}

// AsGoMap returns map with interface{} values
func AsGoMap(mv map[string]*Value) map[string]interface{} {
	m := make(map[string]interface{})
	for key, value := range mv {
		m[key] = value.GoValue()
	}

	return m
}

// MarshalJSON marshal Value as JSON object
func (v *Value) MarshalJSON() ([]byte, error) {
	switch v.GetValue().(type) {
	case *Value_Bval:
		return json.Marshal(v.GetBval())
	case *Value_Fval:
		return json.Marshal(v.GetFval())
	case *Value_Ival:
		return json.Marshal(v.GetIval())
	case *Value_Sval:
		return json.Marshal(v.GetSval())
	case *Value_Tval:
		return json.Marshal(v.GetTval())
	}

	return nil, fmt.Errorf("unknown Value type - %T", v.GetValue())
}

// UnmarshalJSON will unmarshal encoded native Go type to value
// (Implement json.Unmarshaler interface)
func (v *Value) UnmarshalJSON(data []byte) error {
	var i interface{}
	if err := json.Unmarshal(data, &i); err != nil {
		return err
	}

	// FIXME: Time
	switch i.(type) {
	case float64: // JSON encodes numbers as floats
		f := i.(float64)
		if intRe.Match(data) {
			v.Value = &Value_Ival{Ival: int64(f)}
		} else {
			v.Value = &Value_Fval{Fval: f}
		}
	case string:
		v.Value = &Value_Sval{Sval: i.(string)}
	case bool:
		v.Value = &Value_Bval{Bval: i.(bool)}
	default:
		return fmt.Errorf("unsupported type for value - %T", i)
	}

	return nil
}

// Property return a schema property
func (s *SchemaField) Property(key string) (interface{}, bool) {
	if s.Properties == nil {
		return nil, false
	}

	val, ok := s.Properties[key]
	if !ok {
		return nil, false
	}

	return val.GoValue(), true
}
