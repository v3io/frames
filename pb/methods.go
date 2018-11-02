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
	"time"
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

	// TODO: Panic?
	return nil
}

// Attributes return the attibutes
// TODO: Calculate once (how to add field in generate protobuf code?)
func (r *CreateRequest) Attributes() map[string]interface{} {
	m := make(map[string]interface{})
	for key, value := range r.AttributeMap {
		m[key] = value.GoValue()
	}

	return m
}

// NSToTime returns time from epoch nanoseconds
func NSToTime(ns int64) time.Time {
	return time.Unix(ns/1000, ns%1000)
}
