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
	"time"
)

func TestReadRequestStep(t *testing.T) {

	duration := 10 * time.Second

	r := &ReadRequest{
		StepRaw: duration.String(),
	}

	step, err := r.Step()
	if err != nil {
		t.Fatalf("can't parse step - %s", err)
	}

	if step != duration {
		t.Fatalf("bad step: %v != %v", step, duration)
	}

	r = &ReadRequest{
		StepRaw: "daffy duck",
	}

	step, err = r.Step()
	if err == nil {
		t.Fatalf("parsed bad step")
	}
}

func TestSchemaFieldProperty(t *testing.T) {
	key, value := "yale", 42
	field := &SchemaField{
		Properties: map[string]interface{}{
			key: value,
		},
	}

	prop, ok := field.Property(key)
	if !ok {
		t.Fatal("property not found")
	}

	if prop != value {
		t.Fatalf("%q property mismatch: %v != %v", key, prop, value)
	}

	prop, ok = field.Property("no such property")
	if ok {
		t.Fatal("found non existing property")
	}

	// Check with nil Properties
	field = &SchemaField{}
	prop, ok = field.Property(key)
	if ok {
		t.Fatal("found non existing property (nil)")
	}
}
