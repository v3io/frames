package v3ioutils

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/v3io/frames"
	"github.com/v3io/v3io-go-http"
)

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

func NewSchema() V3ioSchema {
	return &OldV3ioSchema{Fields: []OldSchemaField{}}
}

type V3ioSchema interface {
	AddField(name string, col frames.Column, nullable bool) error
	UpdateSchema(container *v3io.Container, tablePath string, newSchema V3ioSchema) error
}

type OldV3ioSchema struct {
	Fields []OldSchemaField `json:"Fields"`
}

type OldSchemaField struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable,omitempty"`
}

func (s *OldV3ioSchema) AddField(name string, col frames.Column, nullable bool) error {

	var ftype string
	switch col.DType() {
	case frames.IntType:
		ftype = "long"
	case frames.FloatType:
		ftype = "double"
	case frames.StringType:
		ftype = "string"
	case frames.TimeType:
		ftype = "time"
	}

	field := OldSchemaField{Name: name, Type: ftype, Nullable: nullable}
	s.Fields = append(s.Fields, field)
	return nil
}

func (s *OldV3ioSchema) toJson() ([]byte, error) {
	return json.Marshal(s)
}

func (s *OldV3ioSchema) merge(new *OldV3ioSchema) (bool, error) {
	changed := false
	for _, field := range new.Fields {
		index := -1
		for j := 0; j < len(s.Fields); j++ {
			if s.Fields[j].Name == field.Name {
				index = j
			}
		}

		if index >= 0 && field.Type != s.Fields[index].Type {
			if field.Type == "string" {
				s.Fields[index].Type = "string"
				changed = true
				continue
			}

			if field.Type == "double" && s.Fields[index].Type == "long" {
				s.Fields[index].Type = "double"
				changed = true
				continue
			}

			if field.Type == "time" || s.Fields[index].Type == "time" {
				return changed, fmt.Errorf(
					"Schema change from %s to %s is not allowed", s.Fields[index].Type, field.Type)
			}
		} else {
			s.Fields = append(s.Fields, field)
			changed = true
		}
	}

	return changed, nil
}

func (s *OldV3ioSchema) UpdateSchema(container *v3io.Container, tablePath string, newSchema V3ioSchema) error {
	changed, err := s.merge(newSchema.(*OldV3ioSchema))
	if err != nil {
		return errors.Wrap(err, "failed to merge schema")
	}

	if changed {
		body, err := s.toJson()
		if err != nil {
			return errors.Wrap(err, "failed to marshal schema")
		}
		err = container.Sync.PutObject(&v3io.PutObjectInput{
			Path: tablePath + ".schema", Body: body})
		if err != nil {
			return errors.Wrap(err, "failed to update schema")
		}
	}

	return nil
}
