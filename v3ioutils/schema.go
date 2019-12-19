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

package v3ioutils

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/v3io/frames"
	v3io "github.com/v3io/v3io-go/pkg/dataplane"
)

const (
	LongType   = "long"
	DoubleType = "double"
	StringType = "string"
	TimeType   = "timestamp"
	BoolType   = "boolean"

	DefaultKeyColumn = "idx"
)

// NewSchema returns a new schema
func NewSchema(key string, sortingKey string) V3ioSchema {
	return &OldV3ioSchema{Fields: []OldSchemaField{}, Key: key, SortingKey: sortingKey}
}

// SchemaFromJSON return a schema from JSON data
func SchemaFromJSON(data []byte) (V3ioSchema, error) {
	var schema OldV3ioSchema
	err := json.Unmarshal(data, &schema)
	return &schema, err
}

// V3ioSchema is schema for v3io
type V3ioSchema interface {
	AddColumn(name string, col frames.Column, nullable bool) error
	AddField(name string, val interface{}, nullable bool) error
	UpdateSchema(container v3io.Container, tablePath string, newSchema V3ioSchema) error
}

// OldV3ioSchema is old v3io schema
type OldV3ioSchema struct {
	Fields           []OldSchemaField `json:"fields"`
	Key              string           `json:"key"`
	SortingKey       string           `json:"sortingKey"`
	HashingBucketNum int              `json:"hashingBucketNum"`
}

// OldSchemaField is OldV3ioSchema field
type OldSchemaField struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

// AddColumn adds a column
func (s *OldV3ioSchema) AddColumn(name string, col frames.Column, nullable bool) error {

	field := OldSchemaField{Name: name, Type: ConvertDTypeToString(col.DType()), Nullable: nullable}
	s.Fields = append(s.Fields, field)
	return nil
}

// AddField adds a field
func (s *OldV3ioSchema) AddField(name string, val interface{}, nullable bool) error {
	var ftype string
	switch val.(type) {
	case int, int32, int64:
		ftype = LongType
	case float32, float64:
		ftype = DoubleType
	case string:
		ftype = StringType
	case time.Time:
		ftype = TimeType
	case bool:
		ftype = BoolType
	}

	field := OldSchemaField{Name: name, Type: ftype, Nullable: nullable}
	s.Fields = append(s.Fields, field)
	return nil
}

func (s *OldV3ioSchema) GetField(name string) (OldSchemaField, error) {
	for _, f := range s.Fields {
		if f.Name == name {
			return f, nil
		}
	}
	return OldSchemaField{}, fmt.Errorf("no field named %v ", name)
}

// toJSON retrun JSON representation of schema
func (s *OldV3ioSchema) toJSON() ([]byte, error) {
	return json.Marshal(s)
}

func (s *OldV3ioSchema) merge(new *OldV3ioSchema) (bool, error) {
	isFirstSchema := len(s.Fields) == 0
	changed := false
	for _, field := range new.Fields {
		index := -1
		for j := 0; j < len(s.Fields); j++ {
			if s.Fields[j].Name == field.Name {
				index = j
			}
		}

		if index < 0 {
			s.Fields = append(s.Fields, field)
			changed = true
		} else if field.Type != s.Fields[index].Type {
			return changed, fmt.Errorf(
				"schema change for column %v from type %s to %s is not allowed", field.Name, s.Fields[index].Type, field.Type)
		}
	}

	// Do not accept key name change, unless it's the first time we are saving to this table
	if s.Key != new.Key && new.Key != "" {
		if isFirstSchema {
			s.Key = new.Key
			changed = true
		} else {
			return changed, fmt.Errorf("changing primary key is not allowed, old: %v, new:%v", s.Key, new.Key)
		}
	}

	if s.SortingKey != new.SortingKey && new.SortingKey != "" {
		if isFirstSchema {
			s.SortingKey = new.SortingKey
			changed = true
		} else {
			return changed, fmt.Errorf("changing sorting key is not allowed, old: %v, new:%v", s.SortingKey, new.SortingKey)
		}
	}

	return changed, nil
}

// UpdateSchema updates the schema
func (s *OldV3ioSchema) UpdateSchema(container v3io.Container, tablePath string, newSchema V3ioSchema) error {
	changed, err := s.merge(newSchema.(*OldV3ioSchema))
	if err != nil {
		return errors.Wrap(err, "failed to merge schema")
	}

	if changed {
		body, err := s.toJSON()
		if err != nil {
			return errors.Wrap(err, "failed to marshal schema")
		}
		err = container.PutObjectSync(&v3io.PutObjectInput{Path: tablePath + ".#schema", Body: body})
		if err != nil {
			if strings.Contains(err.Error(), "status 401") {
				return errors.New("unauthorized update (401), may be caused by wrong password or credentials")
			}

			return errors.Wrap(err, "failed to update schema")
		}
	}

	return nil
}

func ConvertDTypeToString(dType frames.DType) string {
	switch dType {
	case frames.IntType:
		return LongType
	case frames.FloatType:
		return DoubleType
	case frames.StringType:
		return StringType
	case frames.TimeType:
		return TimeType
	case frames.BoolType:
		return BoolType
	}
	return ""
}

func ContainsField(fields []OldSchemaField, fieldName string) (bool, OldSchemaField) {
	for _, f := range fields {
		if f.Name == fieldName {
			return true, f
		}
	}

	return false, OldSchemaField{}
}

func GetSchema(tablePath string, container v3io.Container) (V3ioSchema, error) {
	schemaInput := &v3io.GetObjectInput{Path: tablePath + ".#schema"}
	resp, err := container.GetObjectSync(schemaInput)
	if err != nil {
		return nil, err
	}
	schema := &OldV3ioSchema{}
	if err := json.Unmarshal(resp.HTTPResponse.Body(), schema); err != nil {
		return nil, err
	}
	return schema, nil
}
