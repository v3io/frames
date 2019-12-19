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

package kv

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/v3io/frames"
	"github.com/v3io/frames/v3ioutils"
	"github.com/v3io/v3io-go/pkg/dataplane"
)

func (b *Backend) inferSchema(request *frames.ExecRequest) error {

	container, table, err := b.newConnection(request.Proto.Session, request.Password.Get(), request.Token.Get(), request.Proto.Table, true)
	if err != nil {
		return err
	}

	var keyField string
	if val, ok := request.Proto.Args["key"]; ok {
		keyField = val.GetSval()
	}
	maxrec := 10

	input := v3io.GetItemsInput{Path: table, Filter: "", AttributeNames: []string{"*"}}
	b.logger.DebugWith("GetItems for schema", "input", input)
	iter, err := v3ioutils.NewAsyncItemsCursor(container, &input, b.numWorkers, []string{}, b.logger, 0, []string{table}, "", "")
	if err != nil {
		return err
	}

	var rowSet []map[string]interface{}
	var keys []string

	for rowNum := 0; rowNum < maxrec && iter.Next(); rowNum++ {
		row := iter.GetFields()
		rowSet = append(rowSet, row)
		key, ok := row["__name"]
		if !ok {
			return fmt.Errorf("key (__name) was not found in row")
		}
		keys = append(keys, key.(string))
	}

	if iter.Err() != nil {
		return iter.Err()
	}

	newSchema, err := schemaFromKeys(keyField, rowSet)
	if err != nil {
		return err
	}

	nullSchema := v3ioutils.NewSchema(keyField, "")

	return nullSchema.UpdateSchema(container, table, newSchema)
}

func schemaFromKeys(keyField string, rowSet []map[string]interface{}) (v3ioutils.V3ioSchema, error) {
	columnNameToValue := make(map[string]interface{})
	columnCanBeKey := make(map[string]bool)

	for _, row := range rowSet {
		keyValue := row["__name"]
		for attrName, attrValue := range row {
			if attrName == "__name" {
				continue
			}
			previousValue, ok := columnNameToValue[attrName]
			if ok {
				previousType := reflect.TypeOf(previousValue)
				currentType := reflect.TypeOf(attrValue)
				if previousType != currentType {
					return nil, errors.Errorf("Type %v of %v did not match type %v of %v for column %s.", previousType, previousValue, currentType, attrValue, attrName)
				}
			}
			columnNameToValue[attrName] = attrValue
			if _, ok = columnCanBeKey[attrName]; !ok {
				columnCanBeKey[attrName] = true
			}
			columnCanBeKey[attrName] = columnCanBeKey[attrName] && attrValue == keyValue
		}
	}

	if keyField == "" {
		var possibleKeys []string
		for columnName, canBeKey := range columnCanBeKey {
			if canBeKey {
				possibleKeys = append(possibleKeys, columnName)
			}
		}
		if len(possibleKeys) == 1 {
			keyField = possibleKeys[0]
		} else {
			var reason string
			if len(possibleKeys) == 0 {
				reason = "no column matched __name attribute"
			} else {
				sort.Strings(possibleKeys)
				reason = fmt.Sprintf("%d columns (%s) matched __name attribute", len(possibleKeys), strings.Join(possibleKeys, ", "))
			}
			return nil, errors.Errorf("Could not determine which column is the table key because %s.", reason)
		}
	}

	newSchema := v3ioutils.NewSchema(keyField, "")

	for name, value := range columnNameToValue {
		err := newSchema.AddField(name, value, name != keyField)
		if err != nil {
			return nil, err
		}
	}

	return newSchema, nil
}
