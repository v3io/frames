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
	"regexp"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/v3io/frames"
	"github.com/v3io/frames/v3ioutils"
	"github.com/v3io/v3io-go/pkg/dataplane"
)

var (
	intType            = reflect.TypeOf(1)
	floatType          = reflect.TypeOf(1.0)
	hashedBucketFormat = regexp.MustCompile("^[a-zA-z0-9]+_[0-9]+$")
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

	for rowNum := 0; rowNum < maxrec && iter.Next(); rowNum++ {
		row := iter.GetFields()
		rowSet = append(rowSet, row)
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
	columnCanBeFullKey := make(map[string]bool)
	columnCanBePrimaryKey := make(map[string]bool)
	columnCanBeSortingKey := make(map[string]bool)
	columnCanBeHashedPrimeryKey := make(map[string]bool)

	for _, row := range rowSet {
		keyValue := row["__name"].(string)
		var primaryKeyValue string
		var sortingKeyValue string
		var hashedPrimaryKeyValue string
		indexOfDot := strings.Index(keyValue, ".")
		if indexOfDot >= 0 && indexOfDot < len(keyValue)-1 {
			sortingKeyValue = keyValue[indexOfDot+1:]
			primaryKeyValue = keyValue[:indexOfDot]

			if hashedBucketFormat.MatchString(primaryKeyValue) {
				indexOfUnderscore := strings.Index(primaryKeyValue, "_")
				hashedPrimaryKeyValue = keyValue[:indexOfUnderscore]
				primaryKeyValue = ""
			}
		}
		for attrName, attrValue := range row {
			if attrName == "__name" {
				continue
			}
			previousValue, ok := columnNameToValue[attrName]
			if ok {
				previousType := reflect.TypeOf(previousValue)
				currentType := reflect.TypeOf(attrValue)
				if previousType != currentType {
					// if one value is float and the other is int, convert the value to float to infer this field as float
					if previousType == floatType && currentType == intType {
						attrValue = float64(attrValue.(int))
					} else if previousType == intType && currentType == floatType {
						// continue to set the `columnNameToValue` to float
					} else {
						return nil, errors.Errorf("type '%v' of value '%v' doesn't match type '%v' of value '%v' for column '%s'.", previousType, previousValue, currentType, attrValue, attrName)
					}
				}
			}
			columnNameToValue[attrName] = attrValue
			if _, ok = columnCanBeFullKey[attrName]; !ok {
				columnCanBeFullKey[attrName] = true
			}

			attrValueAsString := fmt.Sprintf("%v", attrValue)
			columnCanBeFullKey[attrName] = columnCanBeFullKey[attrName] && attrValueAsString == keyValue
			if primaryKeyValue != "" {
				if _, ok = columnCanBePrimaryKey[attrName]; !ok {
					columnCanBePrimaryKey[attrName] = true
				}
				columnCanBePrimaryKey[attrName] = columnCanBePrimaryKey[attrName] && attrValueAsString == primaryKeyValue
			}
			if sortingKeyValue != "" {
				if _, ok = columnCanBeSortingKey[attrName]; !ok {
					columnCanBeSortingKey[attrName] = true
				}
				columnCanBeSortingKey[attrName] = columnCanBeSortingKey[attrName] && attrValueAsString == sortingKeyValue
			}
			if hashedPrimaryKeyValue != "" {
				if _, ok = columnCanBeHashedPrimeryKey[attrName]; !ok {
					columnCanBeHashedPrimeryKey[attrName] = true
				}
				columnCanBeHashedPrimeryKey[attrName] = columnCanBeHashedPrimeryKey[attrName] && attrValueAsString == hashedPrimaryKeyValue
			}
		}
	}

	var primaryKeyField string
	var sortingKeyField string
	var hashingBuckets int
	if keyField == "" {
		possibleFullKeys := filterOutFalse(columnCanBeFullKey)
		possiblePrimaryKeys := filterOutFalse(columnCanBePrimaryKey)
		possibleSortingKeys := filterOutFalse(columnCanBeSortingKey)
		possibleHashedPrimaryKeys := filterOutFalse(columnCanBeHashedPrimeryKey)
		if len(possibleHashedPrimaryKeys) == 1 {
			primaryKeyField = possibleHashedPrimaryKeys[0]
			hashingBuckets = 64 // we cannot infer the hashing buckets, hence we assume it's the default
			if len(possibleSortingKeys) == 1 {
				sortingKeyField = possibleSortingKeys[0]
			}
		} else if len(possiblePrimaryKeys) == 1 {
			primaryKeyField = possiblePrimaryKeys[0]
			if len(possibleSortingKeys) == 1 {
				sortingKeyField = possibleSortingKeys[0]
			}
		}
		if primaryKeyField != "" && sortingKeyField != "" {
			keyField = primaryKeyField
		} else if len(possibleFullKeys) == 1 {
			keyField = possibleFullKeys[0]
			sortingKeyField = ""
		} else {
			var reason string
			if len(possibleFullKeys) == 0 {
				reason = "no column matches the primary-key attribute"
			} else {
				sort.Strings(possibleFullKeys)
				reason = fmt.Sprintf("%d columns (%s) match the primary-key attribute", len(possibleFullKeys), strings.Join(possibleFullKeys, ", "))
			}
			return nil, errors.Errorf("could not determine which column is the table's primary-key attribute, because %s", reason)
		}
	}

	newSchema := v3ioutils.NewSchemaWithHashingBuckets(keyField, sortingKeyField, hashingBuckets)

	for name, value := range columnNameToValue {
		err := newSchema.AddField(name, value, name != keyField && name != sortingKeyField)
		if err != nil {
			return nil, err
		}
	}

	return newSchema, nil
}

func filterOutFalse(m map[string]bool) []string {
	var res []string
	for key, val := range m {
		if val {
			res = append(res, key)
		}
	}
	return res
}
