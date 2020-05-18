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
	"net/http"
	"strings"
	"time"

	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/v3io/frames"
	"github.com/v3io/frames/backends/utils"
	"github.com/v3io/frames/v3ioutils"
	v3io "github.com/v3io/v3io-go/pkg/dataplane"
	v3ioerrors "github.com/v3io/v3io-go/pkg/errors"
)

// Appender is key/value appender
type Appender struct {
	request       *frames.WriteRequest
	container     v3io.Container
	tablePath     string
	requestChan   chan *v3io.UpdateItemInput
	doneChan      chan struct{}
	logger        logger.Logger
	schema        v3ioutils.V3ioSchema
	asyncErr      error
	rowsProcessed int
}

const (
	errorCodeString                        = "ErrorCode"
	falseConditionOuterErrorCode           = "16777244"
	falseConditionInnerErrorCode           = "16777245"
	createNewItemOnlyExistingItemErrorCode = "369098809"
)

// Write supports writing to the backend
func (kv *Backend) Write(request *frames.WriteRequest) (frames.FrameAppender, error) {

	container, tablePath, err := kv.newConnection(request.Session, request.Password.Get(), request.Token.Get(), request.Table, true)
	if err != nil {
		return nil, err
	}

	var schema v3ioutils.V3ioSchema
	schema, err = v3ioutils.GetSchema(tablePath, container)

	// Ignore 404 error, since it makes sense there is no schema yet.
	tableAlreadyExists := true
	if err != nil {
		if errorWithStatus, ok := err.(v3ioerrors.ErrorWithStatusCode); !ok || errorWithStatus.StatusCode() != http.StatusNotFound {
			return nil, err
		}
		tableAlreadyExists = false
	}

	if tableAlreadyExists {
		switch request.SaveMode {
		case frames.OverwriteTable:
			// If this is the first time we writing to the table, there is nothing to delete.
			err = v3ioutils.DeleteTable(kv.logger, container, tablePath, "", kv.numWorkers, true)
			if err != nil {
				return nil, fmt.Errorf("error occured while deleting table '%v', err: %v", tablePath, err)
			}
			schema = nil
		case frames.ErrorIfTableExists:
			return nil, fmt.Errorf("table '%v' already exists; either use a differnet save mode or save to a different table", tablePath)
		}
	} else if request.SaveMode == frames.ErrorIfTableExists {
		exists, err := checkPathExists(tablePath, container)
		if err != nil {
			return nil, err
		}
		if exists {
			return nil, fmt.Errorf("folder '%v' already exists; you can't write to an existing folder unless it already contains a schema file", tablePath)
		}
	}

	if schema == nil {
		schema = v3ioutils.NewSchema(v3ioutils.DefaultKeyColumn, "")
	}

	appender := Appender{
		request:     request,
		container:   container,
		tablePath:   tablePath,
		requestChan: make(chan *v3io.UpdateItemInput, kv.updateWorkersPerVN*2),
		doneChan:    make(chan struct{}, 1),
		logger:      kv.logger,
		schema:      schema,
	}

	internalDoneChan := make(chan struct{}, kv.updateWorkersPerVN)

	for i := 0; i < kv.updateWorkersPerVN; i++ {
		go appender.updateItemWorker(internalDoneChan)
	}

	go func() {
		for i := 0; i < kv.updateWorkersPerVN; i++ {
			<-internalDoneChan
		}
		appender.doneChan <- struct{}{}
	}()

	if request.ImmidiateData != nil {
		err := appender.Add(request.ImmidiateData)
		if err != nil {
			appender.Close()
			return nil, err
		}
	}

	return &appender, nil
}

func checkPathExists(tablePath string, container v3io.Container) (bool, error) {
	input := &v3io.CheckPathExistsInput{Path: tablePath}
	err := container.CheckPathExistsSync(input)
	if err != nil {
		if errorWithStatusCode, ok := err.(v3ioerrors.ErrorWithStatusCode); !ok || errorWithStatusCode.StatusCode() != http.StatusNotFound {
			return false, errors.Wrapf(err, "check path failed '%s'.", tablePath)
		}
		return false, nil
	}

	return true, nil
}

func validateFrameInput(frame frames.Frame, request *frames.WriteRequest) error {
	names := frame.Names()
	if len(names) == 0 {
		return fmt.Errorf("empty frame")
	}
	if len(frame.Indices()) > 2 {
		return fmt.Errorf("can only write up to two indices")
	}
	if len(request.PartitionKeys) > 0 {
		for _, partitionColumnName := range request.PartitionKeys {
			_, err := frame.Column(partitionColumnName)
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("column '%v' does not exist in the dataframe", partitionColumnName))
			}
		}
	}
	return nil
}

// Add adds a frame
func (a *Appender) Add(frame frames.Frame) error {
	err := validateFrameInput(frame, a.request)
	if err != nil {
		return err
	}

	if a.request.Expression != "" {
		return a.update(frame)
	}

	columns := make(map[string]frames.Column)
	indexName, sortingKeyName := "", ""
	var newSchema v3ioutils.V3ioSchema
	indices := frame.Indices()
	var sortingFunc func(int) interface{}

	if len(indices) > 0 {
		indexName = indices[0].Name()
		if indexName == "" {
			indexName = a.schema.(*v3ioutils.OldV3ioSchema).Key
		}
		sortingKeyName = a.schema.(*v3ioutils.OldV3ioSchema).SortingKey
		if len(indices) > 1 {
			sortingKeyName = indices[1].Name()
			sortingFunc, err = a.funcFromCol(indices[1])
			if err != nil {
				return err
			}
		}
		newSchema = v3ioutils.NewSchema(indexName, sortingKeyName)
		err = newSchema.AddColumn(indexName, indices[0], false)
		if err != nil {
			return err
		}
		if len(indices) > 1 {
			err = newSchema.AddColumn(indices[1].Name(), indices[1], false)
			if err != nil {
				return err
			}
		}
	} else {
		indexName = a.schema.(*v3ioutils.OldV3ioSchema).Key
		newSchema = v3ioutils.NewSchema(indexName, "")
		err = newSchema.AddField(indexName, 0, false)
		if err != nil {
			return err
		}
	}

	for _, name := range frame.Names() {
		col, err := frame.Column(name)
		if err != nil {
			return err
		}
		name = validColName(name)
		err = newSchema.AddColumn(name, col, true)
		if err != nil {
			return err
		}
		columns[name] = col
	}
	for name, val := range frame.Labels() {
		err := newSchema.AddField(name, val, true)
		if err != nil {
			return err
		}
	}

	err = a.schema.UpdateSchema(a.container, a.tablePath, newSchema)
	if err != nil {
		return err
	}

	indexVal, err := a.indexValFunc(frame)
	if err != nil {
		return err
	}

	for r := 0; r < frame.Len(); r++ {
		var rowMap map[string]interface{}
		var expression *string
		var err error
		var keyVal, sortingKeyVal interface{}

		if a.request.SaveMode == frames.UpdateItem {
			var expressionStr string
			expressionStr, keyVal, sortingKeyVal, err = getUpdateExpressionFromRow(columns, r, frame.IsNull,
				indexVal, sortingFunc,
				indexName, sortingKeyName)
			expression = &expressionStr
		} else {
			rowMap, keyVal, sortingKeyVal, err = getMapFromRow(columns, r, frame.IsNull,
				indexVal, sortingFunc,
				indexName, sortingKeyName)
		}
		if err != nil {
			return err
		}

		var itemSubPath strings.Builder
		if len(a.request.PartitionKeys) > 0 {
			for _, partitionColumnName := range a.request.PartitionKeys {
				itemSubPath.WriteString(partitionColumnName)
				itemSubPath.WriteString("=")
				val, err := utils.ColAt(columns[partitionColumnName], r)
				if err != nil {
					return err
				}
				itemSubPath.WriteString(fmt.Sprintf("%v", val))
				itemSubPath.WriteString("/")
			}
		}

		subPathString := itemSubPath.String()

		if keyVal == "" {
			return errors.Errorf("invalid input. key %q should not be empty", indexName)
		}
		if sortingKeyName != "" && sortingKeyVal == "" {
			return errors.Errorf("invalid input. sorting key %q should not be empty", sortingKeyName)
		}

		var condition string
		if a.request.Condition != "" {
			condition, err = genExpr(a.request.Condition, frame, r)
			if err != nil {
				a.logger.ErrorWith("error generating condition", "error", err)
				return err
			}
		}

		input := v3io.UpdateItemInput{Path: fmt.Sprintf("%v%v%v", a.tablePath, subPathString, a.formatKeyName(keyVal, sortingKeyVal)),
			Attributes: rowMap,
			Expression: expression,
			Condition:  condition,
			UpdateMode: a.request.SaveMode.GetNginxModeName()}
		a.logger.DebugWith("write", "input", input)
		a.requestChan <- &input
	}

	a.rowsProcessed += frame.Len()
	return nil
}

func (a *Appender) formatKeyName(key interface{}, sortingVal interface{}) string {
	var format string
	if sortingVal != nil {
		format = fmt.Sprintf("%v.%v", key, sortingVal)
	} else {
		format = fmt.Sprintf("%v", key)
	}
	return format
}

// update updates rows from a frame
func (a *Appender) update(frame frames.Frame) error {
	indexVal, err := a.indexValFunc(frame)
	if err != nil {
		return err
	}

	var sortingFunc func(int) interface{}
	if len(frame.Indices()) > 1 {
		sortingFunc, err = a.funcFromCol(frame.Indices()[1])
		if err != nil {
			return err
		}
	}
	for r := 0; r < frame.Len(); r++ {

		var expr *string
		if a.request.Expression != "" {
			exprString, err := genExpr(a.request.Expression, frame, r)
			if err != nil {
				a.logger.ErrorWith("error generating expression", "error", err)
				return err
			}
			expr = &exprString
		}

		var cond string
		if a.request.Condition != "" {
			cond, err = genExpr(a.request.Condition, frame, r)
			if err != nil {
				a.logger.ErrorWith("error generating condition", "error", err)
				return err
			}
		}

		key := indexVal(r)
		var sortingVal interface{}
		if len(frame.Indices()) > 1 {
			sortingVal = sortingFunc(r)
		}

		input := v3io.UpdateItemInput{Path: a.tablePath + a.formatKeyName(key, sortingVal),
			Expression: expr,
			Condition:  cond,
			UpdateMode: a.request.SaveMode.GetNginxModeName()}
		a.logger.DebugWith("write update", "input", input)
		a.requestChan <- &input
	}

	return nil
}

// Generates an update expression or condition
func genExpr(expr string, frame frames.Frame, index int) (string, error) {
	args := make([]string, 0)

	for _, name := range frame.Names() {
		col, err := frame.Column(name)
		if err != nil {
			return "", err
		}

		val, err := utils.ColAt(col, index)
		if err != nil {
			return "", err
		}

		args = append(args, fmt.Sprintf("{%v}", name))
		valString := valueToTypedExpressionString(val)

		args = append(args, valString)
	}

	for _, indexCol := range frame.Indices() {
		indexName := indexCol.Name()
		val, err := utils.ColAt(indexCol, index)
		if err != nil {
			return "", err
		}

		args = append(args, fmt.Sprintf("{%v}", indexName))
		valString := valueToTypedExpressionString(val)

		args = append(args, valString)
	}

	r := strings.NewReplacer(args...)
	return r.Replace(expr), nil
}

// Converts a column name to a valid platform (v3io) attribute name
// TODO: may want to also update the name in the Column object
func validColName(name string) string {
	for i := 0; i < len(name); i++ {
		if name[i] == ' ' || name[i] == ':' {
			name = name[:i] + "_" + name[i+1:]
		}
	}
	return name
}

// WaitForComplete waits for write to complete
func (a *Appender) WaitForComplete(timeout time.Duration) error {
	a.Close()
	<-a.doneChan
	return a.asyncErr
}

func (a *Appender) Close() {
	close(a.requestChan)
}

func (a *Appender) indexValFunc(frame frames.Frame) (func(int) interface{}, error) {
	var indexCol frames.Column

	if indices := frame.Indices(); len(indices) > 0 {
		indexCol = indices[0]
	} else {
		// If no index column exists, use range index
		return func(i int) interface{} {
			return a.rowsProcessed + i
		}, nil
	}

	return a.funcFromCol(indexCol)
}

func (a *Appender) funcFromCol(indexCol frames.Column) (func(int) interface{}, error) {
	var fn func(int) interface{}
	switch indexCol.DType() {
	// strconv.Format* is about twice as fast as fmt.Sprintf
	case frames.IntType:
		fn = func(i int) interface{} {
			ival, _ := indexCol.IntAt(i)
			return ival
		}
	case frames.FloatType:
		fn = func(i int) interface{} {
			fval, _ := indexCol.FloatAt(i)
			return fval
		}
	case frames.StringType:
		fn = func(i int) interface{} {
			sval, _ := indexCol.StringAt(i)
			return sval
		}
	case frames.TimeType:
		fn = func(i int) interface{} {
			tval, _ := indexCol.TimeAt(i)
			return tval
		}
	case frames.BoolType:
		fn = func(i int) interface{} {
			bval, _ := indexCol.BoolAt(i)
			if bval {
				return true
			}
			return false
		}
	default:
		return nil, fmt.Errorf("unknown column type - '%v'", indexCol.DType())
	}
	return fn, nil
}

func (a *Appender) updateItemWorker(doneChan chan<- struct{}) {
	for req := range a.requestChan {
		a.logger.DebugWith("write request", "request", req)

		resp, err := a.container.UpdateItemSync(req)
		if err != nil {
			// If condition evaluated to false, log this and discard error
			if isFalseConditionError(err) {
				a.logger.Info("condition for item '%v' evaluated to false", req)
			} else if isOnlyNewItemUpdateModeItemExistError(err, req.UpdateMode) {
				a.logger.Info("trying to write to an existing item with update mode 'CreateNewItemsOnly' (item: '%v')", req)
			} else {
				a.logger.ErrorWith("failed to update item", "error", err)
				a.asyncErr = err
			}
		} else {
			resp.Release()
		}
	}

	doneChan <- struct{}{}
}

// Check whether the current error was caused specifically because the condition was evaluated to false.
func isFalseConditionError(err error) bool {
	errString := err.Error()

	if strings.Count(errString, errorCodeString) == 2 &&
		strings.Contains(errString, falseConditionOuterErrorCode) &&
		strings.Contains(errString, falseConditionInnerErrorCode) {
		return true
	}

	return false
}

func isOnlyNewItemUpdateModeItemExistError(err error, mode string) bool {
	errString := err.Error()

	if mode == frames.CreateNewItemsOnly.GetNginxModeName() &&
		strings.Count(errString, errorCodeString) == 2 &&
		strings.Contains(errString, createNewItemOnlyExistingItemErrorCode) {
		return true
	}

	return false
}

func getMapFromRow(columns map[string]frames.Column,
	index int,
	isNull func(int, string) bool,
	indexValFunc, sortingKeyValFunc func(int) interface{},
	indexName, sortingKeyName string) (map[string]interface{}, interface{}, interface{}, error) {
	row := make(map[string]interface{})

	// Set row values from columns
	for name, col := range columns {
		if isNull(index, name) {
			continue
		}
		val, err := utils.ColAt(col, index)
		if err != nil {
			return nil, nil, nil, err
		}

		if val64, ok := val.(int64); ok {
			val = int(val64)
		}

		row[name] = val
	}

	key := indexValFunc(index)
	// Add key column as an attribute
	row[indexName] = key

	var sortingVal interface{}
	if sortingKeyName != "" {
		sortingVal = sortingKeyValFunc(index)
		row[sortingKeyName] = sortingVal
	}

	return row, key, sortingVal, nil
}

func getUpdateExpressionFromRow(columns map[string]frames.Column,
	index int,
	isNull func(int, string) bool,
	indexValFunc, sortingKeyValFunc func(int) interface{},
	indexName, sortingKeyName string) (string, interface{}, interface{}, error) {
	expression := strings.Builder{}

	// Set row values from columns
	for name, col := range columns {
		if isNull(index, name) {
			expression.WriteString("delete(")
			expression.WriteString(name)
			expression.WriteString(");")
			continue
		}

		val, err := utils.ColAt(col, index)
		if err != nil {
			return "", nil, nil, err
		}

		expression.WriteString(name)
		expression.WriteString("=")
		expression.WriteString(valueToTypedExpressionString(val))
		expression.WriteString(";")
	}

	key := indexValFunc(index)
	// Add key column as an attribute
	expression.WriteString(indexName)
	expression.WriteString("=")
	expression.WriteString(valueToTypedExpressionString(key))
	expression.WriteString(";")

	var sortingVal interface{}
	if sortingKeyName != "" {
		sortingVal = sortingKeyValFunc(index)
		expression.WriteString(sortingKeyName)
		expression.WriteString("=")
		expression.WriteString(valueToTypedExpressionString(sortingVal))
		expression.WriteString(";")
	}

	return expression.String(), key, sortingVal, nil
}

func valueToTypedExpressionString(value interface{}) string {
	switch typedVal := value.(type) {
	case string:
		return fmt.Sprintf("'%v'", typedVal)
	case time.Time:
		seconds := typedVal.Unix()
		nanos := typedVal.Nanosecond()
		return fmt.Sprintf("%v:%v", seconds, nanos)
	default:
		return fmt.Sprintf("%v", value)
	}
}
