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
	"strings"
	"time"

	"github.com/nuclio/logger"
	v3io "github.com/v3io/v3io-go/pkg/dataplane"

	"github.com/v3io/frames"
	"github.com/v3io/frames/backends/utils"
	"github.com/v3io/frames/v3ioutils"
)

// Appender is key/value appender
type Appender struct {
	request       *frames.WriteRequest
	container     v3io.Container
	tablePath     string
	responseChan  chan *v3io.Response
	commChan      chan int
	doneChan      chan bool
	sent          int
	logger        logger.Logger
	schema        v3ioutils.V3ioSchema
	asyncErr      error
	rowsProcessed int
}

// Write support writing to backend
func (kv *Backend) Write(request *frames.WriteRequest) (frames.FrameAppender, error) {

	container, tablePath, err := kv.newConnection(request.Session, request.Password.Get(), request.Token.Get(), request.Table, true)
	if err != nil {
		return nil, err
	}

	appender := Appender{
		request:      request,
		container:    container,
		tablePath:    tablePath,
		responseChan: make(chan *v3io.Response, 1000),
		commChan:     make(chan int, 2),
		logger:       kv.logger,
		schema:       v3ioutils.NewSchema("idx"),
	}
	go appender.respWaitLoop(time.Minute)

	if request.ImmidiateData != nil {
		err := appender.Add(request.ImmidiateData)
		if err != nil {
			return &appender, err
		}
	}

	return &appender, nil
}

// Add adds a frame
func (a *Appender) Add(frame frames.Frame) error {
	names := frame.Names()
	if len(names) == 0 {
		return fmt.Errorf("empty frame")
	}
	if len(frame.Indices()) > 1 {
		return fmt.Errorf("can't set key from multi-index frame")
	}

	if a.request.Expression != "" {
		return a.update(frame)
	}

	columns := make(map[string]frames.Column)
	indexName := ""
	var newSchema v3ioutils.V3ioSchema
	if indices := frame.Indices(); len(indices) > 0 {
		indexName = indices[0].Name()
		if indexName == "" {
			indexName = a.schema.(*v3ioutils.OldV3ioSchema).Key
		}
		newSchema = v3ioutils.NewSchema(indexName)
		newSchema.AddColumn(indexName, indices[0], false)
	} else {
		indexName = a.schema.(*v3ioutils.OldV3ioSchema).Key
		newSchema = v3ioutils.NewSchema(indexName)
		newSchema.AddField(indexName, 0, false)
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

	err := a.schema.UpdateSchema(a.container, a.tablePath, newSchema)
	if err != nil {
		return err
	}

	indexVal, err := a.indexValFunc(frame)
	if err != nil {
		return err
	}

	for r := 0; r < frame.Len(); r++ {
		row := make(map[string]interface{})

		// set row values from columns
		for name, col := range columns {
			val, err := utils.ColAt(col, r)
			if err != nil {
				return err
			}

			if val64, ok := val.(int64); ok {
				val = int(val64)
			}

			row[name] = val
		}

		key := indexVal(r)

		// Add key column as an attribute
		row[indexName] = key

		var condition string
		if a.request.Condition != "" {
			condition, err = genExpr(a.request.Condition, frame, r)
			if err != nil {
				a.logger.ErrorWith("error generating condition", "error", err)
				return err
			}
		}

		input := v3io.PutItemInput{Path: a.tablePath + fmt.Sprintf("%v", key), Attributes: row, Condition: condition}
		a.logger.DebugWith("write", "input", input)
		_, err := a.container.PutItem(&input, r, a.responseChan)
		if err != nil {
			a.logger.ErrorWith("write error", "error", err)
			return err
		}

		a.sent++
	}

	a.rowsProcessed += frame.Len()
	return nil
}

// update updates rows from a frame
func (a *Appender) update(frame frames.Frame) error {
	indexVal, err := a.indexValFunc(frame)
	if err != nil {
		return err
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
		input := v3io.UpdateItemInput{Path: a.tablePath + fmt.Sprintf("%v", key), Expression: expr, Condition: cond}
		a.logger.DebugWith("write update", "input", input)
		_, err = a.container.UpdateItem(&input, r, a.responseChan)
		if err != nil {
			a.logger.ErrorWith("write update error", "error", err)
			return err
		}

		a.sent++
	}

	return nil
}

// generate the update expression or condition
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

		args = append(args, "{"+name+"}")
		valString := ""

		switch col.DType() {
		case frames.IntType:
			valString = fmt.Sprintf("%d", val)
		case frames.FloatType:
			valString = fmt.Sprintf("%f", val.(float64))
		case frames.StringType, frames.TimeType:
			valString = "'" + val.(string) + "'"
		default:
			valString = fmt.Sprintf("%v", val)
		}

		args = append(args, valString)
	}

	r := strings.NewReplacer(args...)
	return r.Replace(expr), nil
}

// convert Col name to a v3io valid attr name
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
	a.logger.DebugWith("WaitForComplete", "sent", a.sent)
	a.commChan <- a.sent
	<-a.doneChan
	return a.asyncErr
}

func (a *Appender) indexValFunc(frame frames.Frame) (func(int) interface{}, error) {
	var indexCol frames.Column

	if indices := frame.Indices(); len(indices) > 0 {
		indexCol = indices[0]
	} else {
		// If no index column exist use range index
		return func(i int) interface{} {
			return a.rowsProcessed + i
		}, nil
	}

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
		return nil, fmt.Errorf("unknown column type - %v", indexCol.DType())
	}

	return fn, nil
}

func (a *Appender) respWaitLoop(timeout time.Duration) {
	responses := 0
	requests := -1
	a.doneChan = make(chan bool)
	a.logger.Debug("write wait loop started")
	timer := time.NewTimer(timeout)

	active := false
	for {
		select {

		case resp := <-a.responseChan:
			a.logger.DebugWith("write response", "response", resp)
			responses++
			active = true
			timer.Reset(timeout)

			if resp.Error != nil {
				a.logger.ErrorWith("failed write response", "error", resp.Error)
				a.asyncErr = resp.Error
			}

			if requests == responses {
				a.doneChan <- true
				return
			}

		case requests = <-a.commChan:
			if requests <= responses {
				a.doneChan <- true
				return
			}

		case <-timer.C:
			if !active {
				a.logger.ErrorWith("Resp loop timed out! ", "requests", requests, "response", responses)
				a.asyncErr = fmt.Errorf("Resp loop timed out!")
				a.doneChan <- true
				return
			}
			active = false
		}
	}
}
