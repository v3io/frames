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
	"strconv"
	"strings"
	"time"

	"github.com/v3io/frames"
	"github.com/v3io/frames/backends/utils"

	"github.com/nuclio/logger"
	"github.com/v3io/v3io-go-http"
)

// Appender is key/value appender
type Appender struct {
	request      *frames.WriteRequest
	container    *v3io.Container
	tablePath    string
	responseChan chan *v3io.Response
	commChan     chan int
	doneChan     chan bool
	sent         int
	logger       logger.Logger
}

// Write support writing to backend
func (kv *Backend) Write(request *frames.WriteRequest) (frames.FrameAppender, error) {
	tablePath := request.Table
	if !strings.HasSuffix(tablePath, "/") {
		tablePath += "/"
	}

	appender := Appender{
		request:      request,
		container:    kv.container,
		tablePath:    tablePath,
		responseChan: make(chan *v3io.Response, 1000),
		commChan:     make(chan int, 2),
		logger:       kv.logger,
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

	columns := make(map[string]frames.Column)
	for _, name := range frame.Names() {
		col, err := frame.Column(name)
		if err != nil {
			return err
		}
		columns[name] = col
	}

	indexVal, err := a.indexValFunc(frame, names[0])
	if err != nil {
		return err
	}

	for r := 0; r < frame.Len(); r++ {
		row := make(map[string]interface{})
		for name, col := range columns {
			val, err := utils.ColAt(col, r)
			if err != nil {
				return err
			}

			row[name] = val
		}

		key := indexVal(r)
		input := v3io.PutItemInput{Path: a.tablePath + key, Attributes: row}
		a.logger.DebugWith("write", "input", input)
		_, err := a.container.PutItem(&input, r, a.responseChan)
		if err != nil {
			a.logger.ErrorWith("write error", "error", err)
			return err
		}

		a.sent++
	}

	return nil
}

// WaitForComplete waits for write to complete
func (a *Appender) WaitForComplete(timeout time.Duration) error {
	a.logger.DebugWith("WaitForComplete", "sent", a.sent)
	a.commChan <- a.sent
	<-a.doneChan
	return nil
}

func (a *Appender) indexValFunc(frame frames.Frame, name string) (func(int) string, error) {

	indexName := name
	if iName := frame.IndexName(); iName != "" {
		indexName = iName
	}

	indexCol, err := frame.Column(indexName)
	if err != nil {
		return nil, err
	}

	var fn func(int) string
	switch indexCol.DType() {
	// strconv.Format* is about twice as fast as fmt.Sprintf
	case frames.IntType:
		fn = func(i int) string {
			return strconv.FormatInt(int64(indexCol.IntAt(i)), 10)
		}
	case frames.FloatType:
		fn = func(i int) string {
			return strconv.FormatFloat(indexCol.FloatAt(i), 'f', -1, 64)
		}
	case frames.StringType:
		fn = func(i int) string {
			return indexCol.StringAt(i)
		}
	case frames.TimeType:
		fn = func(i int) string {
			return indexCol.TimeAt(i).String()
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

	active := false
	for {
		select {

		case resp := <-a.responseChan:
			a.logger.DebugWith("write response", "response", resp)
			responses++
			active = true

			if resp.Error != nil {
				a.logger.ErrorWith("failed write response", "error", resp.Error)
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

		case <-time.After(timeout):
			if !active {
				a.logger.ErrorWith("Resp loop timed out! ", "requests", requests, "response", responses)
				a.doneChan <- true
				return
			}
			active = false
		}
	}
}
