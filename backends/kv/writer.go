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

	"github.com/v3io/frames"

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
}

// Write support writing to backend
func (kv *Backend) Write(request *frames.WriteRequest) (frames.FrameAppender, error) {

	tablePath := request.Table
	if !strings.HasSuffix(tablePath, "/") {
		tablePath += "/"
	}

	appender := Appender{
		request:      request,
		container:    kv.ctx.Container,
		tablePath:    tablePath,
		responseChan: make(chan *v3io.Response, 1000),
		commChan:     make(chan int, 2),
	}
	appender.respWaitLoop(10 * time.Second)

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
	names := frame.Columns()
	columns := make(map[string]frames.Column)
	for _, name := range frame.Columns() {
		col, err := frame.Column(name)
		if err != nil {
			return err
		}
		columns[name] = col
	}

	for r := 0; r < frame.Len(); r++ {
		row := make(map[string]interface{})
		for name, col := range columns {
			switch col.DType() {
			case frames.IntType:
				row[name] = col.IntAt(r)
			case frames.FloatType:
				row[name] = col.FloatAt(r)
			case frames.StringType:
				row[name] = col.StringAt(r)
			case frames.TimeType: // TODO: Does v3io support time.Time?
				row[name] = col.TimeAt(r)
			default:
				return fmt.Errorf("unknown column type - %s", col.DType())
			}
		}

		key := names[0] // TODO: Index?
		input := v3io.PutItemInput{Path: a.tablePath + key, Attributes: row}
		_, err := a.container.PutItem(&input, r, a.responseChan)
		if err != nil {
			a.sent += r // TODO: Bug? (should be ++)
			return err
		}
		a.sent += r // TODO: Bug? (should be ++)

	}

	return nil
}

// WaitForComplete waits for write to complete
func (a *Appender) WaitForComplete(timeout time.Duration) error {
	a.commChan <- a.sent
	<-a.doneChan
	return nil
}

func (a *Appender) respWaitLoop(timeout time.Duration) {
	responses := 0
	requests := -1
	a.doneChan = make(chan bool)

	go func() {
		active := false
		for {
			select {

			case resp := <-a.responseChan:
				responses++
				active = true

				if resp.Error != nil {
					fmt.Println(resp.Error, "failed write response")
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
					fmt.Println("\nResp loop timed out! ", requests, responses)
					a.doneChan <- true
					return
				}
				active = false
			}
		}
	}()
}
