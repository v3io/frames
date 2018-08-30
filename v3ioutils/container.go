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
	"encoding/binary"
	"time"

	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-go-http"
)

// CreateContainer creates a new container
func CreateContainer(logger logger.Logger, addr, cont, username, password string, workers int) (*v3io.Container, error) {
	// create context
	context, err := v3io.NewContext(logger, addr, workers)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create client")
	}

	// create session
	session, err := context.NewSession(username, password, "v3test")
	if err != nil {
		return nil, errors.Wrap(err, "failed to create session")
	}

	// create the container
	container, err := session.NewContainer(cont)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create container")
	}

	return container, nil
}

// AsInt64Array convert v3io blob to Int array
func AsInt64Array(val []byte) []uint64 {
	var array []uint64
	bytes := val
	for i := 16; i+8 <= len(bytes); i += 8 {
		val := binary.LittleEndian.Uint64(bytes[i : i+8])
		array = append(array, val)
	}
	return array
}

// DeleteTable deletes a table
func DeleteTable(logger logger.Logger, container *v3io.Container, path, filter string, workers int) error {

	input := v3io.GetItemsInput{Path: path, AttributeNames: []string{"__name"}, Filter: filter}
	iter, err := NewAsyncItemsCursor(nil, container, &input, workers, []string{})
	//iter, err := container.Sync.GetItemsCursor(&input)
	if err != nil {
		return err
	}

	responseChan := make(chan *v3io.Response, 1000)
	commChan := make(chan int, 2)
	doneChan := respWaitLoop(logger, commChan, responseChan, 10*time.Second)
	reqMap := map[uint64]bool{}

	i := 0
	for iter.Next() {
		name := iter.GetField("__name").(string)
		req, err := container.DeleteObject(&v3io.DeleteObjectInput{Path: path + "/" + name}, nil, responseChan)
		if err != nil {
			commChan <- i
			return errors.Wrap(err, "failed to delete object "+name)
		}
		reqMap[req.ID] = true
		i++
	}

	commChan <- i
	if iter.Err() != nil {
		return errors.Wrap(iter.Err(), "failed to delete object ")
	}

	<-doneChan

	return nil
}

func respWaitLoop(logger logger.Logger, comm chan int, responseChan chan *v3io.Response, timeout time.Duration) chan bool {
	responses := 0
	requests := -1
	done := make(chan bool)

	go func() {
		active := false
		for {
			select {

			case resp := <-responseChan:
				responses++
				active = true

				if resp.Error != nil {
					logger.ErrorWith("failed Delete response", "error", resp.Error)
					// TODO: signal done and return?
				}

				if requests == responses {
					done <- true
					return
				}

			case requests = <-comm:
				if requests <= responses {
					done <- true
					return
				}

			case <-time.After(timeout):
				if !active {
					logger.ErrorWith("Resp loop timed out!", "requests", requests, "response", responses)
					done <- true
					return
				}
				active = false
			}
		}
	}()

	return done
}
