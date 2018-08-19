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

package mock

import (
	"fmt"
	"math/rand"

	"github.com/v3io/frames"
)

type Backend struct{}

func (mb *Backend) ReadRequest(request *frames.DataReadRequest) (frames.MessageIterator, error) {
	it := &Iterator{
		request: request,
	}

	return it, nil
}

func (mb *Backend) WriteRequest(request *frames.DataWriteRequest) (frames.MessageAppender, error) {
	return nil, fmt.Errorf("write not implemented")
}

type Iterator struct {
	request *frames.DataReadRequest
	count   int
}

func (it *Iterator) Next() bool {
	it.count++
	return it.count < it.request.Limit
}

func (it *Iterator) Err() error {
	return nil
}

func (it *Iterator) At() *frames.Message {
	msg := &frames.Message{
		Columns: map[string]interface{}{},
	}

	if it.count == 1 {
		msg.Labels = map[string]string{
			"server": "srv1",
			"env":    "testing",
		}
	}

	for _, col := range it.request.Columns {
		msg.Columns[col] = randCol(12)
	}

	return msg
}

func randCol(size int) []interface{} {
	col := make([]interface{}, size)
	for i := 0; i < size; i++ {
		col[i] = rand.Float64()
	}

	return col
}
