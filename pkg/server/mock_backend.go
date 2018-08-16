package server

import (
	"fmt"
	"math/rand"

	"github.com/v3io/frames/pkg/common"
)

type MockBackend struct{}

func (mb *MockBackend) ReadRequest(request *common.DataReadRequest) (common.MessageIterator, error) {
	it := &MockIterator{
		request: request,
	}

	return it, nil
}

func (mb *MockBackend) WriteRequest(request *common.DataWriteRequest) (common.MessageAppender, error) {
	return nil, fmt.Errorf("write not implemented")
}

type MockIterator struct {
	request *common.DataReadRequest
	count   int
}

func (it *MockIterator) Next() bool {
	it.count++
	return it.count < it.request.Limit
}

func (it *MockIterator) Err() error {
	return nil
}

func (it *MockIterator) At() *common.Message {
	msg := &common.Message{
		Columns: map[string][]interface{}{},
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
