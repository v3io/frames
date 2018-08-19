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
