package kv

import (
	"fmt"
	"strings"

	"github.com/v3io/v3io-go-http"

	"github.com/v3io/frames"
	"github.com/v3io/frames/backends/utils"
)

type Backend struct {
	ctx *frames.DataContext
}

func NewBackend(ctx *frames.DataContext) (frames.DataBackend, error) {
	newBackend := Backend{ctx: ctx}
	return &newBackend, nil
}

func (kv *Backend) ReadRequest(request *frames.DataReadRequest) (frames.MessageIterator, error) {

	kvRequest, ok := request.Extra.(frames.KVRead)
	if !ok {
		return nil, fmt.Errorf("not a KV request")
	}

	tablePath := request.Table
	if !strings.HasSuffix(tablePath, "/") {
		tablePath += "/"
	}

	if request.MaxInMessage == 0 {
		request.MaxInMessage = 256
	}

	input := v3io.GetItemsInput{Path: tablePath, Filter: request.Filter, AttributeNames: request.Columns}
	fmt.Println(input, request)
	iter, err := frames.NewAsyncItemsCursor(kv.ctx.Container, &input, kv.ctx.Workers, kvRequest.ShardingKeys)
	if err != nil {
		return nil, err
	}

	newKVIter := Iterator{ctx: kv.ctx, request: request, iter: iter}
	return &newKVIter, nil
}

type Iterator struct {
	ctx     *frames.DataContext
	request *frames.DataReadRequest
	iter    *frames.AsyncItemsCursor
	err     error
	currMsg *frames.Message
}

func (ki *Iterator) Next() bool {

	message := frames.Message{}
	message.Columns = map[string]interface{}{}
	var i int

	for ki.iter.Next() {
		i++
		row := ki.iter.GetFields()
		for name, field := range row {
			col, ok := message.Columns[name]
			if !ok {
				var err error
				col, err = utils.NewColumn(field, i-1)
				if err != nil {
					ki.err = err
					return false
				}
				message.Columns[name] = col
			}

			col, err := utils.AppendValue(col, field)
			if err != nil {
				ki.err = err
				return false
			}
			message.Columns[name] = col
		}

		// fill columns with nil if there was no value
		for name, col := range message.Columns {
			if _, ok := row[name]; ok {
				continue
			}
			var err error
			col, err = utils.AppendNil(col)
			if err != nil {
				ki.err = err
				return false
			}
			message.Columns[name] = col
		}

		if i == ki.request.MaxInMessage {
			ki.currMsg = &message
			return true
		}
	}

	if ki.iter.Err() != nil {
		ki.err = ki.iter.Err()
		return false
	}

	if i == 0 {
		return false
	}

	ki.currMsg = &message
	return true
}

func (ki *Iterator) Err() error {
	return ki.err
}

func (ki *Iterator) At() *frames.Message {
	return ki.currMsg
}

func Rows2Col(cols *map[string][]interface{}, row *map[string]interface{}, index int) {
	for name, field := range *row {
		if col, ok := (*cols)[name]; ok {
			col = append(col, field)
			(*cols)[name] = col
		} else {
			col = make([]interface{}, index)
			col = append(col, field)
			(*cols)[name] = col
		}
	}

	// fill columns with nil if there was no value
	for name, col := range *cols {
		if len(col) <= index {
			(*cols)[name] = append(col, nil)
		}
	}
}
