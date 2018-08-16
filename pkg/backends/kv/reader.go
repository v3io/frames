package kv

import (
	"fmt"
	"github.com/v3io/frames/pkg/common"
	"github.com/v3io/frames/pkg/utils"
	"github.com/v3io/v3io-go-http"
	"strings"
)

type KVBackend struct {
	ctx *common.DataContext
}

func NewKVBackend(ctx *common.DataContext) (common.DataBackend, error) {
	newKVBackend := KVBackend{ctx: ctx}
	return &newKVBackend, nil
}

func (kv *KVBackend) ReadRequest(request *common.DataReadRequest) (common.MessageIterator, error) {

	kvRequest, ok := request.Extra.(common.KVRead)
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
	iter, err := utils.NewAsyncItemsCursor(kv.ctx.Container, &input, kv.ctx.Workers, kvRequest.ShardingKeys)
	if err != nil {
		return nil, err
	}

	newKVIter := KVIterator{ctx: kv.ctx, request: request, iter: iter}
	return &newKVIter, nil
}

type KVIterator struct {
	ctx     *common.DataContext
	request *common.DataReadRequest
	iter    *utils.AsyncItemsCursor
	err     error
	currMsg *common.Message
}

func (ki *KVIterator) Next() bool {

	message := common.Message{}
	message.Columns = map[string][]interface{}{}
	var i int

	for ki.iter.Next() {
		i++
		row := ki.iter.GetFields()
		for name, field := range row {
			if col, ok := message.Columns[name]; ok {

				col = append(col, field)
				message.Columns[name] = col
			} else {
				col = make([]interface{}, i-1)
				col = append(col, field)
				message.Columns[name] = col
			}
		}

		// fill columns with nil if there was no value
		for name, col := range message.Columns {
			if len(col) < i {
				message.Columns[name] = append(col, nil)
			}
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

func (ki *KVIterator) Err() error {
	return ki.err
}

func (ki *KVIterator) At() *common.Message {
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
