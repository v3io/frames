package kv

import (
	"fmt"
	"github.com/v3io/frames/pkg/common"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

type KVBackend struct {
	ctx *common.DataContext
}

func NewKVBackend(ctx *common.DataContext) (common.DataBackend, error) {
	newKVBackend := KVBackend{ctx: ctx}
	return &newKVBackend, nil
}

func (kv *KVBackend) ReadRequest(request *common.DataRequest) (common.MessageIterator, error) {

	tablePath := request.Table
	if request.Limit == 0 {
		request.Limit = 64
	}

	input := v3io.GetItemsInput{Path: tablePath, Filter: request.Filter, AttributeNames: request.Columns}
	fmt.Println(input, request)
	iter, err := utils.NewAsyncItemsCursor(kv.ctx.Container, &input, kv.ctx.Workers, request.ShardingKeys)
	if err != nil {
		return nil, err
	}

	newKVIter := KVIterator{ctx: kv.ctx, request: request, iter: iter}
	return &newKVIter, nil
}

type KVIterator struct {
	ctx     *common.DataContext
	request *common.DataRequest
	iter    *utils.AsyncItemsCursor
	err     error
	currMsg *common.Message
}

func (ki *KVIterator) Next() bool {

	message := common.Message{}
	message.Columns = map[string][]interface{}{}

	for i := 0; i < ki.request.Limit; i++ {
		if ki.iter.Next() {
			row := ki.iter.GetFields()
			for name, field := range row {
				if col, ok := message.Columns[name]; ok {
					col = append(col, field)
					message.Columns[name] = col
				} else {
					col = make([]interface{}, i)
					col = append(col, field)
					message.Columns[name] = col
				}
			}

		} else {
			if ki.iter.Err() != nil {
				ki.err = ki.iter.Err()
			}
			ki.currMsg = &message
			return false
		}
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
