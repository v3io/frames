package kv

import (
	"fmt"
	"github.com/v3io/frames/pkg/common"
	"github.com/v3io/v3io-go-http"
	"strings"
	"time"
)

type KVAppender struct {
	//request      *common.DataWriteRequest
	container    *v3io.Container
	tablePath    string
	responseChan chan *v3io.Response
	commChan     chan int
	doneChan     chan bool
	sent         int
}

func (kv *KVBackend) WriteRequest(request *common.DataWriteRequest) (common.MessageAppender, error) {

	tablePath := request.Table
	if !strings.HasSuffix(tablePath, "/") {
		tablePath += "/"
	}

	appender := KVAppender{
		container:    kv.ctx.Container,
		tablePath:    tablePath,
		responseChan: make(chan *v3io.Response, 1000),
		commChan:     make(chan int, 2),
	}
	appender.respWaitLoop(10 * time.Second)
	return &appender, nil
}

func (a *KVAppender) Add(message *common.Message) error {

	indexCol, ok := message.Columns[message.IndexCol]
	// validate that we have row keys for all rows before writing
	if !ok {
		return fmt.Errorf("Missing indec column %s", message.IndexCol)
	}
	for j, row := range indexCol {
		if row == nil {
			return fmt.Errorf("Missing row key %s in row %d", message.IndexCol, j)
		}
	}

	i := 0
OuterLoop:
	for {

		newRow := map[string]interface{}{}
		for name, col := range message.Columns {
			if len(col) <= i {
				break OuterLoop
			}
			if col[i] != nil {
				newRow[name] = col[i]
			}
		}
		key := newRow[message.IndexCol]
		input := v3io.PutItemInput{Path: a.tablePath + fmt.Sprintf("%s", key), Attributes: newRow}
		_, err := a.container.PutItem(&input, i, a.responseChan)
		if err != nil {
			a.sent += i
			return err
		}

		i++
	}
	a.sent += i

	return nil
}

func (a *KVAppender) WaitForComplete(timeout time.Duration) error {

	a.commChan <- a.sent
	<-a.doneChan
	return nil
}

func (a *KVAppender) respWaitLoop(timeout time.Duration) {
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
				} else {
					active = false
				}
			}
		}
	}()
}
