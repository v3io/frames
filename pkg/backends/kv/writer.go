package kv

import (
	"fmt"
	"github.com/v3io/frames/pkg/common"
	"github.com/v3io/v3io-go-http"
	"strings"
	"time"
)

func (kv *KVBackend) WriteRequest(request *common.DataWriteRequest) error {

	tablePath := request.Table
	if !strings.HasSuffix(tablePath, "/") {
		tablePath += "/"
	}

	responseChan := make(chan *v3io.Response, 1000)
	commChan := make(chan int, 2)
	doneChan := respWaitLoop(commChan, responseChan, 10*time.Second)

	i := 0
	for _, row := range request.Rows {
		key, ok := row[request.Key]
		if !ok {
			return fmt.Errorf("Missing row key %s in row %d", request.Key, i)
		}
		input := v3io.PutItemInput{Path: tablePath + key.(string), Attributes: row}
		_, err := kv.ctx.Container.PutItem(&input, i, responseChan)
		if err != nil {
			return err
		}

		i++
	}

	commChan <- i
	<-doneChan

	return nil
}

func respWaitLoop(comm chan int, responseChan chan *v3io.Response, timeout time.Duration) chan bool {
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
					fmt.Println(resp.Error, "failed write response")
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
					fmt.Println("\nResp loop timed out! ", requests, responses)
					done <- true
					return
				} else {
					active = false
				}
			}
		}
	}()

	return done
}
