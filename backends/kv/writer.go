package kv

import (
	"fmt"
	"strings"
	"time"

	"github.com/v3io/frames"

	"github.com/v3io/v3io-go-http"
)

type Appender struct {
	//request      *common.DataWriteRequest
	container    *v3io.Container
	tablePath    string
	responseChan chan *v3io.Response
	commChan     chan int
	doneChan     chan bool
	sent         int
}

func (kv *Backend) WriteRequest(request *frames.DataWriteRequest) (frames.MessageAppender, error) {

	tablePath := request.Table
	if !strings.HasSuffix(tablePath, "/") {
		tablePath += "/"
	}

	appender := Appender{
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

func (a *Appender) Add(message *frames.Message) error {
	/* TODO: We don't have nil values in columns
	indexCol, ok := message.Columns[message.IndexCol]
	// validate that we have row keys for all rows before writing
	if !ok {
		return fmt.Errorf("Missing index column %s", message.IndexCol)
	}

	for j, row := range indexCol {
		if row == nil {
			return fmt.Errorf("Missing row key %s in row %d", message.IndexCol, j)
		}
	}
	*/

	i := 0
OuterLoop:
	for {
		// TODO: Move to utils?
		newRow := map[string]interface{}{}
		for name := range message.Columns {
			colType, _ := message.ColumnType(name)
			switch colType {
			case frames.IntType:
				icol, _ := message.Ints(name)
				if len(icol) <= i {
					break OuterLoop
				}

				newRow[name] = icol[i]
			case frames.FloatType:
				fcol, _ := message.Floats(name)
				if len(fcol) <= i {
					break OuterLoop
				}
				newRow[name] = fcol[i]
			case frames.StringType:
				scol, _ := message.Strings(name)
				if len(scol) <= i {
					break OuterLoop
				}
				newRow[name] = scol[i]
			case frames.TimeType:
				tcol, _ := message.Times(name)
				if len(tcol) <= i {
					break OuterLoop
				}
				newRow[name] = tcol[i]
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
				} else {
					active = false
				}
			}
		}
	}()
}
