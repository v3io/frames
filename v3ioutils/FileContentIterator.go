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
	"net/http"

	"github.com/nuclio/logger"
	v3io "github.com/v3io/v3io-go/pkg/dataplane"
	v3ioerrors "github.com/v3io/v3io-go/pkg/errors"
)

const (
	maxRetries = 10
)

type FileContentIterator struct {
	container    v3io.Container
	nextOffset   int
	step         int
	path         string
	responseChan chan *v3io.Response
	currentData  []byte
	err          error
	gotLastChunk bool
	retries      int
	logger       logger.Logger
}

func NewFileContentIterator(path string, bytesStep int, container v3io.Container, logger logger.Logger) (*FileContentIterator, error) {
	iter := &FileContentIterator{container: container,
		step:         bytesStep,
		path:         path,
		responseChan: make(chan *v3io.Response, 1),
		logger:       logger}

	input := &v3io.GetObjectInput{Path: path, NumBytes: bytesStep}
	_, err := container.GetObject(input, nil, iter.responseChan)

	if err != nil {
		return nil, err
	}

	iter.nextOffset = bytesStep
	return iter, nil
}

func (iter *FileContentIterator) Next() bool {
	if iter.gotLastChunk {
		return false
	}

	res := <-iter.responseChan
	if res.Error != nil {
		if errWithStatusCode, ok := res.Error.(v3ioerrors.ErrorWithStatusCode); ok &&
			iter.retries < maxRetries &&
			(errWithStatusCode.StatusCode() >= http.StatusInternalServerError ||
				errWithStatusCode.StatusCode() == http.StatusConflict) {
			iter.retries++

			input := res.Request().Input.(*v3io.GetObjectInput)
			_, err := iter.container.GetObject(input, nil, iter.responseChan)
			if err != nil {
				iter.logger.WarnWith("got error fetching file content",
					"input", input, "num-retries", iter.retries, "err", res.Error)
				iter.err = err
				return false
			}
			return iter.Next()
		}

		iter.logger.WarnWith("got error fetching file content after all retries",
			"input", res.Request().Input.(*v3io.GetObjectInput),
			"num-retries", iter.retries, "err", res.Error)
		iter.err = res.Error
		return false
	}

	iter.retries = 0
	iter.currentData = res.Body()

	if res.HTTPResponse.StatusCode() == http.StatusPartialContent {

		input := &v3io.GetObjectInput{Path: iter.path,
			Offset:   iter.nextOffset,
			NumBytes: iter.step}
		_, err := iter.container.GetObject(input, nil, iter.responseChan)

		if err != nil {
			iter.err = err
			return false
		}

		iter.nextOffset = iter.nextOffset + iter.step
	} else {
		iter.gotLastChunk = true
	}

	return true
}

func (iter *FileContentIterator) At() []byte {
	return iter.currentData
}

func (iter *FileContentIterator) Error() error {
	return iter.err
}
