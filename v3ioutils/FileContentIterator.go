package v3ioutils

import (
	v3ioerrors "github.com/v3io/v3io-go/pkg/errors"
	"net/http"

	v3io "github.com/v3io/v3io-go/pkg/dataplane"
)

const (
	maxRetries = 5
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
}

func NewFileContentIterator(path string, bytesStep int, container v3io.Container) (*FileContentIterator, error) {
	iter := &FileContentIterator{container: container,
		step:         bytesStep,
		path:         path,
		responseChan: make(chan *v3io.Response, 1)}

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
		if errWithStatusCode, ok := res.Error.(*v3ioerrors.ErrorWithStatusCode); ok &&
			iter.retries < maxRetries &&
			(errWithStatusCode.StatusCode() >= http.StatusInternalServerError ||
				errWithStatusCode.StatusCode() == http.StatusConflict) {
			iter.retries++

			input := res.Request().Input.(*v3io.GetObjectInput)
			_, err := iter.container.GetObject(input, nil, iter.responseChan)
			if err != nil {
				iter.err = err
				return false
			}
			return iter.Next()
		}

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
