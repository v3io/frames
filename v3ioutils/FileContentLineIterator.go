package v3ioutils

import (
	"bytes"
	"fmt"

	v3io "github.com/v3io/v3io-go/pkg/dataplane"
)

type FileContentLineIterator struct {
	fileContentIterator *FileContentIterator
	currentLines        [][]byte
	currentRow          int
	err                 error
}

func NewFileContentLineIterator(path string, bytesStep int, container v3io.Container) (*FileContentLineIterator, error) {

	contentIter, err := NewFileContentIterator(path, bytesStep, container)
	if err != nil {
		return nil, err
	}
	iter := &FileContentLineIterator{fileContentIterator: contentIter, currentLines: nil}
	return iter, nil
}

func (iter *FileContentLineIterator) Next() bool {
	if iter.currentLines == nil {
		if iter.fileContentIterator.Next() {
			iter.currentLines = bytes.Split(iter.fileContentIterator.At(), []byte{'\n'})
			return true
		}

		iter.err = iter.fileContentIterator.Error()
		return false

	}

	if iter.currentRow == len(iter.currentLines)-2 {
		leftover := iter.currentLines[len(iter.currentLines)-1] // will be either a partial line or an empty string
		if iter.fileContentIterator.Next() {
			iter.currentLines = bytes.Split(iter.fileContentIterator.At(), []byte{'\n'})
			iter.currentLines[0] = append(leftover, iter.currentLines[0]...)
			iter.currentRow = 0
			return true
		}

		if iter.fileContentIterator.Error() != nil {
			iter.err = iter.fileContentIterator.Error()
		} else if len(leftover) > 0 {
			iter.err = fmt.Errorf("got partial data in last line: %v", leftover)
		}

		return false
	}

	iter.currentRow++
	return true
}

func (iter *FileContentLineIterator) At() []byte {
	return iter.currentLines[iter.currentRow]
}

func (iter *FileContentLineIterator) Error() error {
	return iter.err
}
