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
	"bytes"
	"fmt"

	"github.com/nuclio/logger"
	v3io "github.com/v3io/v3io-go/pkg/dataplane"
)

type FileContentLineIterator struct {
	fileContentIterator *FileContentIterator
	currentLines        [][]byte
	currentRow          int
	err                 error
}

func NewFileContentLineIterator(path string, bytesStep int, container v3io.Container, logger logger.Logger) (*FileContentLineIterator, error) {

	contentIter, err := NewFileContentIterator(path, bytesStep, container, logger)
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
