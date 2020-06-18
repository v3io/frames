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
	v3io "github.com/v3io/v3io-go/pkg/dataplane"
)

type FileCursor interface {
	Err() error
	Next() bool
	GetFilePath() string
}

type FilesCursor struct {
	input     *v3io.GetContainerContentsInput
	container v3io.Container

	currentFile     v3io.Content
	currentError    error
	itemIndex       int
	currentResponse *v3io.GetContainerContentsOutput
}

func NewFilesCursor(container v3io.Container, input *v3io.GetContainerContentsInput) (FileCursor, error) {

	newFilesIterator := &FilesCursor{
		container: container,
		input:     input,
	}

	res, err := container.GetContainerContentsSync(input)
	if err != nil {
		return nil, err
	}

	newFilesIterator.currentResponse = res.Output.(*v3io.GetContainerContentsOutput)
	return newFilesIterator, nil
}

// error returns the last error
func (ic *FilesCursor) Err() error {
	return ic.currentError
}

// Next gets the next matching item. this may potentially block as this lazy loads items from the collection
func (ic *FilesCursor) Next() bool {
	if ic.itemIndex >= len(ic.currentResponse.Contents) && ic.currentResponse.NextMarker == "" {
		return false
	}

	if ic.itemIndex < len(ic.currentResponse.Contents) {
		ic.currentFile = ic.currentResponse.Contents[ic.itemIndex]
		ic.itemIndex++
	} else {
		newInput := &v3io.GetContainerContentsInput{Path: ic.input.Path, Marker: ic.currentResponse.NextMarker}
		res, err := ic.container.GetContainerContentsSync(newInput)
		if err != nil {
			ic.currentError = err
			return false
		}

		ic.currentResponse = res.Output.(*v3io.GetContainerContentsOutput)
		ic.itemIndex = 0
		return ic.Next()
	}

	return true
}

func (ic *FilesCursor) GetFilePath() string {
	return ic.currentFile.Key
}
