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
package test

import (
	"strings"
	"testing"

	"github.com/v3io/frames"
	"github.com/v3io/frames/v3ioutils"
	v3io "github.com/v3io/v3io-go/pkg/dataplane"
)

func deleteObj(path string, container v3io.Container) {
	_ = container.DeleteObjectSync(&v3io.DeleteObjectInput{Path: path})
}

func TestFileContentIterator(t *testing.T) {
	container := createTestContainer(t)

	path := "/test_file_iterator.txt"
	fileSize := 1024 * 1024 * 3
	expected := make([]byte, fileSize)

	for i := range expected {
		expected[i] = 'a'
	}

	putObjectInput := &v3io.PutObjectInput{
		Path: path,
		Body: []byte(expected),
	}

	err := container.PutObjectSync(putObjectInput)
	if err != nil {
		t.Fatalf("failed to put object, err: %v", err)
	}
	defer deleteObj(path, container)
	logger, _ := frames.NewLogger("")
	iter, err := v3ioutils.NewFileContentIterator(path, 2*1024*1024, container, logger)

	if err != nil {
		t.Fatal(err)
	}

	var actual []byte
	for iter.Next() {
		actual = append(actual, iter.At()...)
	}

	if iter.Error() != nil {
		t.Fatalf("failed to iterate over file, err: %v", iter.Error())
	}

	if string(actual) != string(expected) {
		t.Fatalf("actual does not match expected\n expected: %v\n actual: %v", expected, actual)
	}
}

func TestFileContentLineIterator(t *testing.T) {
	container := createTestContainer(t)

	path := "/test_file_line_iterator.txt"
	lineSize := 10
	expected := make([]string, lineSize)

	for i := 0; i < lineSize; i++ {
		expected[i] = "12345"
	}

	putObjectInput := &v3io.PutObjectInput{
		Path: path,
		Body: []byte(strings.Join(expected, "\n") + "\n"),
	}

	err := container.PutObjectSync(putObjectInput)
	if err != nil {
		t.Fatalf("failed to put object, err: %v", err)
	}
	defer deleteObj(path, container)
	logger, _ := frames.NewLogger("")
	iter, err := v3ioutils.NewFileContentLineIterator(path, 20, container, logger)

	if err != nil {
		t.Fatal(err)
	}

	var i int
	for iter.Next() {
		if string(iter.At()) != expected[i] {
			t.Fatalf("actual does not match expected\n expected: %v\n actual: %v", expected[i], iter.At())
		}
		i++
	}

	if iter.Error() != nil {
		t.Fatalf("failed to iterate over file, err: %v", iter.Error())
	}

	if i != len(expected) {
		t.Fatalf("nunmber of lines do not match, expected: %v, actual: %v", len(expected), i)
	}
}
