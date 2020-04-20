package test

import (
	"github.com/v3io/frames/v3ioutils"
	v3io "github.com/v3io/v3io-go/pkg/dataplane"
	"testing"
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
	iter, err := v3ioutils.NewFileContentIterator(path, 2*1024*1024, container)

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
