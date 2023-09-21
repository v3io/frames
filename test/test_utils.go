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
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/nuclio/logger"
	"github.com/stretchr/testify/suite"
	"github.com/v3io/frames"
	"github.com/v3io/frames/pb"
	v3io "github.com/v3io/v3io-go/pkg/dataplane"
)

type SuiteCreateFunc = func(frames.Client, v3io.Container, logger.Logger) suite.TestingSuite

func FloatCol(t testing.TB, name string, size int) frames.Column {
	floats := make([]float64, size)
	for i := range floats {
		floats[i] = float64(i)
	}

	col, err := frames.NewSliceColumn(name, floats)
	if err != nil {
		t.Fatal(err)
	}

	return col
}

func IntCol(t testing.TB, name string, size int) frames.Column {
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	floats := make([]int64, size)
	for i := range floats {
		floats[i] = random.Int63()
	}

	col, err := frames.NewSliceColumn(name, floats)
	if err != nil {
		t.Fatal(err)
	}

	return col
}

func StringCol(t testing.TB, name string, size int) frames.Column {
	strings := make([]string, size)
	for i := range strings {
		strings[i] = fmt.Sprintf("val-%d", i)
	}

	col, err := frames.NewSliceColumn(name, strings)
	if err != nil {
		t.Fatal(err)
	}
	return col
}

func BoolCol(t testing.TB, name string, size int) frames.Column {
	bools := make([]bool, size)
	for i := range bools {
		bools[i] = true
	}

	col, err := frames.NewSliceColumn(name, bools)
	if err != nil {
		t.Fatal(err)
	}
	return col
}

func TimeCol(t testing.TB, name string, size int) frames.Column {
	times := make([]time.Time, size)
	now := time.Now()
	for i := range times {
		times[i] = now.Add(time.Duration(i) * time.Hour)
	}

	col, err := frames.NewSliceColumn(name, times)
	if err != nil {
		t.Fatal(err)
	}
	return col
}

func initializeNullColumns(length int) []*pb.NullValuesMap {
	nullValues := make([]*pb.NullValuesMap, length)
	for i := 0; i < length; i++ {
		nullValues[i] = &pb.NullValuesMap{NullColumns: make(map[string]bool)}
	}
	return nullValues
}

func validateFramesAreEqual(s suite.Suite, frame1, frame2 frames.Frame) {
	// Check length
	s.Require().Equal(frame1.Len(), frame2.Len(), "frames length is different")

	// Check Indices
	frame1IndicesCount, frame2IndicesCount := len(frame1.Indices()), len(frame2.Indices())
	s.Require().Equal(frame1IndicesCount, frame2IndicesCount, "frames indices length is different")
	frame1IndicesNames, frame2IndicesNames := make([]string, frame1IndicesCount), make([]string, frame2IndicesCount)
	for i := 0; i < frame1IndicesCount; i++ {
		frame1IndicesNames[i] = frame1.Indices()[i].Name()
		frame2IndicesNames[i] = frame2.Indices()[i].Name()
	}
	s.Require().EqualValues(frame1IndicesNames, frame2IndicesNames, "frames index names are different")

	// Check columns
	s.Require().EqualValues(frame1.Names(), frame2.Names(), "frames column names are different")
	frame1Data := iteratorToSlice(frame1.IterRows(true))
	frame2Data := iteratorToSlice(frame2.IterRows(true))

	s.Require().True(compareMapSlice(frame1Data, frame2Data),
		"frames values mismatch, frame1: %v \n, frame2: %v", frame1Data, frame2Data)
}

func iteratorToSlice(iter frames.RowIterator) []map[string]interface{} {
	var response []map[string]interface{}
	for iter.Next() {
		response = append(response, iter.Row())
	}
	return response
}

func FrameToDataMap(frame frames.Frame) map[string]map[string]interface{} {
	iter := frame.IterRows(true)
	keyColumnName := frame.Indices()[0].Name()

	response := make(map[string]map[string]interface{})
	for iter.Next() {
		currentKey := fmt.Sprintf("%v", iter.Row()[keyColumnName])
		response[currentKey] = iter.Row()
	}

	return response
}

func compareMapSlice(a, b []map[string]interface{}) bool {
	if len(a) != len(b) {
		return false
	}

	for _, currentMapA := range a {
		foundMap := false
		for _, currentMapB := range b {
			if reflect.DeepEqual(currentMapA, currentMapB) {
				foundMap = true
				break
			}
		}

		if !foundMap {
			return false
		}
	}

	return true
}
