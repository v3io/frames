package test

import (
	"fmt"
	"github.com/stretchr/testify/suite"
	"github.com/v3io/frames"
	"math/rand"
	"testing"
	"time"
)

type SuiteCreateFunc = func(frames.Client) suite.TestingSuite

func floatCol(t testing.TB, name string, size int) frames.Column {
	random := rand.New(rand.NewSource(time.Now().Unix()))
	floats := make([]float64, size)
	for i := range floats {
		floats[i] = random.Float64()
	}

	col, err := frames.NewSliceColumn(name, floats)
	if err != nil {
		t.Fatal(err)
	}

	return col
}

func stringCol(t testing.TB, name string, size int) frames.Column {
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
