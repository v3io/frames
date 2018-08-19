package main

import (
	"fmt"
	"github.com/ghodss/yaml"
	"github.com/pkg/errors"
	"github.com/v3io/frames/pkg/backends/kv"
	"github.com/v3io/frames/pkg/common"
	"github.com/v3io/frames/pkg/utils"
	"io/ioutil"
)

func main() {

	data, err := ioutil.ReadFile("v3io.yaml")
	if err != nil {
		panic(err)
	}

	cfg := common.V3ioConfig{}
	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		panic(err)
	}

	ctx, err := NewContext(cfg)
	if err != nil {
		panic(err)
	}

	backend, err := kv.NewKVBackend(ctx)
	if err != nil {
		panic(err)
	}

	if false {
		readExample(backend)
	} else {
		writeExample(backend)
	}
}

func readExample(backend common.DataBackend) error {
	iter, err := backend.ReadRequest(&common.DataReadRequest{
		Table:     "test",
		Columns:   []string{"__name", "__size", "user", "age", "city"},
		Filter:    "",
		RowLayout: false,
	})

	if err != nil {
		return err
	}

	for iter.Next() {
		fmt.Println(iter.At().Columns)
	}

	return iter.Err()
}

func writeExample(backend common.DataBackend) error {

	rows := []map[string]interface{}{
		{"user": "joe", "age": 5, "city": "tel-aviv"},
		{"user": "ben", "age": 7, "city": "bon"},
		{"user": "amit", "age": 12},
		{"user": "kim", "age": 23, "city": "london"},
	}

	columns := map[string][]interface{}{}
	for i, row := range rows {
		kv.Rows2Col(&columns, &row, i)
	}
	fmt.Println(columns)

	appender, err := backend.WriteRequest(&common.DataWriteRequest{Table: "test"})
	if err != nil {
		return err
	}

	err = appender.Add(&common.Message{
		IndexCol: "user",
		Columns:  columns,
	})

	if err != nil {
		return err
	}

	return appender.WaitForComplete(0)
}

func NewContext(cfg common.V3ioConfig) (*common.DataContext, error) {
	logger, _ := utils.NewLogger(cfg.Verbose)
	container, err := utils.CreateContainer(
		logger, cfg.V3ioUrl, cfg.Container, cfg.Username, cfg.Password, cfg.Workers)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create data container")
	}

	return &common.DataContext{Container: container, Logger: logger}, nil
}
