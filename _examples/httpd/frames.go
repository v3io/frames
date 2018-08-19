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
