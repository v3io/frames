package main

import (
	"fmt"
	"github.com/ghodss/yaml"
	"github.com/pkg/errors"
	"github.com/v3io/frames/pkg/backends/kv"
	"github.com/v3io/frames/pkg/common"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"io/ioutil"
)

func main() {

	data, err := ioutil.ReadFile("v3io.yaml")
	if err != nil {
		panic(err)
	}

	cfg := v3ioConfig{}
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

	iter, err := backend.ReadRequest(&common.DataRequest{
		Table:        "sandp500/0/",
		Columns:      []string{"__name", "__size"},
		Filter:       "",
		ShardingKeys: []string{},
	})

	if err != nil {
		panic(err)
	}

	for iter.Next() {
		fmt.Println(iter.At())
	}

	if iter.Err() != nil {
		panic(iter.Err())
	}

}

type v3ioConfig struct {
	// V3IO Connection details: Url, Data container, relative path for this dataset, credentials
	V3ioUrl   string `json:"v3ioUrl"`
	Container string `json:"container"`
	Path      string `json:"path"`
	Username  string `json:"username"`
	Password  string `json:"password"`

	// Set logging level: debug | info | warn | error (info by default)
	Verbose string `json:"verbose,omitempty"`
	// Number of parallel V3IO worker routines
	Workers      int `json:"workers"`
	DefaultLimit int `json:"limit,omitempty"`
}

func NewContext(cfg v3ioConfig) (*common.DataContext, error) {
	logger, _ := utils.NewLogger(cfg.Verbose)
	container, err := utils.CreateContainer(
		logger, cfg.V3ioUrl, cfg.Container, cfg.Username, cfg.Password, cfg.Workers)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create data container")
	}

	return &common.DataContext{Container: container, Logger: logger}, nil
}
