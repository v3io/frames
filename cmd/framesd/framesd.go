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
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/ghodss/yaml"
	"github.com/v3io/frames"
	"github.com/v3io/frames/grpc"
	"github.com/v3io/frames/http"
	"io/ioutil"
)

func main() {
	var config struct {
		file     string
		addr     string
		protocol string // grpc or http
	}

	flag.StringVar(&config.file, "config", "", "path to configuration file (YAML)")
	flag.StringVar(&config.addr, "addr", ":8080", "address to listen on")
	flag.StringVar(&config.protocol, "proto", "grpc", "Server protocol (grpc or http)")
	flag.Parse()

	log.SetFlags(0) // Show only messages

	if config.file == "" {
		log.Fatal("error: no config file given")
	}

	data, err := ioutil.ReadFile(config.file)
	if err != nil {
		log.Fatalf("error: can't read config - %s", err)
	}

	cfg := &frames.Config{}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		log.Fatalf("error: can't unmarshal config - %s", err)
	}

	frames.DefaultLogLevel = cfg.Log.Level

	var srv frames.Server
	switch config.protocol {
	case "http":
		srv, err = http.NewServer(cfg, config.addr, nil)
	case "grpc":
		srv, err = grpc.NewServer(cfg, config.addr, nil)
	}

	if err != nil {
		log.Fatalf("error: can't create server - %s", err)
	}

	if err = srv.Start(); err != nil {
		log.Fatalf("error: can't start server - %s", err)
	}

	fmt.Println("server running")
	for srv.State() == frames.RunningState {
		time.Sleep(time.Second)
	}

	if err := srv.Err(); err != nil {
		log.Fatalf("error: server error - %s", err)
	}
}
