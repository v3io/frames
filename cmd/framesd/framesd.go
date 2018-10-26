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
	"github.com/v3io/frames/server"
	"io/ioutil"
)

func main() {
	var configFile string
	var addr string

	flag.StringVar(&configFile, "config", "", "path to configuration file (YAML)")
	flag.StringVar(&addr, "addr", ":8080", "address to listen on")
	flag.Parse()

	log.SetFlags(0) // Show only messages

	if configFile == "" {
		log.Fatal("error: no config file given")
	}

	data, err := ioutil.ReadFile(configFile)
	if err != nil {
		log.Fatalf("error: can't read config - %s", err)
	}

	cfg := &frames.Config{}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		log.Fatalf("error: can't unmarshal config - %s", err)
	}

	frames.DefaultLogLevel = cfg.Log.Level

	srv, err := server.New(cfg, addr, nil)
	if err = srv.Start(); err != nil {
		log.Fatalf("error: can't start server - %s", err)
	}

	fmt.Println("server running")
	for {
		time.Sleep(60 * time.Second)
	}
}
