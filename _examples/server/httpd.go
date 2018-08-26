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
	"io/ioutil"
	"log"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/v3io/frames"
	"github.com/v3io/frames/server"
)

func main() {
	var port int
	flag.IntVar(&port, "port", 8080, "port to listen on")
	flag.Parse()

	data, err := ioutil.ReadFile("config.toml")
	if err != nil {
		log.Fatalf("error: can't open config - %s", err)
	}

	cfg := &frames.V3ioConfig{}
	if err := toml.Unmarshal(data, cfg); err != nil {
		log.Fatalf("error: can't read config - %s", err)
	}

	srv, err := server.New(cfg, fmt.Sprintf(":%d", port))
	if err := toml.Unmarshal(data, cfg); err != nil {
		log.Fatalf("error: can't create server - %s", err)
	}

	if err = srv.Start(); err != nil {
		log.Fatalf("error: can't start server - %s", err)
	}

	fmt.Println("Server running")
	for {
		time.Sleep(60 * time.Second)
	}

}
