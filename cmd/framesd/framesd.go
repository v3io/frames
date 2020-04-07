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
	"net/http"
	_ "net/http/pprof"
	"os"
	"path"
	"time"

	"github.com/ghodss/yaml"
	"github.com/v3io/frames"
	"github.com/v3io/frames/backends/utils"
	"github.com/v3io/frames/grpc"
	framesHttp "github.com/v3io/frames/http"
)

var (
	// Version is framesd version (populated by the build process)
	Version = "unknown"
)

func main() {
	var config struct {
		file        string
		httpAddr    string
		grpcAddr    string
		showVersion bool
	}

	flag.StringVar(&config.file, "config", "", "path to configuration file (YAML)")
	flag.StringVar(&config.httpAddr, "httpAddr", ":8080", "address to listen on HTTP")
	flag.StringVar(&config.grpcAddr, "grpcAddr", ":8081", "address to listen on gRPC")
	flag.BoolVar(&config.showVersion, "version", false, "show version and exit")
	flag.Parse()

	log.SetFlags(0) // Show only messages

	if config.showVersion {
		fmt.Printf("%s version %s\n", path.Base(os.Args[0]), Version)
		return
	}

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

	framesLogger, err := frames.NewLogger(cfg.Log.Level)
	if err != nil {
		log.Fatalf("error: can't create logger - %s", err)
	}
	monitoring := utils.NewMonitoring(framesLogger)

	hsrv, err := framesHttp.NewServer(cfg, config.httpAddr, framesLogger, monitoring)
	if err != nil {
		log.Fatalf("error: can't create HTTP server - %s", err)
	}

	if err := hsrv.Start(); err != nil {
		log.Fatalf("error: can't start HTTP server - %s", err)
	}

	gsrv, err := grpc.NewServer(cfg, config.grpcAddr, framesLogger, monitoring)
	if err != nil {
		log.Fatalf("error: can't create gRPC server - %s", err)
	}

	if err := gsrv.Start(); err != nil {
		log.Fatalf("error: can't start gRPC server - %s", err)
	}

	fmt.Printf("server running on http=%s, grpc=%s\n", config.httpAddr, config.grpcAddr)

	// Start profiling endpoint
	if !cfg.DisableProfiling {
		go func() {
			hasLog := framesLogger != nil

			if hasLog {
				framesLogger.Info("creating profiling endpoint at :8082. To start profiling run one of the following commands:\n" +
					" -> profiling: curl -o cpu.pprof localhost:8082/debug/pprof/profile?seconds=30\n" +
					" -> trace: curl -o trace.pprof localhost:8082/debug/pprof/trace?seconds=30")
			}

			err := http.ListenAndServe(":8082", nil)
			if err != nil && hasLog {
				framesLogger.Warn("failed to create profiling endpoint, err: %v", err)
			}
		}()
	}

	for hsrv.State() == frames.RunningState && gsrv.State() == frames.RunningState {
		time.Sleep(time.Second)
	}

	if err := hsrv.Err(); err != nil {
		log.Fatalf("error: HTTP server error - %s", err)
	}

	if err := gsrv.Err(); err != nil {
		log.Fatalf("error: gRPC server error - %s", err)
	}

	fmt.Println("server down")
}
