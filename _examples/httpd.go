package main

import (
	//	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	//	"os"
	"time"

	"github.com/v3io/frames/pkg/common"

	"github.com/BurntSushi/toml"
	"github.com/v3io/frames/pkg/server"
)

func main() {
	data, err := ioutil.ReadFile("config.toml")
	if err != nil {
		log.Fatalf("error: can't open config - %s", err)
	}

	cfg := &common.V3ioConfig{}
	if err := toml.Unmarshal(data, cfg); err != nil {
		log.Fatalf("error: can't read config - %s", err)
	}

	srv, err := server.New(cfg, ":8080")
	if err := toml.Unmarshal(data, cfg); err != nil {
		log.Fatalf("error: can't create server - %s", err)
	}

	if err = srv.Start(); err != nil {
		log.Fatalf("error: can't start server - %s", err)
	}

	/*
		fmt.Println("Server running (hit Enter to quit)")
		rdr := bufio.NewReader(os.Stdin)
		rdr.ReadString('\n')
	*/
	for {
		time.Sleep(60 * time.Second)
	}

	fmt.Println("Shutting down")
	if err = srv.Stop(nil); err != nil {
		log.Fatalf("error: can't stop server - %s", err)
	}
}
