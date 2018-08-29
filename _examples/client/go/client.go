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

	"github.com/v3io/frames"
)

func main() {
	tableName := ""
	flag.StringVar(&tableName, "table", "", "table name (e.g. weather.csv)")
	flag.Parse()

	if tableName == "" {
		log.Fatal("no table name")
	}

	url := "http://localhost:8080"
	apiKey := "t0ps3cr3t"
	client, err := frames.NewClient(url, apiKey, nil)
	if err != nil {
		log.Fatalf("can't connect to %q - %s", url, err)
	}

	req := &frames.ReadRequest{
		Backend:      "weather",
		Table:        tableName,
		MaxInMessage: 1000,
	}

	it, err := client.Read(req)
	if err != nil {
		log.Fatalf("can't query - %s", err)
	}

	for it.Next() {
		frame := it.At()
		fmt.Println(frame.Columns())
		fmt.Printf("%d rows\n", frame.Len())
		fmt.Println("-----------")
	}

	if err := it.Err(); err != nil {
		log.Fatalf("error in iterator - %s", err)
	}
}
