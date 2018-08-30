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
	"log"
	"time"

	"github.com/v3io/frames"
)

func main() {
	tableName := fmt.Sprintf("%s-data.csv", time.Now().Format("2006-01-02T15:04:05"))

	url := "http://localhost:8080"
	apiKey := "t0ps3cr3t"
	fmt.Println(">>> connecting")
	client, err := frames.NewClient(url, apiKey, nil)
	if err != nil {
		log.Fatalf("can't connect to %q - %s", url, err)
	}

	frame, err := makeFrame()
	if err != nil {
		log.Fatalf("can't create frame - %s", err)
	}

	fmt.Println(">>> writing")
	writeReq := &frames.WriteRequest{
		Backend: "weather",
		Table:   tableName,
	}

	appender, err := client.Write(writeReq)
	if err := appender.Add(frame); err != nil {
		log.Fatalf("can't write frame - %s", err)
	}

	if err := appender.WaitForComplete(10 * time.Second); err != nil {
		log.Fatalf("can't wait - %s", err)
	}

	fmt.Println(">>> reading")
	readReq := &frames.ReadRequest{
		Backend:      "weather",
		Table:        tableName,
		MaxInMessage: 100,
	}

	it, err := client.Read(readReq)
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

func makeFrame() (frames.Frame, error) {
	size := 1027
	now := time.Now()
	idata := make([]int, size)
	fdata := make([]float64, size)
	sdata := make([]string, size)
	tdata := make([]time.Time, size)

	for i := 0; i < size; i++ {
		idata[i] = i
		fdata[i] = float64(i)
		sdata[i] = fmt.Sprintf("val%d", i)
		tdata[i] = now.Add(time.Duration(i) * time.Second)
	}

	columns := map[string]interface{}{
		"ints":    idata,
		"floats":  fdata,
		"strings": sdata,
		"times":   tdata,
	}
	return frames.NewMapFrameFromMap(columns)
}
