package main

import (
	"fmt"
	"log"
	"time"

	"github.com/v3io/frames"
)

func main() {
	tableName := time.Now().Format("20060102T150405")
	size := 10000
	fmt.Printf("size = %d\n", size)

	url := "http://localhost:8080"
	apiKey := "t0ps3cr3t"
	backendName := "v3io"

	fmt.Println(">>> connecting")
	client, err := frames.NewClient(url, apiKey, nil)
	if err != nil {
		log.Fatalf("can't connect to %q - %s", url, err)
	}

	frame, err := makeFrame(size)
	if err != nil {
		log.Fatalf("can't create frame - %s", err)
	}

	fmt.Println(">>> writing")
	writeReq := &frames.WriteRequest{
		Backend: backendName,
		Table:   tableName,
	}

	writeStart := time.Now()
	appender, err := client.Write(writeReq)
	if err := appender.Add(frame); err != nil {
		log.Fatalf("can't write frame - %s", err)
	}

	fmt.Println(time.Now().Sub(writeStart))
	fmt.Println(">>> waiting")
	if err := appender.WaitForComplete(time.Minute); err != nil {
		log.Fatalf("can't wait - %s", err)
	}
	fmt.Println(time.Now().Sub(writeStart))

	readStart := time.Now()
	fmt.Println(">>> reading")
	readReq := &frames.ReadRequest{
		Backend:      backendName,
		Table:        tableName,
		MaxInMessage: 100,
	}

	it, err := client.Read(readReq)
	if err != nil {
		log.Fatalf("can't query - %s", err)
	}

	for it.Next() {
		frame := it.At()
		fmt.Println(frame.Names())
		fmt.Printf("%d rows\n", frame.Len())
		fmt.Println("-----------")
	}
	fmt.Println(time.Now().Sub(readStart))

	if err := it.Err(); err != nil {
		log.Fatalf("error in iterator - %s", err)
	}
}

func makeFrame(size int) (frames.Frame, error) {
	// now := time.Now()
	idata := make([]int, size)
	fdata := make([]float64, size)
	sdata := make([]string, size)
	// tdata := make([]time.Time, size)

	for i := 0; i < size; i++ {
		idata[i] = i
		fdata[i] = float64(i)
		sdata[i] = fmt.Sprintf("val%d", i)
		// 	tdata[i] = now.Add(time.Duration(i) * time.Second)
	}

	columns := map[string]interface{}{
		"ints":    idata,
		"floats":  fdata,
		"strings": sdata,
		//	"times":   tdata,
	}
	return frames.NewFrameFromMap(columns)
}
