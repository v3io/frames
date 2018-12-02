package frames_test

import (
	"testing"
	"time"

	"github.com/v3io/frames"
	"github.com/v3io/frames/grpc"
	"github.com/v3io/frames/http"
)

var (
	rreq = &frames.ReadRequest{
		Backend: "csv",
		Table:   "weather.csv",
	}
	wreq = &frames.WriteRequest{
		Backend: "csv",
		Table:   "write-bench.csv",
	}
	wRows = 1982
)

func read(c frames.Client, b *testing.B) {
	it, err := c.Read(rreq)
	if err != nil {
		b.Fatal(err)
	}

	for it.Next() {
		frame := it.At()
		if frame.Len() == 0 {
			b.Fatal("empty frame")
		}
	}

	if err := it.Err(); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkRead_gRPC(b *testing.B) {
	b.StopTimer()
	info := setupTest(b)
	defer info.process.Kill()
	c, err := grpc.NewClient(info.grpcAddr, nil, nil)
	if err != nil {
		b.Fatal(err)
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		read(c, b)
	}
}

func BenchmarkRead_HTTP(b *testing.B) {
	b.StopTimer()
	info := setupTest(b)
	defer info.process.Kill()
	c, err := http.NewClient(info.httpAddr, nil, nil)
	if err != nil {
		b.Fatal(err)
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		read(c, b)
	}
}

func write(c frames.Client, req *frames.WriteRequest, frame frames.Frame, b *testing.B) {
	fa, err := c.Write(req)
	if err != nil {
		b.Fatal(err)
	}

	if err := fa.Add(frame); err != nil {
		b.Fatal(err)
	}

	if err := fa.WaitForComplete(time.Second); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkWrite_gRPC(b *testing.B) {
	b.StopTimer()
	info := setupTest(b)
	defer info.process.Kill()
	c, err := grpc.NewClient(info.grpcAddr, nil, nil)
	if err != nil {
		b.Fatal(err)
	}

	frame := csvFrame(b, wRows)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		write(c, wreq, frame, b)
	}
}

func BenchmarkWrite_HTTP(b *testing.B) {
	b.StopTimer()
	info := setupTest(b)
	defer info.process.Kill()
	c, err := http.NewClient(info.httpAddr, nil, nil)
	if err != nil {
		b.Fatal(err)
	}

	frame := csvFrame(b, wRows)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		write(c, wreq, frame, b)
	}
}
