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

package frames_test

import (
	"testing"
	"time"

	"github.com/v3io/frames"
	"github.com/v3io/frames/grpc"
	"github.com/v3io/frames/http"
	"github.com/v3io/frames/pb"
)

var (
	rreq = &frames.ReadRequest{Proto: &pb.ReadRequest{
		Backend: "csv",
		Table:   "weather.csv",
	}}
	wreq = &frames.WriteRequest{
		Backend: "csv",
		Table:   "write-bench.csv",
	}
	wRows = 1982
)

// FIXME: We're measuring the speed of CSV parsing here as well
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
