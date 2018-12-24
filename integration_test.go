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
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/ghodss/yaml"

	"github.com/v3io/frames"
	"github.com/v3io/frames/grpc"
	"github.com/v3io/frames/http"
)

const (
	configFile = "config.yaml"
	frameSize  = 293
)

var (
	random      *rand.Rand
	testsConfig map[string]*testConfig
)

type frameFn func(t testing.TB, size int) frames.Frame
type testConfig struct {
	frameFn frameFn
	create  func(*frames.CreateRequest) // if not defined, create won't be called
	write   func(*frames.WriteRequest)
	read    func(*frames.ReadRequest)
	del     func(*frames.DeleteRequest) // can't use "delete", it's a keyword
	exec    func(*frames.ExecRequest)   // if not defined, exec won't be called
}

func setupRoot(t testing.TB) string {
	root, err := ioutil.TempDir("", "frames-e2e")
	if err != nil {
		t.Fatal(err)
	}

	csvFile := "weather.csv"
	src := fmt.Sprintf("./testdata/%s", csvFile)
	dest := fmt.Sprintf("%s/%s", root, csvFile)

	in, err := os.Open(src)
	if err != nil {
		t.Fatal(err)
	}

	out, err := os.Create(dest)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := io.Copy(out, in); err != nil {
		t.Fatal(err)
	}

	return root
}

func sessionInfo(t testing.TB) *frames.Session {
	data := os.Getenv("V3IO_SESSION")
	if data == "" {
		return nil
	}

	var s struct {
		Address string
		frames.Session
	}
	if err := json.Unmarshal([]byte(data), &s); err != nil {
		t.Fatal(err)
	}

	if s.Address != "" {
		s.Session.Url = s.Address
	}

	return &s.Session
}

func genConfig(root string, session *frames.Session) *frames.Config {
	backends := []*frames.BackendConfig{
		{
			Type:    "csv",
			RootDir: root,
		},
	}

	if session != nil {
		backends = append(backends, &frames.BackendConfig{
			Type: "kv",
		})
		backends = append(backends, &frames.BackendConfig{
			Type: "stream",
		})
		backends = append(backends, &frames.BackendConfig{
			Type:    "tsdb",
			Workers: 16,
		})
	}

	return &frames.Config{
		Log: frames.LogConfig{
			Level: "debug",
		},
		Backends: backends,
	}
}

func encodeConfig(t testing.TB, config *frames.Config, path string) {
	data, err := yaml.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}

	if err := ioutil.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func freePort(t testing.TB) int {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}

	l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func waitForServer(t testing.TB, port int) {
	address := fmt.Sprintf("localhost:%d", port)
	timeout := 30 * time.Second

	for start := time.Now(); time.Now().Sub(start) < timeout; {
		conn, err := net.Dial("tcp", address)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("server not up after %v", timeout)
}

func runServer(t testing.TB, root string, grpcPort int, httpPort int) *exec.Cmd {
	exePath := fmt.Sprintf("%s/framesd", root)
	cmd := exec.Command(
		"go", "build",
		"-mod=vendor",
		"-o", exePath,
		"cmd/framesd/framesd.go",
	)
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}

	logFile, err := os.Create(fmt.Sprintf("%s/framesd.log", root))
	if err != nil {
		t.Fatal(err)
	}

	cmd = exec.Command(
		exePath,
		"-grpcAddr", fmt.Sprintf(":%d", grpcPort),
		"-httpAddr", fmt.Sprintf(":%d", httpPort),
		"-config", fmt.Sprintf("%s/%s", root, configFile),
	)
	cmd.Stderr = logFile
	cmd.Stdout = logFile
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	waitForServer(t, httpPort)
	return cmd
}

func floatCol(t testing.TB, name string, size int) frames.Column {
	floats := make([]float64, size)
	for i := range floats {
		floats[i] = random.Float64()
	}

	col, err := frames.NewSliceColumn(name, floats)
	if err != nil {
		t.Fatal(err)
	}

	return col
}

func csvFrame(t testing.TB, size int) frames.Frame {
	var (
		columns []frames.Column
		col     frames.Column
		err     error
	)

	bools := make([]bool, size)
	for i := range bools {
		if random.Float64() < 0.5 {
			bools[i] = true
		}
	}
	col, err = frames.NewSliceColumn("bools", bools)
	if err != nil {
		t.Fatal(err)
	}
	columns = append(columns, col)

	col = floatCol(t, "floats", size)
	columns = append(columns, col)

	ints := make([]int64, size)
	for i := range ints {
		ints[i] = random.Int63()
	}
	col, err = frames.NewSliceColumn("ints", ints)
	if err != nil {
		t.Fatal(err)
	}
	columns = append(columns, col)

	strings := make([]string, size)
	for i := range strings {
		strings[i] = fmt.Sprintf("val-%d", i)
	}
	col, err = frames.NewSliceColumn("strings", strings)
	if err != nil {
		t.Fatal(err)
	}
	columns = append(columns, col)

	times := make([]time.Time, size)
	for i := range times {
		times[i] = time.Now().Add(time.Duration(i) * time.Second)
	}
	col, err = frames.NewSliceColumn("times", times)
	if err != nil {
		t.Fatal(err)
	}
	columns = append(columns, col)

	frame, err := frames.NewFrame(columns, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	return frame
}

func kvFrame(t testing.TB, size int) frames.Frame {
	var icol frames.Column

	index := []string{"mike", "joe", "jim", "rose", "emily", "dan"}
	icol, err := frames.NewSliceColumn("idx", index)
	if err != nil {
		t.Fatal(err)
	}

	columns := []frames.Column{
		floatCol(t, "n1", len(index)),
		floatCol(t, "n2", len(index)),
		floatCol(t, "n3", len(index)),
	}

	frame, err := frames.NewFrame(columns, []frames.Column{icol}, nil)
	if err != nil {
		t.Fatal(err)
	}

	return frame
}

func streamFrame(t testing.TB, size int) frames.Frame {
	size = 60 // TODO
	times := make([]time.Time, size)
	end := time.Now().Truncate(time.Hour)
	for i := range times {
		times[i] = end.Add(-time.Duration(size-i) * time.Second * 300)
	}

	index, err := frames.NewSliceColumn("idx", times)
	if err != nil {
		t.Fatal(err)
	}

	columns := []frames.Column{
		floatCol(t, "cpu", index.Len()),
		floatCol(t, "mem", index.Len()),
		floatCol(t, "disk", index.Len()),
	}

	frame, err := frames.NewFrame(columns, []frames.Column{index}, nil)
	if err != nil {
		t.Fatal(err)
	}

	return frame
}

func integrationTest(t *testing.T, client frames.Client, backend string) {
	cfg, ok := testsConfig[backend]
	if !ok {
		t.Skipf("no config for %s backend", backend)
	}

	table := fmt.Sprintf("gointegtest%d", time.Now().UnixNano())

	if cfg.create != nil {
		t.Log("create")
		req := &frames.CreateRequest{
			Backend: backend,
			Table:   table,
		}
		cfg.create(req)
		if err := client.Create(req); err != nil {
			t.Fatal(err)
		}
	}

	t.Log("write")
	frame := cfg.frameFn(t, frameSize)
	wreq := &frames.WriteRequest{
		Backend: backend,
		Table:   table,
	}
	if cfg.write != nil {
		cfg.write(wreq)
	}

	appender, err := client.Write(wreq)
	if err != nil {
		t.Fatal(err)
	}

	if err := appender.Add(frame); err != nil {
		t.Fatal(err)
	}

	if err := appender.WaitForComplete(3 * time.Second); err != nil {
		t.Fatal(err)
	}

	time.Sleep(3 * time.Second) // Let DB sync

	t.Log("read")
	rreq := &frames.ReadRequest{
		Backend: backend,
		Table:   table,
	}
	if cfg.read != nil {
		cfg.read(rreq)
	}

	it, err := client.Read(rreq)
	if err != nil {
		t.Fatal(err)
	}

	for it.Next() {
		// TODO: More checks
		fr := it.At()
		switch {
		case strings.Contains(t.Name(), "tsdb"):
			if fr.Len() == 0 {
				t.Fatalf("empty frame")
			}
		case strings.Contains(t.Name(), "kv"):
			// FIXME: kv sometimes return extra "na"
			if !(fr.Len() == frame.Len() || fr.Len()-1 == frame.Len()) {
				t.Fatalf("wrong length: %d != %d", fr.Len(), frame.Len())
			}
		default:
			if fr.Len() != frame.Len() {
				t.Fatalf("wrong length: %d != %d", fr.Len(), frame.Len())
			}
		}
	}

	if err := it.Err(); err != nil {
		t.Fatal(err)
	}

	t.Log("exec")
	if cfg.exec != nil {
		ereq := &frames.ExecRequest{
			Backend: backend,
			Table:   table,
		}
		cfg.exec(ereq)
		if err := client.Exec(ereq); err != nil {
			t.Fatal(err)
		}
	}

	t.Log("delete")
	dreq := &frames.DeleteRequest{
		Backend: backend,
		Table:   table,
	}
	if cfg.del != nil {
		cfg.del(dreq)
	}

	if err := client.Delete(dreq); err != nil {
		t.Fatal(err)
	}

}

type testInfo struct {
	config   *frames.Config
	grpcAddr string
	httpAddr string
	process  *os.Process
	root     string
	session  *frames.Session
}

func setupTest(t testing.TB) *testInfo {
	info := &testInfo{}
	info.root = setupRoot(t)
	t.Logf("root: %s", info.root)
	info.session = sessionInfo(t)
	t.Logf("session: %+v", info.session)
	info.config = genConfig(info.root, info.session)
	t.Logf("config: %+v", info.config)
	configPath := fmt.Sprintf("%s/%s", info.root, configFile)
	encodeConfig(t, info.config, configPath)

	grpcPort, httpPort := freePort(t), freePort(t)
	cmd := runServer(t, info.root, grpcPort, httpPort)
	info.process = cmd.Process

	info.grpcAddr = fmt.Sprintf("localhost:%d", grpcPort)
	info.httpAddr = fmt.Sprintf("http://localhost:%d", httpPort)

	return info
}

func TestIntegration(t *testing.T) {
	info := setupTest(t)
	defer info.process.Kill()

	logger, err := frames.NewLogger("debug")
	if err != nil {
		t.Fatal(err)
	}

	for _, proto := range []string{"grpc", "http"} {
		for _, backend := range info.config.Backends {
			var client frames.Client
			var err error
			if proto == "grpc" {
				client, err = grpc.NewClient(info.grpcAddr, info.session, logger)
			} else {
				client, err = http.NewClient(info.httpAddr, info.session, logger)
			}

			if err != nil {
				t.Fatal(err)
			}

			testName := fmt.Sprintf("%s-%s", backend.Type, proto)
			t.Run(testName, func(t *testing.T) {
				integrationTest(t, client, backend.Type)
			})
		}
	}
}

func init() {
	random = rand.New(rand.NewSource(time.Now().Unix()))
	testsConfig = map[string]*testConfig{
		"csv": &testConfig{
			frameFn: csvFrame,
			exec: func(req *frames.ExecRequest) {
				req.Command = "ping"
			},
		},
		"kv": &testConfig{
			frameFn: kvFrame,
		},
		"stream": &testConfig{
			frameFn: streamFrame,
			create: func(req *frames.CreateRequest) {
				req.SetAttribute("retention_hours", 48)
				req.SetAttribute("shards", 1)
			},
			read: func(req *frames.ReadRequest) {
				req.Seek = "earliest"
				req.ShardId = "0"
			},
		},
		"tsdb": &testConfig{
			frameFn: streamFrame, // We use the same frame as stream
			create: func(req *frames.CreateRequest) {
				req.SetAttribute("rate", "1/m")
			},
			read: func(req *frames.ReadRequest) {
				req.Step = "10m"
				req.Aggragators = "avg,max,count"
				req.Start = "now-5h"
				req.End = "now"
			},
		},
	}
}
