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
	"testing"
	"time"

	"github.com/ghodss/yaml"
	"github.com/v3io/frames"
	"github.com/v3io/frames/pb"
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
	create  func(*pb.CreateRequest) // if not defined, create won't be called
	write   func(*frames.WriteRequest)
	read    func(*pb.ReadRequest)
	del     func(*pb.DeleteRequest) // can't use "delete", it's a keyword
	exec    func(*pb.ExecRequest)   // if not defined, exec won't be called
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

func stringCol(t testing.TB, name string, size int) frames.Column {
	strings := make([]string, size)
	for i := range strings {
		strings[i] = fmt.Sprintf("val-%d", i)
	}
	col, err := frames.NewSliceColumn(name, strings)
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

	col = stringCol(t, "strings", size)
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

func init() {
	random = rand.New(rand.NewSource(time.Now().Unix()))
}
