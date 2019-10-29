package test

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"reflect"
	"testing"
	"time"

	"github.com/ghodss/yaml"
	"github.com/nuclio/logger"
	"github.com/stretchr/testify/suite"
	"github.com/v3io/frames"
	"github.com/v3io/frames/grpc"
	"github.com/v3io/frames/http"
)

const (
	configFile = "config.yaml"
)

var (
	kvSuites     = []SuiteCreateFunc{GetKvTestsConstructorFunc()}
	tsdbSuites   = []SuiteCreateFunc{GetTsdbTestsConstructorFunc()}
	streamSuites = []SuiteCreateFunc{GetStreamTestsConstructorFunc()}
	csvSuites    = []SuiteCreateFunc{GetCsvTestsConstructorFunc()}
)

type testInfo struct {
	config   *frames.Config
	grpcAddr string
	httpAddr string
	process  *os.Process
	root     string
	session  *frames.Session
}

type mainTestSuite struct {
	suite.Suite
	info   *testInfo
	logger logger.Logger
}

func (mainSuite *mainTestSuite) newGrpcClient() frames.Client {
	client, err := grpc.NewClient(mainSuite.info.grpcAddr, mainSuite.info.session, mainSuite.logger)
	if err != nil {
		mainSuite.T().Fatalf("could not craete grpc client, err: %v", err)
	}
	return client
}

func (mainSuite *mainTestSuite) newHttpClient() frames.Client {
	client, err := http.NewClient(mainSuite.info.httpAddr, mainSuite.info.session, mainSuite.logger)
	if err != nil {
		mainSuite.T().Fatalf("could not craete http client, err: %v", err)
	}
	return client
}

func (mainSuite *mainTestSuite) SetupSuite() {
	mainSuite.info = setupTest(mainSuite.T())
}

func (mainSuite *mainTestSuite) TearDownSuite() {
	mainSuite.info.process.Kill()
}

func (mainSuite *mainTestSuite) TestKVBackend() {
	mainSuite.runSubSuites(kvSuites)
}

func (mainSuite *mainTestSuite) TestTSDBBackend() {
	mainSuite.runSubSuites(tsdbSuites)
}

func (mainSuite *mainTestSuite) TestStreamBackend() {
	mainSuite.runSubSuites(streamSuites)
}

func (mainSuite *mainTestSuite) TestCSVBackend() {
	mainSuite.runSubSuites(csvSuites)
}

func (mainSuite *mainTestSuite) runSubSuites(suites []SuiteCreateFunc) {
	for _, currSuite := range suites {
		// Run both Grpc and Http tests
		grpcTestSuite := currSuite(mainSuite.newGrpcClient())
		//httpTestSuite := currSuite(mainSuite.newHttpClient())
		currentTestSuiteName := reflect.TypeOf(grpcTestSuite).Elem().Name()

		mainSuite.Run(fmt.Sprintf("%v/grpc_client", currentTestSuiteName),
			func() { suite.Run(mainSuite.T(), grpcTestSuite) })
		//mainSuite.Run(fmt.Sprintf("%v/http_client", currentTestSuiteName),
		//	func() { suite.Run(mainSuite.T(), httpTestSuite) })
	}
}

func setupRoot(t testing.TB) string {
	root, err := ioutil.TempDir("", "frames-e2e")
	if err != nil {
		t.Fatal(err)
	}

	csvFile := "weather.csv"
	src := fmt.Sprintf("../testdata/%s", csvFile)
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

func generateConfig(root string, session *frames.Session) *frames.Config {
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

func runServer(t testing.TB, root string, grpcPort int, httpPort int) *exec.Cmd {
	exePath := fmt.Sprintf("%s/framesd", root)
	cmd := exec.Command(
		"go", "build",
		"-o", exePath,
		"../cmd/framesd/framesd.go",
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

func setupTest(t testing.TB) *testInfo {
	info := &testInfo{}
	info.root = setupRoot(t)
	t.Logf("root: %s", info.root)
	info.session = sessionInfo(t)
	t.Logf("session: %+v", info.session)
	info.config = generateConfig(info.root, info.session)
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

func TestFrames(t *testing.T) {
	logger, err := frames.NewLogger("integration-test")
	if err != nil {
		t.Fatal(err)
	}
	suite.Run(t, &mainTestSuite{logger: logger})
}
