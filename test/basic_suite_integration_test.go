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
	"strings"
	"testing"
	"time"

	"github.com/ghodss/yaml"
	"github.com/nuclio/logger"
	"github.com/stretchr/testify/suite"
	"github.com/v3io/frames"
	"github.com/v3io/frames/grpc"
	"github.com/v3io/frames/http"
	"github.com/v3io/frames/v3ioutils"
	v3io "github.com/v3io/v3io-go/pkg/dataplane"
	v3iohttp "github.com/v3io/v3io-go/pkg/dataplane/http"
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
	config         *frames.Config
	grpcAddr       string
	httpAddr       string
	process        *os.Process
	root           string
	session        *frames.Session
	v3ioContainer  v3io.Container
	debugMode      bool
	backendsToTest string
}

type mainTestSuite struct {
	suite.Suite
	info   *testInfo
	logger logger.Logger
}

func (mainSuite *mainTestSuite) newGrpcClient() frames.Client {
	client, err := grpc.NewClient(mainSuite.info.grpcAddr, mainSuite.info.session, mainSuite.logger)
	mainSuite.Require().NoError(err, "could not craete grpc client")
	return client
}

func (mainSuite *mainTestSuite) newHttpClient() frames.Client {
	client, err := http.NewClient(mainSuite.info.httpAddr, mainSuite.info.session, mainSuite.logger)
	mainSuite.Require().NoError(err, "could not craete http client")
	return client
}

func (mainSuite *mainTestSuite) SetupSuite() {
	mainSuite.info = setupTest(mainSuite.T(), mainSuite.logger)
}

func (mainSuite *mainTestSuite) TearDownSuite() {
	if !mainSuite.info.debugMode {
		mainSuite.info.process.Kill()
	}
}

func (mainSuite *mainTestSuite) TestKVBackend() {
	if !strings.Contains(mainSuite.info.backendsToTest, "kv") {
		mainSuite.T().Skip("skipping kv backend tests")
	}
	mainSuite.runSubSuites(kvSuites)
}

func (mainSuite *mainTestSuite) TestTSDBBackend() {
	if !strings.Contains(mainSuite.info.backendsToTest, "tsdb") {
		mainSuite.T().Skip("skipping tsdb backend tests")
	}
	mainSuite.runSubSuites(tsdbSuites)
}

func (mainSuite *mainTestSuite) TestStreamBackend() {
	if !strings.Contains(mainSuite.info.backendsToTest, "stream") {
		mainSuite.T().Skip("skipping stream backend tests")
	}
	mainSuite.runSubSuites(streamSuites)
}

func (mainSuite *mainTestSuite) TestCSVBackend() {
	if !strings.Contains(mainSuite.info.backendsToTest, "csv") {
		mainSuite.T().Skip("skipping csv backend tests")
	}
	mainSuite.runSubSuites(csvSuites)
}

func (mainSuite *mainTestSuite) runSubSuites(suites []SuiteCreateFunc) {
	for _, currSuite := range suites {
		// Run both Grpc and Http tests
		grpcTestSuite := currSuite(mainSuite.newGrpcClient(), mainSuite.info.v3ioContainer, mainSuite.logger)
		httpTestSuite := currSuite(mainSuite.newHttpClient(), mainSuite.info.v3ioContainer, mainSuite.logger)
		currentTestSuiteName := reflect.TypeOf(grpcTestSuite).Elem().Name()

		mainSuite.Run(fmt.Sprintf("%v/grpc_client", currentTestSuiteName),
			func() { suite.Run(mainSuite.T(), grpcTestSuite) })
		mainSuite.Run(fmt.Sprintf("%v/http_client", currentTestSuiteName),
			func() { suite.Run(mainSuite.T(), httpTestSuite) })
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
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
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

func setupTest(t testing.TB, internalLogger logger.Logger) *testInfo {
	info := &testInfo{}
	info.debugMode = strings.ToLower(os.Getenv("DEBUG")) == "true"
	info.backendsToTest = os.Getenv("TEST_BACKENDS")
	if info.backendsToTest == "" {
		info.backendsToTest = "kv,tsdb,stream,csv"
	}

	info.root = setupRoot(t)
	t.Logf("root: %s", info.root)
	info.session = sessionInfo(t)
	t.Logf("session: %+v", info.session)
	info.config = generateConfig(info.root, info.session)
	t.Logf("config: %+v", info.config)
	configPath := fmt.Sprintf("%s/%s", info.root, configFile)
	encodeConfig(t, info.config, configPath)

	grpcPort, httpPort := 8081, 8080
	if !info.debugMode {
		grpcPort, httpPort = freePort(t), freePort(t)
		cmd := runServer(t, info.root, grpcPort, httpPort)
		info.process = cmd.Process
	}

	info.grpcAddr = fmt.Sprintf("localhost:%d", grpcPort)
	info.httpAddr = fmt.Sprintf("http://localhost:%d", httpPort)

	newClient := v3iohttp.NewClient(&v3iohttp.NewClientInput{DialTimeout: 0, MaxConnsPerHost: 100})
	newContextInput := &v3iohttp.NewContextInput{
		HTTPClient:     newClient,
		NumWorkers:     8,
		RequestChanLen: 4096,
	}
	v3ioContext, _ := v3iohttp.NewContext(internalLogger, newContextInput)
	container, _ := v3ioutils.NewContainer(
		v3ioContext,
		info.session,
		info.session.Password,
		info.session.Token,
		internalLogger)
	info.v3ioContainer = container

	return info
}

func TestFrames(t *testing.T) {
	logger, err := frames.NewLogger("integration-test")
	if err != nil {
		t.Fatal(err)
	}
	suite.Run(t, &mainTestSuite{logger: logger})
}

func createTestContainer(t testing.TB) v3io.Container {
	session := sessionInfo(t)
	logger, err := frames.NewLogger("integration-test")
	if err != nil {
		t.Fatal(err)
	}
	newClient := v3iohttp.NewClient(&v3iohttp.NewClientInput{DialTimeout: 0, MaxConnsPerHost: 100})
	newContextInput := &v3iohttp.NewContextInput{
		HTTPClient:     newClient,
		NumWorkers:     8,
		RequestChanLen: 4096,
	}
	v3ioContext, _ := v3iohttp.NewContext(logger, newContextInput)
	container, _ := v3ioutils.NewContainer(
		v3ioContext,
		session,
		session.Password,
		session.Token,
		logger)

	return container
}
