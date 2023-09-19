package framulate

import (
	"os"
	"time"

	"github.com/ghodss/yaml"
	"github.com/nuclio/errors"
)

type scenarioKind string

const (
	scenarioKindWriteVerify = "writeVerify"
)

type WriteVerifyConfig struct {
	NumTables               int    `json:"num_tables,omitempty"`
	NumSeriesPerTable       int    `json:"num_series_per_table,omitempty"`
	MaxParallelTablesCreate int    `json:"max_parallel_tables_create,omitempty"`
	MaxParallelSeriesWrite  int    `json:"max_parallel_series_write,omitempty"`
	MaxParallelSeriesVerify int    `json:"max_parallel_series_verify,omitempty"`
	WriteDummySeries        bool   `json:"write_dummy_series,omitempty"`
	NumDatapointsPerSeries  int    `json:"num_datapoints_per_series,omitempty"`
	WriteDelay              string `json:"write_delay,omitempty"`
	VerificationDelay       string `json:"verification_delay,omitempty"`
	Verify                  bool   `json:"verify,omitempty"`

	verificationDelay time.Duration
	writeDelay        time.Duration
}

type ScenarioConfig struct {
	Kind        scenarioKind
	WriteVerify WriteVerifyConfig
}

type Transport struct {
	URL                 string `json:"url,omitempty"`
	MaxInflightRequests int    `json:"max_inflight_requests,omitempty"`
}

type Config struct {
	ContainerName string         `json:"container_name,omitempty"`
	UserName      string         `json:"username,omitempty"`
	AccessKey     string         `json:"access_key,omitempty"`
	Cleanup       bool           `json:"cleanup,omitempty"`
	MaxTasks      int            `json:"max_tasks,omitempty"`
	Scenario      ScenarioConfig `json:"scenario,omitempty"`
	Transport     Transport      `json:"transport,omitempty"`
}

func NewConfigFromContentsOrPath(configContents []byte, configPath string) (*Config, error) {
	var err error

	if len(configContents) == 0 {
		if configPath == "" {
			return nil, errors.New("Config contents or path must be specified")
		}

		configContents, err = os.ReadFile(configPath)
		if err != nil {
			return nil, errors.Wrapf(err, "Failed to config file at %s", configPath)
		}
	}

	newConfig := Config{}

	if err := yaml.Unmarshal(configContents, &newConfig); err != nil {
		return nil, errors.Wrap(err, "Failed to unmarshal env spec file")
	}

	if err := newConfig.validateAndPopulateDefaults(); err != nil {
		return nil, errors.Wrap(err, "Failed to validate/popualte defaults")
	}

	return &newConfig, nil
}

func (c *Config) validateAndPopulateDefaults() error {
	var err error

	if c.Transport.URL == "" {
		c.Transport.URL = "grpc://framesd:8081"
	}

	if c.Transport.MaxInflightRequests == 0 {
		c.Transport.MaxInflightRequests = 512
	}

	if c.MaxTasks == 0 {
		c.MaxTasks = 1024 * 1024
	}

	if c.Scenario.WriteVerify.VerificationDelay != "" {
		c.Scenario.WriteVerify.verificationDelay, err = time.ParseDuration(c.Scenario.WriteVerify.VerificationDelay)
		if err != nil {
			return errors.Wrap(err, "Failed to parse verification delay")
		}
	}

	if c.Scenario.WriteVerify.WriteDelay != "" {
		c.Scenario.WriteVerify.writeDelay, err = time.ParseDuration(c.Scenario.WriteVerify.WriteDelay)
		if err != nil {
			return errors.Wrap(err, "Failed to parse write delay")
		}
	}

	return nil
}
