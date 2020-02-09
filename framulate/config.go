package framulate

import (
	"github.com/ghodss/yaml"
	"github.com/nuclio/errors"
	"io/ioutil"
)

type scenarioKind string

const (
	scenarioKindWriteVerify = "writeVerify"
)

type WriteVerifyConfig struct {
	NumTables               int    `json:"num_tables,omitempty"`
	NumSeriesPerTable       int    `json:"num_series_per_table,omitempty"`
	MaxParallelTablesCreate int    `json:"max_parallel_tables_create,omitempty"`
	MaxParallelSeriesCreate int    `json:"max_parallel_series_create,omitempty"`
	WriteDummySeries        bool   `json:"write_dummy_series,omitempty"`
	NumDatapointsPerSeries  int    `json:"num_datapoints_per_series,omitempty"`
	VerificationDelay       string `json:"verification_delay,omitempty"`
	Verify                  bool   `json:"verify,omitempty"`
}

type ScenarioConfig struct {
	Kind        scenarioKind
	WriteVerify WriteVerifyConfig
}

type Config struct {
	FramesURL           string         `json:"frames_url,omitempty"`
	ContainerName       string         `json:"container_name,omitempty"`
	UserName            string         `json:"username,omitempty"`
	AccessKey           string         `json:"access_key,omitempty"`
	MaxInflightRequests int            `json:"max_inflight_requests,omitempty"`
	Cleanup             bool           `json:"cleanup,omitempty"`
	MaxTasks            int            `json:"max_tasks,omitempty"`
	Scenario            ScenarioConfig `json:"scenario,omitempty"`
}

func NewConfigFromContentsOrPath(configContents []byte, configPath string) (*Config, error) {
	var err error

	if len(configContents) == 0 {
		if configPath == "" {
			return nil, errors.New("Config contents or path must be specified")
		}

		configContents, err = ioutil.ReadFile(configPath)
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
	if c.FramesURL == "" {
		c.FramesURL = "http://framesd:8080"
	}

	if c.MaxTasks == 0 {
		c.MaxTasks = 1024 * 1024
	}

	return nil
}
