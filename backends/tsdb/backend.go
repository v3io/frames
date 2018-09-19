package tsdb

import (
	"github.com/nuclio/logger"
	"github.com/v3io/frames"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
)

type Backend struct {
	adapter *tsdb.V3ioAdapter
	logger  logger.Logger
}

// NewBackend return a new key/value backend
func NewBackend(logger logger.Logger, cfg *frames.BackendConfig) (frames.DataBackend, error) {

	tsdbConfig := config.V3ioConfig{
		V3ioUrl:   cfg.V3ioURL,
		Container: cfg.Container,
		Path:      cfg.Path,
		Username:  cfg.Username,
		Password:  cfg.Password,
		Workers:   cfg.Workers,
	}

	adapter, err := tsdb.NewV3ioAdapter(&tsdbConfig, nil, nil)
	if err != nil {
		return nil, err
	}

	newBackend := Backend{
		adapter: adapter,
		logger:  logger,
	}

	return &newBackend, nil
}

func (b *Backend) Create(request *frames.CreateRequest) error {
	return nil
}

func (b *Backend) Delete(request *frames.DeleteRequest) error {
	return nil
}
