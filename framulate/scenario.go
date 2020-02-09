package framulate

import (
	"fmt"
	"github.com/nuclio/errors"
	"github.com/nuclio/logger"
	"github.com/v3io/frames"
	"math/rand"
	"time"
)

type scenario interface {
	Start() error
	LogStatistics()
}

type abstractScenario struct {
	logger    logger.Logger
	framulate *Framulate
	config    *Config
}

func newAbstractScenario(loggerInstance logger.Logger,
	framulateInstance *Framulate,
	config *Config) (*abstractScenario, error) {
	return &abstractScenario{
		logger:    loggerInstance,
		framulate: framulateInstance,
		config:    config,
	}, nil
}

func (s *abstractScenario) getTableName(index int) string {
	return fmt.Sprintf("tsdb-%d", index)
}

func (s *abstractScenario) getSeriesName(index int) string {
	return fmt.Sprintf("series-%d", index)
}

func (s *abstractScenario) writeSeriesToTable(cookie interface{},
	columns map[string]interface{},
	indices map[string]interface{}) error {
	tableName := cookie.(string)

	framesAppender, err := s.framulate.framesClient.Write(&frames.WriteRequest{
		Backend: "tsdb",
		Table:   tableName,
	})

	if err != nil {
		return errors.Wrap(err, "Failed to create err")
	}

	frame, err := frames.NewFrameFromMap(columns, indices)
	if err != nil {
		return errors.Wrap(err, "Failed to create frame")
	}

	// create a frame
	if err := framesAppender.Add(frame); err != nil {
		return errors.Wrap(err, "Failed to add frame")
	}

	err = framesAppender.WaitForComplete(60 * time.Second)
	if err != nil {
		s.logger.WarnWith("Failed writing to series", "err", err.Error())
	}

	return err
}

func (s *abstractScenario) getRandomSeriesValues(numItems int, min float64, max float64) []float64 {
	series := make([]float64, numItems)
	for itemIdx := 0; itemIdx < numItems; itemIdx++ {
		series[itemIdx] = min + rand.Float64()*(max-min)
	}

	return series
}

func (s *abstractScenario) getIncrementingSeriesTimestamps(numItems int, increment time.Duration) []time.Time {
	baselineTimestamp := time.Now()

	timestamps := make([]time.Time, numItems)
	for timestampIdx := 0; timestampIdx < numItems; timestampIdx++ {
		timestamps[timestampIdx] = baselineTimestamp.Add(time.Duration(timestampIdx) * increment)
	}

	return timestamps
}
