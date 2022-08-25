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
package framulate

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/nuclio/errors"
	"github.com/nuclio/logger"
	"github.com/v3io/frames"
	"github.com/v3io/frames/pb"
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
	return fmt.Sprintf("series%d", index)
}

func (s *abstractScenario) writeTSDBSeries(tsdbSeriesInstance *tsdbSeries) error {
	framesAppender, err := s.framulate.framesClient.Write(&frames.WriteRequest{
		Backend: "tsdb",
		Table:   tsdbSeriesInstance.tableName,
	})

	if err != nil {
		return errors.Wrap(err, "Failed to write frame")
	}

	columns := map[string]interface{}{
		tsdbSeriesInstance.name: tsdbSeriesInstance.values,
	}

	indices := map[string]interface{}{
		"timestamps": tsdbSeriesInstance.timestamps,
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

func (s *abstractScenario) readSeries(tableName string, seriesName string) (*tsdbSeries, error) {
	framesIterator, err := s.framulate.framesClient.Read(&pb.ReadRequest{
		Backend: "tsdb",
		Table:   tableName,
		Columns: []string{seriesName},
	})

	if err != nil {
		return nil, errors.Wrap(err, "Failed to read frame")
	}

	tsdbSeriesInstance := tsdbSeries{
		tableName: tableName,
		name:      seriesName,
	}

	for framesIterator.Next() {
		currentFrame := framesIterator.At()

		currentFrameValuesColumn, err := currentFrame.Column(tsdbSeriesInstance.name)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to get values column")
		}

		values, err := currentFrameValuesColumn.Floats()
		if err != nil {
			return nil, errors.Wrap(err, "Failed to get values")
		}

		tsdbSeriesInstance.values = append(tsdbSeriesInstance.values, values...)

		currentFrameTimestampsColumn := currentFrame.Indices()[0]

		timestamps, err := currentFrameTimestampsColumn.Times()
		if err != nil {
			return nil, errors.Wrap(err, "Failed to get timestamps")
		}

		tsdbSeriesInstance.timestamps = append(tsdbSeriesInstance.timestamps, timestamps...)
	}

	return &tsdbSeriesInstance, err
}

func (s *abstractScenario) verifyTSDBSeries(tableName string,
	seriesName string,
	expectedTimestamps []time.Time,
	expectedValues []float64) error {

	// read the series
	tsdbSeriesInstance, err := s.readSeries(tableName, seriesName)
	if err != nil {
		return errors.Wrapf(err, "Failed to read series %s:%s", tableName, seriesName)
	}

	if len(tsdbSeriesInstance.values) != len(expectedValues) {
		return errors.Errorf("Invalid number of values for %s:%s. Expected %d, got %d",
			tableName,
			seriesName,
			len(expectedValues),
			len(tsdbSeriesInstance.values))
	}

	for valueIdx := 0; valueIdx < len(tsdbSeriesInstance.values); valueIdx++ {
		if tsdbSeriesInstance.values[valueIdx] != expectedValues[valueIdx] {
			return errors.Errorf("Invalid value for %s:%s at index %d. Expected %f got %f",
				tableName,
				seriesName,
				valueIdx,
				expectedValues[valueIdx],
				tsdbSeriesInstance.values[valueIdx])
		}
	}

	if len(tsdbSeriesInstance.timestamps) != len(expectedTimestamps) {
		return errors.Errorf("Invalid number of timestamps. Expected %d, got %d", len(expectedTimestamps), len(tsdbSeriesInstance.timestamps))
	}

	for timestampIdx := 0; timestampIdx < len(tsdbSeriesInstance.timestamps); timestampIdx++ {
		if tsdbSeriesInstance.timestamps[timestampIdx].Round(10*time.Millisecond) != expectedTimestamps[timestampIdx].Round(10*time.Millisecond) {
			return errors.Errorf("Invalid timestamp for %s:%s at index %d. Expected %s got %s",
				tableName,
				seriesName,
				timestampIdx,
				expectedTimestamps[timestampIdx],
				tsdbSeriesInstance.timestamps[timestampIdx])
		}
	}

	return nil
}

func (s *abstractScenario) getIncrementingSeriesValues(numItems int, start float64, increment float64) []float64 {
	series := make([]float64, numItems)
	for itemIdx := 0; itemIdx < numItems; itemIdx++ {
		series[itemIdx] = start + increment*float64(itemIdx)
	}

	return series
}

func (s *abstractScenario) getRandomSeriesValues(numItems int, min float64, max float64) []float64 {
	series := make([]float64, numItems)
	for itemIdx := 0; itemIdx < numItems; itemIdx++ {
		series[itemIdx] = min + rand.Float64()*(max-min)
	}

	return series
}

func (s *abstractScenario) getIncrementingSeriesTimestamps(numItems int, increment time.Duration) []time.Time {
	baselineTimestamp := time.Now().Add(time.Duration(-numItems) * time.Second)

	timestamps := make([]time.Time, numItems)
	for timestampIdx := 0; timestampIdx < numItems; timestampIdx++ {
		timestamps[timestampIdx] = baselineTimestamp.Add(time.Duration(timestampIdx) * increment)
	}

	return timestamps
}
