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

package tsdb

import (
	"fmt"
	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/v3io/frames"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"sort"
	"time"
)

func (b *Backend) Write(request *frames.WriteRequest) (frames.FrameAppender, error) {

	adapter, err := b.GetAdapter(request.Table)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create adapter")
	}

	appender, err := adapter.Appender()
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Appender")
	}

	newTsdbAppender := tsdbAppender{
		request:  request,
		appender: appender,
		logger:   b.logger,
	}

	if request.ImmidiateData != nil {
		err := newTsdbAppender.Add(request.ImmidiateData)
		if err != nil {
			return &newTsdbAppender, err
		}
	}

	return &newTsdbAppender, nil
}

// Appender is key/value appender
type tsdbAppender struct {
	request  *frames.WriteRequest
	appender tsdb.Appender
	logger   logger.Logger
}

type metricCtx struct {
	lset utils.Labels
	ref  uint64
}

func (a *tsdbAppender) Add(frame frames.Frame) error {

	if frame.Len() == 0 {
		return nil
	}

	names := frame.Names()
	if len(names) == 0 {
		return fmt.Errorf("empty frame")
	}

	tarray := make([]int64, frame.Len())
	timeColIndex := -1

	if frame.Indices() == nil || len(frame.Indices()) == 0 {
		return fmt.Errorf("no indices, must have at least one Time index")
	}

	for i, col := range frame.Indices() {
		if col.DType() == frames.TimeType {
			timeColIndex = i
		}
	}

	if timeColIndex == -1 {
		return fmt.Errorf("there is no index of type time/date")
	}

	times, err := frame.Indices()[timeColIndex].Times()

	for i := 0; i < frame.Len(); i++ {
		t := times[i].UnixNano() / 1000 / 1000
		fmt.Printf("t: %s ,", times[i].String())
		tarray[i] = t
	}

	if len(tarray) > 1 && tarray[0] > tarray[len(tarray)-1] {
		return errors.Wrap(err, "time column is out of order (need to be sorted by ascending time)")
	}

	values := make([][]float64, len(names))
	for i, name := range names {
		col, err := frame.Column(name)
		if err != nil {
			return err
		}

		switch col.DType() {
		case frames.FloatType:
			asFloat, _ := col.Floats()
			values[i] = asFloat
		case frames.IntType:
			asInt, _ := col.Ints()
			data := make([]float64, frame.Len())
			for i := 0; i < frame.Len(); i++ {
				data[i] = float64(asInt[i])
			}
			values[i] = data
		default:
			return fmt.Errorf("cannot write type %v as time series value", col.DType())
		}
	}

	if len(frame.Indices()) == 1 {

		// in case we have a single index with or without labels
		metrics := make([]*metricCtx, 0, len(names))
		for _, name := range names {
			lset, err := newLset(a.request.Labels, name, len(names) == 1, nil, nil)
			if err != nil {
				return err
			}
			metrics = append(metrics, &metricCtx{lset: lset})
		}

		for i := 0; i < frame.Len(); i++ {

			for idx, metric := range metrics {
				if i == 0 {
					metric.ref, err = a.appender.Add(metric.lset, tarray[0], values[idx][0])
					if err != nil {
						return errors.Wrap(err, "failed to Add")
					}
				} else {
					err := a.appender.AddFast(metric.lset, metric.ref, tarray[i], values[idx][i])
					if err != nil {
						return errors.Wrap(err, "failed to AddFast")
					}
				}
			}
		}
	} else {

		// in case of multi-index (extra indexes converted to labels)
		indexLen := len(frame.Indices()) - 1
		indexCols := make([][]string, indexLen)
		indexNames := make([]string, indexLen)
		for i, idx := range frame.Indices() {
			if i != timeColIndex {
				asString := idx.Strings()
				indexCols[i] = asString
				indexNames[i] = idx.Name()
			}
		}

		lastIndexes := make([]string, indexLen)
		metrics := make([]*metricCtx, 0, len(names))

		for i := 0; i < frame.Len(); i++ {

			sameIndex := true
			for colidx, colval := range indexCols {
				if colval[i] != lastIndexes[colidx] {
					sameIndex = false
					lastIndexes[colidx] = colval[i]
				}
			}

			if !sameIndex {
				for idx, name := range names {
					lset, err := newLset(a.request.Labels, name, len(names) == 1, indexNames, lastIndexes)
					if err != nil {
						return err
					}
					metric := metricCtx{lset: lset}
					metric.ref, err = a.appender.Add(metric.lset, tarray[0], values[idx][0])
					if err != nil {
						return errors.Wrap(err, "failed to Add")
					}
					metrics[idx] = &metric
				}
			} else {
				for idx, metric := range metrics {
					err := a.appender.AddFast(metric.lset, metric.ref, tarray[i], values[idx][i])
					if err != nil {
						return errors.Wrap(err, "failed to AddFast")
					}
				}
			}
		}
	}

	return nil
}

func newLset(labels map[string]interface{}, name string, singleCol bool, extraIdx, extraIdxVals []string) (utils.Labels, error) {
	lset := make(utils.Labels, 0, len(labels))
	var hadName bool
	for name, val := range labels {
		if name == "__name__" {
			if !singleCol {
				return nil, fmt.Errorf("label __name__ cannot be set with multi column TSDB frames")
			}
			val = name
			hadName = true
		}
		lset = append(lset, utils.Label{Name: name, Value: fmt.Sprintf("%s", val)})
	}
	if !hadName {
		lset = append(lset, utils.Label{Name: "__name__", Value: name})
	}

	if extraIdx != nil {
		for i, idx := range extraIdx {
			lset = append(lset, utils.Label{Name: idx, Value: extraIdxVals[i]})
		}
	}
	sort.Sort(lset)
	return lset, nil
}

func (a *tsdbAppender) WaitForComplete(timeout time.Duration) error {
	_, err := a.appender.WaitForCompletion(timeout)
	return err
}
