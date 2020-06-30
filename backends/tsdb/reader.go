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
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/v3io/frames"
	"github.com/v3io/frames/backends"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/pquerier"
	tsdbutils "github.com/v3io/v3io-tsdb/pkg/utils"
)

type tsdbIterator struct {
	request          *frames.ReadRequest
	set              pquerier.FrameSet
	err              error
	withColumns      bool
	currFrame        frames.Frame
	currTsdbFramePos int
	currTsdbFrame    frames.Frame
}

var allowedReadRequestFields = map[string]bool{
	"MultiIndex":        true,
	"Query":             true,
	"GroupBy":           true,
	"Start":             true,
	"End":               true,
	"Step":              true,
	"Aggregators":       true,
	"AggregationWindow": true,
}

func (b *Backend) Read(request *frames.ReadRequest) (frames.FrameIterator, error) {

	err := backends.ValidateRequest("tsdb", request.Proto, allowedReadRequestFields)
	if err != nil {
		return nil, err
	}

	step, err := tsdbutils.Str2duration(request.Proto.Step)
	if err != nil {
		return nil, err
	}

	aggregationWindow, err := tsdbutils.Str2duration(request.Proto.AggregationWindow)
	if err != nil {
		return nil, err
	}

	// TODO: start & end times
	to := time.Now().Unix() * 1000
	if request.Proto.End != "" {
		to, err = tsdbutils.Str2unixTime(request.Proto.End)
		if err != nil {
			return nil, err
		}
	}

	from := to - 1000*3600 // default of last hour
	if request.Proto.Start != "" {
		from, err = tsdbutils.Str2unixTime(request.Proto.Start)
		if err != nil {
			return nil, err
		}
	}

	b.logger.DebugWith("Query", "from", from, "to", to, "table", request.Proto.Table,
		"filter", request.Proto.Filter, "functions", request.Proto.Aggregators, "step", step)

	table := request.Proto.Table
	var selectParams *pquerier.SelectParams
	if request.Proto.Query != "" {
		selectParams, table, err = pquerier.ParseQuery(request.Proto.Query)
		if err != nil {
			return nil, err
		}
	}
	qry, err := b.GetQuerier(request.Proto.Session, request.Password.Get(), request.Token.Get(), table)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create adapter")
	}

	iter := tsdbIterator{request: request}
	name := ""
	if len(request.Proto.Columns) > 0 {
		name = strings.Join(request.Proto.Columns, ",")
		iter.withColumns = true
	}

	if selectParams != nil {
		selectParams.From = from
		selectParams.To = to
		selectParams.Step = step
		selectParams.AggregationWindow = aggregationWindow
	} else {
		selectParams = &pquerier.SelectParams{Name: name,
			From:              from,
			To:                to,
			Step:              step,
			Functions:         request.Proto.Aggregators,
			Filter:            request.Proto.Filter,
			GroupBy:           request.Proto.GroupBy,
			AggregationWindow: aggregationWindow}
	}

	iter.set, err = qry.SelectDataFrame(selectParams)
	if err != nil {
		return nil, errors.Wrap(err, "Failed on TSDB Select")
	}

	return &iter, nil
}

func (i *tsdbIterator) Next() bool {
	var frame frames.Frame
	var err error

	frameSizeLimit := 256
	if i.request.Proto.MessageLimit > 0 {
		frameSizeLimit = int(i.request.Proto.MessageLimit)
	}

	for i.currTsdbFrame == nil || i.currTsdbFrame.Len() == 0 {
		if i.set.NextFrame() {
			i.currTsdbFrame, err = i.set.GetFrame()
			if err != nil {
				i.err = err
				return false
			}
		} else {
			if i.set.Err() != nil {
				i.err = i.set.Err()
			}
			return false
		}
	}

	nextPos := i.currTsdbFramePos + frameSizeLimit
	if nextPos > i.currTsdbFrame.Len() {
		nextPos = i.currTsdbFrame.Len()
	}
	frame, err = i.currTsdbFrame.Slice(i.currTsdbFramePos, nextPos)
	if err != nil {
		i.err = errors.Wrap(err, "Failed to slice data frame")
		return false
	}
	if nextPos == i.currTsdbFrame.Len() {
		i.currTsdbFrame = nil
		i.currTsdbFramePos = 0
	} else {
		i.currTsdbFramePos = nextPos
	}

	labels := map[string]interface{}{}

	columns := make([]frames.Column, len(frame.Names()))
	indices := frame.Indices()
	for i, colName := range frame.Names() {
		columns[i], _ = frame.Column(colName) // Because we are iterating over the Names() it is safe to discard the error
	}

	for labelName, labelValue := range frame.Labels() {
		name := labelName
		if name == config.PrometheusMetricNameAttribute {
			name = "metric_name"
		}

		labels[name] = labelValue
		icol, err := frames.NewLabelColumn(name, labelValue, frame.Len())
		if err != nil {
			i.err = err
			return false
		}

		if i.request.Proto.MultiIndex {
			indices = append(indices, icol)
		} else {
			columns = append(columns, icol)
		}
	}

	i.currFrame, err = frames.NewFrameWithNullValues(columns, indices, labels, frame.NullValuesMap())
	if err != nil {
		i.err = err
		return false
	}

	return true
}

func (i *tsdbIterator) Err() error {
	return i.err
}

func (i *tsdbIterator) At() frames.Frame {
	return i.currFrame
}
