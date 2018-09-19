package tsdb

import (
	"github.com/pkg/errors"
	"github.com/v3io/frames"
	"github.com/v3io/v3io-tsdb/pkg/querier"
	tsdbutils "github.com/v3io/v3io-tsdb/pkg/utils"
	"time"
)

type tsdbIterator struct {
	request     *frames.ReadRequest
	set         querier.SeriesSet
	err         error
	withColumns bool
	currFrame   frames.Frame
}

func (b *Backend) Read(request *frames.ReadRequest) (frames.FrameIterator, error) {
	return nil, nil

	step, err := tsdbutils.Str2duration(request.StepRaw)
	if err != nil {
		return nil, err
	}

	// TODO: start & end times
	to := time.Now().Unix() * 1000
	if request.To != "" {
		to, err = tsdbutils.Str2unixTime(request.To)
		if err != nil {
			return nil, err
		}
	}

	from := to - 1000*3600 // default of last hour
	if request.From != "" {
		from, err = tsdbutils.Str2unixTime(request.From)
		if err != nil {
			return nil, err
		}
	}

	b.logger.DebugWith("Query", "from", from, "to", to,
		"filter", request.Filter, "functions", request.Aggragators, "step", step)

	qry, err := b.adapter.Querier(nil, from, to)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to initialize Querier")
	}

	iter := tsdbIterator{request: request}
	name := ""
	if len(request.Columns) > 0 {
		name = request.Columns[0]
		iter.withColumns = true
	}

	iter.set, err = qry.Select(name, request.Aggragators, step, request.Filter)
	if err != nil {
		return nil, errors.Wrap(err, "Failed on TSDB Select")
	}

	return &iter, nil
}

func (i *tsdbIterator) Next() bool {

	if i.set.Next() {
		series := i.set.At()
		labels := map[string]interface{}{}
		values := []float64{}
		times := []time.Time{}

		for _, v := range series.Labels() {
			labels[v.Name] = v.Value
		}

		iter := series.Iterator()
		for iter.Next() {
			t, v := iter.At()
			values = append(values, v)
			times = append(times, time.Unix(t/1000, (t%1000)*1000))
		}

		if iter.Err() != nil {
			i.err = iter.Err()
			return false
		}

		timeCol, err := frames.NewSliceColumn("Date", times)
		if err != nil {
			i.err = err
			return false
		}

		colname := "values"
		if i.withColumns {
			colname = i.request.Columns[0]
		}
		valCol, err := frames.NewSliceColumn(colname, values)
		if err != nil {
			i.err = err
			return false
		}

		columns := []frames.Column{timeCol, valCol}
		i.currFrame, err = frames.NewFrame(columns, "Date", labels)
		if err != nil {
			i.err = err
			return false
		}

		return true
	}

	if i.set.Err() != nil {
		i.err = i.set.Err()
	}

	return false
}

func (i *tsdbIterator) Err() error {
	return i.err
}

func (i *tsdbIterator) At() frames.Frame {
	return i.currFrame
}
