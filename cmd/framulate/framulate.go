package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync/atomic"
	"time"

	"github.com/v3io/frames"
	"github.com/v3io/frames/http"
	"github.com/v3io/frames/pb"
	"github.com/v3io/frames/repeatingtask"

	"github.com/nuclio/errors"
	"github.com/nuclio/logger"
	nucliozap "github.com/nuclio/zap"
)

type seriesCookie struct {
	tableName string
	tableIdx  int
}

type framulate struct {
	ctx                     context.Context
	logger                  logger.Logger
	taskPool                *repeatingtask.Pool
	framesURL               string
	accessKey               string
	framesClient            frames.Client
	maxParallelTablesCreate int
	maxParallelSeriesCreate int
	numSeriesCreatedByTable []int
	numSeriesCreated        uint64
	deleteTables            bool
	writeDummySeries        bool

	seriesTimestamps []time.Time
	seriesValues     []float64
}

func newFramulate(ctx context.Context,
	framesURL string,
	containerName string,
	userName string,
	accessKey string,
	maxInflightRequests int,
	maxParallelTablesCreate int,
	maxParallelSeriesCreate int,
	deleteTables bool,
	writeDummySeries bool,
	numDatapointsPerSeries int) (*framulate, error) {
	var err error

	newFramulate := framulate{
		framesURL:               framesURL,
		maxParallelTablesCreate: maxParallelTablesCreate,
		maxParallelSeriesCreate: maxParallelSeriesCreate,
		deleteTables:            deleteTables,
		writeDummySeries:        writeDummySeries,
	}

	newFramulate.taskPool, err = repeatingtask.NewPool(ctx,
		1024*1024,
		maxInflightRequests)

	if err != nil {
		return nil, errors.Wrap(err, "Failed to create pool")
	}

	newFramulate.logger, err = nucliozap.NewNuclioZapCmd("framulate", nucliozap.DebugLevel)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create logger")
	}

	session := pb.Session{
		Container: containerName,
		User:      userName,
		Token:     accessKey,
	}

	newFramulate.logger.DebugWith("Creating frames client",
		"container", session.Container,
		"user", session.User,
		"deleteTables", deleteTables,
		"writeDummySeries", writeDummySeries,
		"numDatapointsPerSeries", numDatapointsPerSeries)

	newFramulate.framesClient, err = http.NewClient(newFramulate.framesURL, &session, newFramulate.logger)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create client")
	}

	numDatapoints := numDatapointsPerSeries

	newFramulate.seriesTimestamps = newFramulate.getRandomSeriesTimestamps(numDatapoints)
	newFramulate.seriesValues = newFramulate.getRandomSeriesValues(numDatapoints, 0, 200)

	return &newFramulate, nil
}

func (f *framulate) start(numTables int, numSeriesPerTable int) error {
	f.logger.DebugWith("Starting",
		"numTables", numTables,
		"numSeriesPerTable", numSeriesPerTable)

	f.numSeriesCreatedByTable = make([]int, numTables)

	go func() {
		var lastNumSeriesCreated int

		for {
			currentNumSeriesCreated := int(f.numSeriesCreated)

			f.logger.DebugWith("Series created",
				"total", currentNumSeriesCreated,
				"%", currentNumSeriesCreated*100.0/(numTables*numSeriesPerTable),
				"s/s", currentNumSeriesCreated-lastNumSeriesCreated,
			)

			lastNumSeriesCreated = currentNumSeriesCreated

			time.Sleep(1 * time.Second)
		}
	}()

	if err := f.createTSDBTables(numTables); err != nil {
		return errors.Wrap(err, "Failed to create TSDB tables")
	}

	f.logger.DebugWith("Waiting to create series")
	time.Sleep(3 * time.Second)

	if err := f.createTSDBSeries(numTables, numSeriesPerTable); err != nil {
		return errors.Wrap(err, "Failed to create TSDB series")
	}

	f.logger.DebugWith("Done")

	return nil
}

func (f *framulate) createTSDBTables(numTables int) error {
	f.logger.DebugWith("Creating tables")

	rateValue := pb.Value{}
	rateValue.SetValue("1/h")

	f.logger.DebugWith("[4] Preparing tables", "numTables", numTables)

	tableCreationTask := repeatingtask.Task{
		NumReptitions: numTables,
		MaxParallel:   f.maxParallelTablesCreate,
		Handler: func(cookie interface{}, repetitionIndex int) error {
			tableName := f.getTableName(repetitionIndex)

			if f.deleteTables {
				f.logger.DebugWith("Deleting table", "tableName", tableName)

				// try to delete first and ignore error
				f.framesClient.Delete(&pb.DeleteRequest{
					Backend: "tsdb",
					Table:   tableName,
				})
			}

			f.logger.DebugWith("Creating table", "tableName", tableName)

			err := f.framesClient.Create(&pb.CreateRequest{
				Backend: "tsdb",
				Table:   tableName,
				AttributeMap: map[string]*pb.Value{
					"rate": &rateValue,
				},
			})

			if err != nil {
				return errors.Wrap(err, "Failed creating table")
			}

			if f.writeDummySeries {

				// write a dummy series to write the schema not in parallel (workaround)
				return f.writeSeriesToTable(tableName, 9999999)
			} else {
				f.logger.DebugWith("Not writing dummy series", "tableName", tableName)
			}

			return nil
		},
	}

	taskErrors := f.taskPool.SubmitTaskAndWait(&tableCreationTask)
	return taskErrors.Error()
}

func (f *framulate) createTSDBSeries(numTables int, numSeriesPerTable int) error {
	seriesCreationTaskGroup := repeatingtask.TaskGroup{}

	// create a task per table and wait on these
	for tableIdx := 0; tableIdx < numTables; tableIdx++ {

		// create a series creation task
		seriesCreationTask := repeatingtask.Task{
			NumReptitions: numSeriesPerTable,
			MaxParallel:   f.maxParallelSeriesCreate,
			Cookie: &seriesCookie{
				tableName: f.getTableName(tableIdx),
				tableIdx:  tableIdx,
			},
			Handler: func(cookie interface{}, repetitionIndex int) error {
				seriesCookie := cookie.(*seriesCookie)

				err := f.writeSeriesToTable(seriesCookie.tableName, repetitionIndex)

				// stats stuff
				f.numSeriesCreatedByTable[seriesCookie.tableIdx]++
				atomic.AddUint64(&f.numSeriesCreated, 1)

				return err
			},
		}

		// submit the task
		f.taskPool.SubmitTask(&seriesCreationTask)

		// add the task
		seriesCreationTaskGroup.AddTask(&seriesCreationTask)
	}

	// wait for series
	taskGroupErrors := seriesCreationTaskGroup.Wait()

	return taskGroupErrors.Error()
}

func (f *framulate) getTableName(index int) string {
	return fmt.Sprintf("tsdb-%d", index)
}

func (f *framulate) writeSeriesToTable(cookie interface{}, repetitionIndex int) error {
	tableName := cookie.(string)
	seriesName := fmt.Sprintf("series-%d", repetitionIndex)

	framesAppender, err := f.framesClient.Write(&frames.WriteRequest{
		Backend: "tsdb",
		Table:   tableName,
	})

	if err != nil {
		return errors.Wrap(err, "Failed to create err")
	}

	columns := map[string]interface{}{
		seriesName: f.seriesValues,
	}

	indices := map[string]interface{}{
		"timestamp": f.seriesTimestamps,
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
		f.logger.WarnWith("Failed writing to series", "err", err.Error())
	}

	return err
}

func (f *framulate) getRandomSeriesValues(numItems int, min float64, max float64) []float64 {
	series := make([]float64, numItems)
	for itemIdx := 0; itemIdx < numItems; itemIdx++ {
		series[itemIdx] = min + rand.Float64()*(max-min)
	}

	return series
}

func (f *framulate) getRandomSeriesTimestamps(numItems int) []time.Time {
	baselineTimestamp := time.Now()

	timestamps := make([]time.Time, numItems)
	for timestampIdx := 0; timestampIdx < numItems; timestampIdx++ {
		timestamps[timestampIdx] = baselineTimestamp.Add(time.Duration(timestampIdx) * time.Hour)
	}

	return timestamps
}

func main() {
	framesURL := ""
	containerName := ""
	userName := ""
	accessKey := ""
	maxInflightRequests := 0
	numTables := 0
	numSeriesPerTable := 0
	maxParallelSeriesCreate := 0
	maxParallelTablesCreate := 0
	deleteTables := false
	writeDummySeries := true
	numDatapointsPerSeries := 4

	flag.StringVar(&framesURL, "url", "", "")
	flag.StringVar(&containerName, "container-name", "", "")
	flag.StringVar(&userName, "username", "", "")
	flag.StringVar(&accessKey, "access-key", "", "")
	flag.IntVar(&maxInflightRequests, "max-inflight-requests", 256, "")
	flag.IntVar(&numTables, "num-tables", 16, "")
	flag.IntVar(&numSeriesPerTable, "num-series-per-table", 512, "")
	flag.IntVar(&maxParallelTablesCreate, "max-parallel-tables-create", 8, "")
	flag.IntVar(&maxParallelSeriesCreate, "max-parallel-series-create", 512, "")
	flag.BoolVar(&deleteTables, "delete-tables", false, "")
	flag.BoolVar(&writeDummySeries, "write-dummy-series", true, "")
	flag.IntVar(&numDatapointsPerSeries, "num-datapoints-per-series", 4, "")

	flag.Parse()

	framulateInstance, err := newFramulate(context.TODO(),
		framesURL,
		containerName,
		userName,
		accessKey,
		maxInflightRequests,
		maxParallelTablesCreate,
		maxParallelSeriesCreate,
		deleteTables,
		writeDummySeries,
		numDatapointsPerSeries)
	if err != nil {
		os.Exit(1)
	}

	if err := framulateInstance.start(numTables, numSeriesPerTable); err != nil {
		panic(errors.GetErrorStackString(err, 10))
	}
}
