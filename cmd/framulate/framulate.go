package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/v3io/frames"
	"github.com/v3io/frames/http"
	"github.com/v3io/frames/pb"
	"github.com/v3io/frames/repeatingtask"

	"github.com/nuclio/errors"
	"github.com/nuclio/logger"
	nucliozap "github.com/nuclio/zap"
)

type framulate struct {
	ctx                     context.Context
	logger                  logger.Logger
	taskPool                *repeatingtask.Pool
	framesURL               string
	accessKey               string
	framesClient            frames.Client
	maxParallelTablesCreate int
	maxParallelSeriesCreate int
}

func newFramulate(ctx context.Context,
	framesURL string,
	containerName string,
	userName string,
	accessKey string,
	maxInflightRequests int,
	maxParallelTablesCreate int,
	maxParallelSeriesCreate int) (*framulate, error) {
	var err error

	newFramulate := framulate{
		framesURL:               framesURL,
		maxParallelTablesCreate: maxParallelTablesCreate,
		maxParallelSeriesCreate: maxParallelSeriesCreate,
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
		"user", session.User)

	newFramulate.framesClient, err = http.NewClient(newFramulate.framesURL, &session, newFramulate.logger)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create client")
	}

	return &newFramulate, nil
}

func (f *framulate) start(numTables int, numSeriesPerTable int) error {
	f.logger.DebugWith("Starting",
		"numTables", numTables,
		"numSeriesPerTable", numSeriesPerTable)

	if err := f.createTSDBTables(numTables); err != nil {
		return errors.Wrap(err, "Failed to create TSDB tables")
	}

	f.logger.DebugWith("Waiting to create series")
	time.Sleep(5 * time.Second)

	if err := f.createTSDBSeries(numTables, numSeriesPerTable); err != nil {
		return errors.Wrap(err, "Failed to create TSDB series")
	}

	f.logger.DebugWith("Done")

	return nil
}

func (f *framulate) createTSDBTables(numTables int) error {
	f.logger.DebugWith("Creating tables")

	rateValue := pb.Value{}
	rateValue.SetValue("1/s")

	f.logger.DebugWith("Preparing tables", "numTables", numTables)

	tableCreationTask := repeatingtask.Task{
		NumReptitions: numTables,
		MaxParallel:   f.maxParallelTablesCreate,
		Handler: func(cookie interface{}, repetitionIndex int) error {
			tableName := f.getTableName(repetitionIndex)

			f.logger.DebugWith("Deleting table", "tableName", tableName)

			// try to delete first and ignore error
			f.framesClient.Delete(&pb.DeleteRequest{
				Backend: "tsdb",
				Table:   tableName,
			})

			f.logger.DebugWith("Creating table", "tableName", tableName)

			return f.framesClient.Create(&pb.CreateRequest{
				Backend: "tsdb",
				Table:   tableName,
				AttributeMap: map[string]*pb.Value{
					"rate": &rateValue,
				},
			})
		},
	}

	taskErrors := f.taskPool.SubmitTaskAndWait(&tableCreationTask)
	return taskErrors.Error()
}

func (f *framulate) createTSDBSeries(numTables int, numSeriesPerTable int) error {
	// seriesCreationTaskGroup := repeatingtask.TaskGroup{}

	// create a task per table and wait on these
	for tableIdx := 0; tableIdx < numTables; tableIdx++ {

		// create a series creation task
		seriesCreationTask := repeatingtask.Task{
			NumReptitions: numSeriesPerTable,
			MaxParallel:   f.maxParallelSeriesCreate,
			Cookie:        f.getTableName(tableIdx),
			Handler: func(cookie interface{}, repetitionIndex int) error {
				tableName := cookie.(string)
				seriesName := fmt.Sprintf("series-%d", repetitionIndex)

				f.logger.DebugWith("Creating series",
					"tableName", tableName,
					"seriesName", seriesName)

				framesAppender, err := f.framesClient.Write(&frames.WriteRequest{
					Backend: "tsdb",
					Table:   tableName,
				})

				if err != nil {
					return errors.Wrap(err, "Failed to create err")
				}

				columns := map[string]interface{}{
					seriesName: []int{1},
				}

				indices := map[string]interface{}{
					"timestamp": []time.Time{time.Now()},
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
			},
		}

		// parallel table writes
		//// submit the task
		//f.taskPool.SubmitTask(&seriesCreationTask)
		//
		//// add the task
		//seriesCreationTaskGroup.AddTask(&seriesCreationTask)

		if taskErrors := f.taskPool.SubmitTaskAndWait(&seriesCreationTask); taskErrors.Error() != nil {
			return taskErrors.Error()
		}
	}

	// wait for series
	// taskGroupErrors := seriesCreationTaskGroup.Wait()

	// return taskGroupErrors.Error()

	return nil
}

func (f *framulate) getTableName(index int) string {
	return fmt.Sprintf("tsdb-%d", index)
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

	flag.StringVar(&framesURL, "url", "", "")
	flag.StringVar(&containerName, "container-name", "", "")
	flag.StringVar(&userName, "username", "", "")
	flag.StringVar(&accessKey, "access-key", "", "")
	flag.IntVar(&maxInflightRequests, "max-inflight-requests", 256, "")
	flag.IntVar(&numTables, "num-tables", 16, "")
	flag.IntVar(&numSeriesPerTable, "num-series-per-table", 512, "")
	flag.IntVar(&maxParallelTablesCreate, "max-parallel-tables-create", 8, "")
	flag.IntVar(&maxParallelSeriesCreate, "max-parallel-series-create", 512, "")
	flag.Parse()

	framulateInstance, err := newFramulate(context.TODO(),
		framesURL,
		containerName,
		userName,
		accessKey,
		maxInflightRequests,
		maxParallelTablesCreate,
		maxParallelSeriesCreate)
	if err != nil {
		os.Exit(1)
	}

	if err := framulateInstance.start(numTables, numSeriesPerTable); err != nil {
		panic(errors.GetErrorStackString(err, 10))
	}
}
