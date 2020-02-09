package framulate

import (
	"sync/atomic"
	"time"

	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/v3io/frames/pb"
	"github.com/v3io/frames/repeatingtask"
)

type writeVerifyScenario struct {
	*abstractScenario
	numSeriesCreated uint64
}

func newWriteVerifyScenario(parentLogger logger.Logger,
	framulateInstance *Framulate,
	config *Config) (*writeVerifyScenario, error) {
	var err error

	newWriteVerifyScenario := writeVerifyScenario{}

	newWriteVerifyScenario.abstractScenario, err = newAbstractScenario(parentLogger.GetChild("writeVerify"),
		framulateInstance,
		config)

	if err != nil {
		return nil, errors.Wrap(err, "Failed to create abstract scenario")
	}

	return &newWriteVerifyScenario, nil
}

func (s *writeVerifyScenario) Start() error {
	s.logger.DebugWith("Starting", "config", s.config.Scenario.WriteVerify)

	go func() {
		var lastNumSeriesCreated int

		for {
			currentNumSeriesCreated := int(s.numSeriesCreated)

			s.logger.DebugWith("Series created",
				"total", currentNumSeriesCreated,
				"%", currentNumSeriesCreated*100.0/(s.config.Scenario.WriteVerify.NumTables*s.config.Scenario.WriteVerify.NumSeriesPerTable),
				"s/s", currentNumSeriesCreated-lastNumSeriesCreated,
			)

			lastNumSeriesCreated = currentNumSeriesCreated

			time.Sleep(1 * time.Second)
		}
	}()

	if err := s.createTSDBTables(s.config.Scenario.WriteVerify.NumTables); err != nil {
		return errors.Wrap(err, "Failed to create TSDB tables")
	}

	s.logger.DebugWith("Waiting to create series")
	time.Sleep(3 * time.Second)

	if err := s.createTSDBSeries(s.config.Scenario.WriteVerify.NumTables,
		s.config.Scenario.WriteVerify.NumSeriesPerTable); err != nil {
		return errors.Wrap(err, "Failed to create TSDB series")
	}

	s.logger.DebugWith("Done")

	return nil
}

func (s *writeVerifyScenario) createTSDBTables(numTables int) error {
	s.logger.DebugWith("Creating tables")

	rateValue := pb.Value{}
	rateValue.SetValue("1/h")

	s.logger.DebugWith("Preparing tables", "numTables", numTables)

	tableCreationTask := repeatingtask.Task{
		NumReptitions: numTables,
		MaxParallel:   s.config.Scenario.WriteVerify.MaxParallelTablesCreate,
		Handler: func(cookie interface{}, repetitionIndex int) error {
			tableName := s.getTableName(repetitionIndex)

			if s.config.Cleanup {
				s.logger.DebugWith("Deleting table", "tableName", tableName)

				// try to delete first and ignore error
				err := s.framulate.framesClient.Delete(&pb.DeleteRequest{
					Backend: "tsdb",
					Table:   tableName,
				})

				if err != nil {
					// could be that it doesn't exist... TODO: check other errors
				}
			}

			s.logger.DebugWith("Creating table", "tableName", tableName)

			err := s.framulate.framesClient.Create(&pb.CreateRequest{
				Backend: "tsdb",
				Table:   tableName,
				AttributeMap: map[string]*pb.Value{
					"rate": &rateValue,
				},
			})

			if err != nil {
				return errors.Wrap(err, "Failed creating table")
			}

			if s.config.Scenario.WriteVerify.WriteDummySeries {
				columns := map[string]interface{}{
					s.getSeriesName(repetitionIndex): s.getRandomSeriesValues(1, 0, 1),
				}

				indices := map[string]interface{}{
					"timestamp": s.getIncrementingSeriesTimestamps(1, 1*time.Second),
				}

				// write a dummy series to write the schema not in parallel (workaround)
				return s.writeSeriesToTable(tableName, columns, indices)
			} else {
				s.logger.DebugWith("Not writing dummy series", "tableName", tableName)
			}

			return nil
		},
	}

	taskErrors := s.framulate.taskPool.SubmitTaskAndWait(&tableCreationTask)
	return taskErrors.Error()
}

func (s *writeVerifyScenario) createTSDBSeries(numTables int, numSeriesPerTable int) error {
	seriesCreationTaskGroup := repeatingtask.TaskGroup{}

	type seriesCookie struct {
		tableName string
		tableIdx  int
	}

	// create a task per table and wait on these
	for tableIdx := 0; tableIdx < numTables; tableIdx++ {

		// create a series creation task
		seriesCreationTask := repeatingtask.Task{
			NumReptitions: numSeriesPerTable,
			MaxParallel:   s.config.Scenario.WriteVerify.MaxParallelSeriesCreate,
			Cookie: &seriesCookie{
				tableName: s.getTableName(tableIdx),
				tableIdx:  tableIdx,
			},
			Handler: func(cookie interface{}, repetitionIndex int) error {
				seriesCookie := cookie.(*seriesCookie)

				columns := map[string]interface{}{
					s.getSeriesName(repetitionIndex): s.getRandomSeriesValues(s.config.Scenario.WriteVerify.NumDatapointsPerSeries,
						0,
						200),
				}

				indices := map[string]interface{}{
					"timestamp": s.getIncrementingSeriesTimestamps(s.config.Scenario.WriteVerify.NumDatapointsPerSeries,
						1*time.Second),
				}

				err := s.writeSeriesToTable(seriesCookie.tableName, columns, indices)

				// stats stuff
				atomic.AddUint64(&s.numSeriesCreated, 1)

				return err
			},
		}

		// submit the task
		if err := s.framulate.taskPool.SubmitTask(&seriesCreationTask); err != nil {
			return errors.Wrap(err, "Failed to submit task")
		}

		// add the task
		if err := seriesCreationTaskGroup.AddTask(&seriesCreationTask); err != nil {
			return errors.Wrap(err, "Failed to add task")
		}
	}

	// wait for series
	taskGroupErrors := seriesCreationTaskGroup.Wait()

	return taskGroupErrors.Error()
}
