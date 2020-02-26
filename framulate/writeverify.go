package framulate

import (
	"sync/atomic"
	"time"

	"github.com/nuclio/errors"
	"github.com/nuclio/logger"
	"github.com/v3io/frames/pb"
	"github.com/v3io/frames/repeatingtask"
)

type tsdbSeries struct {
	tableName  string
	tableIdx   int
	name       string
	timestamps []time.Time
	values     []float64
}

type writeVerifyScenario struct {
	*abstractScenario
	numSeriesWritten      uint64
	numSeriesVerified     uint64
	lastNumSeriesWritten  int
	lastNumSeriesVerified int
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

	if err := s.createTSDBTables(s.config.Scenario.WriteVerify.NumTables); err != nil {
		return errors.Wrap(err, "Failed to create TSDB tables")
	}

	if s.config.Scenario.WriteVerify.writeDelay != 0 {
		s.logger.DebugWith("Waiting before writing series")
		time.Sleep(s.config.Scenario.WriteVerify.writeDelay)
	}

	// create a task group for write/verify
	taskGroup, err := repeatingtask.NewTaskGroup()
	if err != nil {
		return errors.Wrap(err, "Failed to create task group")
	}

	// execute writes to the series
	err = s.createTSDBWriteTask(taskGroup,
		s.config.Scenario.WriteVerify.NumTables,
		s.config.Scenario.WriteVerify.NumSeriesPerTable)

	if err != nil {
		return errors.Wrap(err, "Failed to start write task")
	}

	// wait for series
	taskGroupErrors := taskGroup.Wait()

	// wait a bit for logs, since flush don't actually flush
	time.Sleep(3 * time.Second)

	return taskGroupErrors.Error()
}

func (s *writeVerifyScenario) LogStatistics() {
	currentNumSeriesWritten := int(s.numSeriesWritten)
	currentNumSeriesVerified := int(s.numSeriesVerified)

	s.logger.DebugWith("Statistics",
		"wrote", currentNumSeriesWritten,
		"% written", currentNumSeriesWritten*100.0/(s.config.Scenario.WriteVerify.NumTables*s.config.Scenario.WriteVerify.NumSeriesPerTable),
		"w/s", currentNumSeriesWritten-s.lastNumSeriesWritten,
		"verified", currentNumSeriesVerified,
		"% verified", currentNumSeriesVerified*100.0/(s.config.Scenario.WriteVerify.NumTables*s.config.Scenario.WriteVerify.NumSeriesPerTable),
		"v/s", currentNumSeriesVerified-s.lastNumSeriesVerified,
	)

	s.lastNumSeriesWritten = currentNumSeriesWritten
	s.lastNumSeriesVerified = currentNumSeriesVerified
}

func (s *writeVerifyScenario) createTSDBTables(numTables int) error {
	s.logger.DebugWith("Creating tables")

	rateValue := pb.Value{}
	_ = rateValue.SetValue("1/h")

	s.logger.DebugWith("Preparing tables", "numTables", numTables)

	tableCreationTask := repeatingtask.Task{
		NumReptitions: numTables,
		MaxParallel:   s.config.Scenario.WriteVerify.MaxParallelTablesCreate,
		Handler: func(cookie interface{}, repetitionIndex int) error {
			tableName := s.getTableName(repetitionIndex)
			s.logger.DebugWith("Deleting table", "tableName", tableName)

			if s.config.Cleanup {
				// try to delete first and ignore error
				err := s.framulate.framesClient.Delete(&pb.DeleteRequest{
					Backend: "tsdb",
					Table:   tableName,
				})

				if err == nil {
					s.logger.DebugWith("Table deleted", "tableName", tableName)
				}
			}

			s.logger.DebugWith("Creating table", "tableName", tableName)

			err := s.framulate.framesClient.Create(&pb.CreateRequest{
				Backend: "tsdb",
				Table:   tableName,
				Rate:    (&rateValue).String(),
			})

			if err != nil {
				return errors.Wrap(err, "Failed creating table")
			}

			if s.config.Scenario.WriteVerify.WriteDummySeries {

				// write a dummy series to write the schema not in parallel (workaround)
				return s.writeTSDBSeries(&tsdbSeries{
					name:       "dummy",
					tableName:  tableName,
					values:     []float64{0},
					timestamps: []time.Time{time.Now()},
				})

			}
			s.logger.DebugWith("Not writing dummy series", "tableName", tableName)

			return nil
		},
	}

	taskErrors := s.framulate.taskPool.SubmitTaskAndWait(&tableCreationTask)

	s.logger.DebugWith("Done creating tables", "err", taskErrors.Error())

	return taskErrors.Error()
}

func (s *writeVerifyScenario) createTSDBWriteTask(taskGroup *repeatingtask.TaskGroup,
	numTables int,
	numSeriesPerTable int) error {

	// create a task per table and wait on these
	for tableIdx := 0; tableIdx < numTables; tableIdx++ {

		// create a series creation task
		seriesCreationTask := repeatingtask.Task{
			NumReptitions: numSeriesPerTable,
			MaxParallel:   s.config.Scenario.WriteVerify.MaxParallelSeriesWrite,
			Cookie: &tsdbSeries{
				tableName: s.getTableName(tableIdx),
				tableIdx:  tableIdx,
			},
			Handler: func(cookie interface{}, repetitionIndex int) error {
				tsdbSeriesInstance := *cookie.(*tsdbSeries)
				tsdbSeriesInstance.name = s.getSeriesName(repetitionIndex)
				tsdbSeriesInstance.timestamps = s.getIncrementingSeriesTimestamps(s.config.Scenario.WriteVerify.NumDatapointsPerSeries, 1*time.Second)
				tsdbSeriesInstance.values = s.getRandomSeriesValues(s.config.Scenario.WriteVerify.NumDatapointsPerSeries, 0, 200)

				err := s.writeTSDBSeries(&tsdbSeriesInstance)
				if err == nil && s.config.Scenario.WriteVerify.Verify {

					// schedule a task to verify this series
					if err = s.createTSDBVerifyTask(taskGroup, &tsdbSeriesInstance); err != nil {
						return errors.Wrap(err, "Failed to create verify task")
					}
				}

				// stats stuff
				atomic.AddUint64(&s.numSeriesWritten, 1)

				return nil
			},
		}

		// submit the task
		if err := s.framulate.taskPool.SubmitTask(&seriesCreationTask); err != nil {
			return errors.Wrap(err, "Failed to submit task")
		}

		// add the task
		if err := taskGroup.AddTask(&seriesCreationTask); err != nil {
			return errors.Wrap(err, "Failed to add task")
		}
	}

	return nil
}

func (s *writeVerifyScenario) createTSDBVerifyTask(taskGroup *repeatingtask.TaskGroup,
	tsdbSeriesInstance *tsdbSeries) error {

	// create a series creation task
	seriesCreationTask := repeatingtask.Task{
		NumReptitions: 1,
		MaxParallel:   1,
		Cookie:        tsdbSeriesInstance,
		Handler: func(cookie interface{}, repetitionIndex int) error {
			tsdbSeriesInstance := cookie.(*tsdbSeries)

			err := s.verifyTSDBSeries(tsdbSeriesInstance.tableName,
				tsdbSeriesInstance.name,
				tsdbSeriesInstance.timestamps,
				tsdbSeriesInstance.values)

			// stats stuff
			atomic.AddUint64(&s.numSeriesVerified, 1)

			return err
		},
	}

	// submit the task
	if err := s.framulate.taskPool.SubmitTask(&seriesCreationTask); err != nil {
		return errors.Wrap(err, "Failed to submit task")
	}

	// add the task
	return taskGroup.AddTask(&seriesCreationTask)
}
