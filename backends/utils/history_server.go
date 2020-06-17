package utils

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/v3io/frames"
	"github.com/v3io/frames/pb"
	"github.com/v3io/frames/v3ioutils"
	v3io "github.com/v3io/v3io-go/pkg/dataplane"
	v3iohttp "github.com/v3io/v3io-go/pkg/dataplane/http"
	v3ioerrors "github.com/v3io/v3io-go/pkg/errors"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

const (
	defaultHistoryFileDurationInSeconds = 24 * 3600
	defaultHistoryFileNum               = 7
	defaultWriteMonitoringLogsTimeout   = 10 * time.Second
	defaultPendingLogsBatchSize         = 100
	defaultLogsFolderPath               = "/monitoring/frames"
	defaultLogsContainer                = "users"
	defaultMaxBytesInNginxRequest       = 5 * 1024 * 1024 // 5MB
	maxLogsInMessage                    = 1000
	maxRetryNum                         = 5

	readType    = "read"
	WriteType   = "write"
	createType  = "create"
	deleteType  = "delete"
	executeType = "execute"

	logFileTimeFormat = "20060102T150405Z"
)

type HistoryEntry struct {
	ActionType     string
	UserName       string
	BackendName    string
	TableName      string
	Container      string
	ActionDuration time.Duration
	StartTime      time.Time

	AdditionalData map[string]string
}

type HistoryServer struct {
	logger    logger.Logger
	logs      []HistoryEntry
	requests  chan HistoryEntry
	container v3io.Container
	config    *frames.Config
	isActive  bool
	writeData []byte

	WriteMonitoringLogsTimeout     time.Duration
	PendingLogsBatchSize           int
	LogsFolderPath                 string
	LogsContainer                  string
	MaxBytesInNginxRequest         int
	HistoryFileDurationSecondSpans int64
	HistoryFileNum                 int
}

func NewHistoryServer(logger logger.Logger, cfg *frames.Config) *HistoryServer {
	mon := HistoryServer{
		logger:   logger,
		requests: make(chan HistoryEntry, 100),
		isActive: false,
		config:   cfg}

	mon.initDefaults()

	mon.createDefaultV3ioClient()
	if mon.isActive {
		mon.Start()
		mon.StartEvictionTask()
	}
	return &mon
}

func (m *HistoryServer) initDefaults() {
	m.WriteMonitoringLogsTimeout = defaultWriteMonitoringLogsTimeout
	if m.config.WriteMonitoringLogsTimeoutSeconds != 0 {
		m.WriteMonitoringLogsTimeout = time.Duration(m.config.WriteMonitoringLogsTimeoutSeconds) * time.Second
	}

	m.PendingLogsBatchSize = defaultPendingLogsBatchSize
	if m.config.PendingLogsBatchSize != 0 {
		m.PendingLogsBatchSize = m.config.PendingLogsBatchSize
	}

	m.LogsFolderPath = defaultLogsFolderPath
	if m.config.LogsFolderPath != "" {
		m.LogsFolderPath = m.config.LogsFolderPath
	}

	m.LogsContainer = defaultLogsContainer
	if m.config.LogsContainer != "" {
		m.LogsContainer = m.config.LogsContainer
	}

	m.MaxBytesInNginxRequest = defaultMaxBytesInNginxRequest
	if m.config.MaxBytesInNginxRequest != 0 {
		m.MaxBytesInNginxRequest = m.config.MaxBytesInNginxRequest
	}

	m.HistoryFileDurationSecondSpans = defaultHistoryFileDurationInSeconds
	if m.config.HistoryFileDurationHourSpans != 0 {
		m.HistoryFileDurationSecondSpans = m.config.HistoryFileDurationHourSpans * 3600
	}

	m.HistoryFileNum = defaultHistoryFileNum
	if m.config.HistoryFileNum != 0 {
		m.HistoryFileNum = m.config.HistoryFileNum
	}
}

func (m *HistoryServer) getCurrentLogFileName() string {
	// Round current time down to the nearest time bucket
	currentTimeInSeconds := time.Now().Unix()
	currentTimeBucketSeconds := m.HistoryFileDurationSecondSpans * (currentTimeInSeconds / m.HistoryFileDurationSecondSpans)
	return m.getLogFileNameByTime(currentTimeBucketSeconds)
}

func (m *HistoryServer) getLogFileNameByTime(timeBucket int64) string {
	desiredTime := time.Unix(timeBucket, 0).UTC()
	return fmt.Sprintf("%v/%v.json", m.LogsFolderPath, desiredTime.Format(logFileTimeFormat))
}

func (m *HistoryServer) Start() {
	go func() {
		var pendingLogs []HistoryEntry

		for {
			select {
			case entry := <-m.requests:
				pendingLogs = append(pendingLogs, entry)
				m.logs = append(m.logs, entry)

				if len(pendingLogs) == m.PendingLogsBatchSize {
					m.writeMonitoringBatch(pendingLogs)
					pendingLogs = pendingLogs[:0]
				}
			case <-time.After(m.WriteMonitoringLogsTimeout):
				if len(pendingLogs) > 0 {
					m.writeMonitoringBatch(pendingLogs)
					pendingLogs = pendingLogs[:0]
				}
			}
		}
	}()
}

func (m *HistoryServer) createDefaultV3ioClient() {

	session := &frames.Session{}

	token := os.Getenv("V3IO_ACCESS_KEY")
	if token == "" {
		m.logger.Error("can not create v3io.client. could not find `V3IO_ACCESS_KEY` environment variable")
		return
	}

	var err error
	m.container, err = m.createV3ioClient(session, "", token)
	if err != nil {
		m.logger.Error(err)
		return
	}
	m.isActive = true
}

func (m *HistoryServer) createV3ioClient(session *frames.Session, password string, token string) (v3io.Container, error) {
	newContextInput := &v3iohttp.NewContextInput{
		HTTPClient: v3iohttp.NewClient(&v3iohttp.NewClientInput{}),
	}
	// create a context for the backend
	v3ioContext, err := v3iohttp.NewContext(m.logger, newContextInput)
	if err != nil {
		return nil, errors.Wrap(err, "can not create v3io-go context for Frames History server")
	}

	session = frames.InitSessionDefaults(session, m.config)
	session.Container = m.LogsContainer
	container, err := v3ioutils.NewContainer(
		v3ioContext,
		session,
		password,
		token,
		m.logger)

	if err != nil {
		return nil, errors.Wrap(err, "can not create v3io-go container for Frames History server")
	}
	return container, nil
}

func (m *HistoryServer) AddReadLog(readRequest *frames.ReadRequest, duration time.Duration, startTime time.Time) {
	if !m.isActive {
		return
	}

	// append the entry in a different goroutine so that it won't block
	go func() {
		entry := HistoryEntry{BackendName: readRequest.Proto.Backend,
			UserName:       readRequest.Proto.Session.User,
			TableName:      readRequest.Proto.Table,
			StartTime:      startTime,
			ActionDuration: duration,
			ActionType:     readType,
			AdditionalData: readRequest.ToMap(),
			Container:      readRequest.Proto.Session.Container}

		m.requests <- entry
	}()
}

func (m *HistoryServer) AddWriteLog(writeRequest *frames.WriteRequest, duration time.Duration, startTime time.Time) {
	if !m.isActive {
		return
	}

	// append the entry in a different goroutine so that it won't block
	go func() {
		entry := HistoryEntry{BackendName: writeRequest.Backend,
			UserName:       writeRequest.Session.User,
			TableName:      writeRequest.Table,
			StartTime:      startTime,
			ActionDuration: duration,
			ActionType:     WriteType,
			AdditionalData: writeRequest.ToMap(),
			Container:      writeRequest.Session.Container}

		m.requests <- entry
	}()
}

func (m *HistoryServer) AddCreateLog(createRequest *frames.CreateRequest, duration time.Duration, startTime time.Time) {
	if !m.isActive {
		return
	}

	// append the entry in a different goroutine so that it won't block
	go func() {
		entry := HistoryEntry{BackendName: createRequest.Proto.Backend,
			UserName:       createRequest.Proto.Session.User,
			TableName:      createRequest.Proto.Table,
			StartTime:      startTime,
			ActionDuration: duration,
			ActionType:     createType,
			AdditionalData: createRequest.ToMap(),
			Container:      createRequest.Proto.Session.Container}

		m.requests <- entry
	}()
}

func (m *HistoryServer) AddDeleteLog(deleteRequest *frames.DeleteRequest, duration time.Duration, startTime time.Time) {
	if !m.isActive {
		return
	}

	// append the entry in a different goroutine so that it won't block
	go func() {
		entry := HistoryEntry{BackendName: deleteRequest.Proto.Backend,
			UserName:       deleteRequest.Proto.Session.User,
			TableName:      deleteRequest.Proto.Table,
			StartTime:      startTime,
			ActionDuration: duration,
			ActionType:     deleteType,
			AdditionalData: deleteRequest.ToMap(),
			Container:      deleteRequest.Proto.Session.Container}

		m.requests <- entry
	}()
}

func (m *HistoryServer) AddExecuteLog(execRequest *frames.ExecRequest, duration time.Duration, startTime time.Time) {
	if !m.isActive {
		return
	}

	// append the entry in a different goroutine so that it won't block
	go func() {
		entry := HistoryEntry{BackendName: execRequest.Proto.Backend,
			UserName:       execRequest.Proto.Session.User,
			TableName:      execRequest.Proto.Table,
			StartTime:      startTime,
			ActionDuration: duration,
			ActionType:     executeType,
			AdditionalData: execRequest.ToMap(),
			Container:      execRequest.Proto.Session.Container}

		m.requests <- entry
	}()
}

func (m *HistoryServer) GetLogs(request *frames.HistoryRequest, out chan frames.Frame) error {
	if request.Proto.MinStartTime == "" {
		request.Proto.MinStartTime = "0"
	}
	if request.Proto.MaxStartTime == "" {
		request.Proto.MaxStartTime = "now"
	}
	startTime, err := utils.Str2unixTime(request.Proto.MinStartTime)
	if err != nil {
		return errors.Wrap(err, "failed to parse 'start_time' filter")
	}
	endTime, err := utils.Str2unixTime(request.Proto.MaxStartTime)
	if err != nil {
		return errors.Wrap(err, "failed to parse 'end_time' filter")
	}

	filter := historyFilter{Backend: request.Proto.Backend,
		Action:       request.Proto.Action,
		User:         request.Proto.User,
		Table:        request.Proto.Table,
		Container:    request.Proto.Container,
		MinStartTime: startTime,
		MaxStartTime: endTime,
		MinDuration:  request.Proto.MinDuration,
		MaxDuration:  request.Proto.MaxDuration}

	iter, err := utils.NewAsyncItemsCursor(m.container,
		&v3io.GetItemsInput{Path: m.LogsFolderPath + "/", AttributeNames: []string{"__name"}},
		8, nil, m.logger)

	if err != nil {
		return fmt.Errorf("Failed to list Frames History log folder %v for read, err: %v", m.LogsFolderPath, err)
	}

	i := 1

	// Create default constant columns. Other type-specific column will be added during iteration
	var userNameColData, backendNameColData, tableNameColData, actionTypeColData, containerColData []string
	var actionDurationColData []int64
	var startTimeColData []time.Time
	var nullColumns []*pb.NullValuesMap
	var hasNullsInCurrentDF bool
	customColumnsByName := make(map[string][]string)
	var indexColData []int
	var rowsInCurrentDF int

	// go over all log files in the folder
	for iter.Next() {
		currentFilePath := path.Join(m.LogsFolderPath, iter.GetField("__name").(string))
		lineIterator, err := v3ioutils.NewFileContentLineIterator(currentFilePath, m.MaxBytesInNginxRequest, m.container)
		if err != nil {
			return errors.Wrapf(err, "Failed to get '%v'", currentFilePath)
		}

		for lineIterator.Next() {
			if rowsInCurrentDF == maxLogsInMessage {

				frame, err := createDF(userNameColData, backendNameColData, tableNameColData, actionTypeColData, containerColData,
					actionDurationColData, startTimeColData,
					indexColData, customColumnsByName, nullColumns, hasNullsInCurrentDF)
				if err != nil {
					return err
				}
				out <- frame

				rowsInCurrentDF = 0
				userNameColData = []string{}
				backendNameColData = []string{}
				tableNameColData = []string{}
				actionDurationColData = []int64{}
				startTimeColData = []time.Time{}
				actionTypeColData = []string{}
				containerColData = []string{}
				indexColData = []int{}

				customColumnsByName = make(map[string][]string)
				nullColumns = []*pb.NullValuesMap{}
				hasNullsInCurrentDF = false
			}

			var entry HistoryEntry

			if err := json.Unmarshal(lineIterator.At(), &entry); err != nil {
				return fmt.Errorf("failed to parse json: %v", err)

			}
			// filter out logs
			if !filter.filter(entry) {
				continue
			}

			userNameColData = append(userNameColData, entry.UserName)
			backendNameColData = append(backendNameColData, entry.BackendName)
			tableNameColData = append(tableNameColData, entry.TableName)
			actionDurationColData = append(actionDurationColData, entry.ActionDuration.Nanoseconds()/1e6)
			startTimeColData = append(startTimeColData, entry.StartTime)
			actionTypeColData = append(actionTypeColData, entry.ActionType)
			containerColData = append(containerColData, entry.Container)

			// Fill columns with nil if there was no value
			var currentNullColumns pb.NullValuesMap
			currentNullColumns.NullColumns = make(map[string]bool)

			for k, v := range entry.AdditionalData {
				// If this column already exists, append new data
				if _, ok := customColumnsByName[k]; !ok {
					// Create a new column with nil values up until this row
					data := make([]string, rowsInCurrentDF+1)
					data[rowsInCurrentDF] = v
					customColumnsByName[k] = data

					// Backwards set as null all previous rows of this column
					if rowsInCurrentDF > 0 {
						hasNullsInCurrentDF = true
						for i := 0; i < rowsInCurrentDF; i++ {
							nullColumns[i].NullColumns[k] = true
						}
					}
					for i := 0; i < rowsInCurrentDF; i++ {
						nullColumns[i].NullColumns[k] = true
						hasNullsInCurrentDF = true
					}
				} else {
					customColumnsByName[k] = append(customColumnsByName[k], v)
				}
			}

			// Add null values
			for columnName := range customColumnsByName {
				if _, ok := entry.AdditionalData[columnName]; !ok {
					customColumnsByName[columnName] = append(customColumnsByName[columnName], "")
					currentNullColumns.NullColumns[columnName] = true
					hasNullsInCurrentDF = true
				}
			}

			indexColData = append(indexColData, i)
			i++
			rowsInCurrentDF++
			nullColumns = append(nullColumns, &currentNullColumns)
		}

		if lineIterator.Error() != nil {
			return lineIterator.Error()
		}
	}

	if iter.Err() != nil {
		return iter.Err()
	}

	if len(indexColData) > 0 {
		frame, err := createDF(userNameColData, backendNameColData, tableNameColData, actionTypeColData, containerColData,
			actionDurationColData, startTimeColData,
			indexColData, customColumnsByName, nullColumns, hasNullsInCurrentDF)
		if err != nil {
			return err
		}

		out <- frame
	}

	return nil
}

func createDF(userNameColData, backendNameColData, tableNameColData, actionTypeColData, containerColData []string,
	actionDurationColData []int64, startTimeColData []time.Time,
	indexColData []int, customColumnsByName map[string][]string, nullColumns []*pb.NullValuesMap, hasNullsInCurrentDF bool) (frames.Frame, error) {
	var columns []frames.Column
	col, _ := frames.NewSliceColumn("UserName", userNameColData)
	columns = append(columns, col)
	col, _ = frames.NewSliceColumn("BackendName", backendNameColData)
	columns = append(columns, col)
	col, _ = frames.NewSliceColumn("TableName", tableNameColData)
	columns = append(columns, col)
	col, _ = frames.NewSliceColumn("ActionDuration", actionDurationColData)
	columns = append(columns, col)
	col, _ = frames.NewSliceColumn("StartTime", startTimeColData)
	columns = append(columns, col)
	col, _ = frames.NewSliceColumn("ActionType", actionTypeColData)
	columns = append(columns, col)
	col, _ = frames.NewSliceColumn("Container", containerColData)
	columns = append(columns, col)

	indexColumn, _ := frames.NewSliceColumn("id", indexColData)

	for colName, colData := range customColumnsByName {
		col, _ = frames.NewSliceColumn(colName, colData)
		columns = append(columns, col)
	}

	nullMask := nullColumns
	if !hasNullsInCurrentDF {
		nullMask = nil
	}

	frame, err := frames.NewFrameWithNullValues(columns, []frames.Column{indexColumn}, nil, nullMask)
	if err != nil {
		return nil, err
	}

	return frame, nil
}

func (m *HistoryServer) writeMonitoringBatch(logs []HistoryEntry) {
	for _, log := range logs {
		d, err := json.Marshal(log)
		if err != nil {
			m.logger.ErrorWith("Failed to marshal log to json", "error", err, "log", log)
			return
		}

		m.writeData = append(m.writeData, d...)
		m.writeData = append(m.writeData, '\n')
	}

	logFilePath := m.getCurrentLogFileName()
	input := &v3io.PutObjectInput{Path: logFilePath, Body: m.writeData, Append: true}
	err := m.container.PutObjectSync(input)

	var retryCount int
	for err != nil && retryCount < maxRetryNum {
		m.logger.WarnWith("failed to write history logs", "retry-num", retryCount, "error", err)
		// retry on 5xx errors
		if errWithStatusCode, ok := err.(v3ioerrors.ErrorWithStatusCode); ok && errWithStatusCode.StatusCode() >= 500 {
			input := &v3io.PutObjectInput{Path: logFilePath, Body: m.writeData, Append: true}
			err = m.container.PutObjectSync(input)

			retryCount++
		} else {
			break
		}
	}

	if err != nil {
		m.logger.ErrorWith("Failed to append Frames history logs to file", "error", err, "logs", logs)
	}

	// reset the slice for future reuse
	m.writeData = m.writeData[:0]
}

func (m *HistoryServer) StartEvictionTask() {
	go func() {
		for {
			// Calculate next time to tick - get the next log time-bucket (the next time we will create a new file)
			currentTimeInSeconds := time.Now().Unix()
			nextTimeBucket := m.HistoryFileDurationSecondSpans*(currentTimeInSeconds/m.HistoryFileDurationSecondSpans) + m.HistoryFileDurationSecondSpans

			// wait for next time to delete.
			time.Sleep(time.Duration(nextTimeBucket-currentTimeInSeconds) * time.Second)

			// Delete oldest file
			fileToDelete := nextTimeBucket - m.HistoryFileDurationSecondSpans*int64(m.HistoryFileNum)
			input := &v3io.DeleteObjectInput{Path: m.getLogFileNameByTime(fileToDelete)}
			err := m.container.DeleteObjectSync(input)
			if err != nil && !utils.IsNotExistsError(err) {
				m.logger.ErrorWith("failed to delete old log file", "log-path", input.Path, "error", err)
			}
		}
	}()
}

type historyFilter struct {
	Backend      string
	Table        string
	User         string
	Action       string
	Container    string
	MinStartTime int64
	MaxStartTime int64
	MinDuration  int64
	MaxDuration  int64
}

func (f historyFilter) filter(entry HistoryEntry) bool {
	if f.Table != "" && f.Table != entry.TableName {
		return false
	}

	if f.User != "" && f.User != entry.UserName {
		return false
	}

	if f.Action != "" && f.Action != entry.ActionType {
		return false
	}

	if f.Backend != "" && f.Backend != entry.BackendName {
		return false
	}

	if f.Container != "" && f.Container != entry.Container {
		return false
	}

	if f.MinStartTime > entry.StartTime.Unix()*1000 {
		return false
	}

	if f.MaxStartTime < entry.StartTime.Unix()*1000 {
		return false
	}

	if f.MinDuration > entry.ActionDuration.Nanoseconds()/1e6 {
		return false
	}

	if f.MaxDuration != 0 && f.MaxDuration < entry.ActionDuration.Nanoseconds()/1e6 {
		return false
	}

	return true
}
