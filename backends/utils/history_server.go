package utils

import (
	"encoding/json"
	"fmt"
	"github.com/v3io/frames/pb"
	"path"
	"time"

	"github.com/google/uuid"
	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/v3io/frames"
	"github.com/v3io/frames/v3ioutils"
	v3io "github.com/v3io/v3io-go/pkg/dataplane"
	v3iohttp "github.com/v3io/v3io-go/pkg/dataplane/http"
	v3ioerrors "github.com/v3io/v3io-go/pkg/errors"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

const (
	defaultWriteMonitoringLogsTimeout = 10 * time.Second
	defaultPendingLogsBatchSize       = 100
	defaultLogsFolderPath             = "/monitoring/frames"
	defaultLogsContainer              = "users"
	defaultMaxBytesInNginxRequest     = 5 * 1024 * 1024 // 5MB
	maxLogsInMessage                  = 2

	queryType = "query"
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
	logFilePath string
	logger      logger.Logger
	logs        []HistoryEntry
	requests    chan HistoryEntry
	container   v3io.Container
	config      *frames.Config
	isActive    bool

	WriteMonitoringLogsTimeout time.Duration
	PendingLogsBatchSize       int
	LogsFolderPath             string
	LogsContainer              string
	MaxBytesInNginxRequest     int
}

func NewHistoryServer(logger logger.Logger, cfg *frames.Config) *HistoryServer {
	mon := HistoryServer{
		logger:   logger,
		requests: make(chan HistoryEntry, 100),
		isActive: false,
		config:   cfg}

	mon.initDefaults()

	mon.logFilePath = fmt.Sprintf("%v/%v_%v.json", mon.LogsFolderPath, uuid.New().String(), time.Now().Unix())
	mon.createDefaultV3ioClient()
	if mon.isActive {
		mon.Start()
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

	token := "b515fb74-e89c-4885-a742-820794e6f9ca" // os.Getenv("V3IO_ACCESS_KEY")
	if token == "" {
		m.logger.Warn("can not create v3io.client. could not find `V3IO_ACCESS_KEY` environment variable")
		return
	}

	var err error
	m.container, err = m.createV3ioClient(session, "", token)
	if err != nil {
		m.logger.Warn(err)
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

func (m *HistoryServer) AddQueryLog(readRequest *frames.ReadRequest, duration time.Duration, startTime time.Time) {
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
			ActionType:     queryType,
			AdditionalData: readRequest.ToMap(),
			Container:      readRequest.Proto.Session.Container}

		m.requests <- entry
	}()
}

func (m *HistoryServer) GetLogs(request *frames.HistoryRequest, out chan frames.Frame) error {
	if request.Proto.StartTime == "" {
		request.Proto.StartTime = "0"
	}
	if request.Proto.EndTime == "" {
		request.Proto.EndTime = "now"
	}
	startTime, err := utils.Str2unixTime(request.Proto.StartTime)
	if err != nil {
		return errors.Wrap(err, "failed to parse 'start_time' filter")
	}
	endTime, err := utils.Str2unixTime(request.Proto.EndTime)
	if err != nil {
		return errors.Wrap(err, "failed to parse 'end_time' filter")
	}

	container, err := m.createV3ioClient(request.Proto.Session, request.Password.Get(), request.Token.Get())
	if err != nil {
		return err
	}

	filter := historyFilter{Backend: request.Proto.Backend,
		Action:    request.Proto.Action,
		User:      request.Proto.User,
		Table:     request.Proto.Table,
		Container: request.Proto.Container,
		StartTime: startTime,
		EndTime:   endTime}

	iter, err := utils.NewAsyncItemsCursor(container,
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

	// go over all log files in the folder
	for iter.Next() {
		currentFilePath := path.Join(m.LogsFolderPath, iter.GetField("__name").(string))
		lineIterator, err := v3ioutils.NewFileContentLineIterator(currentFilePath, m.MaxBytesInNginxRequest, container)
		if err != nil {
			return errors.Wrapf(err, "Failed to get '%v'", currentFilePath)
		}

		var rowsInCurrentDF int
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

			if err := json.Unmarshal([]byte(lineIterator.At()), &entry); err != nil {
				return fmt.Errorf("error reading logs. json marshal error:%v", err)

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

	frame, err := createDF(userNameColData, backendNameColData, tableNameColData, actionTypeColData, containerColData,
		actionDurationColData, startTimeColData,
		indexColData, customColumnsByName, nullColumns, hasNullsInCurrentDF)
	if err != nil {
		return err
	}

	out <- frame

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
	var data []byte

	for _, log := range logs {
		d, err := json.Marshal(log)
		if err != nil {
			m.logger.Warn("Failed to marshal logs to json, err: %v. logs: %v", err, logs)
			return
		}

		data = append(data, d...)
		data = append(data, []byte("\n")...)
	}

	input := &v3io.PutObjectInput{Path: m.logFilePath, Body: data, Append: true}
	err := m.container.PutObjectSync(input)

	if err != nil {
		// retry on 5xx errors
		if errWithStatusCode, ok := err.(v3ioerrors.ErrorWithStatusCode); ok && errWithStatusCode.StatusCode() > 500 {
			input := &v3io.PutObjectInput{Path: m.logFilePath, Body: data, Append: true}
			err = m.container.PutObjectSync(input)
		}

		if err != nil {
			m.logger.Error("Failed to append Frames History logs to file, err: %v. logs: %v", err, logs)
		}
	}
}

type historyFilter struct {
	Backend   string
	Table     string
	User      string
	Action    string
	Container string
	StartTime int64
	EndTime   int64
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

	if f.StartTime > entry.StartTime.Unix()*1000 {
		return false
	}

	if f.EndTime < entry.StartTime.Unix()*1000 {
		return false
	}

	return true
}
