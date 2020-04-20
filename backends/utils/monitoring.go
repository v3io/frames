package utils

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/google/uuid"
	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/v3io/frames"
	"github.com/v3io/frames/v3ioutils"
	v3io "github.com/v3io/v3io-go/pkg/dataplane"
	v3iohttp "github.com/v3io/v3io-go/pkg/dataplane/http"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

const (
	writeMonitoringLogsTimeout = 10 * time.Second
	pendingLogsBatchSize       = 100
	logsPathPrefix             = "/monitoring/frames"
	logsContainer              = "users"
	maxBytesInNginxRequest     = 5 * 1024 * 1024 // 5MB

	queryType = "query"
)

var (
	historyLogsTablePathPrefix = fmt.Sprintf("/v3io/%v/%v", logsContainer, logsPathPrefix)
)

type HistoryEntry struct {
	ActionType     string
	UserName       string
	BackendName    string
	TableName      string
	ActionDuration time.Duration
	StartTime      time.Time

	AdditionalData map[string]string
}

type Monitoring struct {
	logFilePath string
	logger      logger.Logger
	logs        []HistoryEntry
	requests    chan HistoryEntry
	container   v3io.Container
	config      *frames.Config
	isActive    bool
}

func NewMonitoring(logger logger.Logger, cfg *frames.Config) *Monitoring {
	logPath := fmt.Sprintf("%v/%v_%v.json", logsPathPrefix, uuid.New().String(), time.Now().Unix())

	mon := Monitoring{logFilePath: logPath,
		logger:   logger,
		requests: make(chan HistoryEntry, 100),
		isActive: false,
		config:   cfg}

	mon.createDefaultV3ioClient()
	if mon.isActive {
		mon.Start()
	}
	return &mon
}

func (m *Monitoring) Start() {
	go func() {
		var pendingLogs []HistoryEntry

		for {
			select {
			case entry := <-m.requests:
				pendingLogs = append(pendingLogs, entry)
				m.logs = append(m.logs, entry)

				if len(pendingLogs) == pendingLogsBatchSize {
					m.writeMonitoringBatch(pendingLogs)
					pendingLogs = pendingLogs[:0]
				}
			case <-time.After(writeMonitoringLogsTimeout):
				if len(pendingLogs) > 0 {
					m.writeMonitoringBatch(pendingLogs)
					pendingLogs = pendingLogs[:0]
				}
			}
		}
	}()
}

func (m *Monitoring) createDefaultV3ioClient() {

	session := &frames.Session{}

	token := os.Getenv("V3IO_ACCESS_KEY") // "0938b76f-79ff-41f6-981d-864ea3291cca"
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

func (m *Monitoring) createV3ioClient(session *frames.Session, password string, token string) (v3io.Container, error) {
	newContextInput := &v3iohttp.NewContextInput{
		HTTPClient: v3iohttp.NewClient(&v3iohttp.NewClientInput{}),
	}
	// create a context for the backend
	v3ioContext, err := v3iohttp.NewContext(m.logger, newContextInput)
	if err != nil {
		return nil, errors.Wrap(err, "can not create v3io-go context for Frames History server")
	}

	session = frames.InitSessionDefaults(session, m.config)
	session.Container = logsContainer
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

func (m *Monitoring) AddQueryLog(readRequest *frames.ReadRequest, duration time.Duration, startTime time.Time) {
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
			AdditionalData: readRequest.ToMap()}

		m.requests <- entry
	}()
}

func (m *Monitoring) GetLogs(request *frames.HistoryRequest) (frames.Frame, error) {
	if request.Proto.StartTime == "" {
		request.Proto.StartTime = "0"
	}
	if request.Proto.EndTime == "" {
		request.Proto.EndTime = "now"
	}
	startTime, err := utils.Str2unixTime(request.Proto.StartTime)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse 'start_time' filter")
	}
	endTime, err := utils.Str2unixTime(request.Proto.EndTime)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse 'end_time' filter")
	}

	// maybe delete this part
	container, err := m.createV3ioClient(request.Proto.Session, request.Password.Get(), request.Token.Get())
	if err != nil {
		return nil, err
	}
	// ====

	filter := historyFilter{Backend: request.Proto.Backend,
		Action:    request.Proto.Action,
		User:      request.Proto.User,
		Table:     request.Proto.Table,
		StartTime: startTime,
		EndTime:   endTime}

	iter, err := utils.NewAsyncItemsCursor(container,
		&v3io.GetItemsInput{Path: logsPathPrefix + "/", AttributeNames: []string{"__name"}},
		8, nil, m.logger)

	if err != nil {
		return nil, fmt.Errorf("Failed to list Frames History log folder %v for read, err: %v", logsPathPrefix, err)
	}

	// Create default constant columns. Other type-specific column will be added during iteration
	defaultColumns := make([]frames.Column, 6)
	defaultColumns[0], _ = frames.NewSliceColumn("UserName", []string{})
	defaultColumns[1], _ = frames.NewSliceColumn("BackendName", []string{})
	defaultColumns[2], _ = frames.NewSliceColumn("TableName", []string{})
	defaultColumns[3], _ = frames.NewSliceColumn("ActionDuration", []int64{})
	defaultColumns[4], _ = frames.NewSliceColumn("StartTime", []time.Time{})
	defaultColumns[5], _ = frames.NewSliceColumn("ActionType", []string{})
	customColumnsByName := make(map[string]frames.Column)
	indexColumn, _ := frames.NewSliceColumn("id", []int{})
	i := 1

	// go over all log files in the folder
	for iter.Next() {
		currentFilePath := path.Join(logsPathPrefix, iter.GetField("__name").(string))
		fileIterator, err := v3ioutils.NewFileContentIterator(currentFilePath, maxBytesInNginxRequest, container)
		if err != nil {
			return nil, errors.Wrapf(err, "Failed to get '%v'", currentFilePath)
		}

		if !fileIterator.Next() {
			if fileIterator.Error() != nil {
				return nil, err
			}
			continue
		}

	ScanningFileChunk:
		scanner := bufio.NewScanner(bytes.NewReader(fileIterator.At()))
		for scanner.Scan() {
			var currentRow []HistoryEntry

			if err := json.Unmarshal([]byte(scanner.Text()), &currentRow); err != nil {
				// Possibly reached end of file chunk, get next chunk and retry marshaling
				if fileIterator.Next() {
					scanner = bufio.NewScanner(bytes.NewReader(append(scanner.Bytes(), fileIterator.At()...)))
					if scanner.Scan() {
						if innerErr := json.Unmarshal([]byte(scanner.Text()), &currentRow); innerErr != nil {
							return nil, innerErr
						}
					} else {
						return nil, scanner.Err()
					}
				} else {
					return nil, fmt.Errorf("error reading logs. json marshal error:%v\n iterator error: %v", err, fileIterator.Error())
				}
			}

			for _, entry := range currentRow {
				// filter out logs
				if !filter.filter(entry) {
					continue
				}
				_ = AppendColumn(defaultColumns[0], entry.UserName)
				_ = AppendColumn(defaultColumns[1], entry.BackendName)
				_ = AppendColumn(defaultColumns[2], entry.TableName)
				_ = AppendColumn(defaultColumns[3], entry.ActionDuration.Nanoseconds()/1e6)
				_ = AppendColumn(defaultColumns[4], entry.StartTime)
				_ = AppendColumn(defaultColumns[5], entry.ActionType)

				for k, v := range entry.AdditionalData {
					// If this column already exists, append new data
					if _, ok := customColumnsByName[k]; !ok {
						// Create a new column with nil values up until this row
						data := make([]string, i)
						data[i-1] = v
						customColumnsByName[k], err = frames.NewSliceColumn(k, data)
						if err != nil {
							return nil, err
						}
					} else {
						_ = AppendColumn(customColumnsByName[k], v)
					}
				}

				// Add null values
				for columnName, col := range customColumnsByName {
					if _, ok := entry.AdditionalData[columnName]; !ok {
						_ = AppendNil(col)
					}
				}

				_ = AppendColumn(indexColumn, i)
				i++
			}
		}

		if scanner.Err() != nil {
			return nil, err
		}

		// in case previous chunk ended exactly in the end of a row and we still got more data to process
		if fileIterator.Next() {
			goto ScanningFileChunk
		} else if fileIterator.Error() != nil {
			return nil, fileIterator.Error()
		}
	}

	if iter.Err() != nil {
		return nil, iter.Err()
	}
	allColumns := defaultColumns

	for _, col := range customColumnsByName {
		allColumns = append(allColumns, col)
	}

	return frames.NewFrame(allColumns, []frames.Column{indexColumn}, nil)
}

func (m *Monitoring) writeMonitoringBatch(logs []HistoryEntry) {
	d, err := json.Marshal(logs)
	if err != nil {
		m.logger.Warn("Failed to marshal logs to json, err: %v. logs: %v", err, logs)
		return
	}

	d = append(d, []byte("\n")...)
	input := &v3io.PutObjectInput{Path: m.logFilePath, Body: d, Append: true}
	err = m.container.PutObjectSync(input)
	if err != nil {
		m.logger.Warn("Failed to append Frames History logs to file, err: %v. logs: %v", err, logs)
	}
}

type historyFilter struct {
	Backend   string
	Table     string
	User      string
	Action    string
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

	if f.StartTime > entry.StartTime.Unix()*1000 {
		return false
	}

	if f.EndTime < entry.StartTime.Unix()*1000 {
		return false
	}

	return true
}
