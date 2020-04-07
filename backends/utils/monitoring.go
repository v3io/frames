package utils

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"time"

	"github.com/google/uuid"
	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/v3io/frames"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

const (
	writeMonitoringLogsTimeout = 10 * time.Second
	pendingLogsBatchSize       = 100
	logsPathPrefix             = "monitoring/frames"
	logsContainer              = "users"

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

	isActive bool
}

func NewMonitoring(logger logger.Logger) *Monitoring {
	logPath := fmt.Sprintf("%v/%v_%v.csv", historyLogsTablePathPrefix, uuid.New().String(), time.Now().Unix())

	mon := Monitoring{logFilePath: logPath,
		logger:   logger,
		requests: make(chan HistoryEntry, 100),
		isActive: true}

	mon.createLogFile()

	if mon.isActive {
		mon.Start()
	}
	return &mon
}

func (m *Monitoring) createLogFile() {
	err := os.Mkdir(historyLogsTablePathPrefix, 0775)

	if err != nil && !os.IsExist(err) {
		m.logger.Warn("Could not create frames log folder at: %v, err: %v", historyLogsTablePathPrefix, err)
		m.isActive = false
	}

	file, err := os.Create(m.logFilePath)
	if err != nil {
		m.logger.Warn("Frames history server failed to start. Can not create file %v, err: %v", m.logFilePath, err)
		m.isActive = false
	}
	defer file.Close()
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

	filter := historyFilter{Backend: request.Proto.Backend,
		Action:    request.Proto.Action,
		User:      request.Proto.User,
		Table:     request.Proto.Table,
		StartTime: startTime,
		EndTime:   endTime}

	files, err := ioutil.ReadDir(historyLogsTablePathPrefix)
	if err != nil {
		return nil, fmt.Errorf("Failed to list Frames History log folder %v for read, err: %v", historyLogsTablePathPrefix, err)
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
	for _, fileStatus := range files {
		file, err := os.Open(path.Join(historyLogsTablePathPrefix, fileStatus.Name()))
		if err != nil {
			return nil, fmt.Errorf("Failed to open Frames History log file %v for read, err: %v", m.logFilePath, err)
		}

		//columnByName := make(map[string]frames.ColumnBuilder)
		scanner := bufio.NewScanner(file)

		for scanner.Scan() {
			var currentRaw []HistoryEntry

			if err := json.Unmarshal([]byte(scanner.Text()), &currentRaw); err != nil {
				return nil, err
			}

			for _, entry := range currentRaw {
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

		if err := scanner.Err(); err != nil {
			return nil, err
		}
	}

	allColumns := defaultColumns

	for _, col := range customColumnsByName {
		allColumns = append(allColumns, col)
	}

	return frames.NewFrame(allColumns, []frames.Column{indexColumn}, nil)
}

func (m *Monitoring) writeMonitoringBatch(logs []HistoryEntry) {
	file, err := os.OpenFile(m.logFilePath, os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		m.logger.Warn("Failed to open Frames History log file %v for write, err: %v", m.logFilePath, err)
		return
	}
	defer file.Close()

	d, err := json.Marshal(logs)
	if err != nil {
		m.logger.Warn("Failed to marshal logs to json, err: %v. logs: %v", err, logs)
		return
	}
	_, err = file.Write(d)
	if err != nil {
		m.logger.Warn("Failed to append Frames History logs to file, err: %v. logs: %v", err, logs)
		return
	}
	_, err = file.WriteString("\n")
	if err != nil {
		m.logger.Warn("Failed to append new line to Frames History log, err: %v", err)
		return
	}

	err = file.Sync()
	if err != nil {
		m.logger.Warn("Failed to sync Frames History log file, err: %v", err)
		return
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
