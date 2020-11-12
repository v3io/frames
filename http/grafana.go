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

package http

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/v3io/frames"
	"github.com/v3io/frames/pb"
)

const querySeparator = ";"
const fieldsItemsSeperator = ","
const defaultBackend = "tsdb"

type outputType int

// Possible output types
const (
	tableOutputType     outputType = 0
	timeserieOutputType outputType = 1
)

type simpleJSONRequestInterface interface {
	GetReadRequest(*frames.Session) *frames.ReadRequest
	formatTSDB(ch chan frames.Frame) (interface{}, error)
	formatTable(ch chan frames.Frame) (interface{}, error)
	getFormatType() outputType
}

type requestSimpleJSONBase struct {
	Range struct {
		From string `json:"from"`
		To   string `json:"to"`
	} `json:"range"`
	Targets        []map[string]interface{} `json:"targets"`
	Target         string                   `json:"target"`
	MaxDataPoints  int                      `json:"maxDataPoints"`
	responseCreate simpleJSONRequestInterface
}

type simpleJSONQueryRequest struct {
	requestSimpleJSONBase
	Filter    string
	Fields    []string
	Table     string
	Type      string
	Backend   string
	From      string
	To        string
	Container string
	Step      string
	Query     string
}

type simpleJSONSearchRequest struct {
	simpleJSONQueryRequest
}

type tableColumn struct {
	Text string `json:"text"`
	Type string `json:"type"`
}

type tableOutput struct {
	Columns []tableColumn   `json:"columns"`
	Rows    [][]interface{} `json:"rows"`
	Type    string          `json:"type"`
}

type timeSeriesOutput struct {
	Datapoints [][]interface{} `json:"datapoints"`
	Target     string          `json:"target"`
}

func simpleJSONRequestFactory(method string, request []byte) ([]simpleJSONRequestInterface, error) {
	var reqBase requestSimpleJSONBase
	if err := json.Unmarshal(request, &reqBase); err != nil {
		return nil, err
	}
	var requests []simpleJSONRequestInterface
	switch method {
	case "query":
		for _, target := range reqBase.Targets {
			currRequest := &simpleJSONQueryRequest{Backend: defaultBackend, requestSimpleJSONBase: reqBase}
			currRequest.Type = target["type"].(string)
			fieldInput := target["target"].(string)
			if err := currRequest.parseQueryLine(fieldInput); err != nil {
				return nil, errors.Wrap(err, "Failed to parse target")
			}

			requests = append(requests, currRequest)
		}
	case "search":
		currRequest :=
			&simpleJSONSearchRequest{simpleJSONQueryRequest{requestSimpleJSONBase: reqBase}}
		err := currRequest.ParseRequestFromTarget()
		if err != nil {
			return nil, err
		}

		if currRequest.Backend != "" && currRequest.Table != ""{
			requests = append(requests, currRequest)
		}
	default:
		return nil, fmt.Errorf("Unknown method, %s", method)
	}

	return requests, nil
}

func (req *simpleJSONQueryRequest) getFormatType() outputType {
	switch req.Type {
	case "table":
		return tableOutputType
	case "timeseries", "timeserie":
		return timeserieOutputType
	}
	return timeserieOutputType
}

func (req *simpleJSONSearchRequest) getFormatType() outputType {
	return tableOutputType
}

func (req *simpleJSONQueryRequest) GetReadRequest(session *frames.Session) *frames.ReadRequest {
	if session == nil {
		session = &frames.Session{Container: req.Container}
	} else {
		// don't overide the container (if one is already set)
		if session.Container == "" {
			session.Container = req.Container
		}
	}
	if req.Backend == "tsdb" {
		return &frames.ReadRequest{Proto: &pb.ReadRequest{
			Backend: req.Backend,
			Table:   req.Table,
			Columns: req.Fields,
			Start:   req.Range.From,
			End:     req.Range.To,
			Step:    req.Step,
			Session: session,
			Filter:  req.Filter,
			Query:   req.Query}}
	}
	return &frames.ReadRequest{Proto: &pb.ReadRequest{
		Backend: req.Backend,
		Table:   req.Table,
		Columns: req.Fields,
		Session: session,
		Filter:  req.Filter}}
}

func (req *simpleJSONQueryRequest) formatTable(ch chan frames.Frame) (interface{}, error) {
	var retVal []tableOutput
	var err error
	for frame := range ch {
		simpleJSONData := tableOutput{Type: "table", Rows: [][]interface{}{}, Columns: []tableColumn{}}
		fields := req.getFieldNames(frame)
		simpleJSONData.Columns, err = prepareKVColumns(frame, fields)
		if err != nil {
			return nil, err
		}

		iter := frame.IterRows(true)
		for iter.Next() {
			rowData := iter.Row()
			simpleJSONRow := []interface{}{}
			for _, field := range fields {
				if isValidData(rowData[field]) {
					simpleJSONRow = append(simpleJSONRow, rowData[field])
				}
			}
			if len(simpleJSONRow) > 0 {
				simpleJSONData.Rows = append(simpleJSONData.Rows, simpleJSONRow)
			}
		}
		if err := iter.Err(); err != nil {
			return nil, err
		}
		retVal = append(retVal, simpleJSONData)
	}
	return retVal, nil
}

func (req *simpleJSONQueryRequest) formatTSDB(ch chan frames.Frame) (interface{}, error) {
	data := map[string][][]interface{}{}
	fieldToTargets := map[string]map[string]bool{}
	for frame := range ch {
		frameTarget := getBaseTargetTSDB(frame)
		fields := req.getFieldNames(frame)
		iter := frame.IterRows(true)
		for iter.Next() {
			rowData := iter.Row()
			timestamp := formatTimeTSDB(rowData["time"])
			for _, field := range fields {
				target := field + frameTarget
				if _, ok := data[target]; !ok {
					data[target] = [][]interface{}{}
				}
				if isValidData(rowData[field]) {
					targets, ok := fieldToTargets[field]
					if !ok {
						targets = make(map[string]bool)
						fieldToTargets[field] = targets
					}
					targets[target] = true
					data[target] = append(data[target], []interface{}{rowData[field], timestamp})
				}
			}
		}
		if err := iter.Err(); err != nil {
			return nil, err
		}
	}

	var retval []timeSeriesOutput

	if len(req.Fields) == 0 || req.Fields[0] == "*" { // return all fields, sorted
		for target, datapoints := range data {
			retval = append(retval, timeSeriesOutput{datapoints, target})
		}
		sort.Slice(retval, func(i int, j int) bool {
			return retval[i].Target < retval[j].Target
		})
	} else { // return requested fields, in the order requested, sorted internally by label
		for _, field := range req.Fields {
			targetsAsMap := fieldToTargets[field]
			targets := make([]string, 0, len(targetsAsMap))
			for target := range targetsAsMap {
				targets = append(targets, target)
			}
			sort.Strings(targets)
			for _, target := range targets {
				retval = append(retval, timeSeriesOutput{data[target], target})
			}
		}
	}

	return retval, nil
}

func CreateResponse(req simpleJSONRequestInterface, ch chan frames.Frame) (interface{}, error) {
	switch req.getFormatType() {
	case tableOutputType:
		return req.formatTable(ch)
	case timeserieOutputType:
		return req.formatTSDB(ch)
	}
	return nil, fmt.Errorf("Unknown format")
}

func (req *simpleJSONSearchRequest) formatTable(ch chan frames.Frame) (interface{}, error) {
	var retval []interface{}
	for frame := range ch {
		iter := frame.IterRows(true)
		for iter.Next() {
			rowData := iter.Row()
			for _, field := range req.Fields {
				retval = append(retval, rowData[field])
			}
		}
		if err := iter.Err(); err != nil {
			return nil, err
		}
	}
	return retval, nil
}

func (req *simpleJSONSearchRequest) formatTSDB(ch chan frames.Frame) (interface{}, error) {
	return nil, errors.New("TSDB search not implemented yet")
}

func (req *simpleJSONSearchRequest) ParseRequestFromTarget() error {
	for _, target := range strings.Split(req.Target, querySeparator) {
		if err := req.parseQueryLine(strings.TrimSpace(target)); err != nil {
			return err
		}
	}
	return nil
}

func (req *simpleJSONQueryRequest) parseQueryLine(fieldInput string) error {
	// example query: fields=sentiment;table=stock_metrics;backend=tsdb;filter=symbol=="AAPL";container=container_name
	re, err := regexp.Compile(`^\s*(filter|fields|table|backend|container|step|query)\s*=\s*(.*)\s*$`)
	if err != nil {
		return err
	}
	for _, field := range strings.Split(fieldInput, querySeparator) {
		match := re.FindStringSubmatch(field)
		var value interface{}
		if len(match) > 0 {
			fieldName := strings.Title(match[1])
			if fieldName == "Fields" {
				value = strings.Split(match[2], fieldsItemsSeperator)
			} else {
				value = match[2]
			}
			if err := setField(req, fieldName, value); err != nil {
				return err
			}
		}
	}
	return nil
}

func isValidData(fieldData interface{}) bool {
	if v, ok := fieldData.(float64); ok {
		return !math.IsNaN(v)
	}
	return true
}

func setField(obj interface{}, name string, value interface{}) error {
	structValue := reflect.ValueOf(obj).Elem()
	structFieldValue := structValue.FieldByName(name)

	if !structFieldValue.IsValid() {
		return fmt.Errorf("No such field: %s in obj", name)
	}

	if !structFieldValue.CanSet() {
		return fmt.Errorf("Cannot set %s field value", name)
	}

	structFieldType := structFieldValue.Type()
	val := reflect.ValueOf(value)
	if structFieldType != val.Type() {
		return errors.New("Provided value type didn't match obj field type")
	}

	structFieldValue.Set(val)
	return nil
}

func formatTimeTSDB(timestamp interface{}) interface{} {
	if val, ok := timestamp.(time.Time); ok {
		return val.UnixNano() / 1000000
	}
	return timestamp
}

func getBaseTargetTSDB(frame frames.Frame) string {
	lbls := []string{}
	var keys []string
	labels := frame.Labels()

	// Iterating over map produces inconsistent result so we need to sort the keys beforehand.
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		lbls = append(lbls, fmt.Sprintf("%s=%s", key, labels[key]))
	}
	return fmt.Sprintf("[%s]", strings.Join(lbls, ","))
}

func (req *simpleJSONQueryRequest) getFieldNames(frame frames.Frame) []string {
	retVal := req.Fields
	if len(retVal) == 0 || retVal[0] == "*" {
		retVal = frame.Names()
		if req.getFormatType() == timeserieOutputType {
			retVal = getMetricNames(frame)
		} else if len(frame.Indices()) > 0 {
			retVal = append(retVal, frame.Indices()[0].Name())
		}
		sort.Strings(retVal)
	}
	return retVal
}

func getMetricNames(frame frames.Frame) []string {
	retVal := []string{}
	for _, name := range frame.Names() {
		if _, ok := frame.Labels()[name]; !ok {
			retVal = append(retVal, name)
		}
	}

	return retVal
}

func prepareKVColumns(frame frames.Frame, headers []string) ([]tableColumn, error) {
	var retVal []tableColumn
	for _, header := range headers {
		column, err := findColumnOrIndices(frame, header)
		if err != nil {
			return nil, err
		}
		retVal = append(retVal, prepareKVColumnFormat(column, header))
	}
	return retVal, nil
}

func findColumnOrIndices(frame frames.Frame, col string) (frames.Column, error) {
	column, err := frame.Column(col)
	if err != nil {
		if len(frame.Indices()) > 0 && frame.Indices()[0].Name() == col {
			return frame.Indices()[0], nil
		}
		return nil, err
	}

	return column, nil
}

func prepareKVColumnFormat(column frames.Column, field string) tableColumn {
	columnTypeStr := "string"
	switch column.DType() {
	case frames.FloatType, frames.IntType:
		columnTypeStr = "number"
	case frames.TimeType:
		columnTypeStr = "time"
	case frames.BoolType:
		columnTypeStr = "boolean"
	}
	return tableColumn{Text: field, Type: columnTypeStr}
}
