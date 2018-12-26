package simplejson

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/v3io/frames"
	"github.com/v3io/frames/pb"
)

type SimpleJsonRequestInterface interface {
	CreateResponse(frames.FrameIterator) (interface{}, error)
	ParseRequest([]byte) error
	GetReadRequest() *frames.ReadRequest
}

type RequestSimpleJsonBase struct {
	PanelId        int                      `json:"panelId"`
	Range          map[string]interface{}   `json:"range"`
	RangeRaw       map[string]string        `json:"rangeRaw"`
	Interval       string                   `json:"interval"`
	IntervalMs     int                      `json:"intervalMs"`
	Targets        []map[string]interface{} `json:"targets"`
	Target         string                   `json:"target"`
	Format         string                   `json:"format"`
	MaxDataPoints  int                      `json:"maxDataPoints"`
	Path           string                   `json:"path"`
	responseCreate SimpleJsonRequestInterface
}

type SimpleJsonQueryRequest struct {
	RequestSimpleJsonBase
	Filter    string
	Fields    []string
	TableName string
	Type      string
	Backend   string
	From      string
	To        string
	Container string
	Username  string
	Password  string
}

type SimpleJsonSearchRequest struct {
	SimpleJsonQueryRequest
}

type TableOutput struct {
	Columns []map[string]string `json:"columns"`
	Rows    [][]interface{}     `json:"rows"`
	Type    string              `json:"type"`
}

type TimeSeriesOutput struct {
	Datapoints [][]interface{} `json:"datapoints"`
	Target     string          `json:"target"`
}

const QUERY_SEPARATOR = ";"
const FIELDS_ITEMS_SEPARATOR = ","
const DEFAULT_BACKEND = "tsdb"

func SimpleJsonRequestFactory(method string, request []byte) (SimpleJsonRequestInterface, error) {
	var retval SimpleJsonRequestInterface
	switch method {
	case "query":
		retval = &SimpleJsonQueryRequest{Backend: DEFAULT_BACKEND}
		break
	case "search":
		retval = &SimpleJsonSearchRequest{SimpleJsonQueryRequest{Backend: DEFAULT_BACKEND}}
		break
	default:
		return nil, fmt.Errorf("Unknown method, %s", method)
	}

	if err := retval.ParseRequest(request); err != nil {
		return nil, err
	}
	return retval, nil
}

func (req *SimpleJsonQueryRequest) GetReadRequest() *frames.ReadRequest {
	return &frames.ReadRequest{Backend: req.Backend, Table: req.TableName, Columns: req.Fields, Start: req.From, End: req.To, Step: "60m",
		Session: &pb.Session{Container: req.Container, User: req.Username, Password: req.Password},
		Filter:  req.Filter}
}

func (req *SimpleJsonQueryRequest) formatKV(iter frames.FrameIterator) (interface{}, error) {
	columns := []map[string]string{}
	rows := [][]interface{}{}
	for iter.Next() {
		frame := iter.At()
		iface, ok := frame.(pb.Framed)
		if !ok {
			return nil, errors.New("unknown frame type")
		}
		for _, column := range iface.Proto().Columns {
			values, colType := req.extractColumnValuesTSDB(column)
			columns = append(columns, map[string]string{"text": column.Name, "type": colType})
			for rowIndex := 0; rowIndex < values.Len(); rowIndex++ {
				if len(rows) <= rowIndex {
					rows = append(rows, []interface{}{})
				}
				value := values.Index(rowIndex).Interface()
				rows[rowIndex] = append(rows[rowIndex], value)
			}
		}
	}
	if iter.Err() != nil {
		return nil, iter.Err()
	}
	return []TableOutput{TableOutput{columns, rows, req.Type}}, nil
}

func (req *SimpleJsonQueryRequest) formatTSDB(iter frames.FrameIterator) (interface{}, error) {
	retval := []TimeSeriesOutput{}
	for iter.Next() {
		frame := iter.At()
		iface, ok := frame.(pb.Framed)
		if !ok {
			return nil, errors.New("unknown frame type")
		}
		target := req.getTargetTSDB(iface.Proto())
		indices := frame.Indices()
		for _, column := range iface.Proto().Columns {
			if column.Name == "values" {
				datapoints := [][]interface{}{}
				values, _ := req.extractColumnValuesTSDB(column)
				times, err := indices[0].Times()
				if err != nil {
					return nil, err
				}
				for j := 0; j < values.Len(); j++ {
					datapoints = append(datapoints, []interface{}{values.Index(j).Interface(), req.formatTimeTSDB(times[j])})
				}
				retval = append(retval, TimeSeriesOutput{datapoints, target})
			}
		}
	}
	if iter.Err() != nil {
		return nil, iter.Err()
	}
	return retval, nil
}

func (req *SimpleJsonQueryRequest) extractColumnValuesTSDB(column *pb.Column) (reflect.Value, string) {
	colType := strings.ToLower(column.Dtype.String())
	dataTypes := strings.Title(fmt.Sprintf("%ss", colType))
	return reflect.Indirect(reflect.ValueOf(column)).FieldByName(dataTypes), colType
}

func (req *SimpleJsonQueryRequest) formatTimeTSDB(timestamp time.Time) interface{} {
	return timestamp.UnixNano() / 1000000
}
func (req *SimpleJsonQueryRequest) getTargetTSDB(frame *pb.Frame) string {
	labels := ""
	metricName := ""
	for _, column := range frame.Columns {
		if column.Name != "values" {
			values, _ := req.extractColumnValuesTSDB(column)
			val := fmt.Sprintf("%s", values.Index(0).Interface())
			if column.Name == "metric_name" {
				metricName = val
			} else {
				labels += fmt.Sprintf("[%s=%s]", column.Name, val)
			}
		}
	}
	return metricName + labels
}

func (req *SimpleJsonQueryRequest) CreateResponse(iter frames.FrameIterator) (interface{}, error) {
	formatters := map[string]func(iter frames.FrameIterator) (interface{}, error){"kv": req.formatKV, "tsdb": req.formatTSDB}
	if formatter, ok := formatters[req.Backend]; ok {
		return formatter(iter)
	}
	return nil, fmt.Errorf("Unknown format: %s", req.Backend)
}

func (req *SimpleJsonSearchRequest) CreateResponse(iter frames.FrameIterator) (interface{}, error) {
	// Placeholder for actual implementation
	return []string{}, nil
}

func (req *SimpleJsonQueryRequest) ParseRequest(requestBody []byte) error {
	if err := json.Unmarshal(requestBody, req); err != nil {
		return err
	}
	for _, target := range req.Targets {
		req.Type = target["type"].(string)
		fieldInput := target["target"].(string)
		if err := req.parseQueryLine(fieldInput); err != nil {
			return err
		}
	}
	req.parseRange()
	return nil
}

func (req *SimpleJsonSearchRequest) ParseRequest(requestBody []byte) error {
	if err := json.Unmarshal(requestBody, req); err != nil {
		return err
	}
	for _, target := range strings.Split(req.Target, QUERY_SEPARATOR) {
		fmt.Println(target)
		if err := req.parseQueryLine(strings.TrimSpace(target)); err != nil {
			return err
		}
	}
	return nil
}

func (req *SimpleJsonQueryRequest) parseQueryLine(fieldInput string) error {
	translate := map[string]string{"table_name": "TableName"}
	re := regexp.MustCompile(`^\s*(filter|fields|table_name|backend|container|username|password)\s*=\s*(.*)\s*$`)
	for _, field := range strings.Split(fieldInput, QUERY_SEPARATOR) {
		match := re.FindStringSubmatch(field)
		var value interface{}
		if len(match) > 0 {
			fieldName := strings.Title(match[1])
			if fieldNameTranslated, ok := translate[match[1]]; ok {
				fieldName = fieldNameTranslated
			}
			if fieldName == "Fields" {
				value = strings.Split(match[2], FIELDS_ITEMS_SEPARATOR)
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

func (req *SimpleJsonQueryRequest) parseRange() {
	req.From = req.Range["from"].(string)
	req.To = req.Range["to"].(string)
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
