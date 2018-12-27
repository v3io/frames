package http

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

const querySeparator = ";"
const fieldsItemsSeperator = ","
const defaultBackend = "tsdb"

type simpleJsonRequestInterface interface {
	CreateResponse(frames.FrameIterator) (interface{}, error)
	ParseRequest([]byte) error
	GetReadRequest(*pb.Session) *frames.ReadRequest
}

type requestSimpleJsonBase struct {
	Range struct {
		From string `json:"from"`
		To   string `json:"to"`
	} `json:"range"`
	Targets        []map[string]interface{} `json:"targets"`
	Target         string                   `json:"target"`
	MaxDataPoints  int                      `json:"maxDataPoints"`
	responseCreate simpleJsonRequestInterface
}

type simpleJsonQueryRequest struct {
	requestSimpleJsonBase
	Filter    string
	Fields    []string
	TableName string
	Type      string
	Backend   string
	From      string
	To        string
	Container string
}

type simpleJsonSearchRequest struct {
	simpleJsonQueryRequest
}

type Column struct {
	Text string `json:"text"`
	Type string `json:"type"`
}

type TableOutput struct {
	Columns []Column        `json:"columns"`
	Rows    [][]interface{} `json:"rows"`
	Type    string          `json:"type"`
}

type TimeSeriesOutput struct {
	Datapoints [][]interface{} `json:"datapoints"`
	Target     string          `json:"target"`
}

func SimpleJsonRequestFactory(method string, request []byte) (simpleJsonRequestInterface, error) {
	var retval simpleJsonRequestInterface
	switch method {
	case "query":
		retval = &simpleJsonQueryRequest{Backend: defaultBackend}
		break
	case "search":
		retval = &simpleJsonSearchRequest{simpleJsonQueryRequest{Backend: defaultBackend}}
		break
	default:
		return nil, fmt.Errorf("Unknown method, %s", method)
	}

	if err := retval.ParseRequest(request); err != nil {
		return nil, err
	}
	return retval, nil
}

func (req *simpleJsonQueryRequest) GetReadRequest(session *pb.Session) *frames.ReadRequest {
	if session == nil {
		session = &pb.Session{Container: req.Container}
	} else {
		// don't overide the container (if one is already set)
		if session.Container == "" {
			session.Container = req.Container
		}
	}
	return &frames.ReadRequest{Backend: req.Backend, Table: req.TableName, Columns: req.Fields,
		Start: req.Range.From, End: req.Range.To, Step: "60m",
		Session: session, Filter: req.Filter}
}

func (req *simpleJsonQueryRequest) formatKV(iter frames.FrameIterator) (interface{}, error) {
	retVal := TableOutput{Type: "table", Rows: [][]interface{}{}}
	for iter.Next() {
		frame := iter.At()
		iface, ok := frame.(pb.Framed)
		if !ok {
			return nil, errors.New("unknown frame type")
		}
		for _, column := range iface.Proto().Columns {
			values, colType := req.extractColumnValues(column)
			retVal.Columns = append(retVal.Columns, Column{Text: column.Name, Type: colType})
			for rowIndex := 0; rowIndex < values.Len(); rowIndex++ {
				if len(retVal.Rows) <= rowIndex {
					retVal.Rows = append(retVal.Rows, []interface{}{})
				}
				value := values.Index(rowIndex).Interface()
				retVal.Rows[rowIndex] = append(retVal.Rows[rowIndex], value)
			}
		}
	}
	if iter.Err() != nil {
		return nil, iter.Err()
	}
	return []TableOutput{retVal}, nil
}

func (req *simpleJsonQueryRequest) formatTSDB(iter frames.FrameIterator) (interface{}, error) {
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
				values, _ := req.extractColumnValues(column)
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

func (req *simpleJsonQueryRequest) extractColumnValues(column *pb.Column) (reflect.Value, string) {
	colTypeStr := strings.ToLower(column.Dtype.String())
	var dataSlice interface{}
	switch column.Dtype {
	case pb.DType_INTEGER:
		dataSlice = &column.Ints
		break
	case pb.DType_FLOAT:
		dataSlice = &column.Floats
		break
	case pb.DType_STRING:
		dataSlice = &column.Strings
		break
	case pb.DType_TIME:
		dataSlice = &column.Times
		break
	case pb.DType_BOOLEAN:
		dataSlice = &column.Bools
		break
	}
	return reflect.ValueOf(dataSlice).Elem(), colTypeStr
}

func (req *simpleJsonQueryRequest) formatTimeTSDB(timestamp time.Time) interface{} {
	return timestamp.UnixNano() / 1000000
}

func (req *simpleJsonQueryRequest) getTargetTSDB(frame *pb.Frame) string {
	labels := ""
	metricName := ""
	for _, column := range frame.Columns {
		if column.Name != "values" {
			values, _ := req.extractColumnValues(column)
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

func (req *simpleJsonQueryRequest) CreateResponse(iter frames.FrameIterator) (interface{}, error) {
	formatters := map[string]func(iter frames.FrameIterator) (interface{}, error){"kv": req.formatKV, "tsdb": req.formatTSDB}
	if formatter, ok := formatters[req.Backend]; ok {
		return formatter(iter)
	}
	return nil, fmt.Errorf("Unknown format: %s", req.Backend)
}

func (req *simpleJsonSearchRequest) CreateResponse(iter frames.FrameIterator) (interface{}, error) {
	// Placeholder for actual implementation
	return []string{}, nil
}

func (req *simpleJsonQueryRequest) ParseRequest(requestBody []byte) error {
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
	return nil
}

func (req *simpleJsonSearchRequest) ParseRequest(requestBody []byte) error {
	if err := json.Unmarshal(requestBody, req); err != nil {
		return err
	}
	for _, target := range strings.Split(req.Target, querySeparator) {
		fmt.Println(target)
		if err := req.parseQueryLine(strings.TrimSpace(target)); err != nil {
			return err
		}
	}
	return nil
}

func (req *simpleJsonQueryRequest) parseQueryLine(fieldInput string) error {
	translate := map[string]string{"table_name": "TableName"}
	// example query: fields=sentiment;table_name=stock_metrics;backend=tsdb;filter=symbol=="AAPL";container=container_name;username=user_name;password=pass
	re := regexp.MustCompile(`^\s*(filter|fields|table_name|backend|container)\s*=\s*(.*)\s*$`)
	for _, field := range strings.Split(fieldInput, querySeparator) {
		match := re.FindStringSubmatch(field)
		var value interface{}
		if len(match) > 0 {
			fieldName := strings.Title(match[1])
			if fieldNameTranslated, ok := translate[match[1]]; ok {
				fieldName = fieldNameTranslated
			}
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
