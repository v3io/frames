package frames

import (
	"fmt"
	"reflect"
	"time"

	"github.com/pkg/errors"
)

const (
	sliceColumnTag = "sliceCol"
	labelColumnTag = "labelCol"
	mapFrameTag    = "mapFrame"
)

// SliceColumn is a column with slice data
type SliceColumn struct {
	name string
	data interface{}
	size int
}

// NewSliceColumn return a new SliceColumn
func NewSliceColumn(name string, data interface{}) (*SliceColumn, error) {
	var size int

	switch reflect.TypeOf(data) {
	case IntType:
		size = len(data.([]int))
	case FloatType:
		size = len(data.([]float64))
	case StringType:
		size = len(data.([]string))
	case TimeType:
		size = len(data.([]time.Time))
	default:
		return nil, fmt.Errorf("unspported data type - %T", data)
	}

	sc := &SliceColumn{
		data: data,
		name: name,
		size: size,
	}

	return sc, nil
}

// Name returns the column name
func (sc *SliceColumn) Name() string {
	return sc.name
}

// Len returns the number of elements
func (sc *SliceColumn) Len() int {
	return sc.size
}

// DType returns the data type
func (sc *SliceColumn) DType() Type {
	return reflect.TypeOf(sc.data)
}

// Ints returns data as []int
func (sc *SliceColumn) Ints() ([]int, error) {
	typedCol, ok := sc.data.([]int)
	if !ok {
		return nil, fmt.Errorf("wrong type (type is %s)", sc.DType())
	}

	return typedCol, nil
}

// Floats returns data as []float64
func (sc *SliceColumn) Floats() ([]float64, error) {
	typedCol, ok := sc.data.([]float64)
	if !ok {
		return nil, fmt.Errorf("wrong type (type is %s)", sc.DType())
	}

	return typedCol, nil
}

// Strings returns data as []string
func (sc *SliceColumn) Strings() ([]string, error) {
	typedCol, ok := sc.data.([]string)
	if !ok {
		return nil, fmt.Errorf("wrong type (type is %s)", sc.DType())
	}

	return typedCol, nil
}

// Times returns data as []time.Time
func (sc *SliceColumn) Times() ([]time.Time, error) {
	typedCol, ok := sc.data.([]time.Time)
	if !ok {
		return nil, fmt.Errorf("wrong type (type is %s)", sc.DType())
	}

	return typedCol, nil
}

// Slice returns a Column with is slice of data
func (sc *SliceColumn) Slice(start int, end int) (Column, error) {
	if err := validateSlice(start, end, sc.Len()); err != nil {
		return nil, err
	}

	var slice interface{}
	switch sc.DType() {
	case IntType:
		typedCol, _ := sc.Ints()
		slice = typedCol[start:end]
	case FloatType:
		typedCol, _ := sc.Floats()
		slice = typedCol[start:end]
	case StringType:
		typedCol, _ := sc.Strings()
		slice = typedCol[start:end]
	case TimeType:
		typedCol, _ := sc.Times()
		slice = typedCol[start:end]
	}

	return NewSliceColumn(sc.Name(), slice)
}

// Marshal marshals to native type
func (sc *SliceColumn) Marshal() (map[string]interface{}, error) {
	data := map[string]interface{}{
		"tag":  sliceColumnTag,
		"data": sc.data,
	}

	return data, nil
}

// LabelColumn is a column with same value
type LabelColumn struct {
	name  string
	value interface{}
	size  int
}

// NewLabelColumn returns new label column
func NewLabelColumn(name string, value interface{}, size int) (*LabelColumn, error) {
	switch value.(type) {
	case int, float64, string, time.Time:
		// OK
	default:
		return nil, fmt.Errorf("unspported type - %T", value)
	}

	lc := &LabelColumn{
		name:  name,
		value: value,
		size:  size,
	}

	return lc, nil
}

// Name returns the column name
func (lc *LabelColumn) Name() string {
	return lc.name
}

// Len returns the number of elements
func (lc *LabelColumn) Len() int {
	return lc.size
}

// DType returns the data type
func (lc *LabelColumn) DType() Type {
	switch lc.value.(type) {
	case string:
		return StringType
	case int:
		return IntType
	case float64:
		return FloatType
	case time.Time:
		return TimeType
	}

	return nil
}

// Ints returns data as []int
func (lc *LabelColumn) Ints() ([]int, error) {
	typedVal, ok := lc.value.(int)
	if !ok {
		return nil, fmt.Errorf("wrong type (type is %s)", lc.DType())
	}

	data := make([]int, lc.Len())
	for i := range data {
		data[i] = typedVal
	}

	return data, nil
}

// Floats returns data as []float64
func (lc *LabelColumn) Floats() ([]float64, error) {
	typedVal, ok := lc.value.(float64)
	if !ok {
		return nil, fmt.Errorf("wrong type (type is %s)", lc.DType())
	}

	data := make([]float64, lc.Len())
	for i := range data {
		data[i] = typedVal
	}

	return data, nil
}

// Strings returns data as []string
func (lc *LabelColumn) Strings() ([]string, error) {
	typedVal, ok := lc.value.(string)
	if !ok {
		return nil, fmt.Errorf("wrong type (type is %s)", lc.DType())
	}

	data := make([]string, lc.Len())
	for i := range data {
		data[i] = typedVal
	}

	return data, nil
}

// Times returns data as []time.Time
func (lc *LabelColumn) Times() ([]time.Time, error) {
	typedVal, ok := lc.value.(time.Time)
	if !ok {
		return nil, fmt.Errorf("wrong type (type is %s)", lc.DType())
	}

	data := make([]time.Time, lc.Len())
	for i := range data {
		data[i] = typedVal
	}

	return data, nil
}

// Slice returns a Column with is slice of data
func (lc *LabelColumn) Slice(start int, end int) (Column, error) {
	if err := validateSlice(start, end, lc.Len()); err != nil {
		return nil, err
	}

	return NewLabelColumn(lc.name, lc.value, end-start)
}

// MapFrame is a frame based on map
type MapFrame struct {
	byIndex map[int]Column
}

// NewMapFrame returns a new MapFrame
func NewMapFrame(columns []Column) (*MapFrame, error) {
	if err := checkEqualLen(columns); err != nil {
		return nil, err
	}

	byIndex := make(map[int]Column)
	for i, col := range columns {
		byIndex[i] = col
	}

	return &MapFrame{byIndex}, nil
}

// Marshal marshals to native type
func (lc *LabelColumn) Marshal() (map[string]interface{}, error) {
	data := map[string]interface{}{
		"tag":   labelColumnTag,
		"value": lc.value,
		"size":  lc.size,
	}

	return data, nil
}

// Columns returns the column names
func (mf *MapFrame) Columns() []string {
	names := make([]string, len(mf.byIndex))

	for i := 0; i < len(mf.byIndex); i++ {
		names[i] = mf.byIndex[i].Name() // TODO: Check if exists?
	}

	return names
}

// Len is the number of rows
func (mf *MapFrame) Len() int {
	for _, col := range mf.byIndex {
		return col.Len()
	}

	return 0
}

// Column gets a column by name
func (mf *MapFrame) Column(name string) (Column, error) {
	// TODO: We can speed it up by calculating once, but then we'll use more memory
	for _, col := range mf.byIndex {
		if col.Name() == name {
			return col, nil
		}
	}

	return nil, fmt.Errorf("column %q not found", name)
}

// Slice return a new Frame with is slice of the original
func (mf *MapFrame) Slice(start int, end int) (Frame, error) {
	if err := validateSlice(start, end, mf.Len()); err != nil {
		return nil, err
	}

	frameSlice := make([]Column, len(mf.byIndex))
	for i, col := range mf.byIndex {
		slice, err := col.Slice(start, end)
		if err != nil {
			return nil, errors.Wrapf(err, "can't get slice from %q", col.Name())
		}

		frameSlice[i] = slice
	}

	return NewMapFrame(frameSlice)
}

// Marshal marshals to native type
func (mf *MapFrame) Marshal() (map[string]interface{}, error) {
	data := map[string]interface{}{
		"tag": mapFrameTag,
	}

	columns := make([]map[string]interface{}, len(mf.byIndex))
	for i := range columns {
		col := mf.byIndex[i]
		marshaler, ok := col.(Marshaler)
		if !ok {
			return nil, fmt.Errorf("column %q is not a Marshaler", col.Name())
		}

		colData, err := marshaler.Marshal()
		if err != nil {
			return nil, errors.Wrapf(err, "can't marshal column %q", col.Name())
		}

		columns[i] = colData
	}

	data["columns"] = columns
	return data, nil
}

func validateSlice(start int, end int, size int) error {
	if start < 0 || end < 0 {
		return fmt.Errorf("negative indexing not supported")
	}

	if end < start {
		return fmt.Errorf("end <= start")
	}

	if start >= size {
		return fmt.Errorf("start out of bounds")
	}

	if end >= size {
		return fmt.Errorf("end out of bounds")
	}

	return nil
}

func checkEqualLen(columns []Column) error {
	size := -1
	for _, col := range columns {
		if size == -1 { // first column
			size = col.Len()
			continue
		}

		if colSize := col.Len(); colSize != size {
			return fmt.Errorf("%q column size mismatch (%d != %d)", col.Name(), colSize, size)
		}
	}

	return nil
}
