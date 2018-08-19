package utils

import (
	"fmt"
	"math"
	"time"
)

// AppendValue appends a value to data
func AppendValue(data interface{}, value interface{}) (interface{}, error) {
	switch data.(type) {
	case []int:
		ival, ok := value.(int)
		if !ok {
			return nil, fmt.Errorf("append type mismatch data is %T while value is %T", data, value)
		}
		idata := data.([]int)
		idata = append(idata, ival)
		return idata, nil
	case []float64:
		fval, ok := value.(float64)
		if !ok {
			return nil, fmt.Errorf("append type mismatch data is %T while value is %T", data, value)
		}
		fdata := data.([]float64)
		fdata = append(fdata, fval)
		return fdata, nil
	case []string:
		sval, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("append type mismatch data is %T while value is %T", data, value)
		}
		sdata := data.([]string)
		sdata = append(sdata, sval)
		return sdata, nil
	case []time.Time:
		tval, ok := value.(time.Time)
		if !ok {
			return nil, fmt.Errorf("append type mismatch data is %T while value is %T", data, value)
		}
		tdata := data.([]time.Time)
		tdata = append(tdata, tval)
		return tdata, nil
	}

	return nil, fmt.Errorf("unsupported data type - %T", data)
}

// NewColumn creates a new column from type of value
func NewColumn(value interface{}, size int) (interface{}, error) {
	switch value.(type) {
	case int:
		return make([]int, size), nil
	case float64:
		return make([]float64, size), nil
	case string:
		return make([]string, size), nil
	case time.Time:
		return make([]time.Time, size), nil
	}

	return nil, fmt.Errorf("Unknown type - %T", value)
}

// AppendNil appends an empty value to data
func AppendNil(data interface{}) (interface{}, error) {
	switch data.(type) {
	case []int:
		idata := data.([]int)
		idata = append(idata, 0)
		return idata, nil
	case []float64:
		fdata := data.([]float64)
		fdata = append(fdata, math.NaN())
		return fdata, nil
	case []string:
		sdata := data.([]string)
		sdata = append(sdata, "")
		return sdata, nil
	case []time.Time:
		tdata := data.([]time.Time)
		tdata = append(tdata, time.Time{})
		return tdata, nil
	}

	return nil, fmt.Errorf("unsupported data type - %T", data)
}
