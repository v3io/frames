// +build carrow

// TODO: Should this be in carrow?

package frames

import (
	"github.com/v3io/frames/carrow"
)

// ArrowColumnBuilder builds arrow based columns
type ArrowColumnBuilder struct {
	field        *carrow.Field
	boolBuilder  *carrow.BoolArrayBuilder
	floatBuilder *carrow.Float64ArrayBuilder
	intBuilder   *carrow.Int64ArrayBuilder
	strBuilder   *carrow.StringArrayBuilder
	tsBuilder    *carrow.TimestampArrayBuilder
}

// NewArrowColumnBuilder return new ArrowColumnBuilder
func NewArrowColumnBuilder(name string, dtype DType, size int) (*ArrowColumnBuilder, error) {
	var typ carrow.DType
	bld := &ArrowColumnBuilder{}
	switch dtype {
	case BoolType:
		typ = carrow.BoolType
		bld.boolBuilder = carrow.NewBoolArrayBuilder()
	case FloatType:
		typ = carrow.Float64Type
		bld.floatBuilder = carrow.NewFloat64ArrayBuilder()
	case IntType:
		typ = carrow.Integer64Type
		bld.intBuilder = carrow.NewInt64ArrayBuilder()
	case StringType:
		typ = carrow.StringType
		bld.strBuilder = carrow.NewStringArrayBuilder()
	case TimeType:
		typ = carrow.TimestampType
		bld.tsBuilder = carrow.NewTimestampArrayBuilder()
	default:
		return nil, fmt.Errorf("unsupported dtype - %s", dtype)
	}

	var err error
	bld.field, err = carrow.NewField(name, typ)
	if err != nil {
		return nil, err
	}

	return bld, nil
}

func (b *ArrowColumnBuilder) Append(value interface{}) error {
	switch b.field.DType() {
	case carrow.BoolType:
		bval, ok := value.(bool)
		if !ok {
			return fmt.Errorf("bad type for bool - %T", value)
		}
	case carrow.Float64Type:
	case carrow.Integer64Type:
	case carrow.StringType:
	case carrow.TimestampType:
	default:
		return fmt.Errorf("unsupported dtype - %s", b.field.DType())
	}
}

func (b *ArrowColumnBuilder) At(index int) (interface{}, error) {
}

func (b *ArrowColumnBuilder) Set(index int, value interface{}) error {
}

func (b *ArrowColumnBuilder) Delete(index int) error {
}

func (b *ArrowColumnBuilder) Finish() Column {
}
