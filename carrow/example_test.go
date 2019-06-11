// Tentative API
package carrow_test

import (
	"fmt"

	"github.com/v3io/frames/carrow"
)

func Example() {
	size := 100
	intBld := carrow.NewInt64ArrayBuilder()
	floatBld := carrow.NewFloat64ArrayBuilder()
	for i := 0; i < size; i++ {
		if err := intBld.Append(int64(i)); err != nil {
			fmt.Printf("intBld.Append error: %s", err)
			return
		}
		if err := floatBld.Append(float64(i)); err != nil {
			fmt.Printf("floatBld.Append error: %s", err)
			return
		}
	}

	intArr, err := intBld.Finish()
	if err != nil {
		fmt.Printf("intBld.Finish error: %s", err)
		return
	}

	floatArr, err := floatBld.Finish()
	if err != nil {
		fmt.Printf("floatBld.Finish error: %s", err)
		return
	}

	intField, err := carrow.NewField("incCol", carrow.Integer64Type)
	if err != nil {
		fmt.Printf("intField error: %s", err)
		return
	}

	floatField, err := carrow.NewField("floatCol", carrow.Float64Type)
	if err != nil {
		fmt.Printf("floatField error: %s", err)
		return
	}

	intCol, err := carrow.NewColumn(intField, intArr)
	if err != nil {
		fmt.Printf("intCol error: %s", err)
		return
	}

	floatCol, err := carrow.NewColumn(floatField, floatArr)
	if err != nil {
		fmt.Printf("floatCol error: %s", err)
		return
	}

	cols := []*carrow.Column{intCol, floatCol}
	table, err := carrow.NewTableFromColumns(cols)
	if err != nil {
		fmt.Printf("table creation error: %s", err)
		return
	}

	fmt.Printf("num cols: %d\n", table.NumCols())
	fmt.Printf("num rows: %d\n", table.NumRows())

	// Output:
	// num cols: 2
	// num rows: 100
}
