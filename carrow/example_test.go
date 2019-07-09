// +build carrow

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
