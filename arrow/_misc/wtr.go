package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/v3io/frames/arrow"
	"github.com/v3io/frames/arrow/plasma"
)

func main() {
	var dbPath string
	var id int

	flag.StringVar(&dbPath, "db", "", "path to database")
	flag.IntVar(&id, "id", 0, "object id")
	flag.Parse()

	if dbPath == "" {
		fmt.Println("error: missing db path")
		os.Exit(1)
	}

	if id == 0 {
		fmt.Println("error: missing object id")
		os.Exit(1)
	}

	sid := fmt.Sprintf("%020d", id)
	oid, err := plasma.IDFromString(sid)
	if err != nil {
		fmt.Println("error id", err)
		os.Exit(1)
	}

	fmt.Println("oid", oid)

	bld := arrow.NewInt64ArrayBuilder()
	for i := int64(0); i < 10; i++ {
		bld.Append(i)
	}
	arr, err := bld.Finish()
	if err != nil {
		fmt.Println("error finish", err)
		os.Exit(1)
	}
	fmt.Println("arr", arr)

	field, err := arrow.NewField("i", arrow.Integer64Type)
	if err != nil {
		fmt.Println("error field", err)
		os.Exit(1)
	}
	fmt.Println("field", field)

	col, err := arrow.NewColumn(field, arr)
	if err != nil {
		fmt.Println("error column", err)
		os.Exit(1)
	}

	fmt.Println("column", col)

	table, err := arrow.NewTableFromColumns([]*arrow.Column{col})
	if err != nil {
		fmt.Println("error table", err)
		os.Exit(1)
	}
	fmt.Println("table", table)

	client, err := plasma.Connect(dbPath)
	if err != nil {
		fmt.Println("error connect", err)
		os.Exit(1)
	}
	fmt.Println("client", client)

	err = client.WriteTable(table, oid)
	if err != nil {
		fmt.Println("error write", err)
		os.Exit(1)
	}

	table2, err := client.ReadTable(oid, 100*time.Millisecond)
	if err != nil {
		fmt.Println("error read", err)
		os.Exit(1)
	}
	fmt.Println("table2", table2)
	fmt.Println(table2.NumRows())

	err = client.Release(oid)
	fmt.Println("release", err)

	client.Disconnect()
}
