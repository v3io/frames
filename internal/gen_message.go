// +build ignore

package main

import (
	"os"
	"text/template"
)

type ColType struct {
	Name string
	Type string
}

func main() {
	tmpl, err := template.New("message.go.tmpl").ParseGlob("message.go.tmpl")
	if err != nil {
		panic(err)
	}

	colTypes := []ColType{
		{"Int", "int"},
		{"Float", "float64"},
		{"String", "string"},
		{"Time", "time.Time"},
	}

	if err := tmpl.Execute(os.Stdout, colTypes); err != nil {
		panic(err)
	}
}
