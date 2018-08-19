package frames

import (
	"reflect"
	"testing"
)

func TestSimple(t *testing.T) {
	sql := `
	SELECT first, last
	FROM employees
	WHERE last IS NOT NULL
	GROUP BY dept
	`
	query, err := ParseSQL(sql)
	if err != nil {
		t.Fatalf("error parsing - %s", err)
	}

	expected := &Query{
		Columns: []string{"first", "last"},
		Table:   "employees",
		Filter:  "where last is not null",
		GroupBy: "group by dept",
	}

	if !reflect.DeepEqual(query, expected) {
		t.Logf("q: %q", query.GroupBy)
		t.Logf("e: %q", expected.GroupBy)
		t.Fatalf("wrong result - %+v", query)
	}
}
