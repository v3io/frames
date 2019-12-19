package kv

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/v3io/frames/v3ioutils"
)

type InferSchemaTestSuite struct {
	suite.Suite
}

func (suite *InferSchemaTestSuite) TestInferSchemaSimple() {
	expectedFields := []v3ioutils.OldSchemaField{
		{Name: "name", Type: "string", Nullable: false},
		{Name: "animal", Type: "string", Nullable: true},
		{Name: "age", Type: "long", Nullable: true},
	}

	keyField := ""
	rowSet := []map[string]interface{}{
		{"__name": "rocky", "name": "rocky", "animal": "dog", "age": 2},
		{"__name": "mocha", "name": "mocha", "animal": "dog", "age": 3},
		{"__name": "scratchy", "name": "scratchy", "animal": "cat", "age": 9},
	}
	schema, err := schemaFromKeys(keyField, rowSet)
	concreteSchema := schema.(*v3ioutils.OldV3ioSchema)
	suite.NoError(err)
	suite.Equal("name", concreteSchema.Key)
	suite.ElementsMatch(expectedFields, concreteSchema.Fields)
}

func (suite *InferSchemaTestSuite) TestInferSchemaWhenNoColumnMatches() {
	keyField := ""
	rowSet := []map[string]interface{}{
		{"__name": "rocky", "name": "CORRUPTED", "animal": "dog", "age": 2},
		{"__name": "mocha", "name": "mocha", "animal": "dog", "age": 3},
		{"__name": "scratchy", "name": "scratchy", "animal": "cat", "age": 9},
	}
	_, err := schemaFromKeys(keyField, rowSet)
	suite.Error(err)
	suite.Equal("Could not determine which column is the table key because no column matched __name attribute.", err.Error())
}

func (suite *InferSchemaTestSuite) TestInferSchemaWhenTwoColumnsMatch() {
	keyField := ""
	rowSet := []map[string]interface{}{
		{"__name": "rocky", "name": "rocky", "second_name": "rocky", "age": 2},
		{"__name": "mocha", "name": "mocha", "second_name": "mocha", "age": 3},
		{"__name": "scratchy", "name": "scratchy", "second_name": "scratchy", "age": 9},
	}
	_, err := schemaFromKeys(keyField, rowSet)
	suite.Error(err)
	suite.Equal("Could not determine which column is the table key because 2 columns (name, second_name) matched __name attribute.", err.Error())
}

func (suite *InferSchemaTestSuite) TestInferSchemaWhenTwoColumnsMatchButKeyIsProvided() {
	expectedFields := []v3ioutils.OldSchemaField{
		{Name: "name", Type: "string", Nullable: false},
		{Name: "second_name", Type: "string", Nullable: true},
		{Name: "age", Type: "long", Nullable: true},
	}

	keyField := "name"
	rowSet := []map[string]interface{}{
		{"__name": "rocky", "name": "rocky", "second_name": "rocky", "age": 2},
		{"__name": "mocha", "name": "mocha", "second_name": "mocha", "age": 3},
		{"__name": "scratchy", "name": "scratchy", "second_name": "scratchy", "age": 9},
	}
	schema, err := schemaFromKeys(keyField, rowSet)
	concreteSchema := schema.(*v3ioutils.OldV3ioSchema)
	suite.NoError(err)
	suite.Equal("name", concreteSchema.Key)
	suite.ElementsMatch(expectedFields, concreteSchema.Fields)
}

func (suite *InferSchemaTestSuite) TestInferSchemaWhenOneColumnMatches() {
	expectedFields := []v3ioutils.OldSchemaField{
		{Name: "name", Type: "string", Nullable: false},
		{Name: "second_name", Type: "string", Nullable: true},
		{Name: "age", Type: "long", Nullable: true},
	}

	keyField := ""
	rowSet := []map[string]interface{}{
		{"__name": "rocky", "name": "rocky", "second_name": "rocky", "age": 2},
		{"__name": "mocha", "name": "mocha", "second_name": "mocha", "age": 3},
		{"__name": "scratchy", "name": "scratchy", "second_name": "NOT_scratchy", "age": 9},
	}
	schema, err := schemaFromKeys(keyField, rowSet)
	concreteSchema := schema.(*v3ioutils.OldV3ioSchema)
	suite.NoError(err)
	suite.Equal("name", concreteSchema.Key)
	suite.ElementsMatch(expectedFields, concreteSchema.Fields)
}

func (suite *InferSchemaTestSuite) TestInferSchemaWhenTypesDontMatch() {
	keyField := ""
	rowSet := []map[string]interface{}{
		{"__name": "rocky", "name": "rocky", "animal": "dog", "age": "2"},
		{"__name": "mocha", "name": "mocha", "animal": "dog", "age": 3},
		{"__name": "scratchy", "name": "scratchy", "animal": "cat", "age": 9},
	}
	_, err := schemaFromKeys(keyField, rowSet)
	suite.Error(err)
	suite.Equal("Type string of 2 did not match type int of 3 for column age.", err.Error())
}

func TestInferSchemaTestSuite(t *testing.T) {
	suite.Run(t, new(InferSchemaTestSuite))
}
