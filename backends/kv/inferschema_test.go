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
	suite.NoError(err)
	concreteSchema := schema.(*v3ioutils.OldV3ioSchema)
	suite.Equal("name", concreteSchema.Key)
	suite.Equal("", concreteSchema.SortingKey)
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
	suite.Equal("could not determine which column is the table's primary-key attribute, because no column matches the primary-key attribute", err.Error())
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
	suite.Equal("could not determine which column is the table's primary-key attribute, because 2 columns (name, second_name) match the primary-key attribute", err.Error())
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
	suite.NoError(err)
	concreteSchema := schema.(*v3ioutils.OldV3ioSchema)
	suite.Equal("name", concreteSchema.Key)
	suite.Equal("", concreteSchema.SortingKey)
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
	suite.NoError(err)
	concreteSchema := schema.(*v3ioutils.OldV3ioSchema)
	suite.Equal("name", concreteSchema.Key)
	suite.Equal("", concreteSchema.SortingKey)
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
	suite.Equal("type 'string' of value '2' doesn't match type 'int' of value '3' for column 'age'.", err.Error())
}

func (suite *InferSchemaTestSuite) TestInferSchemaWhenColumnIsIntAndFloat() {
	keyField := ""
	rowSet := []map[string]interface{}{
		{"__name": "topper", "name": "topper", "animal": "pig", "age": 1},
		{"__name": "rocky", "name": "rocky", "animal": "dog", "age": 5.3},
		{"__name": "mocha", "name": "mocha", "animal": "dog", "age": 3},
		{"__name": "scratchy", "name": "scratchy", "animal": "cat", "age": 6.2},
	}
	schema, err := schemaFromKeys(keyField, rowSet)
	suite.Require().NoError(err)

	expectedFields := []v3ioutils.OldSchemaField{
		{Name: "name", Type: "string", Nullable: false},
		{Name: "animal", Type: "string", Nullable: true},
		{Name: "age", Type: "double", Nullable: true},
	}
	concreteSchema := schema.(*v3ioutils.OldV3ioSchema)
	suite.Equal("name", concreteSchema.Key)
	suite.ElementsMatch(expectedFields, concreteSchema.Fields)
}

func (suite *InferSchemaTestSuite) TestInferSchemaWithSortingKey() {
	expectedFields := []v3ioutils.OldSchemaField{
		{Name: "name", Type: "string", Nullable: false},
		{Name: "animal", Type: "string", Nullable: false},
		{Name: "age", Type: "long", Nullable: true},
	}

	keyField := ""
	rowSet := []map[string]interface{}{
		{"__name": "rocky.dog", "name": "rocky", "animal": "dog", "age": 2},
		{"__name": "mocha.dog", "name": "mocha", "animal": "dog", "age": 3},
		{"__name": "mocha.cat", "name": "mocha", "animal": "cat", "age": 9},
	}
	schema, err := schemaFromKeys(keyField, rowSet)
	suite.NoError(err)
	concreteSchema := schema.(*v3ioutils.OldV3ioSchema)
	suite.Equal("name", concreteSchema.Key)
	suite.Equal("animal", concreteSchema.SortingKey)
	suite.ElementsMatch(expectedFields, concreteSchema.Fields)
}

func (suite *InferSchemaTestSuite) TestInferSchemaWithPrimaryKeyWithDot() {
	expectedFields := []v3ioutils.OldSchemaField{
		{Name: "name", Type: "string", Nullable: false},
		{Name: "suffix", Type: "string", Nullable: true},
		{Name: "age", Type: "long", Nullable: true},
	}

	keyField := ""
	rowSet := []map[string]interface{}{
		{"__name": "rock.y", "name": "rock.y", "suffix": "y", "age": 2},
		{"__name": "moch.a", "name": "moch.a", "suffix": "a", "age": 3},
		{"__name": "scratch.y", "name": "scratch.y", "suffix": "y", "age": 9},
	}
	schema, err := schemaFromKeys(keyField, rowSet)
	suite.NoError(err)
	concreteSchema := schema.(*v3ioutils.OldV3ioSchema)
	suite.Equal("name", concreteSchema.Key)
	suite.Equal("", concreteSchema.SortingKey)
	suite.ElementsMatch(expectedFields, concreteSchema.Fields)
}

func TestInferSchemaTestSuite(t *testing.T) {
	suite.Run(t, new(InferSchemaTestSuite))
}
