package v3ioutils

import (
	"fmt"
	"github.com/stretchr/testify/suite"
	"github.com/v3io/frames"
	"testing"
)

type testContainerSuite struct {
	suite.Suite
}

func (suite *testContainerSuite) TestDefaultPaths() {

	// test case for nil container
	session := frames.Session{Container: "", User: "joe"}
	_, _, err := ProcessPaths(&session, "mypath", true)
	suite.Require().NotNil(err)

	// test case for $HOME overwrite
	session = frames.Session{Container: "bigdata", User: "joe"}
	container, path, err := ProcessPaths(&session, "$V3IO_HOME/mypath", true)
	fmt.Println(container, path, err)
	suite.Require().Equal(v3ioUsersContainer, container)
	suite.Require().Equal("joe/mypath/", path)

	// test case w/o overwrite
	container, path, err = ProcessPaths(&session, "mypath", true)
	fmt.Println(container, path, err)
	suite.Require().Equal("bigdata", container)
	suite.Require().Equal("mypath/", path)
}

func TestContainerSuite(t *testing.T) {
	suite.Run(t, new(testContainerSuite))
}
