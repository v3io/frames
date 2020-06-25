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

package backends

import (
	"fmt"
	"strings"
	"testing"

	"github.com/nuclio/logger"
	"github.com/stretchr/testify/suite"
	"github.com/v3io/frames"
	"github.com/v3io/frames/pb"
	v3io "github.com/v3io/v3io-go/pkg/dataplane"
)

type BackendsTestSuite struct {
	suite.Suite
}

// Special error return from testFactory so we can see it's this function
var errorBackendsTest = fmt.Errorf("backends test")

func testFactory(logger.Logger, v3io.Context, *frames.BackendConfig, *frames.Config) (frames.DataBackend, error) {
	return nil, errorBackendsTest
}

func (suite *BackendsTestSuite) TestBackends() {
	typ := "testBackend"
	err := Register(typ, testFactory)
	suite.Require().NoError(err)

	err = Register(typ, testFactory)
	suite.Require().Error(err)

	capsType := strings.ToUpper(typ)
	factory := GetFactory(capsType)
	suite.Require().NotNil(factory)

	_, err = factory(nil, nil, nil, nil)
	suite.Require().Equal(errorBackendsTest, err)
}

func (suite *BackendsTestSuite) TestValidateEmptyReadRequest() {
	err := ValidateRequest("mybackend", &pb.ReadRequest{}, nil)
	suite.NoError(err)
}

func (suite *BackendsTestSuite) TestValidateSimpleReadRequest() {
	err := ValidateRequest("kv", &pb.ReadRequest{Segments: []int64{5}}, map[string]bool{"Segments": true})
	suite.Require().NoError(err)
}

func (suite *BackendsTestSuite) TestValidateBadReadRequest() {
	err := ValidateRequest("tsdb", &pb.ReadRequest{Segments: []int64{5}}, nil)
	suite.Require().Error(err)
	expected := "Segments cannot be used as an argument to a ReadRequest to tsdb backend"
	suite.Require().Equal(expected, err.Error())
}

func TestBackendsTestSuite(t *testing.T) {
	suite.Run(t, new(BackendsTestSuite))
}
