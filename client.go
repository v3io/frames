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

package frames

import (
	"encoding/json"
	"os"
	"strings"

	"github.com/pkg/errors"
	"github.com/v3io/frames/pb"
)

// Client interface
type Client interface {
	// Read reads data from server
	Read(request *pb.ReadRequest) (FrameIterator, error)
	// Write writes data to server
	Write(request *WriteRequest) (FrameAppender, error)
	// Create creates a table
	Create(request *pb.CreateRequest) error
	// Delete deletes data or table
	Delete(request *pb.DeleteRequest) error
	// Exec executes a command on the backend
	Exec(request *pb.ExecRequest) (Frame, error)
}

// SessionFromEnv return a session from V3IO_SESSION environment variable (JSON encoded)
func SessionFromEnv() (*pb.Session, error) {
	session := &pb.Session{}
	envKey := "V3IO_SESSION"

	data := os.Getenv(envKey)
	if data == "" {
		return session, nil
	}

	// Support var1=val1,var2=val2,... format
	data = strings.TrimSpace(data)
	if !strings.HasPrefix(data, "{") {
		parts := strings.Split(data, ",")
		newData := map[string]string{}
		for _, part := range parts {
			pair := strings.SplitN(part, "=", 2)
			if len(pair) != 2 {
				return nil, errors.Errorf("%s was not recognized as either a JSON dictionary or comma-separated value pairs", envKey)
			}
			newData[pair[0]] = pair[1]
		}
		bytes, err := json.Marshal(newData)
		data = string(bytes)
		if err != nil {
			return nil, err
		}
	}

	dec := json.NewDecoder(strings.NewReader(data))
	if err := dec.Decode(session); err != nil {
		return nil, errors.Wrapf(err, "can't read JSON from %s environment", envKey)
	}

	if session.Url == "" {
		session.Url = os.Getenv("V3IO_API")
	}
	if session.Token == "" {
		session.Token = os.Getenv("V3IO_ACCESS_KEY")
	}

	return session, nil
}
