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

package v3ioutils

import (
	"encoding/binary"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"strings"
	"time"

	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	v3iohttp "github.com/v3io/v3io-go/pkg/dataplane/http"

	"github.com/v3io/frames"
	v3io "github.com/v3io/v3io-go/pkg/dataplane"
)

const v3ioUsersContainer = "users"
const v3ioHomeVar = "$V3IO_HOME"

func NewContainer(session *frames.Session, password string, token string, logger logger.Logger, workers int) (v3io.Container, error) {

	var pass string
	if password == "" {
		pass = session.Password
	} else {
		pass = password
	}

	var tok string
	if token == "" {
		tok = session.Token
	} else {
		tok = token
	}

	newSessionInput := v3io.NewSessionInput{
		Username:  session.User,
		Password:  pass,
		AccessKey: tok,
	}
	container, err := createContainer(
		logger, session.Url, session.Container, &newSessionInput, workers)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create data container")
	}

	return container, nil

}

// CreateContainer creates a new container
func createContainer(logger logger.Logger, addr, cont string, newSessionInput *v3io.NewSessionInput, workers int) (v3io.Container, error) {
	// create context
	if workers == 0 {
		workers = 8
	}

	// Backward compatibility for non-URL addr parameter.
	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		addr = "http://" + addr
	}

	context, err := v3iohttp.NewContext(logger, &v3io.NewContextInput{ClusterEndpoints: []string{addr}, NumWorkers: workers})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create client")
	}

	session, err := context.NewSession(newSessionInput)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create session")
	}

	container, err := session.NewContainer(&v3io.NewContainerInput{ContainerName: cont})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create container")
	}

	return container, nil
}

// AsInt64Array convert v3io blob to Int array
func AsInt64Array(val []byte) []uint64 {
	var array []uint64
	bytes := val
	for i := 16; i+8 <= len(bytes); i += 8 {
		val := binary.LittleEndian.Uint64(bytes[i : i+8])
		array = append(array, val)
	}
	return array
}

// DeleteTable deletes a table
func DeleteTable(logger logger.Logger, container v3io.Container, path, filter string, workers int) error {

	fileNameChan := make(chan string, 1024)
	getItemsTerminationChan := make(chan error, workers)
	deleteTerminationChan := make(chan error, workers)
	terminationChan := make(chan struct{}, 2*workers)

	for i := 0; i < workers; i++ {
		input := &v3io.GetItemsInput{
			Path:           path,
			AttributeNames: []string{"__name"},
			Filter:         filter,
			Segment:        i,
			TotalSegments:  workers,
		}
		go getItemsWorker(container, input, fileNameChan, getItemsTerminationChan, terminationChan)
		go deleteObjectWorker(path, container, fileNameChan, deleteTerminationChan, terminationChan)
	}

	var getItemsTerminated, deletesTerminated int
	for deletesTerminated < workers {
		select {
		case err := <-getItemsTerminationChan:
			if err != nil {
				for i := 0; i < 2*workers; i++ {
					terminationChan <- struct{}{}
				}
				return errors.Wrapf(err, "GetItems failed during recursive delete of '%s'.", path)
			}
			getItemsTerminated++
			if getItemsTerminated == workers {
				close(fileNameChan)
			}
		case err := <-deleteTerminationChan:
			if err != nil {
				for i := 0; i < 2*workers; i++ {
					terminationChan <- struct{}{}
				}
				return errors.Wrapf(err, "Delete failed during recursive delete of '%s'.", path)
			}
			deletesTerminated++
		}
	}

	err := container.DeleteObjectSync(&v3io.DeleteObjectInput{Path: path})
	if err != nil {
		if !utils.IsNotExistsError(err) {
			return errors.Wrapf(err, "Failed to delete table object '%s'.", path)
		}
	}

	return nil
}

func getItemsWorker(container v3io.Container, input *v3io.GetItemsInput, fileNameChan chan<- string, terminationChan chan<- error, onErrorTerminationChannel <-chan struct{}) {
	for {
		select {
		case _ = <-onErrorTerminationChannel:
			terminationChan <- nil
			return
		default:
		}
		resp, err := container.GetItemsSync(input)
		if err != nil {
			terminationChan <- err
			return
		}
		resp.Release()
		output := resp.Output.(*v3io.GetItemsOutput)
		for _, item := range output.Items {
			fileNameChan <- item.GetField("__name").(string)
		}
		if output.Last {
			terminationChan <- nil
			return
		}
		input.Marker = output.NextMarker
	}
}

func deleteObjectWorker(tablePath string, container v3io.Container, fileNameChan <-chan string, outgoingTerminationChan chan<- error, onErrorTerminationChannel <-chan struct{}) {
	for {
		select {
		case fileName, ok := <-fileNameChan:
			if !ok {
				outgoingTerminationChan <- nil
				return
			}
			input := &v3io.DeleteObjectInput{Path: tablePath + "/" + fileName}
			err := container.DeleteObjectSync(input)
			if err != nil {
				outgoingTerminationChan <- err
				return
			}
		case _ = <-onErrorTerminationChannel:
			return
		}
	}
}

func respWaitLoop(logger logger.Logger, comm chan int, responseChan chan *v3io.Response, timeout time.Duration) chan bool {
	responses := 0
	requests := -1
	done := make(chan bool)

	go func() {
		active := false
		for {
			select {

			case resp := <-responseChan:
				responses++
				active = true

				if resp.Error != nil {
					logger.ErrorWith("failed Delete response", "error", resp.Error)
					// TODO: signal done and return?
				}

				if requests == responses {
					done <- true
					return
				}

			case requests = <-comm:
				if requests <= responses {
					done <- true
					return
				}

			case <-time.After(timeout):
				if !active {
					logger.ErrorWith("Resp loop timed out!", "requests", requests, "response", responses)
					done <- true
					return
				}
				active = false
			}
		}
	}()

	return done
}

func ProcessPaths(session *frames.Session, path string, addSlash bool) (string, string, error) {

	container := session.Container

	if container == "" {
		sp := strings.SplitN(path, "/", 2)
		if len(sp) < 2 {
			return "", "", errors.New("Please specify a data container name via the container parameters or the path prefix e.g. bigdata/mytable")
		}
		container = sp[0]
		path = sp[1]
	}

	if container == v3ioHomeVar {
		container = v3ioUsersContainer
		path = session.User + "/" + path
	}

	if strings.HasPrefix(path, v3ioHomeVar+"/") {
		container = v3ioUsersContainer
		path = session.User + "/" + path[len(v3ioHomeVar)+1:]
	}

	if addSlash && !strings.HasSuffix(path, "/") {
		path += "/"
	}

	return container, path, nil
}
