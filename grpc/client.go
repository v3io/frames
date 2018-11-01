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

package grpc

import (
	"context"
	"github.com/v3io/frames"
	"io"

	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

// Client is frames gRPC client
type Client struct {
	client FramesClient
}

// NewClient returns a new gRPC client
func NewClient(address string, logger logger.Logger) (*Client, error) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return nil, errors.Wrap(err, "can't create gRPC connection")
	}

	client := &Client{
		client: NewFramesClient(conn),
	}

	return client, nil
}

func (c *Client) Read(request *frames.ReadRequest) (frames.FrameIterator, error) {
	req := &ReadRequest{
		Backend:      request.Backend,
		Table:        request.Table,
		Query:        request.Query,
		MessageLimit: int64(request.MaxInMessage),
		// TODO: More fields
	}

	client, err := c.client.Read(context.Background(), req)
	if err != nil {
		return nil, err
	}

	it := &frameIterator{
		client: client,
	}
	return it, nil
}

type frameIterator struct {
	client Frames_ReadClient
	frame  frames.Frame
	err    error
	done   bool
}

func (it *frameIterator) Next() bool {
	if it.done || it.err != nil {
		return false
	}

	it.frame = nil
	msg, err := it.client.Recv()
	if err != nil {
		if err != io.EOF {
			it.err = err
		}
		return false
	}

	frame, err := asFrame(msg)
	if err != nil {
		it.err = err
		return false
	}
	it.frame = frame

	return true
}

func (it *frameIterator) Err() error {
	return it.err
}

func (it *frameIterator) At() frames.Frame {
	return it.frame
}
