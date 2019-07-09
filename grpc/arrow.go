// +build carrow

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
	"fmt"

	"github.com/pkg/errors"

	"github.com/v3io/frames"
	"github.com/v3io/frames/carrow/plasma"
	"github.com/v3io/frames/pb"
)

// ShmRead executes a read via shared memory
func (s *Server) ShmRead(ctx context.Context, req *pb.ShmReadRequest) (*pb.ShmReadResponse, error) {
	client, err := plasma.Connect(req.DbPath)
	if err != nil {
		return nil, errors.Wrapf(err, "can't connect to %q", req.DbPath)
	}
	defer client.Disconnect() // TODO: Keep pool of path -> client?, error?

	id, err := plasma.RandomID()
	if err != nil {
		return nil, errors.Wrap(err, "can't generate ID")
	}

	req.Request.UseArrow = true
	r := s.prepareRequest(req.Request)
	ch := make(chan frames.Frame)

	var apiError error
	go func() {
		apiError = s.api.Read(r, ch)
		if apiError != nil {
			s.logger.ErrorWith("API error reading", "error", apiError)
		}
	}()

	frame := <-ch // TODO: timeout
	af, ok := frame.(*frames.ArrowFrame)
	if !ok {
		return nil, fmt.Errorf("backend returned a non-arrow frame")
	}

	err = client.WriteTable(af.Table, id)
	if err != nil {
		return nil, errors.Wrapf(err, "can't write table to %s:%s", req.DbPath, id)
	}

	resp := &pb.ShmReadResponse{
		DbPath:   req.DbPath,
		ObjectId: id[:],
	}

	return resp, err
}

// ShmWrite executes a write via shared memory
func (s *Server) ShmWrite(ctx context.Context, req *pb.ShmWriteRequest) (*pb.WriteResponse, error) {
	return nil, fmt.Errorf("not implemented... yet")
}
