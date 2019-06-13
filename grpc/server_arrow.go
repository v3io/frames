// +build carrow

package grpc

import (
	"context"

	/*
		"github.com/v3io/frames/carrow"
		"github.com/v3io/frames/carrow/plasma"
	*/
	"github.com/v3io/frames/pb"
)

// ShmRead reads a table
func (s *Server) ShmRead(context.Context, *pb.ShmReadRequest) (*pb.ShmReadResponse, error) {
	return nil, nil
}

// ShmWrite write data to table
func (s *Server) ShmWrite(context.Context, *pb.ShmWriteRequest) (*pb.WriteResponse, error) {
	return nil, nil
}
