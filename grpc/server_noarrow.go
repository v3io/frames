// +build !carrow

package grpc

import (
	"context"
	"fmt"

	"github.com/v3io/frames/pb"
)

// ShmRead reads a table
func (s *Server) ShmRead(context.Context, *pb.ShmReadRequest) (*pb.ShmReadResponse, error) {
	return nil, fmt.Errorf("arrow  not enabled")
}

// ShmWrite write data to table
func (s *Server) ShmWrite(context.Context, *pb.ShmWriteRequest) (*pb.WriteResponse, error) {
	return nil, fmt.Errorf("arrow  not enabled")
}
