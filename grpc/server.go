package grpc

import (
	"github.com/nuclio/logger"
	"github.com/v3io/frames"
)

// Server is a frames gRPC server
type Server struct {
}

// New returns a new gRPC server
func New(config *frames.Config, addr string, logger logger.Logger) (*Server, error) {
	return nil, nil
}

func (s *Server) Read(*ReadRequest, Frames_ReadServer) error {
	return nil
}

func (s *Server) Write(Frames_WriteServer) error {
	return nil
}
