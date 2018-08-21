package server

import (
	"testing"

	"github.com/v3io/frames"
)

func TestNew(t *testing.T) {
	cfg := &frames.V3ioConfig{}
	address := ":8080"
	srv, err := New(cfg, address)
	if err != nil {
		t.Fatal(err)
	}

	if srv.State() != ReadyState {
		t.Fatalf("bad initial state - %q", srv.State())
	}
}
