// +build arrow

package plasma

import (
	"encoding/hex"
	"math/rand"
	"runtime"
	"time"
	"unsafe"

	"github.com/pkg/errors"

	"github.com/v3io/frames/arrow"
)

/*
#cgo pkg-config: arrow plasma
#cgo LDFLAGS: -lframesarrow -L..
#cgo CFLAGS: -I..

#include "arrow.h"
#include <stdlib.h>
*/
import "C"

const (
	// IDLength is length of ObjectID in bytes
	IDLength = 20
)

var (
	idRnd = rand.New(rand.NewSource(time.Now().UnixNano()))
)

// Client is a client to Arrow's plasma store
type Client struct {
	ptr unsafe.Pointer
}

// ObjectID is store ID for an object
type ObjectID [IDLength]byte

// TODO: United with one in arrow (internal?)
func errFromResult(r C.result_t) error {
	err := errors.Errorf(C.GoString(r.err))
	C.free(unsafe.Pointer(r.err))
	return err
}

// Connect connects to plasma store
func Connect(path string) (*Client, error) {
	cStr := C.CString(path)
	r := C.plasma_connect(cStr)
	C.free(unsafe.Pointer(cStr))

	if r.err != nil {
		return nil, errFromResult(r)
	}

	ptr := C.result_ptr(r)
	client := &Client{ptr}
	runtime.SetFinalizer(client, func(c *Client) {
		c.Disconnect()
	})

	return client, nil
}

// WriteTable write a table to plasma store
// If id is empty, a new random id will be generated
func (c *Client) WriteTable(t *arrow.Table, id ObjectID) error {
	cID := C.CString(string(id[:]))
	r := C.plasma_write(c.ptr, t.Ptr(), cID)
	C.free(unsafe.Pointer(cID))

	if r.err != nil {
		return errFromResult(r)
	}
	// TODO: Return number of bytes written?
	return nil
}

// ReadTable reads a table from plasma store
func (c *Client) ReadTable(id ObjectID, timeout time.Duration) (*arrow.Table, error) {
	cID := C.CString(string(id[:]))
	msec := C.int64_t(timeout / time.Millisecond)
	r := C.plasma_read(c.ptr, cID, msec)
	C.free(unsafe.Pointer(cID))

	if r.err != nil {
		return nil, errFromResult(r)
	}

	ptr := C.result_ptr(r)
	return arrow.NewTableFromPtr(ptr), nil
}

// Release releases (deletes) object from plasma store
func (c *Client) Release(id ObjectID) error {
	cID := C.CString(string(id[:]))
	r := C.plasma_release(c.ptr, cID)
	C.free(unsafe.Pointer(cID))

	if r.err != nil {
		return errFromResult(r)
	}

	return nil
}

// Disconnect disconnects from plasma store
func (c *Client) Disconnect() error {
	if c.ptr == nil {
		return nil
	}

	r := C.plasma_disconnect(c.ptr)
	if r.err != nil {
		return errFromResult(r)
	}
	c.ptr = nil
	return nil
}

func (oid ObjectID) String() string {
	return hex.EncodeToString(oid[:])
}

// RandomID return a new random plasma ID
func RandomID() (ObjectID, error) {
	var oid ObjectID
	_, err := idRnd.Read(oid[:])
	if err != nil {
		return oid, err
	}

	oid[8] = (oid[8] | 0x80) & 0xBF
	oid[6] = (oid[6] | 0x40) & 0x4F
	return oid, nil
}

// IDFromBytes converts a []byte to ObjectID
func IDFromBytes(data []byte) (ObjectID, error) {
	var oid ObjectID
	if len(data) != IDLength {
		return oid, errors.Errorf("wrong length, expected %d, got %d", IDLength, len(data))
	}

	copy(oid[:], data)
	return oid, nil
}
