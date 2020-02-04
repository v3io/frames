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

package http

import (
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/valyala/fasthttp"
	"io"
	"net/http"
	neturl "net/url"
	"os"
	"time"

	"github.com/nuclio/logger"
	"github.com/pkg/errors"

	"github.com/v3io/frames"
	"github.com/v3io/frames/pb"
)

type httpResponseReaderCloser struct {
	httpResponse *fasthttp.Response
	bodyReader   bytes.Reader
}

func newHTTPResponseReaderCloser(httpResponse *fasthttp.Response) httpResponseReaderCloser {
	newHTTPResponseReaderCloser := httpResponseReaderCloser{}
	newHTTPResponseReaderCloser.bodyReader.Reset(httpResponse.Body())

	return newHTTPResponseReaderCloser
}

func (rc *httpResponseReaderCloser) Read(p []byte) (n int, err error) {
	return rc.bodyReader.Read(p)
}

func (rc *httpResponseReaderCloser) Close() error {
	fmt.Println("RESPONSE RELEASED")
	fasthttp.ReleaseResponse(rc.httpResponse)
	return nil
}

// Client is v3io HTTP streaming client
type Client struct {
	url        *neturl.URL
	logger     logger.Logger
	session    *frames.Session
	httpClient *fasthttp.Client
}

var (
	// Make sure we're implementing frames.Client
	_ frames.Client = &Client{}
)

// NewClient returns a new HTTP client
func NewClient(url string, session *frames.Session, logger logger.Logger) (*Client, error) {
	var err error
	if logger == nil {
		logger, err = frames.NewLogger("info")
		if err != nil {
			return nil, errors.Wrap(err, "can't create logger")
		}
	}

	if url == "" {
		url = os.Getenv("V3IO_URL")
	}

	if url == "" {
		return nil, fmt.Errorf("empty URL")
	}

	netURL, err := neturl.Parse(url)
	if err != nil {
		return nil, fmt.Errorf("bad URL - %s", err)
	}

	if netURL.Scheme == "" {
		netURL.Scheme = "http"
	}

	if session == nil {
		var err error
		session, err = frames.SessionFromEnv()
		if err != nil {
			return nil, err
		}
	}

	httpClient := fasthttp.Client{
		TLSConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}

	client := &Client{
		url:        netURL,
		session:    session,
		logger:     logger,
		httpClient: &httpClient,
	}

	return client, nil
}

// Read runs a query on the client
func (c *Client) Read(request *pb.ReadRequest) (frames.FrameIterator, error) {
	if request.Session == nil {
		request.Session = c.session
	}

	marshalledRequest, err := json.Marshal(request)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to marshall request")
	}

	httpRequest := fasthttp.AcquireRequest()
	httpRequest.URI().SetScheme(c.url.Scheme)
	httpRequest.URI().SetHost(c.url.Host)
	httpRequest.URI().SetPath(c.url.Path + "/read")
	httpRequest.SetBody(marshalledRequest)
	httpRequest.Header.SetContentType("application/json")
	httpRequest.Header.SetMethod("POST")

	httpResponse := fasthttp.AcquireResponse()

	err = c.httpClient.Do(httpRequest, httpResponse)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to call API")
	}

	fasthttp.ReleaseRequest(httpRequest)

	if httpResponse.StatusCode() != http.StatusOK {
		defer fasthttp.ReleaseResponse(httpResponse)
		var buf bytes.Buffer
		buf.Write(httpResponse.Body())

		return nil, fmt.Errorf("API returned with bad code - %d\n%s", httpResponse.StatusCode(), buf.String())
	}

	httpResponseReaderCloser := newHTTPResponseReaderCloser(httpResponse)

	it := &streamFrameIterator{
		reader:  &httpResponseReaderCloser,
		decoder: frames.NewDecoder(&httpResponseReaderCloser),
		logger:  c.logger,
	}

	return it, nil
}

// Write writes data
func (c *Client) Write(request *frames.WriteRequest) (frames.FrameAppender, error) {
	if request.Backend == "" || request.Table == "" {
		return nil, fmt.Errorf("missing request parameters")
	}

	if request.Session == nil {
		request.Session = c.session
	}

	msg, err := pbWriteReq(request)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	enc := frames.NewEncoder(&buf)
	if err := enc.Encode(msg); err != nil {
		return nil, errors.Wrap(err, "Can't encode request")
	}

	reader, writer := io.Pipe()

	httpRequest := fasthttp.AcquireRequest()
	httpRequest.URI().SetScheme(c.url.Scheme)
	httpRequest.URI().SetHost(c.url.Host)
	httpRequest.URI().SetPath(c.url.Path + "/write")
	httpRequest.Header.SetContentType("application/json")
	httpRequest.Header.SetMethod("POST")
	httpRequest.SetBodyStream(io.MultiReader(&buf, reader), -1)

	appender := &streamFrameAppender{
		writer:  writer,
		encoder: frames.NewEncoder(writer),
		ch:      make(chan *appenderHTTPResponse, 1),
		logger:  c.logger,
	}

	// Call API in a goroutine since it's going to block reading from pipe
	go func() {
		httpResponse := fasthttp.AcquireResponse()
		err := c.httpClient.Do(httpRequest, httpResponse)
		if err != nil {
			c.logger.ErrorWith("error calling API", "error", err)
		}

		appender.ch <- &appenderHTTPResponse{httpResponse, err}
	}()

	return appender, nil
}

// Delete deletes data
func (c *Client) Delete(request *pb.DeleteRequest) error {
	if request.Session == nil {
		request.Session = c.session
	}

	_, err := c.jsonCall("/delete", request, false)
	return err
}

// Create creates a table
func (c *Client) Create(request *pb.CreateRequest) error {
	if request.Session == nil {
		request.Session = c.session
	}

	_, err := c.jsonCall("/create", request, false)
	return err
}

// Exec executes a command
func (c *Client) Exec(request *pb.ExecRequest) (frames.Frame, error) {
	if request.Session == nil {
		request.Session = c.session
	}

	httpResponse, err := c.jsonCall("/exec", request, true)
	if err != nil {
		return nil, err
	}

	defer fasthttp.ReleaseResponse(httpResponse)
	var reply struct {
		Frame string `json:"frame"`
	}

	if err := json.Unmarshal(httpResponse.Body(), &reply); err != nil {
		return nil, errors.Wrap(err, "bad JSON reply")
	}

	if reply.Frame == "" {
		return nil, nil
	}

	data, err := base64.StdEncoding.DecodeString(reply.Frame)
	if err != nil {
		return nil, errors.Wrap(err, "bad base64 encoding of frame")
	}

	return frames.UnmarshalFrame(data)
}

func (c *Client) jsonCall(path string, request interface{}, returnResponse bool) (*fasthttp.Response, error) {
	var buf bytes.Buffer

	if err := json.NewEncoder(&buf).Encode(request); err != nil {
		return nil, errors.Wrap(err, "can't encode request")
	}

	httpRequest := fasthttp.AcquireRequest()
	httpRequest.URI().SetScheme(c.url.Scheme)
	httpRequest.URI().SetHost(c.url.Host)
	httpRequest.URI().SetPath(path)
	httpRequest.SetBody(buf.Bytes())
	httpRequest.Header.SetContentType("application/json")
	httpRequest.Header.SetMethod("POST")

	httpResponse := fasthttp.AcquireResponse()

	err := c.httpClient.Do(httpRequest, httpResponse)
	fasthttp.ReleaseRequest(httpRequest)
	if err != nil {
		fasthttp.ReleaseResponse(httpResponse)
		return nil, err
	}

	if httpResponse.StatusCode() != http.StatusOK {
		return httpResponse, fmt.Errorf("error calling server - %q", httpResponse.StatusCode())
	}

	if !returnResponse {
		fasthttp.ReleaseResponse(httpResponse)
		httpResponse = nil
	}

	return httpResponse, nil
}

// streamFrameIterator implements FrameIterator over io.Reader
type streamFrameIterator struct {
	frame   frames.Frame
	err     error
	reader  io.Reader
	decoder *frames.Decoder
	logger  logger.Logger
}

func (it *streamFrameIterator) Next() bool {
	var err error
	msg := &pb.Frame{}

	err = it.decoder.Decode(msg)
	if err == nil {
		it.frame = frames.NewFrameFromProto(msg)
		return true
	}

	if err == io.EOF {
		closer, ok := it.reader.(io.Closer)
		if ok {
			if err := closer.Close(); err != nil {
				it.logger.WarnWith("can't close reader", "error", err)
			}
		}

		return false
	}

	it.err = err
	return false
}

func (it *streamFrameIterator) At() frames.Frame {
	return it.frame
}

func (it *streamFrameIterator) Err() error {
	return it.err
}

type appenderHTTPResponse struct {
	httpResponse *fasthttp.Response
	err          error
}

// streamFrameAppender implements FrameAppender over io.Writer
type streamFrameAppender struct {
	writer  io.Writer
	encoder *frames.Encoder
	ch      chan *appenderHTTPResponse
	logger  logger.Logger
}

func (a *streamFrameAppender) Add(frame frames.Frame) error {
	iface, ok := frame.(pb.Framed)
	if !ok {
		return errors.New("unknown frame type")
	}

	if err := a.encoder.Encode(iface.Proto()); err != nil {
		return errors.Wrap(err, "can't encode frame")
	}

	return nil
}

func (a *streamFrameAppender) WaitForComplete(timeout time.Duration) error {
	closer, ok := a.writer.(io.Closer)
	if !ok {
		return fmt.Errorf("writer is not a closer")
	}

	if err := closer.Close(); err != nil {
		return errors.Wrap(err, "can't close writer")
	}

	select {
	case hr := <-a.ch:
		if hr.httpResponse.StatusCode() != http.StatusOK {
			err := fmt.Errorf("Server returned error: %d\n%s",
				hr.httpResponse.StatusCode(),
				string(hr.httpResponse.Body()))
			fasthttp.ReleaseResponse(hr.httpResponse)

			return err
		}

		fasthttp.ReleaseResponse(hr.httpResponse)
		return hr.err
	case <-time.After(timeout):
		return fmt.Errorf("timeout after %s", timeout)
	}
}

func pbWriteReq(req *frames.WriteRequest) (*pb.InitialWriteRequest, error) {
	var frMsg *pb.Frame
	if req.ImmidiateData != nil {
		iface, ok := req.ImmidiateData.(pb.Framed)
		if !ok {
			return nil, errors.New("unknown frame type")
		}
		frMsg = iface.Proto()
	}

	msg := &pb.InitialWriteRequest{
		Session:     req.Session,
		Backend:     req.Backend,
		Table:       req.Table,
		InitialData: frMsg,
		Expression:  req.Expression,
		More:        req.HaveMore,
	}

	return msg, nil
}
