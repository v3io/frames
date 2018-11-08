# frames

[![Build Status](https://travis-ci.com/v3io/frames.svg?branch=master)](https://travis-ci.com/v3io/frames)
[![GoDoc](https://godoc.org/github.com/v3io/frames?status.svg)](https://godoc.org/github.com/v3io/frames)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Server and client library of streaming data from v3io

## Components

- Go server ([msgpack][msgpack] over chunked HTTP)
- Go client
- Python client

[msgpack]: https://msgpack.org/

## Server API

### POST /read

Read from database. POST body is a JSON encoded `ReadRequest`
Return value is chunked HTTP where each chunk is msgpack encoded `Frame`

### POST /write?backend=BACKEND&table=TABLE

Write to database, `backend` and `table` are fields in `WriteRequest`
Body should be chunked msgpack encoded frames
Response is JSON in the format

    {
	"num_frames": 10,
	"num_rows": 1000
    }

### Development

Core is written in [Go](https://golang.org/).

To run the tests run `go test -v ./...`

#### Adding Dependencies

If you add dependencies please run

    go mod tidy
    go mod vendor
    git add vendor go.mod go.sum

### Docker Image

#### Build

    make build-docker

#### Running

     docker run -v /path/to/config.yaml:/etc/framesd/framesd.yaml v3io/framesd

## LICENSE

[Apache 2](LICENSE)
