# frames

[![Build Status](https://travis-ci.org/v3io/frames.svg?branch=master)](https://travis-ci.org/v3io/frames)
[![GoDoc](https://godoc.org/github.com/v3io/frames?status.svg)](https://godoc.org/github.com/v3io/frames)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Server and client library of streaming data from v3io

## Components

- Go server, both gRPC and HTTP protocols are supported
- Go client
- Python client

### Development

Core is written in [Go](https://golang.org/), we work on `development` branch
and release to `master.

- To run the Go tests run `make test`.
- To run the Python tests run `make test-python`

#### Adding/Changing Dependencies

- If you add Go dependencies run `make update-go-deps`
- If you add Python dependencies, updates `clients/py/Pipfile` and run `make
  update-py-deps`

### Docker Image

#### Build

    make build-docker

#### Running

     docker run -v /path/to/config.yaml:/etc/framesd/framesd.yaml quay.io/v3io/framesd

## LICENSE

[Apache 2](LICENSE)
