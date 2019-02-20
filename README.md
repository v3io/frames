# V3IO Frames

[![Build Status](https://travis-ci.org/v3io/frames.svg?branch=master)](https://travis-ci.org/v3io/frames)
[![GoDoc](https://godoc.org/github.com/v3io/frames?status.svg)](https://godoc.org/github.com/v3io/frames)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

V3IO Frames is a high-speed server and client library for accessing time-series (TSDB), NoSQL, and streaming data in the [Iguazio Continuous Data Platform](https://www.iguazio.com).

## Components

- Go server with support for both the gRPC and HTTP protocols
- Go client
- Python client

### Development

The core is written in [Go](https://golang.org/).
The development is done on the `development` branch and then released to the `master` branch.

- To execute the Go tests, run `make test`.
- To execute the Python tests, run `make test-python`.

#### Adding/Changing Dependencies

- If you add Go dependencies, run `make update-go-deps`.
- If you add Python dependencies, update **clients/py/Pipfile** and run `make
  update-py-deps`.

#### Travis CI

Integration tests are run on [Travis CI](https://travis-ci.org/).
See **.travis.yml** for details.

The following environment variables are defined in the [Travis settings](https://travis-ci.org/v3io/frames/settings):

- Docker Container Registry ([Quay.io](https://quay.io/))
    - `DOCKER_PASSWORD` &mdash; Password for pushing images to Quay.io.
    - `DOCKER_USERNAME` &mdash; Username for pushing images to Quay.io.
- Python Package Index ([PyPI](https://pypi.org/))
    - `V3IO_PYPI_PASSWORD` &mdash; Password for pushing a new release to PyPi.
    - `V3IO_PYPI_USER` &mdash; Username for pushing a new release to PyPi.
- Iguazio Container Data Platform
    - `V3IO_SESSION` &mdash; A JSON encoded map with session information for running tests.
      For example:

      ```
      '{"url":"45.39.128.5:8081","container":"mitzi","user":"daffy","password":"rabbit season"}'
      ```
      > **Note:** Make sure to embed the JSON object within single quotes (`'{...}'`).

### Docker Image

#### Building the Image

Use the following command to build the Docker image:

```sh
make build-docker
```

#### Running the Image

Use the following command to run the Docker image:

```sh
docker run \
	-v /path/to/config.yaml:/etc/framesd.yaml \
	quay.io/v3io/frames:unstable
```

## LICENSE

[Apache 2](LICENSE)

