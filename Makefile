# Copyright 2018 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
FRAMES_TAG ?= latest
FRAMES_REPOSITORY ?= iguazio/
FRAMES_PATH ?= src/github.com/v3io/frames
FRAMES_BUILD_COMMAND ?= GO111MODULE=on go build -o framesd-$(FRAMES_TAG)-$(GOOS)-$(GOARCH) -ldflags "-X main.Version=$(FRAMES_TAG)" ./cmd/framesd

GOPATH ?= ~/go

.PHONY: build
build:
	docker build \
		--build-arg FRAMES_VERSION=$(FRAMES_TAG) \
		--file cmd/framesd/Dockerfile \
		--tag $(FRAMES_REPOSITORY)framesd:$(FRAMES_TAG) \
		.

build-framulate:
	docker build \
		--build-arg FRAMES_VERSION=$(FRAMES_TAG) \
		--file cmd/framulate/Dockerfile \
		--tag $(FRAMES_REPOSITORY)framulate:$(FRAMES_TAG) \
		.

.PHONY: flake8
flake8:
	cd clients/py && make flake8

.PHONY: test
test: test-go test-py

.PHONY: test-go
test-go:
	GO111MODULE=on go test -v $(testflags) -timeout 20m ./...

.PHONY: test-py
test-py:
	cd clients/py && $(MAKE) test

.PHONY: wheel
wheel:
	cd clients/py && python setup.py bdist_wheel

.PHONY: python-dist
python-dist: python-deps
	cd clients/py && $(MAKE) dist

.PHONY: grpc
grpc: grpc-go grpc-py

.PHONY: grpc-go
grpc-go:
	protoc frames.proto --go_out=plugins=grpc:pb

.PHONY: grpc-py
grpc-py:
	cd clients/py && \
	python -m grpc_tools.protoc \
		-I../.. --python_out=v3io_frames\
		--grpc_python_out=v3io_frames \
		../../frames.proto
	python _scripts/fix_pb_import.py \
	    clients/py/v3io_frames/frames_pb2_grpc.py

.PHONY: pypi
pypi:
	cd clients/py && make upload

.PHONY: cloc
cloc:
	cloc \
	    --exclude-dir=_t,.ipynb_checkpoints,_examples,_build \
	    .

.PHONY: update-deps
update-deps: update-go-deps update-py-deps update-tsdb-deps

.PHONY: update-go-deps
update-go-deps:
	go mod tidy
	git add go.mod go.sum
	@echo "Don't forget to test & commit"

.PHONY: python-deps
python-deps:
	cd clients/py && $(MAKE) sync-deps

.PHONY: bench
bench:
	@echo Go
	$(MAKE) bench-go
	@echo Python
	$(MAKE) bench-py

.PHONY: bench-go
bench-go:
	./_scripts/go_benchmark.py

.PHONY: bench-py
bench-py:
	./_scripts/py_benchmark.py

.PHONY: frames-bin
frames-bin:
	$(FRAMES_BUILD_COMMAND)

.PHONY: frames
frames:
	docker run \
		--volume $(shell pwd):/go/$(FRAMES_PATH) \
		--volume $(shell pwd):/go/bin \
		--workdir /go/$(FRAMES_PATH) \
		--env GOOS=$(GOOS) \
		--env GOARCH=$(GOARCH) \
		--env FRAMES_TAG=$(FRAMES_TAG) \
		golang:1.14 \
		make frames-bin

PHONY: gofmt
gofmt:
ifeq ($(shell gofmt -l .),)
	# gofmt OK
else
	$(error Please run `go fmt ./...` to format the code)
endif

.PHONY: impi
impi:
	@echo Installing impi...
	GO111MODULE=off go get -u github.com/pavius/impi/cmd/impi
	@echo Verifying imports...
	$(GOPATH)/bin/impi \
		--local github.com/iguazio/provazio \
		--skip pkg/controller/apis \
		--skip pkg/controller/client \
		--ignore-generated \
		--scheme stdLocalThirdParty \
		./...

$(GOPATH)/bin/golangci-lint:
	@echo Installing golangci-lint...
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s v1.49.0
	cp ./bin/golangci-lint $(GOPATH)/bin/

.PHONY: lint
lint: gofmt impi $(GOPATH)/bin/golangci-lint
	@echo Linting...
	@$(GOPATH)/bin/golangci-lint run \
     --disable-all --enable=deadcode --enable=goconst --enable=golint --enable=ineffassign \
     --enable=interfacer --enable=unconvert --enable=varcheck --enable=errcheck --enable=gofmt --enable=misspell \
     --enable=staticcheck --enable=gosimple --enable=govet --enable=goconst \
     --timeout=10m \
    api/... backends/... cmd/... framulate/... grpc/... http/... repeatingtask/... v3ioutils/...
	@echo done linting
