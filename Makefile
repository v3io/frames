FRAMES_TAG ?= latest
FRAMES_REPOSITORY ?= iguazio/
FRAMES_PATH ?= src/github.com/v3io/frames
FRAMES_BUILD_COMMAND ?= GO111MODULE=on go build -o $(GOPATH)/bin/framesd-$(FRAMES_TAG)-$(GOOS)-$(GOARCH) -ldflags "-X main.Version=$(FRAMES_TAG)" ./cmd/framesd

.PHONY: build
build:
	docker build \
		--build-arg FRAMES_VERSION=$(FRAMES_TAG) \
		--file cmd/framesd/Dockerfile \
		--tag $(FRAMES_REPOSITORY)frames:$(FRAMES_TAG) \
		.

build-framulate:
	docker build \
		--build-arg FRAMES_VERSION=$(FRAMES_TAG) \
		--file cmd/framulate/Dockerfile \
		--tag $(FRAMES_REPOSITORY)framulate:$(FRAMES_TAG) \
		.

.PHONY: test
test: test-go test-py

.PHONY: test-go
test-go:
	GO111MODULE=on go test -v $(testflags) ./...

.PHONY: test-py
test-py:
	cd clients/py && $(MAKE) test

.PHONY: wheel
wheel:
	cd clients/py && python setup.py bdist_wheel

.PHONY: grpc
grpc: grpc-go grpc-py

.PHONY: grpc-go
grpc-go:
	protoc frames.proto --go_out=plugins=grpc:pb

.PHONY: grpc-py
grpc-py:
	cd clients/py && \
	pipenv run python -m grpc_tools.protoc \
		-I../.. --python_out=v3io_frames\
		--grpc_python_out=v3io_frames \
		../../frames.proto
	python _scripts/fix_pb_import.py \
	    clients/py/v3io_frames/frames_pb2_grpc.py

.PHONY: pypi
pypi:
	cd clients/py && \
	    pipenv run make upload

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

.PHONY: update-py-deps
update-py-deps:
	cd clients/py && $(MAKE) update-deps
	git add clients/py/Pipfile*
	@echo "Don't forget to test & commit"

.PHONY: update-tsdb-deps
update-tsdb-deps:
	GO111MODULE=on go get github.com/v3io/v3io-tsdb@master
	@echo "Done. Don't forget to commit ☺"

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
frames-bin: ensure-gopath
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
		golang:1.12 \
		make frames-bin

.PHONY: ensure-gopath
ensure-gopath:
ifndef GOPATH
	$(error GOPATH must be set)
endif
