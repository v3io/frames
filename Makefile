FRAMES_TAG ?= latest
FRAMES_REPOSITORY ?= iguazio/
FRAMES_PATH ?= src/github.com/v3io/frames
#FRAMES_BUILD_COMMAND ?= GO111MODULE=on go build -v ./...
TSDB_BUILD_COMMAND ?= CGO_ENABLED=0 go build $(BUILD_OPTS) ./cmd/tsdbctl
#FRAMES_BUILD_COMMAND ?= CGO_ENABLED=0 go build -a -installsuffix cgo -ldflags="-X main.Version="${PROVAZIO_TAG} -o $(GOPATH)/bin/framesd-$(PROVAZIO_TAG)-$(GOOS)-$(GOARCH) $(GOPATH)/$(FRAMES_PATH)/cmd/framesd
FRAMES_BUILD_COMMAND ?= GO111MODULE=on go build -o framesd-$(FRAMES_TAG)-$(GOOS)-$(GOARCH) -ldflags "-X main.Version=$(FRAMES_TAG)" ./cmd/framesd

#	version := os.Getenv("TRAVIS_TAG")
#	if version == "" {
#		version = buildSha()
#	}
#
#	os.Setenv("GOARCH", "amd64")
#	for _, goos := range []string{"linux", "darwin", "windows"} {
#		exe := fmt.Sprintf("framesd-%s-amd64", goos)
#		if goos == "windows" {
#			exe += ".exe"
#		}
#		ldFlags := fmt.Sprintf("-X main.Version=%s", version)
#
#		os.Setenv("GOOS", goos)
#		err := run(
#			"go", "build",
#			"-o", exe,
#			"-ldflags", ldFlags,
#			"./cmd/framesd",
#		)
#		if err != nil {
#			log.Fatalf("error: can't build for %s", goos)
#		}
#	}

.PHONY: all
all: lint build
	@echo Done.

.PHONY: lint
lint: ensure-gopath
	@echo Installing linters...
	go get -u github.com/pavius/impi/cmd/impi
	go get -u gopkg.in/alecthomas/gometalinter.v2
	@$(GOPATH)/bin/gometalinter.v2 --install

	@echo Verifying imports...
	$(GOPATH)/bin/impi \
		--local github.com/iguazio/provazio \
		--skip pkg/controller/apis \
		--skip pkg/controller/client \
		--scheme stdLocalThirdParty \
		./pkg/... ./cmd/...

	@echo Linting...
	@$(GOPATH)/bin/gometalinter.v2 \
		--deadline=300s \
		--disable-all \
		--enable-gc \
		--enable=deadcode \
		--enable=gofmt \
		--enable=golint \
		--enable=gosimple \
		--enable=ineffassign \
		--enable=interfacer \
		--enable=misspell \
		--enable=staticcheck \
		--enable=unconvert \
		--enable=varcheck \
		--enable=vet \
		--enable=vetshadow \
		--enable=errcheck \
		--exclude="_test.go" \
		--exclude="comment on" \
		--exclude="error should be the last" \
		--exclude="should have comment" \
		--skip pkg/controller/apis \
		--skip pkg/controller/client \
		./pkg/...

	@echo Done.

.PHONY: bin
bin: ensure-gopath
	$(FRAMES_BUILD_COMMAND)

.PHONY: build
build:
	docker build \
		--build-arg FRAMES_VERSION=$(FRAMES_TAG) \
		--file cmd/framesd/Dockerfile \
		--tag $(FRAMES_REPOSITORY)frames:$(FRAMES_TAG) \
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
