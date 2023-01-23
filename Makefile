COMMIT_SHA ?= $(shell git rev-parse HEAD)
REPONAME ?= metrico
IMAGE_NAME ?= "otel-collector"
CONFIG_FILE ?= ./config/default-config.yaml
DOCKER_TAG ?= latest

GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)
GOPATH ?= $(shell go env GOPATH)
GOTEST=go test -v $(RACE)
GOFMT=gofmt
FMT_LOG=.fmt.log
IMPORT_LOG=.import.log


.PHONY: install-tools
install-tools:
	go mod tidy
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.42.0

.DEFAULT_GOAL := test-and-lint

.PHONY: test-and-lint
test-and-lint: test fmt lint

.PHONY: test
test:
	go test -count=1 -v -race -cover ./...

.PHONY: build
build:
	GOOS=$(GOOS) GOARCH=$(GOARCH) go build -o otel-collector ./cmd/otel-collector

.PHONY: run
run:
	go run cmd/otel-collector/*.go --config ${CONFIG_FILE}

.PHONY: fmt
fmt:
	@echo Running go fmt on query service ...
	@$(GOFMT) -e -s -l -w .

.PHONY: build-qryn-collector
build-qryn-collector:
	@echo "------------------"
	@echo "--> Building qryn collector docker image"
	@echo "------------------"
	docker buildx build --progress plane \
		--no-cache -f cmd/otel-collector/Dockerfile \
		--tag $(REPONAME)/$(IMAGE_NAME):$(DOCKER_TAG) .

.PHONY: lint
lint:
	@echo "Running linters..."
	@$(GOPATH)/bin/golangci-lint -v --config .golangci.yml run && echo "Done."

.PHONY: install-ci
install-ci: install-tools

.PHONY: test-ci
test-ci: lint
