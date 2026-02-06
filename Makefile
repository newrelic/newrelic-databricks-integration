export PATH := $(PATH):$(GOPATH)/bin

INTEGRATION  		:= newrelic-databricks-integration
BINARY_NAME   		= $(INTEGRATION)
LAMBDA_BINARY_NAME	= bootstrap
BIN_FILES			:= ./cmd/databricks/...
#LAMBDA_BIN_FILES	:= ./cmd/bitmovin-lambda/...

GIT_COMMIT = $(shell git rev-parse HEAD)
BUILD_DATE = $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
GIT_TAG    = $(shell git describe --tags --abbrev=0 --exact-match 2>/dev/null)

SYM_PATH   = github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/build
LDFLAGS += -X $(SYM_PATH).gBuildVersion=$(GIT_TAG)
LDFLAGS += -X $(SYM_PATH).gBuildCommit=${GIT_COMMIT}
LDFLAGS += -X $(SYM_PATH).gBuildDate=${BUILD_DATE}

all: build

#build: clean compile compile-lambda compile-docker
build: clean compile

clean:
	@echo "=== $(INTEGRATION) === [ clean ]: removing binaries..."
	@rm -rfv bin

bin/$(BINARY_NAME):
	@echo "=== $(INTEGRATION) === [ compile ]: building $(BINARY_NAME)..."
	@go mod tidy
	@go build -v -ldflags '$(LDFLAGS)' -o bin/$(BINARY_NAME) $(BIN_FILES)

#bin/docker/$(BINARY_NAME):
#	@echo "=== $(INTEGRATION) === [ compile ]: building Docker binary $(BINARY_NAME)..."
#	@go mod tidy
#	@GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -v -ldflags '$(LDFLAGS)' \
#		-o bin/docker/$(BINARY_NAME) $(BIN_FILES)
#
#bin/$(LAMBDA_BINARY_NAME):
#	@echo "=== $(INTEGRATION) === [ compile ]: building $(LAMBDA_BINARY_NAME)..."
#	@go mod tidy
#	@GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -v -ldflags '$(LDFLAGS)' \
#		-tags lambda.norpc -o bin/$(LAMBDA_BINARY_NAME) $(LAMBDA_BIN_FILES)

compile: bin/$(BINARY_NAME)

test:
	@go test ./... -coverprofile=coverage.out
	@GO_COVER_IGNORE_SPEC_PATH=".coverage-ignore.yaml" GO_COVER_IGNORE_COVER_PROFILE_PATH="coverage.out" go-cover-ignore
	@go tool cover -func=coverage.out | awk '{sub(/^.*\/newrelic-databricks-integration\//,""); print}'

test-html:
	@go test ./... -coverprofile=coverage.out
	@GO_COVER_IGNORE_SPEC_PATH=".coverage-ignore.yaml" GO_COVER_IGNORE_COVER_PROFILE_PATH="coverage.out" go-cover-ignore
	@go tool cover -html=coverage.out

#compile-lambda: bin/$(LAMBDA_BINARY_NAME)
#
#compile-docker: bin/docker/$(BINARY_NAME)
#
#docker: build
#	@docker build -t newrelic-bitmovin-analytics:latest \
#		-f build/package/Dockerfile \
#		.
#
#package-lambda: build
#	@./scripts/lambda/build.sh
#
#deploy-lambda: build
#	@./scripts/lambda/deploy.sh
#
#update-lambda: build
#	@./scripts/lambda/update.sh
#
#delete-lambda:
#	@./scripts/lambda/delete.sh
#
#.PHONY: all build clean compile compile-lambda compile-docker docker deploy-lambda update-lambda delete-lambda
.PHONY: all build clean compile test test-html
