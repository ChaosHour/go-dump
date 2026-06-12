# Makefile for go-dump

# Go parameters
GOCMD    = go
GOBUILD  = $(GOCMD) build
GOCLEAN  = $(GOCMD) clean
GOTEST   = $(GOCMD) test
GOVET    = $(GOCMD) vet
GOMOD    = $(GOCMD) mod

# Main package paths
MAIN_PATH      = ./cmd/go-dump
GOLOAD_PATH    = ./cmd/go-load

# Binary names and output directory
BINARY_NAME    = go-dump
GOLOAD_NAME    = go-load
BUILD_DIR      = ./bin
BINARY_PATH    = $(BUILD_DIR)/$(BINARY_NAME)
GOLOAD_BINARY  = $(BUILD_DIR)/$(GOLOAD_NAME)

# Version from VERSION file (override with: make build VERSION=1.2.3)
VERSION    = $(shell cat VERSION)
LDFLAGS    = -ldflags "-X main.AppVersion=$(VERSION)"
CGO_ENABLED = 0

# Unit tests: skip targets that require a live MySQL connection
UNIT_TEST_FLAGS = -run "TestTable|TestNewSingleDataChunk|TestNewDataChunk|TestNewLastDataChunk|TestParseWhereCondition"

.PHONY: all build build-go-load build-all-binaries \
        build-linux build-linux-arm64 build-darwin build-darwin-arm64 \
        build-go-load-linux build-go-load-linux-arm64 build-go-load-darwin build-go-load-darwin-arm64 \
        clean test test-unit test-integration coverage vet fmt lint run deps version help

# Default target
all: clean deps vet test-unit build

# ── Build ──────────────────────────────────────────────────────────────────────

build:
	@echo "Building $(BINARY_NAME) $(VERSION) (native)..."
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=$(CGO_ENABLED) $(GOBUILD) $(LDFLAGS) -o $(BINARY_PATH) $(MAIN_PATH)
	@echo "  → $(BINARY_PATH)"

build-go-load:
	@echo "Building $(GOLOAD_NAME) $(VERSION) (native)..."
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=$(CGO_ENABLED) $(GOBUILD) $(LDFLAGS) -o $(GOLOAD_BINARY) $(GOLOAD_PATH)
	@echo "  → $(GOLOAD_BINARY)"

build-all-binaries: build build-go-load

build-linux:
	@echo "Building $(BINARY_NAME) $(VERSION) for linux/amd64..."
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=$(CGO_ENABLED) GOOS=linux GOARCH=amd64 \
		$(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-linux-amd64 $(MAIN_PATH)
	@echo "  → $(BUILD_DIR)/$(BINARY_NAME)-linux-amd64"

build-linux-arm64:
	@echo "Building $(BINARY_NAME) $(VERSION) for linux/arm64..."
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=$(CGO_ENABLED) GOOS=linux GOARCH=arm64 \
		$(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-linux-arm64 $(MAIN_PATH)
	@echo "  → $(BUILD_DIR)/$(BINARY_NAME)-linux-arm64"

build-darwin:
	@echo "Building $(BINARY_NAME) $(VERSION) for darwin/amd64..."
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=$(CGO_ENABLED) GOOS=darwin GOARCH=amd64 \
		$(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-darwin-amd64 $(MAIN_PATH)
	@echo "  → $(BUILD_DIR)/$(BINARY_NAME)-darwin-amd64"

build-darwin-arm64:
	@echo "Building $(BINARY_NAME) $(VERSION) for darwin/arm64..."
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=$(CGO_ENABLED) GOOS=darwin GOARCH=arm64 \
		$(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-darwin-arm64 $(MAIN_PATH)
	@echo "  → $(BUILD_DIR)/$(BINARY_NAME)-darwin-arm64"

build-go-load-linux:
	@echo "Building $(GOLOAD_NAME) $(VERSION) for linux/amd64..."
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=$(CGO_ENABLED) GOOS=linux GOARCH=amd64 \
		$(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(GOLOAD_NAME)-linux-amd64 $(GOLOAD_PATH)
	@echo "  → $(BUILD_DIR)/$(GOLOAD_NAME)-linux-amd64"

build-go-load-linux-arm64:
	@echo "Building $(GOLOAD_NAME) $(VERSION) for linux/arm64..."
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=$(CGO_ENABLED) GOOS=linux GOARCH=arm64 \
		$(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(GOLOAD_NAME)-linux-arm64 $(GOLOAD_PATH)
	@echo "  → $(BUILD_DIR)/$(GOLOAD_NAME)-linux-arm64"

build-go-load-darwin:
	@echo "Building $(GOLOAD_NAME) $(VERSION) for darwin/amd64..."
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=$(CGO_ENABLED) GOOS=darwin GOARCH=amd64 \
		$(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(GOLOAD_NAME)-darwin-amd64 $(GOLOAD_PATH)
	@echo "  → $(BUILD_DIR)/$(GOLOAD_NAME)-darwin-amd64"

build-go-load-darwin-arm64:
	@echo "Building $(GOLOAD_NAME) $(VERSION) for darwin/arm64..."
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=$(CGO_ENABLED) GOOS=darwin GOARCH=arm64 \
		$(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(GOLOAD_NAME)-darwin-arm64 $(GOLOAD_PATH)
	@echo "  → $(BUILD_DIR)/$(GOLOAD_NAME)-darwin-arm64"

build-all: build-linux build-linux-arm64 build-darwin build-darwin-arm64 \
           build-go-load-linux build-go-load-linux-arm64 build-go-load-darwin build-go-load-darwin-arm64
	@echo "All cross-compile targets built in $(BUILD_DIR)/"

# ── Test ───────────────────────────────────────────────────────────────────────

# Unit tests only — no MySQL connection required
test-unit:
	@echo "Running unit tests..."
	$(GOTEST) -v ./internal/dump/ $(UNIT_TEST_FLAGS)

# Integration tests — require a live MySQL instance (see test/test.ini)
test-integration:
	@echo "Running integration tests (requires MySQL)..."
	$(GOTEST) -v -timeout 60s ./internal/dump/

# Alias: same as test-unit by default
test: test-unit

# Coverage over unit tests
coverage:
	@echo "Running tests with coverage..."
	$(GOTEST) -race -coverprofile=coverage.out -covermode=atomic \
		./internal/dump/ $(UNIT_TEST_FLAGS)
	$(GOCMD) tool cover -html=coverage.out -o coverage.html
	@echo "  → coverage.html"

# ── Quality ────────────────────────────────────────────────────────────────────

vet:
	@echo "Running go vet..."
	$(GOVET) ./...

fmt:
	@echo "Formatting code..."
	$(GOCMD) fmt ./...

lint:
	@echo "Linting code (requires golangci-lint)..."
	golangci-lint run

# ── Misc ───────────────────────────────────────────────────────────────────────

run: build
	$(BINARY_PATH) --help

deps:
	@echo "Tidying dependencies..."
	$(GOMOD) download
	$(GOMOD) tidy

clean:
	@echo "Cleaning..."
	$(GOCLEAN)
	@rm -rf $(BUILD_DIR)
	@rm -f $(BINARY_NAME)
	@echo "  done"

version:
	@echo "$(BINARY_NAME) $(VERSION)"

help:
	@echo "Usage: make <target> [VERSION=x.y.z]"
	@echo ""
	@echo "Build:"
	@echo "  build                  go-dump native binary → $(BUILD_DIR)/$(BINARY_NAME)"
	@echo "  build-go-load          go-load native binary → $(BUILD_DIR)/$(GOLOAD_NAME)"
	@echo "  build-all-binaries     Both binaries (native)"
	@echo "  build-linux            go-dump linux/amd64"
	@echo "  build-linux-arm64      go-dump linux/arm64"
	@echo "  build-darwin           go-dump darwin/amd64"
	@echo "  build-darwin-arm64     go-dump darwin/arm64"
	@echo "  build-go-load-linux        go-load linux/amd64"
	@echo "  build-go-load-linux-arm64  go-load linux/arm64"
	@echo "  build-go-load-darwin       go-load darwin/amd64"
	@echo "  build-go-load-darwin-arm64 go-load darwin/arm64"
	@echo "  build-all              All cross-compile targets (both tools)"
	@echo ""
	@echo "Test:"
	@echo "  test               Unit tests (no MySQL required)"
	@echo "  test-unit          Same as test"
	@echo "  test-integration   All tests (requires live MySQL)"
	@echo "  coverage           Unit tests + HTML coverage report"
	@echo ""
	@echo "Quality:"
	@echo "  vet                go vet ./..."
	@echo "  fmt                go fmt ./..."
	@echo "  lint               golangci-lint run"
	@echo ""
	@echo "Other:"
	@echo "  all                clean + deps + vet + test-unit + build"
	@echo "  run                build + --help"
	@echo "  deps               go mod download + tidy"
	@echo "  clean              Remove bin/ and build cache"
	@echo "  version            Print version"
