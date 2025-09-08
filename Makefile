# Makefile for go-dump

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod

# Main package path
MAIN_PATH=./go/cmd/go-dump

# Binary name and output directory
BINARY_NAME=go-dump
BINARY_UNIX=$(BINARY_NAME)_unix
BUILD_DIR=./bin
BINARY_PATH=$(BUILD_DIR)/$(BINARY_NAME)

# Build flags
LDFLAGS=-ldflags "-X main.AppVersion=$(VERSION)"
CGO_ENABLED=0

# Version from VERSION file
VERSION=$(shell cat VERSION)

.PHONY: all build clean test coverage run deps help

# Default target
all: clean deps test build

# Build the binary
build:
	@echo "Building $(BINARY_NAME) version $(VERSION)..."
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=$(CGO_ENABLED) $(GOBUILD) $(LDFLAGS) -o $(BINARY_PATH) $(MAIN_PATH)
	@echo "Binary built: $(BINARY_PATH)"

# Build for Linux
build-linux:
	@echo "Building $(BINARY_NAME) for Linux..."
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=$(CGO_ENABLED) GOOS=linux GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_UNIX) $(MAIN_PATH)
	@echo "Linux binary built: $(BUILD_DIR)/$(BINARY_UNIX)"

# Clean build artifacts
clean:
	@echo "Cleaning..."
	$(GOCLEAN)
	@rm -rf $(BUILD_DIR)
	@rm -f $(BINARY_NAME)
	@echo "Clean complete"

# Run tests
test:
	@echo "Running tests..."
	$(GOTEST) -v ./...

# Run tests with coverage
coverage:
	@echo "Running tests with coverage..."
	$(GOTEST) -race -coverprofile=coverage.out -covermode=atomic ./...
	$(GOCMD) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Run the binary
run: build
	@echo "Running $(BINARY_NAME)..."
	./$(BINARY_PATH) --help

# Download dependencies
deps:
	@echo "Downloading dependencies..."
	$(GOMOD) download
	$(GOMOD) tidy

# Install dependencies
install-deps:
	@echo "Installing dependencies..."
	$(GOGET) -v ./...

# Format code
fmt:
	@echo "Formatting code..."
	$(GOCMD) fmt ./...

# Lint code (requires golangci-lint)
lint:
	@echo "Linting code..."
	golangci-lint run

# Show version
version:
	@echo "$(BINARY_NAME) version: $(VERSION)"

# Show help
help:
	@echo "Available targets:"
	@echo "  all         - Clean, download deps, test, and build"
	@echo "  build       - Build the binary"
	@echo "  build-linux - Build for Linux"
	@echo "  clean       - Clean build artifacts"
	@echo "  test        - Run tests"
	@echo "  coverage    - Run tests with coverage report"
	@echo "  run         - Build and run the binary with --help"
	@echo "  deps        - Download and tidy dependencies"
	@echo "  install-deps- Install project dependencies"
	@echo "  fmt         - Format Go code"
	@echo "  lint        - Lint code (requires golangci-lint)"
	@echo "  version     - Show version"
	@echo "  help        - Show this help message"
