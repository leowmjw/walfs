.PHONY: help test test-coverage build example-basic example-idle-rotate example-basic-local lint fmt clean

# Default target
help:
	@echo "Available targets:"
	@echo "  make test                - Run all tests"
	@echo "  make test-coverage       - Run tests with coverage report"
	@echo "  make build               - Build the project"
	@echo "  make example-basic       - Run basic remote WAL example with Tigris"
	@echo "  make example-idle-rotate - Run idle rotation example"
	@echo "  make example-basic-local - Run basic example in local-only mode"
	@echo "  make lint                - Run linter"
	@echo "  make fmt                 - Format code"
	@echo "  make clean               - Clean build artifacts and test data"
	@echo ""
	@echo "For Tigris examples, configure .env first:"
	@echo "  cp .env.sample .env"
	@echo "  # Edit .env with your Tigris credentials"

test:
	gotest -v ./...

test-coverage:
	go test -v -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

build:
	go build ./...

example-basic:
	@if [ ! -f .env ]; then \
		echo "⚠️  No .env file found. Copy .env.sample to .env and configure your Tigris credentials."; \
		echo "   Running in local-only mode..."; \
		echo ""; \
	fi
	@cd examples/basic && \
		if [ -f ../../.env ]; then export $$(grep -v '^#' ../../.env | xargs); fi && \
		go run main.go

example-idle-rotate:
	@cd examples/idle-rotate && go run main.go

example-basic-local:
	@cd examples/basic && \
		unset TIGRIS_ACCESS_KEY_ID TIGRIS_SECRET_ACCESS_KEY TIGRIS_BUCKET_NAME && \
		go run main.go

lint:
	golangci-lint run

fmt:
	go fmt ./...

clean:
	rm -rf coverage.out coverage.html
	rm -rf examples/*/wal-data
	rm -rf examples/*/wal-data-node2
	find . -name '*.test' -delete 2>/dev/null || true
	find . -name '*.out' -delete 2>/dev/null || true
	@echo "✓ Cleaned build artifacts and test data"
