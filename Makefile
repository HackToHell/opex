BINARY    := opex
MODULE    := github.com/hacktohell/opex
BUILD_DIR := bin

# Build-time variables
VERSION   ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
REVISION  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BRANCH    ?= $(shell git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")
BUILD_DATE := $(shell date -u '+%Y-%m-%dT%H:%M:%SZ')
GO_VERSION := $(shell go version | awk '{print $$3}')

LDFLAGS := -X '$(MODULE)/internal/api.Version=$(VERSION)' \
           -X '$(MODULE)/internal/api.Revision=$(REVISION)' \
           -X '$(MODULE)/internal/api.Branch=$(BRANCH)' \
           -X '$(MODULE)/internal/api.BuildDate=$(BUILD_DATE)'

.PHONY: build test run clean up down logs seed fmt vet lint

## build: Compile the opex binary
build:
	@mkdir -p $(BUILD_DIR)
	go build -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY) ./cmd/opex

## test: Run unit tests
test:
	go test -v -race ./...

## test-integration: Run integration tests (requires ClickHouse)
test-integration:
	go test -v -race -tags integration ./...

## run: Build and run the server
run: build
	./$(BUILD_DIR)/$(BINARY) --config config.yaml

## run-dev: Run with go run (faster iteration)
run-dev:
	go run -ldflags "$(LDFLAGS)" ./cmd/opex --config config.yaml

## up: Start ClickHouse and Grafana via docker-compose
up:
	docker-compose -f deploy/docker-compose.yml up -d
	@echo "Waiting for ClickHouse to be healthy..."
	@until docker exec opex-clickhouse clickhouse-client --query "SELECT 1" >/dev/null 2>&1; do sleep 1; done
	@echo "ClickHouse is ready."
	@echo "Grafana:    http://localhost:3000  (admin/admin)"
	@echo "ClickHouse: http://localhost:8123"

## down: Stop and remove containers and volumes
down:
	docker-compose -f deploy/docker-compose.yml down -v

## logs: Tail docker-compose logs
logs:
	docker-compose -f deploy/docker-compose.yml logs -f

## seed: Re-run seed data (ClickHouse must be running)
seed:
	docker exec -i opex-clickhouse clickhouse-client < deploy/clickhouse/seed.sql

## matviews: Create materialized views for query optimization (ClickHouse must be running)
matviews:
	docker exec -i opex-clickhouse clickhouse-client --multiquery < deploy/clickhouse/materialized_views.sql
	@echo "Materialized views created."

## fmt: Format Go source
fmt:
	go fmt ./...
	gofmt -s -w .

## vet: Run go vet
vet:
	go vet ./...

## lint: Run golangci-lint
lint:
	golangci-lint run ./...

## clean: Remove build artifacts
clean:
	rm -rf $(BUILD_DIR)

## help: Show this help
help:
	@echo "Usage: make [target]"
	@echo ""
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## /  /'
