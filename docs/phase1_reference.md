# Phase 1 Reference - Foundation & Dev Environment

**Status:** COMPLETE

## What Was Built

### Project Structure
```
opex/
├── cmd/opex/main.go                    # Entry point: flag parsing, config, logger, server
├── internal/
│   ├── config/config.go                # Config struct + YAML loading (DefaultConfig, LoadFromFile)
│   ├── server/server.go                # HTTP server: mux router, route registration, graceful shutdown
│   ├── api/handler.go                  # Handlers struct with Echo, Ready, BuildInfo handlers
│   ├── transpiler/                     # (empty, Phase 3)
│   ├── clickhouse/                     # (empty, Phase 4)
│   └── response/                       # (empty, Phase 4)
├── deploy/
│   ├── docker-compose.yml              # ClickHouse 24.8 + Grafana 11.2.0
│   ├── clickhouse/init.sql             # otel_traces table DDL in 'otel' database
│   ├── clickhouse/seed.sql             # 7 traces, 22 spans across 4 services
│   └── grafana/datasources.yml         # Tempo datasource -> host.docker.internal:8080
├── config.yaml                         # Default config file (DSN: clickhouse://localhost:9000/otel)
├── Makefile                            # build, test, run, up, down, seed, fmt, vet, clean
├── go.mod                              # module github.com/hacktohell/opex
└── go.sum
```

### Dependencies
- `github.com/gorilla/mux v1.8.1` -- HTTP router
- `gopkg.in/yaml.v3 v3.0.1` -- YAML config parsing

### Key Types

**Config** (`internal/config/config.go`):
- `Config.ListenAddr` (string, default ":8080")
- `Config.ClickHouse.DSN` (string, default "clickhouse://localhost:9000/otel")
- `Config.ClickHouse.TracesTable` (string, default "otel_traces")
- `Config.Query.DefaultLimit` (int, default 20)
- `Config.Query.DefaultSpss` (int, default 3)
- `Config.Query.Timeout` (time.Duration, default 30s)

**Server** (`internal/server/server.go`):
- `New(cfg, logger) *Server` -- creates server with routes
- `Server.Run() error` -- starts HTTP server, blocks until signal
- Routes registered: `/api/echo`, `/api/status/buildinfo`, `/ready`
- Logging middleware on all routes

**Handlers** (`internal/api/handler.go`):
- `NewHandlers(logger) *Handlers`
- Build-time vars: `Version`, `Revision`, `Branch`, `BuildDate` (set via ldflags)
- `Echo` -- returns "echo" (200)
- `Ready` -- returns "ready" (200)
- `BuildInfo` -- returns JSON with version/revision/branch/buildDate/goVersion

### ClickHouse Schema
- Database: `otel`
- Table: `otel.otel_traces`
- Engine: `MergeTree()` partitioned by `toDate(Timestamp)`, ordered by `(ServiceName, SpanName, toDateTime(Timestamp))`
- Key columns: Timestamp (DateTime64(9)), TraceId, SpanId, ParentSpanId, SpanName, SpanKind, ServiceName, Duration (UInt64 nanoseconds), StatusCode, StatusMessage
- Map columns: ResourceAttributes, SpanAttributes (Map(LowCardinality(String), String))
- Array columns: Events.*, Links.*
- Indexes: bloom_filter on TraceId, map keys/values; minmax on Duration

### Seed Data (7 traces)
| TraceId | Root Span | Services | Status | Duration |
|---------|-----------|----------|--------|----------|
| aaaa... | GET /login | frontend, api-gateway, user-service | OK | 350ms |
| bbbb... | POST /orders | frontend, api-gateway, order-service, payment-service | ERROR | 2.5s |
| cccc... | GET /products | frontend, api-gateway | OK (cache hit) | 15ms |
| dddd... | GET /orders/history | frontend, api-gateway, order-service | OK (slow DB) | 4.8s |
| eeee... | GET /healthz | frontend | UNSET | 1ms |
| ffff... | RetryPayment | order-service, payment-service | OK (has Links) | 500ms |
| 1111... | GET /dashboard | frontend, api-gateway, user-service, order-service | OK (parallel) | 800ms |

### Build & Run
```bash
make build          # -> bin/opex (with ldflags)
make run            # -> build + run with config.yaml
make up             # -> docker-compose up (ClickHouse + Grafana)
make down           # -> docker-compose down -v
make seed           # -> re-insert seed data
make test           # -> go test -v -race ./...
```

### Verified
- `go build ./...` succeeds
- `make build` produces `bin/opex` with version ldflags
- `go test -v -race ./...` passes (no test files yet, no failures)

## What's Next (Phase 2)
- Import `github.com/grafana/tempo/pkg/traceql` as a dependency
- Use `traceql.Parse(query)` to parse TraceQL strings into `*RootExpr` AST
- Build an AST walker/visitor that traverses all node types
- Write unit tests with 20+ TraceQL queries
- Add debug parse endpoint (dev only)
