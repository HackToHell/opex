# Opex

A TraceQL-to-ClickHouse SQL transpiler with Tempo-compatible HTTP APIs. Opex lets you query OpenTelemetry traces stored in ClickHouse using [TraceQL](https://grafana.com/docs/tempo/latest/traceql/) and plugs directly into Grafana as a Tempo datasource -- no Tempo deployment required.

## Why Opex?

If you already store OTEL traces in ClickHouse (via the [OpenTelemetry Collector ClickHouse exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/clickhouseexporter) or similar), you can query them with the full TraceQL language without running Grafana Tempo. Opex translates TraceQL queries into efficient ClickHouse SQL, serves Tempo-compatible API responses, and integrates seamlessly with Grafana's Tempo datasource plugin.

```
Grafana  --(TraceQL)--> Opex --(SQL)--> ClickHouse
  ^                       |
  |                       |
  +--- Tempo-compatible --+
       JSON / protobuf
```

## Features

- **Full TraceQL support** -- span filters, boolean/arithmetic operators, regex matching, pipelines, aggregates (`count`, `min`, `max`, `sum`, `avg`), spanset operations (`&&`, `||`), structural operators (`>`, `>>`, `~`), and existence checks
- **Metrics queries** -- `rate()`, `count_over_time()`, `quantile_over_time()`, `histogram_over_time()`, `min_over_time()`, `max_over_time()`, `avg_over_time()`, `sum_over_time()` with `by()` grouping and `topk()`/`bottomk()` second-stage operations
- **Tempo API compatibility** -- all endpoints Grafana's Tempo plugin calls are implemented (trace by ID, search, tags, tag values, metrics query range/instant, metrics summary)
- **Grafana integration** -- works as a drop-in Tempo datasource with tag autocomplete, TraceQL editor support, trace detail views, and metrics panels
- **Production features** -- Prometheus metrics (`/metrics`), circuit breaker, automatic reconnection, query concurrency limits, graceful shutdown, configurable query timeouts
- **Materialized view support** -- optional pre-computed tables for faster tag discovery and trace metadata lookups
- **Query optimization** -- PREWHERE support, SAMPLE clause via `with(sample=0.1)` hints, LIMIT pushdown

## Quick Start

### Prerequisites

- [Go](https://go.dev/) 1.25.5+
- [Docker](https://www.docker.com/) and Docker Compose
- (Optional) [Grafana](https://grafana.com/) for the UI

### One-command setup

The fastest way to get everything running (ClickHouse with sample data, Opex, and Grafana):

```bash
make up
```

This starts three containers:
- **Opex** at [http://localhost:8080](http://localhost:8080)
- **Grafana** at [http://localhost:3000](http://localhost:3000) (login: `admin` / `admin`)
- **ClickHouse** at [http://localhost:8123](http://localhost:8123) (native port: 9000)

Grafana is pre-configured with Opex as a Tempo datasource. Open Grafana, go to **Explore**, select the **Opex (Tempo)** datasource, and start writing TraceQL queries.

To stop everything:

```bash
make down
```

### Run locally (without Docker for Opex)

If you want to iterate on Opex itself while using Docker only for ClickHouse and Grafana:

1. Start ClickHouse (the `make up` command starts all three, or start just ClickHouse):
   ```bash
   docker-compose -f deploy/docker-compose.yml up -d clickhouse
   ```

2. Wait for ClickHouse to be ready, then seed sample data:
   ```bash
   make seed
   ```

3. Run Opex on your host:
   ```bash
   make run-dev
   ```
   This starts Opex on `:8080` using `config.yaml`, which points to `localhost:9000` for ClickHouse.

4. (Optional) Start Grafana and point the Tempo datasource URL to `http://host.docker.internal:8080`.

### Build from source

```bash
make build          # Produces bin/opex
./bin/opex --config config.yaml
```

## Configuration

Opex is configured via a YAML file passed with `--config`. All fields have sensible defaults.

```yaml
listen_addr: ":8080"

clickhouse:
  dsn: "clickhouse://localhost:9000/otel"
  traces_table: "otel_traces"
  run_migrations: true          # Auto-apply embedded schema migrations on startup
  max_open_conns: 10
  max_idle_conns: 5
  conn_max_lifetime: 5m
  dial_timeout: 5s
  read_timeout: 30s
  use_materialized_views: false       # Enable after running `make matviews`
  health_check_interval: 5s
  max_retries: 2
  retry_base_delay: 50ms
  circuit_breaker_threshold: 5
  circuit_breaker_timeout: 10s

query:
  max_limit: 100
  default_limit: 20
  default_spss: 3         # Spans per spanset in search results
  max_duration: 168h      # Maximum query time range (7 days)
  timeout: 30s
  max_concurrent: 20

logging:
  level: info             # debug, info, warn, error
  format: text            # text or json
```

### Key configuration options

| Option | Description | Default |
|--------|-------------|---------|
| `listen_addr` | HTTP server bind address | `:8080` |
| `clickhouse.dsn` | ClickHouse connection string | `clickhouse://localhost:9000/default` |
| `clickhouse.traces_table` | OTEL traces table name | `otel_traces` |
| `clickhouse.run_migrations` | Render and apply embedded schema migrations on startup | `false` |
| `clickhouse.use_materialized_views` | Use pre-computed tables for tag/metadata queries | `false` |
| `query.timeout` | Per-query execution timeout | `30s` |
| `query.max_concurrent` | Maximum number of concurrent queries | `20` |
| `query.max_limit` | Maximum number of traces returned per search | `100` |
| `query.default_spss` | Default spans per spanset in search results | `3` |
| `logging.level` | Log level | `info` |
| `logging.format` | Log output format (`text` or `json`) | `json` |

## API Endpoints

Opex implements the Grafana Tempo HTTP API. All endpoints support JSON (default) and protobuf (`Accept: application/protobuf`) response formats.

### Core Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/echo` | Health check, returns `echo` |
| `GET /api/status/buildinfo` | Build metadata (version, revision, Go version) |
| `GET /ready` | Readiness probe (checks ClickHouse connectivity) |
| `GET /metrics` | Prometheus metrics |

### Trace Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/traces/{traceID}` | Retrieve a full trace by ID (OTLP format) |
| `GET /api/v2/traces/{traceID}` | Retrieve a trace by ID (V2 envelope) |

### Search Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/search?q={traceql}` | Search traces with a TraceQL query |
| `GET /api/search/tags` | List available tag names |
| `GET /api/v2/search/tags` | List tag names grouped by scope (resource/span/intrinsic) |
| `GET /api/search/tag/{tagName}/values` | List values for a specific tag |
| `GET /api/v2/search/tag/{tagName}/values` | List typed values for a tag |

### Metrics Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/metrics/query_range?q={traceql}` | Time-series metrics from traces |
| `GET /api/metrics/query?q={traceql}` | Instant metrics query |
| `GET /api/metrics/summary?q={traceql}` | Span metrics summary with percentiles |

### Common Query Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `q` | string | TraceQL query |
| `start` | int | Start time (Unix seconds) |
| `end` | int | End time (Unix seconds) |
| `limit` | int | Max results to return |
| `spss` | int | Spans per spanset |
| `minDuration` | duration | Minimum span duration filter (e.g., `100ms`, `1s`) |
| `maxDuration` | duration | Maximum span duration filter |

## TraceQL Examples

```
# Find all spans from the frontend service
{ resource.service.name = "frontend" }

# Find error spans longer than 1 second
{ status = error && duration > 1s }

# Find traces where both GET requests and errors occur (possibly in different spans)
{ .http.method = "GET" } && { status = error }

# Pipeline: filter then aggregate
{ resource.service.name = "api-gateway" } | count() > 5

# Regex matching on span name
{ name =~ "GET /api/.*" }

# Check attribute existence
{ .http.method != nil }

# Metrics: error rate by service over time
{ status = error } | rate() by(resource.service.name)

# Metrics: p99 latency by service
{ } | quantile_over_time(duration, 0.99) by(resource.service.name)

# Structural: find child spans of a parent
{ resource.service.name = "frontend" } > { resource.service.name = "api-gateway" }
```

## ClickHouse Schema

Opex expects the standard OpenTelemetry ClickHouse traces schema. The table DDL is in [`deploy/clickhouse/init.sql`](deploy/clickhouse/init.sql) and is automatically applied when using `make up`.

Key columns:

| Column | Type | Description |
|--------|------|-------------|
| `TraceId` | String | Trace identifier |
| `SpanId` | String | Span identifier |
| `ParentSpanId` | String | Parent span ID (empty for root spans) |
| `SpanName` | LowCardinality(String) | Operation name |
| `ServiceName` | LowCardinality(String) | `resource.service.name` |
| `Duration` | UInt64 | Span duration in nanoseconds |
| `StatusCode` | LowCardinality(String) | `STATUS_CODE_OK`, `STATUS_CODE_ERROR`, `STATUS_CODE_UNSET` |
| `SpanAttributes` | Map(String, String) | Span-level attributes |
| `ResourceAttributes` | Map(String, String) | Resource-level attributes |
| `Events.*` | Nested arrays | Span events (e.g., exceptions) |
| `Links.*` | Nested arrays | Span links |

### Materialized Views

For better performance on tag discovery and search queries, Opex can read from optional materialized view tables.

If you enable startup migrations, Opex creates and backfills those tables automatically:

```yaml
clickhouse:
  run_migrations: true
```

If you manage schema outside Opex, create the materialized views manually instead:

```bash
make matviews
```

Then enable them in your config:

```yaml
clickhouse:
  use_materialized_views: true
```

## Architecture

```
cmd/opex/main.go              Entry point, config + logging setup
internal/
  api/                         HTTP handlers (Tempo-compatible endpoints)
  clickhouse/                  ClickHouse client with connection pooling,
                               circuit breaker, and retry logic
  config/                      YAML configuration with defaults
  metrics/                     Prometheus instrumentation
  response/                    Response types, JSON/protobuf marshaling,
                               ClickHouse row -> OTLP trace conversion
  server/                      HTTP server, routing, middleware
  traceql/                     TraceQL lexer, parser, AST types
  transpiler/                  AST-to-ClickHouse SQL transpiler
deploy/
  docker-compose.yml           ClickHouse + Opex + Grafana
  Dockerfile                   Multi-stage Docker build
  clickhouse/                  DDL (init.sql), seed data, materialized views
  grafana/                     Pre-provisioned Tempo datasource config
```

### Request Flow

1. Grafana sends a TraceQL query to an Opex API endpoint
2. The handler parses and validates the request parameters
3. The TraceQL parser produces an AST from the query string
4. The transpiler converts the AST into ClickHouse SQL
5. The ClickHouse client executes the SQL query
6. Results are converted to Tempo-compatible response types (OTLP protobuf structures)
7. The response is serialized as JSON or protobuf based on the `Accept` header

## Development

### Running tests

```bash
make test                        # All tests with race detector
go test ./internal/...           # All unit tests (faster, no race)
go test ./internal/transpiler/... # Transpiler tests only
```

### Integration tests

Integration tests run against a real ClickHouse instance via [testcontainers](https://testcontainers.com/):

```bash
make test-integration
```

### E2E tests

End-to-end tests use [Playwright](https://playwright.dev/) and require the full stack running:

```bash
make up
make test-e2e            # All E2E tests
make test-e2e-api        # API-only E2E tests
make test-e2e-grafana    # Grafana UI E2E tests
```

### Code quality

```bash
go fmt ./...             # Format code
go vet ./...             # Static analysis
make lint                # Run golangci-lint
```

### Makefile targets

| Target | Description |
|--------|-------------|
| `make build` | Compile `bin/opex` |
| `make test` | Run unit tests with race detector |
| `make test-integration` | Run integration tests (requires Docker) |
| `make run` | Build and run the server |
| `make run-dev` | Run with `go run` (faster iteration) |
| `make up` | Start ClickHouse + Opex + Grafana via Docker Compose |
| `make down` | Stop and remove containers and volumes |
| `make logs` | Tail Docker Compose logs |
| `make seed` | Re-run seed data (ClickHouse must be running) |
| `make matviews` | Create materialized views for optimization |
| `make fmt` | Format Go source |
| `make vet` | Run `go vet` |
| `make lint` | Run `golangci-lint` |
| `make docker-build` | Build the Opex Docker image |
| `make clean` | Remove build artifacts |

## Observability

Opex exposes Prometheus metrics at `/metrics`:

| Metric | Type | Description |
|--------|------|-------------|
| `opex_query_duration_seconds` | Histogram | HTTP request duration by endpoint, method, status |
| `opex_clickhouse_query_duration_seconds` | Histogram | ClickHouse query execution time by query type |
| `opex_active_queries` | Gauge | Number of in-flight queries |
| `opex_query_errors_total` | Counter | Query errors by error type |
| `opex_traces_searched_total` | Counter | Total traces inspected |
| `opex_spans_searched_total` | Counter | Total spans inspected |
| `opex_query_retries_total` | Counter | Query retries due to transient errors |
| `opex_clickhouse_circuit_state` | Gauge | Circuit breaker state (0=closed, 1=half-open, 2=open) |
| `opex_clickhouse_connected` | Gauge | ClickHouse connection status (0/1) |
| `opex_clickhouse_reconnect_attempts_total` | Counter | Reconnection attempts |

## Docker

### Build the image

```bash
make docker-build
```

### Run with Docker Compose

```bash
make up
```

### Standalone Docker

```bash
docker build -f deploy/Dockerfile -t opex .
docker run -p 8080:8080 -v $(pwd)/config.yaml:/etc/opex/config.yaml opex
```

## License

See [LICENSE](LICENSE) for details.
