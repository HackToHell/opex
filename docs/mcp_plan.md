# MCP Server Implementation Plan

> Add a Model Context Protocol (MCP) server to Opex, enabling AI assistants and LLMs
> to query distributed tracing data via TraceQL, retrieve metrics, and explore attributes.

---

## Table of Contents

1. [Background & Motivation](#1-background--motivation)
2. [Architecture](#2-architecture)
3. [Prerequisites — Shared Service Layer](#3-prerequisites--shared-service-layer)
4. [MCP Server Design](#4-mcp-server-design)
5. [Tools](#5-tools)
6. [Resources](#6-resources)
7. [Embedded Documentation](#7-embedded-documentation)
8. [Configuration](#8-configuration)
9. [Resource Protection & Concurrency](#9-resource-protection--concurrency)
10. [Error Handling](#10-error-handling)
11. [Metrics & Observability](#11-metrics--observability)
12. [Security Considerations](#12-security-considerations)
13. [Testing Strategy](#13-testing-strategy)
14. [File Inventory](#14-file-inventory)
15. [Implementation Phases](#15-implementation-phases)
16. [Usage](#16-usage)
17. [Key Design Decisions](#17-key-design-decisions)
18. [Risks & Mitigations](#18-risks--mitigations)

---

## 1. Background & Motivation

### What is MCP?

MCP (Model Context Protocol) is an open standard from Anthropic for connecting AI assistants
and LLMs to external data sources. An MCP server exposes:

- **Tools** — Functions the LLM can call (search traces, run metrics queries, etc.)
- **Resources** — Read-only data the LLM can reference (documentation, schemas)
- **Prompts** — Reusable prompt templates (not used in this implementation)

### Why add MCP to Opex?

Opex already has a complete Tempo-compatible HTTP API. Adding an MCP server allows AI
assistants (Claude Code, Cursor, ChatGPT, Copilot, etc.) to directly:

- Search traces with TraceQL queries
- Compute RED metrics (Rate, Errors, Duration) from traces
- Retrieve specific traces by ID
- Explore available attributes and their values
- Learn TraceQL syntax from embedded documentation

### Prior Art

Grafana Tempo added an MCP server in v2.10 (see
[Tempo MCP docs](https://grafana.com/docs/tempo/latest/api_docs/mcp-server/)).
Their implementation uses `github.com/mark3labs/mcp-go` v0.43.2 and exposes 7 tools
plus documentation resources over Streamable HTTP at `/api/mcp`. Our implementation
follows a similar tool surface but with a cleaner internal architecture.

---

## 2. Architecture

### Current Architecture

```
┌─────────┐     HTTP          ┌──────────────────────────┐
│ Grafana  │ ───────────────> │        Opex Server       │
│          │ <─────────────── │                          │
└─────────┘                   │  ┌────────────────────┐  │
                              │  │  HTTP Handlers      │  │
                              │  │  (api package)      │  │
                              │  └────────┬───────────┘  │
                              │           │              │
                              │  ┌────────▼───────────┐  │
                              │  │  TraceQL Parser     │  │
                              │  │  + SQL Transpiler   │  │
                              │  └────────┬───────────┘  │
                              │           │              │
                              │  ┌────────▼───────────┐  │
                              │  │  ClickHouse Client  │  │
                              │  └────────┬───────────┘  │
                              └───────────┼──────────────┘
                                          │
                              ┌───────────▼──────────────┐
                              │      ClickHouse          │
                              └──────────────────────────┘
```

### With MCP Server

```
┌─────────┐     HTTP          ┌──────────────────────────────────────┐
│ Grafana  │ ───────────────> │            Opex Server               │
│          │ <─────────────── │                                      │
└─────────┘                   │  ┌──────────────┐  ┌──────────────┐  │
                              │  │ HTTP Handlers │  │  MCP Server  │  │
┌─────────┐  Streamable HTTP  │  │ (api package) │  │ /api/mcp/    │  │
│ Claude / │ ───────────────> │  └──────┬───────┘  └──────┬───────┘  │
│ Cursor / │ <─────────────── │         │                 │          │
│ ChatGPT  │                  │         │    ┌────────────┘          │
└─────────┘                   │         │    │                       │
                              │  ┌──────▼────▼─────────┐            │
                              │  │  Service Layer       │            │
                              │  │  (tracequery package)│            │
                              │  └────────┬────────────┘            │
                              │           │                          │
                              │  ┌────────▼───────────┐              │
                              │  │  TraceQL Parser     │              │
                              │  │  + SQL Transpiler   │              │
                              │  └────────┬───────────┘              │
                              │           │                          │
                              │  ┌────────▼───────────┐              │
                              │  │  ClickHouse Client  │              │
                              │  └────────┬───────────┘              │
                              └───────────┼──────────────────────────┘
                                          │
                              ┌───────────▼──────────────┐
                              │      ClickHouse          │
                              └──────────────────────────┘
```

The critical addition is the **service layer** (`internal/tracequery/`). Both the HTTP
handlers and the MCP server call the same service functions, eliminating logic duplication.

---

## 3. Prerequisites — Shared Service Layer

### Problem

The existing HTTP handlers contain tightly coupled business logic: they parse HTTP
parameters, execute queries, build responses, and write HTTP output all in the same
functions. The MCP server needs the query/response logic but not the HTTP plumbing.

Duplicating the business logic would mean every bug fix, TraceQL feature, or query
optimization must be applied in two places. This is unacceptable.

### Solution

Extract a new `internal/tracequery/` package with pure functions that take typed
parameters and return typed responses. Both the HTTP handlers and MCP tools call these
functions.

```
Before:  HTTP handler → [parse params + query logic + response writing]

After:   HTTP handler → [parse HTTP params] → tracequery.Search() → [write HTTP response]
         MCP handler  → [parse MCP params]  → tracequery.Search() → [write MCP result]
```

### Service Function Signatures

```go
// internal/tracequery/search.go
package tracequery

// SearchTraces parses a TraceQL query, transpiles it to SQL, executes against
// ClickHouse, and returns a Tempo-compatible search response.
func SearchTraces(ctx context.Context, ch *clickhouse.Client, cfg config.QueryConfig,
    query string, start, end time.Time, limit, spss int,
    minDuration, maxDuration time.Duration,
) (*response.SearchResponse, error)
```

```go
// internal/tracequery/trace.go

// GetTraceByID retrieves all spans for a trace and builds an OTLP Trace response.
// Returns nil with no error if the trace is not found.
func GetTraceByID(ctx context.Context, ch *clickhouse.Client,
    traceID string,
) (*response.Trace, error)
```

```go
// internal/tracequery/metrics.go

// MetricsQueryRange executes a TraceQL metrics query and returns a time-bucketed
// series response. Step is auto-calculated if zero.
func MetricsQueryRange(ctx context.Context, ch *clickhouse.Client, cfg config.QueryConfig,
    query string, start, end time.Time, step time.Duration,
) (*response.QueryRangeResponse, error)

// MetricsQueryInstant executes a TraceQL metrics query and returns a single
// aggregated value for the given time range.
func MetricsQueryInstant(ctx context.Context, ch *clickhouse.Client, cfg config.QueryConfig,
    query string, start, end time.Time,
) (*response.QueryInstantResponse, error)

// MetricsSummary computes span metrics (count, error rate, latency percentiles)
// grouped by the specified attributes.
func MetricsSummary(ctx context.Context, ch *clickhouse.Client,
    query string, groupBy []string, start, end time.Time, limit int,
) (*response.SpanMetricsSummaryResponse, error)
```

```go
// internal/tracequery/tags.go

// GetTagNames returns available attribute names, optionally filtered by scope
// (span, resource, intrinsic, or empty for all). Uses materialized views when available.
func GetTagNames(ctx context.Context, ch *clickhouse.Client,
    scope string, start, end time.Time,
) (*response.SearchTagsV2Response, error)

// GetTagValues returns distinct values for a given attribute name, optionally
// filtered by a TraceQL filter expression.
func GetTagValues(ctx context.Context, ch *clickhouse.Client,
    tagName string, filterQuery string, start, end time.Time,
) (*response.SearchTagValuesV2Response, error)
```

### Refactoring the Existing HTTP Handlers

After extracting the service layer, each HTTP handler becomes thin:

```go
// internal/api/search.go — after refactoring
func (h *SearchHandlers) Search(w http.ResponseWriter, r *http.Request) {
    // 1. Parse HTTP-specific params (query strings, path vars)
    query := r.URL.Query().Get("q")
    if query == "" { query = r.URL.Query().Get("query") }
    if query == "" { query = "{ }" }
    limit := parseLimit(r, h.cfg)
    spss := parseSpss(r, h.cfg)
    start, end := parseTimeRange(r)
    minDur, maxDur := parseDurationFilters(r)

    // 2. Call shared service
    result, err := tracequery.SearchTraces(r.Context(), h.ch, h.cfg,
        query, start, end, limit, spss, minDur, maxDur)
    if err != nil {
        handleError(w, err, h.logger)
        return
    }

    // 3. Write HTTP response
    response.WriteJSON(w, http.StatusOK, result)
}
```

The existing parse helper functions (`parseTimeRange`, `parseLimit`, etc.) remain in
`internal/api/` since they are HTTP-specific.

---

## 4. MCP Server Design

### Package Structure

```
internal/mcp/
  server.go          — Server struct, constructor, ServeHTTP, shutdown
  tools.go           — Tool handler implementations
  resources.go       — Documentation resource registration
  time.go            — Time parsing utilities (RFC3339 + Unix epoch)
  errors.go          — Error classification for LLM-friendly messages
  docs/
    basic.md         — TraceQL syntax reference
    aggregates.md    — Aggregate functions reference
    structural.md    — Structural operators reference
    metrics.md       — Metrics functions reference
    embed.go         — //go:embed *.md
```

### Server Struct

```go
// internal/mcp/server.go
package mcp

type Server struct {
    ch         *clickhouse.Client
    queryCfg   config.QueryConfig
    mcpCfg     config.MCPConfig
    logger     *slog.Logger
    mcpServer  *server.MCPServer
    httpServer *server.StreamableHTTPServer
    semaphore  chan struct{}  // concurrency limiter
}
```

### Constructor

```go
func New(ch *clickhouse.Client, queryCfg config.QueryConfig, mcpCfg config.MCPConfig,
    logger *slog.Logger) *Server {

    mcpServer := server.NewMCPServer(
        "opex",
        api.Version, // reuse existing version from handler.go
        server.WithToolCapabilities(false),
        server.WithResourceCapabilities(false, false),
    )

    httpServer := server.NewStreamableHTTPServer(mcpServer)

    s := &Server{
        ch:         ch,
        queryCfg:   queryCfg,
        mcpCfg:     mcpCfg,
        logger:     logger,
        mcpServer:  mcpServer,
        httpServer: httpServer,
        semaphore:  make(chan struct{}, mcpCfg.MaxConcurrent),
    }

    s.setupTools()
    s.setupResources()

    return s
}
```

### Transport

Streamable HTTP at `/api/mcp/`, served alongside the existing Gorilla Mux routes.
The `mcp-go` library handles the MCP protocol framing (JSON-RPC over HTTP POST/GET
with optional SSE for streaming).

### Dependency

`github.com/mark3labs/mcp-go` pinned to v0.43.2 (same version Tempo uses, known stable).

---

## 5. Tools

Seven tools are exposed, matching Tempo's MCP surface:

### 5.1 `traceql-search`

Search for traces using a TraceQL query.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `query` | string | yes | TraceQL query string (e.g., `{ resource.service.name = "api" && status = error }`) |
| `start` | string | no | Start time (RFC3339 or Unix epoch seconds). Default: 1 hour ago |
| `end` | string | no | End time (RFC3339 or Unix epoch seconds). Default: now |
| `limit` | integer | no | Max traces to return. Default: `MCPConfig.MaxResults` (10) |

**Implementation:**

```go
func (s *Server) handleSearch(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
    metrics.MCPToolCalls.WithLabelValues("traceql-search").Inc()
    timer := prometheus.NewTimer(metrics.MCPToolDuration.WithLabelValues("traceql-search"))
    defer timer.ObserveDuration()

    query, err := req.RequireString("query")
    if err != nil {
        return mcp.NewToolResultError("query parameter is required"), nil
    }

    start, end, err := s.parseTimeRange(req)
    if err != nil {
        return mcp.NewToolResultError(err.Error()), nil
    }

    limit := req.GetInt("limit", s.mcpCfg.MaxResults)
    spss := s.mcpCfg.DefaultSpss

    // Acquire concurrency slot
    if err := s.acquire(ctx); err != nil {
        return mcp.NewToolResultError("server is busy, please retry"), nil
    }
    defer s.release()

    ctx, cancel := context.WithTimeout(ctx, s.mcpCfg.QueryTimeout)
    defer cancel()

    result, err := tracequery.SearchTraces(ctx, s.ch, s.queryCfg,
        query, start, end, limit, spss, 0, 0)
    if err != nil {
        return mcp.NewToolResultError(classifyError(err)), nil
    }

    data, _ := json.Marshal(result)
    return mcp.NewToolResultText(string(data)), nil
}
```

### 5.2 `traceql-metrics-instant`

Retrieve a single metric value from a TraceQL metrics query. Best for answering
"what is the current value of X?" questions.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `query` | string | yes | TraceQL metrics query (e.g., `{ resource.service.name = "api" } \| rate()`) |
| `start` | string | no | Start time. Default: 1 hour ago |
| `end` | string | no | End time. Default: now |

**When to use:** Most metrics questions can be answered with instant values. The LLM
should prefer this over range queries.

### 5.3 `traceql-metrics-range`

Retrieve a time series from a TraceQL metrics query.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `query` | string | yes | TraceQL metrics query |
| `start` | string | no | Start time. Default: 1 hour ago |
| `end` | string | no | End time. Default: now |
| `step` | string | no | Step size (e.g., `"60s"`, `"5m"`). Auto-calculated if omitted (~100 data points) |

**When to use:** For understanding trends over time. Response can be large; the LLM
should use instant queries when a single value suffices.

### 5.4 `get-trace`

Retrieve a specific trace by ID.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `trace_id` | string | yes | Hex trace ID (16 or 32 characters) |

**Response truncation:** If a trace has more spans than `MCPConfig.MaxTraceSpans`
(default 50), the response is truncated with metadata indicating:

```json
{
  "trace_id": "abc123def456...",
  "total_spans": 342,
  "shown_spans": 50,
  "truncated": true,
  "root_service": "api-gateway",
  "root_span": "GET /checkout",
  "duration_ms": 1234,
  "services": ["api-gateway", "payment-service", "inventory-service"],
  "error_count": 2,
  "spans": [...]
}
```

### 5.5 `get-attribute-names`

List available attribute names for use in TraceQL queries.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `scope` | string | no | Filter by scope: `span`, `resource`, `intrinsic`, or empty for all |

### 5.6 `get-attribute-values`

Get values for a specific attribute. Useful for discovering what services, endpoints,
or status codes exist in the data.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | yes | Fully scoped attribute name (e.g., `resource.service.name`, `span.http.method`) |
| `filter_query` | string | no | TraceQL filter to narrow results (e.g., `{ resource.service.name = "api" }`) |
| `start` | string | no | Start time. Default: 1 hour ago |
| `end` | string | no | End time. Default: now |

**Input validation:** Attribute names are validated against `^[a-zA-Z_][a-zA-Z0-9_.]*$`
before reaching SQL construction. Scope is validated against a fixed enum.

### 5.7 `docs-traceql`

Retrieve TraceQL documentation. Many LLM clients don't proactively read MCP resources,
so docs are also exposed as a tool.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | yes | Doc type: `basic`, `aggregates`, `structural`, `metrics` |

---

## 6. Resources

Four documentation resources are registered with the MCP server. These provide the same
content as the `docs-traceql` tool but via the MCP resource protocol (for clients that
support it).

| URI | Name | Description |
|-----|------|-------------|
| `docs://traceql/basic` | TraceQL Basic Docs | Intrinsics, operators, attribute syntax, simple filters |
| `docs://traceql/aggregates` | TraceQL Aggregates Docs | count, sum, avg, min, max, pipeline usage |
| `docs://traceql/structural` | TraceQL Structural Docs | Parent/child, descendant, ancestor, sibling operators |
| `docs://traceql/metrics` | TraceQL Metrics Docs | rate, count_over_time, quantile_over_time, histogram_over_time |

---

## 7. Embedded Documentation

Documentation files are embedded using Go's `//go:embed` directive and served as both
MCP resources and via the `docs-traceql` tool.

### Content Requirements

These docs are written for LLM consumption, not humans. They must include:

1. **Every supported intrinsic** with its type and description
2. **Every operator** with a concrete example query
3. **Attribute scope syntax** with examples for span, resource, and intrinsic scopes
4. **Common query patterns** that are copy-paste ready
5. **Metrics function signatures** with parameter descriptions
6. **Pitfalls and constraints** (e.g., map values are always strings, numeric comparisons
   need type coercion)

### Example: `basic.md` (partial)

```markdown
# TraceQL Basic Syntax

## Intrinsics

Intrinsics are built-in span attributes that don't require a scope prefix:

| Intrinsic | Type | Description | Example |
|-----------|------|-------------|---------|
| `duration` | duration | Span duration | `{ duration > 500ms }` |
| `name` | string | Span name | `{ name = "GET /api/users" }` |
| `status` | enum | Span status | `{ status = error }` |
| `kind` | enum | Span kind | `{ kind = server }` |
| `rootServiceName` | string | Root span's service | `{ rootServiceName = "api-gateway" }` |
| `rootName` | string | Root span's name | `{ rootName = "GET /checkout" }` |
| `traceDuration` | duration | Total trace duration | `{ traceDuration > 2s }` |

## Attribute Scopes

- `span.<name>` — Span attributes: `{ span.http.method = "GET" }`
- `resource.<name>` — Resource attributes: `{ resource.service.name = "api" }`
- `.<name>` — Unscoped (searches both): `{ .http.status_code = 200 }`

## Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `=` | Equals | `{ span.http.method = "GET" }` |
| `!=` | Not equals | `{ status != ok }` |
| `>` | Greater than | `{ duration > 1s }` |
| `>=` | Greater or equal | `{ .http.status_code >= 400 }` |
| `<` | Less than | `{ duration < 100ms }` |
| `<=` | Less or equal | `{ .http.status_code <= 299 }` |
| `=~` | Regex match | `{ name =~ "GET.*users" }` |
| `!~` | Regex not match | `{ name !~ "health.*" }` |
| `&&` | Logical AND | `{ .http.method = "GET" && status = error }` |
| `\|\|` | Logical OR | `{ status = error \|\| duration > 5s }` |

## Spanset Operations

- `&&` between spansets: traces containing BOTH span patterns (INTERSECT)
- `\|\|` between spansets: traces containing EITHER span pattern (UNION)
- `\|` pipeline: filter results of previous stage

## Duration Literals

`1ns`, `1us`, `1ms`, `1s`, `1m`, `1h`

## Status Values

`ok`, `error`, `unset`

## Kind Values

`server`, `client`, `producer`, `consumer`, `internal`
```

### File Structure

```
internal/mcp/docs/
  embed.go          — //go:embed basic.md aggregates.md structural.md metrics.md
  basic.md          — Intrinsics, operators, scopes, spanset operations
  aggregates.md     — count(), sum(), avg(), min(), max(), by(), coalesce()
  structural.md     — >, <, >>, <<, ~, negated, union variants
  metrics.md        — rate(), count_over_time(), quantile_over_time(), etc.
```

```go
// internal/mcp/docs/embed.go
package docs

import "embed"

//go:embed basic.md aggregates.md structural.md metrics.md
var docsFS embed.FS

const (
    DocsTypeBasic      = "basic"
    DocsTypeAggregates = "aggregates"
    DocsTypeStructural = "structural"
    DocsTypeMetrics    = "metrics"
)

// GetContent returns the documentation content for the given type.
func GetContent(docType string) string {
    filename := docType + ".md"
    data, err := docsFS.ReadFile(filename)
    if err != nil {
        return "Documentation not found for type: " + docType
    }
    return string(data)
}
```

---

## 8. Configuration

### Config Struct

```go
// internal/config/config.go

type MCPConfig struct {
    Enabled       bool          `yaml:"enabled"`
    MaxConcurrent int           `yaml:"max_concurrent"`
    QueryTimeout  time.Duration `yaml:"query_timeout"`
    MaxResults    int           `yaml:"max_results"`
    DefaultSpss   int           `yaml:"default_spss"`
    MaxTraceSpans int           `yaml:"max_trace_spans"`
}
```

### Defaults

```go
func DefaultConfig() *Config {
    return &Config{
        // ... existing fields ...
        MCP: MCPConfig{
            Enabled:       false,
            MaxConcurrent: 5,
            QueryTimeout:  30 * time.Second,
            MaxResults:    10,
            DefaultSpss:   1,
            MaxTraceSpans: 50,
        },
    }
}
```

### Example config.yaml

```yaml
mcp:
  enabled: true
  max_concurrent: 5        # MCP-specific concurrency pool (separate from HTTP)
  query_timeout: 30s       # Per-tool-invocation timeout
  max_results: 10          # Default trace search result limit
  default_spss: 1          # Spans per span set (low for LLM context windows)
  max_trace_spans: 50      # Max spans returned by get-trace (truncates beyond this)
```

### Design Rationale

MCP defaults are intentionally lower than HTTP API defaults because:

1. **LLM context windows are finite.** 10 traces with 1 span each is more useful than
   20 traces with 3 spans each when the consumer is an LLM, not a visual UI.
2. **LLMs are aggressive query generators.** They retry, issue broad time ranges, and
   don't understand cost. Lower limits prevent accidental resource exhaustion.
3. **Separate concurrency pool** prevents MCP traffic from starving the HTTP API.
   A single overeager LLM agent should not be able to consume all query slots.

---

## 9. Resource Protection & Concurrency

### Concurrency Limiter

Every tool invocation acquires a slot from a bounded channel before executing:

```go
func (s *Server) acquire(ctx context.Context) error {
    select {
    case s.semaphore <- struct{}{}:
        return nil
    case <-ctx.Done():
        return ctx.Err()
    }
}

func (s *Server) release() {
    <-s.semaphore
}
```

### Query Timeout

Every tool invocation wraps the context with a deadline:

```go
func (s *Server) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
    return context.WithTimeout(ctx, s.mcpCfg.QueryTimeout)
}
```

### Context Propagation

The `mcp-go` SDK provides a `context.Context` per tool call. This context is:
1. Wrapped with `context.WithTimeout` for the query timeout
2. Passed through to all ClickHouse queries
3. Cancelled if the MCP client disconnects (via the underlying HTTP connection)

This ensures that orphaned queries do not consume ClickHouse resources.

### Response Size

- `traceql-search`: capped by `MaxResults` (default 10) and `DefaultSpss` (default 1)
- `get-trace`: truncated beyond `MaxTraceSpans` (default 50) with metadata
- Metrics responses: naturally bounded by step count (~100 data points)

---

## 10. Error Handling

### Error Classification

LLMs need clear, actionable error messages to decide whether to retry, rephrase,
or give up. Errors are classified into categories:

```go
// internal/mcp/errors.go

func classifyError(err error) string {
    switch {
    case errors.Is(err, clickhouse.ErrNotConnected):
        return "ClickHouse is not connected. The database may be starting up. Please retry in a few seconds."
    case errors.Is(err, clickhouse.ErrCircuitOpen):
        return "ClickHouse is temporarily unavailable due to repeated failures. Please retry in 10-15 seconds."
    case errors.Is(err, context.DeadlineExceeded):
        return "Query timed out. Try narrowing the time range or simplifying the query."
    case errors.Is(err, context.Canceled):
        return "Query was cancelled."
    default:
        return "Query failed: " + err.Error()
    }
}
```

### TraceQL Parse Errors

Parse errors include a hint to consult the documentation:

```go
root, err := traceql.Parse(query)
if err != nil {
    return mcp.NewToolResultError(fmt.Sprintf(
        "Invalid TraceQL query: %v. Use the docs-traceql tool with name='basic' for syntax reference.", err)), nil
}
```

### Wrong Tool Errors

If a metrics query is sent to the search tool (or vice versa), the error message
directs the LLM to the correct tool:

```go
if parsed.MetricsPipeline != nil {
    return mcp.NewToolResultError(
        "This is a metrics query. Use the traceql-metrics-instant or traceql-metrics-range tool instead."), nil
}
```

---

## 11. Metrics & Observability

### Prometheus Metrics

```go
// Added to internal/metrics/metrics.go

var MCPToolCalls = promauto.NewCounterVec(prometheus.CounterOpts{
    Namespace: "opex",
    Name:      "mcp_tool_calls_total",
    Help:      "Total number of MCP tool calls",
}, []string{"tool"})

var MCPToolDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
    Namespace: "opex",
    Name:      "mcp_tool_duration_seconds",
    Help:      "Duration of MCP tool calls",
    Buckets:   prometheus.DefBuckets,
}, []string{"tool"})

var MCPToolErrors = promauto.NewCounterVec(prometheus.CounterOpts{
    Namespace: "opex",
    Name:      "mcp_tool_errors_total",
    Help:      "Total number of MCP tool errors",
}, []string{"tool", "error_type"})
```

### Logging

Each tool invocation logs at Info level with structured fields:

```go
s.logger.Info("mcp tool invoked",
    "tool", "traceql-search",
    "query", query,
    "start", start,
    "end", end,
    "limit", limit,
)
```

Errors log at Error level with the original error:

```go
s.logger.Error("mcp tool failed",
    "tool", "traceql-search",
    "error", err,
    "query", query,
)
```

### Middleware Integration

MCP requests pass through the existing `metrics.Middleware` and `loggingMiddleware`.
The `normalizeEndpoint` function in metrics.go is updated to collapse `/api/mcp/...`
paths to `/api/mcp` to prevent high-cardinality labels.

---

## 12. Security Considerations

### Data Exposure

Enabling the MCP server means tracing data may be passed to an LLM or LLM provider.
This includes:

- Service names and endpoint paths
- Span attributes (which may contain PII, auth tokens, or internal identifiers)
- Error messages and stack traces
- Resource attributes (hostnames, IPs, container IDs)

The config defaults to `enabled: false` to prevent accidental exposure.

### Input Validation

All user-controlled inputs are validated before reaching SQL construction:

- **Attribute names:** Validated against `^[a-zA-Z_][a-zA-Z0-9_.]*$`
- **Scope:** Validated against enum (`span`, `resource`, `intrinsic`, or empty)
- **Trace IDs:** Validated as hex strings (16 or 32 chars) — reuses existing validation
- **TraceQL queries:** Parsed by the TraceQL parser (rejects malformed input)
- **Time parameters:** Parsed as RFC3339 or Unix epoch seconds

### Authentication

No built-in authentication is implemented in the initial version. The MCP endpoint
should be placed behind a reverse proxy with authentication if exposed beyond localhost.

Future enhancement: add `mcp.auth_token` config field for simple bearer token validation.

### SQL Injection

The existing codebase constructs some SQL via `fmt.Sprintf` (notably in tag value
queries). The MCP layer adds input validation as an additional defense layer, but the
underlying SQL construction should be audited and migrated to parameterized queries
where ClickHouse supports them.

---

## 13. Testing Strategy

### Unit Tests

| File | Description |
|------|-------------|
| `internal/tracequery/search_test.go` | Search service function with mock ClickHouse |
| `internal/tracequery/trace_test.go` | Trace retrieval with mock ClickHouse |
| `internal/tracequery/metrics_test.go` | Metrics query service functions |
| `internal/tracequery/tags_test.go` | Tag name/value discovery |

### MCP-Specific Tests

| File | Description |
|------|-------------|
| `internal/mcp/server_test.go` | Verify all 7 tools and 4 resources are registered |
| `internal/mcp/tools_test.go` | Tool handler tests: valid inputs, parse errors, ClickHouse down, circuit breaker open, empty results, timeout, invalid attribute names |
| `internal/mcp/time_test.go` | Time parsing: RFC3339, Unix epoch, both formats, invalid input |
| `internal/mcp/errors_test.go` | Error classification: each error type produces the correct message |
| `internal/mcp/integration_test.go` | End-to-end: boot `httptest.Server` with MCP endpoint, make JSON-RPC calls over HTTP, verify MCP protocol compliance |

### Test Patterns

All tests follow project conventions:

- **No assertion libraries** — stdlib `t.Errorf` / `t.Fatalf` only
- **Table-driven** with `t.Run`
- **Helpers call `t.Helper()`**

### Example Test

```go
func TestHandleSearch(t *testing.T) {
    tests := []struct {
        name      string
        query     string
        wantError bool
        errSubstr string
    }{
        {"valid query", `{ status = error }`, false, ""},
        {"empty query", "", true, "query parameter is required"},
        {"metrics query on search tool", `{ } | rate()`, true, "Use the traceql-metrics"},
        {"invalid TraceQL", `{ invalid !! }`, true, "Invalid TraceQL"},
    }
    for _, tc := range tests {
        t.Run(tc.name, func(t *testing.T) {
            // ... setup mock, call handler, verify result ...
        })
    }
}
```

### Integration Test

```go
func TestMCPEndToEnd(t *testing.T) {
    // 1. Create MCP server with mock ClickHouse
    // 2. Mount on httptest.Server at /api/mcp/
    // 3. Make JSON-RPC POST request: {"method": "tools/call", "params": {"name": "traceql-search", ...}}
    // 4. Verify response is valid JSON-RPC with expected structure
    // 5. Verify PathPrefix routing works with mcp-go's sub-paths
}
```

### Concurrency Test

```go
func TestConcurrencyLimiter(t *testing.T) {
    // 1. Create server with MaxConcurrent=2
    // 2. Launch 5 concurrent tool calls (each with a slow mock)
    // 3. Verify only 2 run simultaneously
    // 4. Verify all 5 eventually complete
}
```

---

## 14. File Inventory

### New Files

| File | Package | Description |
|------|---------|-------------|
| `internal/tracequery/search.go` | tracequery | Search service function |
| `internal/tracequery/trace.go` | tracequery | Trace-by-ID service function |
| `internal/tracequery/metrics.go` | tracequery | Metrics query service functions |
| `internal/tracequery/tags.go` | tracequery | Tag discovery service functions |
| `internal/tracequery/search_test.go` | tracequery | Search tests |
| `internal/tracequery/trace_test.go` | tracequery | Trace tests |
| `internal/tracequery/metrics_test.go` | tracequery | Metrics tests |
| `internal/tracequery/tags_test.go` | tracequery | Tag tests |
| `internal/mcp/server.go` | mcp | MCP server struct, constructor, ServeHTTP |
| `internal/mcp/tools.go` | mcp | Tool handler implementations |
| `internal/mcp/resources.go` | mcp | Documentation resource registration |
| `internal/mcp/time.go` | mcp | Time parsing (RFC3339 + Unix epoch) |
| `internal/mcp/errors.go` | mcp | Error classification |
| `internal/mcp/server_test.go` | mcp | Server registration tests |
| `internal/mcp/tools_test.go` | mcp | Tool handler tests |
| `internal/mcp/time_test.go` | mcp | Time parsing tests |
| `internal/mcp/errors_test.go` | mcp | Error classification tests |
| `internal/mcp/integration_test.go` | mcp | End-to-end protocol tests |
| `internal/mcp/docs/embed.go` | docs | Go embed directive |
| `internal/mcp/docs/basic.md` | — | TraceQL basic syntax docs |
| `internal/mcp/docs/aggregates.md` | — | TraceQL aggregate functions docs |
| `internal/mcp/docs/structural.md` | — | TraceQL structural operators docs |
| `internal/mcp/docs/metrics.md` | — | TraceQL metrics functions docs |

### Modified Files

| File | Change |
|------|--------|
| `go.mod` / `go.sum` | Add `github.com/mark3labs/mcp-go@v0.43.2` |
| `internal/config/config.go` | Add `MCPConfig` struct and field, update `DefaultConfig()` |
| `config.yaml` | Add `mcp:` section |
| `internal/api/search.go` | Refactor to call `tracequery.SearchTraces()` |
| `internal/api/trace.go` | Refactor to call `tracequery.GetTraceByID()` |
| `internal/api/metrics.go` | Refactor to call `tracequery.MetricsQuery*()` |
| `internal/api/tags.go` | Refactor to call `tracequery.GetTagNames()` / `GetTagValues()` |
| `internal/server/server.go` | Register MCP route when enabled, update shutdown, update `normalizeEndpoint` |
| `internal/metrics/metrics.go` | Add MCP-specific metrics, update `normalizeEndpoint` |

---

## 15. Implementation Phases

### Phase 0 — Shared Service Layer (prerequisite)

**Goal:** Extract business logic from HTTP handlers into `internal/tracequery/`.

1. Create `internal/tracequery/` package with service functions
2. Move query logic from `internal/api/search.go` → `tracequery.SearchTraces()`
3. Move query logic from `internal/api/trace.go` → `tracequery.GetTraceByID()`
4. Move query logic from `internal/api/metrics.go` → `tracequery.MetricsQuery*()`
5. Move query logic from `internal/api/tags.go` → `tracequery.GetTag*()`
6. Refactor HTTP handlers to call service functions
7. Write unit tests for service functions
8. Run `make test` — all existing tests must pass

**Verification:** `go test ./internal/...` passes with no regressions.

### Phase 1 — Add Dependency

```bash
GOPROXY=https://proxy.golang.org,direct GONOSUMCHECK='*' GONOSUMDB='*' \
    go get github.com/mark3labs/mcp-go@v0.43.2
```

**Verification:** `go build ./...` succeeds.

### Phase 2 — Configuration

1. Add `MCPConfig` struct to `internal/config/config.go`
2. Add `MCP MCPConfig` field to `Config`
3. Update `DefaultConfig()` with MCP defaults
4. Add `mcp:` section to `config.yaml`

**Verification:** `go test ./internal/config/...` passes. Config loads correctly.

### Phase 3 — Embedded Documentation

1. Create `internal/mcp/docs/` directory
2. Write `basic.md`, `aggregates.md`, `structural.md`, `metrics.md`
3. Create `embed.go` with `//go:embed` directive and `GetContent()` function

**Verification:** `go build ./internal/mcp/docs/` succeeds.

### Phase 4 — MCP Server Core

1. Create `internal/mcp/server.go` — Server struct, constructor, ServeHTTP
2. Create `internal/mcp/time.go` — Time parsing utilities
3. Create `internal/mcp/errors.go` — Error classification
4. Create `internal/mcp/resources.go` — Documentation resources

**Verification:** `go build ./internal/mcp/` succeeds.

### Phase 5 — Tool Handlers

1. Create `internal/mcp/tools.go` — All 7 tool handlers
2. Wire tools in `setupTools()` called from constructor

**Verification:** `go build ./internal/mcp/` succeeds.

### Phase 6 — Server Integration

1. Update `internal/server/server.go` to register MCP route
2. Update `internal/metrics/metrics.go` with MCP metrics
3. Update `normalizeEndpoint()` for `/api/mcp` paths
4. Handle graceful shutdown of MCP server

**Verification:** `go build ./...` succeeds. Server starts with `mcp.enabled: true`.

### Phase 7 — Tests

1. Write all MCP test files
2. Write service layer test files (if not done in Phase 0)
3. Run full test suite

**Verification:** `make test` passes.

### Phase 8 — Validation

1. Start Opex with `mcp.enabled: true`
2. Connect Claude Code: `claude mcp add --transport=http opex http://localhost:8080/api/mcp/`
3. Test: "What services are available?" → should use `get-attribute-values`
4. Test: "Find errors in the last hour" → should use `traceql-search`
5. Test: "What is the p99 latency for service X?" → should use `traceql-metrics-instant`
6. Verify metrics appear at `/metrics` with `opex_mcp_*` prefix

---

## 16. Usage

### Enabling

```yaml
# config.yaml
mcp:
  enabled: true
```

### Connecting MCP Clients

```bash
# Claude Code
claude mcp add --transport=http opex http://localhost:8080/api/mcp/

# Cursor (via mcp-remote in .cursor/mcp.json)
{
  "mcpServers": {
    "opex": {
      "command": "npx",
      "args": ["-y", "mcp-remote", "http://localhost:8080/api/mcp/"]
    }
  }
}
```

### Example Conversations

**Q:** "What services are in the system?"

The LLM calls `get-attribute-values` with `name = "resource.service.name"`, gets back a
list like `["api-gateway", "payment-service", "inventory-service"]`.

**Q:** "What's the error rate for the payment service over the last 6 hours?"

The LLM calls `docs-traceql` with `name = "metrics"` to learn the syntax, then calls
`traceql-metrics-instant` with:
```
query: { resource.service.name = "payment-service" } | rate() by (status)
start: 2025-03-17T10:00:00Z
end:   2025-03-17T16:00:00Z
```

**Q:** "Show me the slowest traces for the checkout endpoint"

The LLM calls `traceql-search` with:
```
query: { resource.service.name = "api-gateway" && name = "GET /checkout" && duration > 1s }
limit: 5
```

**Q:** "Get me trace abc123def456"

The LLM calls `get-trace` with `trace_id = "abc123def456"`.

---

## 17. Key Design Decisions

| Decision | Chosen | Why | Alternative Considered |
|----------|--------|-----|----------------------|
| Share logic via service layer | Yes | Avoid duplicating 600+ lines of handler logic. Single source of truth for query execution. | Tempo's approach: construct fake HTTP requests to own handlers. Rejected: hacky, creates internal HTTP round-trip, harder to test. |
| Transport | Streamable HTTP | Standard for remote MCP clients. Works with Claude Code, Cursor, etc. | Stdio (only works as local subprocess, not applicable for a server). |
| Pin `mcp-go` version | v0.43.2 | Same as Tempo, known stable. The library is pre-1.0 and breaking changes are likely in newer versions. | `@latest` — rejected: builds could break on upstream changes. |
| Separate concurrency pool | Yes (default 5) | Prevents MCP from starving the HTTP API. LLMs are aggressive query generators that can exhaust shared resources. | Share the HTTP concurrency pool — rejected: one bad MCP session could block all Grafana queries. |
| Lower default limits | 10 traces, 1 span/set | LLM context windows are finite. More data = more cost and slower responses. The LLM can always request more. | Same defaults as HTTP (20/3) — rejected: wastes LLM context on data it can't process. |
| Time format | RFC3339 primary, Unix fallback | RFC3339 is natural for LLMs (they generate it reliably). Unix epoch as fallback for compatibility. | Unix only (matches HTTP API) — rejected: LLMs struggle with epoch timestamps. |
| Auth | None (document reverse proxy) | Simple deployments on localhost don't need auth. External exposure should use a reverse proxy. | Bearer token — deferred to a future enhancement. |
| Docs as both resources and tools | Yes | Claude Code and many clients don't proactively read resources. Having docs as a tool ensures the LLM can learn syntax. | Resources only — rejected: most clients ignore them. |

---

## 18. Risks & Mitigations

### Risk 1: `mcp-go` Breaking Changes

The library is pre-1.0 (`v0.43.2`). API changes could break the build.

**Mitigation:** Pin to exact version. Monitor upstream releases. The library is used by
Grafana Tempo (a major project), which reduces the risk of abandonment.

### Risk 2: LLM Query Amplification

LLMs may issue broad queries (`{ }` with no filters over long time ranges) that scan
large portions of the ClickHouse table.

**Mitigation:**
- Separate concurrency pool (default 5) prevents MCP from starving HTTP
- Per-tool query timeout (default 30s)
- Lower result limits (default 10)
- ClickHouse circuit breaker prevents cascading failures
- The existing `QueryConfig.MaxDuration` (168h) caps time ranges

### Risk 3: Data Exposure to LLM Providers

Tracing data may contain sensitive information (PII, auth tokens, internal endpoints).

**Mitigation:**
- Feature is opt-in (`enabled: false` by default)
- Documentation warns about data exposure
- Users should audit their tracing data before enabling

### Risk 4: Service Layer Refactoring Risk

Extracting the service layer (Phase 0) touches core code paths. Bugs in the refactoring
could break the HTTP API.

**Mitigation:**
- Refactoring is purely structural (move code, don't change logic)
- All existing tests must pass before proceeding
- The refactoring can be done incrementally (one handler at a time)

### Risk 5: PathPrefix Routing Conflicts

`PathPrefix("/api/mcp/")` could match unintended paths.

**Mitigation:**
- Use trailing slash to be more specific
- Update `normalizeEndpoint()` to handle MCP paths
- Test the routing explicitly in integration tests

---

## Appendix A — Tempo MCP Reference

Tempo's MCP implementation (v2.10) served as the primary reference for this plan.

**Files in Tempo's codebase:**
- `modules/frontend/mcp.go` — Server setup, tool/resource registration
- `modules/frontend/mcp_tools.go` — Tool handler implementations
- `modules/frontend/docs/` — Embedded documentation

**Key differences from this plan:**
1. Tempo constructs fake HTTP requests to its own handlers; we use a shared service layer
2. Tempo uses `go-kit/log`; we use `log/slog`
3. Tempo's MCP server lives inside the query frontend; ours is a separate package
4. Tempo has no MCP-specific concurrency limiting; we add a bounded semaphore
5. Tempo returns raw OTLP traces; we add response truncation for LLM context windows

## Appendix B — MCP Protocol Overview

The Model Context Protocol uses JSON-RPC 2.0 over HTTP. The Streamable HTTP transport
works as follows:

1. **Initialization:** Client sends `POST /api/mcp/` with `{"method": "initialize", ...}`
2. **Tool discovery:** Client sends `{"method": "tools/list"}`
3. **Tool invocation:** Client sends `{"method": "tools/call", "params": {"name": "traceql-search", "arguments": {"query": "..."}}}`
4. **Resource discovery:** Client sends `{"method": "resources/list"}`
5. **Resource reading:** Client sends `{"method": "resources/read", "params": {"uri": "docs://traceql/basic"}}`

The server responds with JSON-RPC 2.0 responses. For streaming, the server may upgrade
to SSE (Server-Sent Events) on the same endpoint.
