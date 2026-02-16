# Phase 4 Reference - Tempo-Compatible HTTP API

**Status:** COMPLETE

## Files Created/Modified

```
internal/
├── clickhouse/
│   ├── client.go           # ClickHouse connection pool, Ping, Query
│   └── trace.go            # SpanRow type, QueryTraceByID, QueryTraceIDs, QuerySpansByTraceIDs
├── response/
│   ├── types.go            # All Tempo-compatible JSON response types
│   ├── trace.go            # BuildTrace: SpanRow[] -> OTLP Trace JSON
│   └── marshal.go          # Content negotiation, WriteJSON, WriteError
├── api/
│   ├── handler.go          # Echo, Ready, BuildInfo (unchanged)
│   └── trace.go            # TraceByID, TraceByIDV2 handlers
├── server/server.go        # Updated: accepts *clickhouse.Client, registers trace routes
cmd/opex/main.go            # Updated: connects to ClickHouse, passes to server
```

## New Dependencies
- `github.com/ClickHouse/clickhouse-go/v2` -- ClickHouse native protocol driver

## Package: `internal/clickhouse`

### Client

```go
func New(cfg config.ClickHouseConfig, logger *slog.Logger) (*Client, error)
func (c *Client) Close() error
func (c *Client) Ping(ctx context.Context) error
func (c *Client) Table() string
func (c *Client) Query(ctx context.Context, sql string, args ...any) (driver.Rows, error)
```

### SpanRow (ClickHouse row type)

```go
type SpanRow struct {
    Timestamp, TraceId, SpanId, ParentSpanId, TraceState string
    SpanName, SpanKind, ServiceName string
    ResourceAttributes, SpanAttributes map[string]string
    ScopeName, ScopeVersion string
    Duration uint64
    StatusCode, StatusMessage string
    EventsTimestamp []time.Time
    EventsName []string
    EventsAttributes []map[string]string
    LinksTraceId, LinksSpanId, LinksTraceState []string
    LinksAttributes []map[string]string
}
```

### Query Methods

```go
func (c *Client) QueryTraceByID(ctx, traceID) ([]SpanRow, error)
func (c *Client) QueryTraceIDs(ctx, sql) ([]string, error)
func (c *Client) QuerySpansByTraceIDs(ctx, traceIDs) ([]SpanRow, error)
```

## Package: `internal/response`

### Response Types (Tempo-compatible JSON)

- `Trace` -- OTLP trace with `Batches []ResourceSpans`
- `ResourceSpans` -- resource + scope spans
- `Span` -- OTLP span with traceId, spanId, name, kind, timestamps, attributes, status, events, links
- `TraceByIDResponse` -- V2 wrapper: trace + status + message
- `SearchResponse` -- traces + metrics
- `TraceSearchMetadata` -- traceID, rootServiceName, rootTraceName, startTimeUnixNano, durationMs, spanSets
- `SearchTagsResponse`, `SearchTagsV2Response` -- tag names by scope
- `SearchTagValuesResponse`, `SearchTagValuesV2Response` -- tag values
- `QueryRangeResponse` -- time series
- `QueryInstantResponse` -- instant series
- `SpanMetricsSummaryResponse` -- metrics summary

### Key Functions

```go
func BuildTrace(spans []clickhouse.SpanRow) *Trace
func WriteJSON(w, status, v)
func WriteError(w, status, msg)
func MarshalingFormat(r) string  // "application/json" or "application/protobuf"
```

### OTLP Mapping

| ClickHouse | OTLP Span |
|------------|-----------|
| Timestamp (nanoseconds) | startTimeUnixNano (string) |
| Timestamp + Duration | endTimeUnixNano (string) |
| SpanKind string | kind (int: 0-5) |
| StatusCode string | status.code (int: 0=unset, 1=ok, 2=error) |
| SpanAttributes map | attributes []KeyValue |
| Events.* arrays | events [] |
| Links.* arrays | links [] |

## Registered Routes

| Path | Method | Handler |
|------|--------|---------|
| `/api/echo` | GET | Echo (returns "echo") |
| `/api/status/buildinfo` | GET | BuildInfo (JSON with version) |
| `/ready` | GET | Ready (pings ClickHouse) |
| `/api/traces/{traceID}` | GET | TraceByID (returns OTLP Trace) |
| `/api/v2/traces/{traceID}` | GET | TraceByIDV2 (returns wrapped response) |

## Server Startup

```go
// main.go flow:
config.LoadFromFile(path) -> cfg
clickhouse.New(cfg.ClickHouse, logger) -> ch (nil if unavailable)
server.New(cfg, ch, logger) -> srv
srv.Run()
```

Server starts without ClickHouse (only infra endpoints work). Trace endpoints require ClickHouse.

## What's Next (Phase 5)

Build search and tag APIs:
1. `internal/api/search.go` -- GET /api/search (TraceQL query execution)
2. `internal/api/tags.go` -- GET /api/search/tags, /api/v2/search/tags
3. `internal/api/tag_values.go` -- GET /api/search/tag/{tagName}/values, V2
4. Use transpiler to convert TraceQL queries to SQL
5. Wire into server routes
