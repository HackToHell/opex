# Phase 5 Reference - Search & Tag APIs

**Status:** COMPLETE

## Files Created/Modified

```
internal/api/
├── search.go    # GET /api/search - TraceQL search with transpiler
└── tags.go      # GET /api/search/tags (V1/V2), /api/search/tag/{tagName}/values (V1/V2)
internal/server/server.go  # Updated: registers search + tag routes
```

## Registered Routes (cumulative)

| Path | Method | Handler | Phase |
|------|--------|---------|-------|
| `/api/echo` | GET | Echo | P1 |
| `/api/status/buildinfo` | GET | BuildInfo | P1 |
| `/ready` | GET | readyHandler | P1 |
| `/api/traces/{traceID}` | GET | TraceByID | P4 |
| `/api/v2/traces/{traceID}` | GET | TraceByIDV2 | P4 |
| `/api/search` | GET | Search | P5 |
| `/api/search/tags` | GET | SearchTags | P5 |
| `/api/v2/search/tags` | GET | SearchTagsV2 | P5 |
| `/api/search/tag/{tagName}/values` | GET | SearchTagValues | P5 |
| `/api/v2/search/tag/{tagName}/values` | GET | SearchTagValuesV2 | P5 |

## Search Handler Flow

```
Request (q, start, end, limit, spss, minDuration, maxDuration)
  -> traceql.Parse(q)
  -> transpiler.Transpile(ast, opts)
  -> clickhouse.QueryTraceIDs(sql)
  -> clickhouse.QuerySpansByTraceIDs(traceIDs)
  -> buildSearchResponse(spans)
  -> response.WriteJSON(SearchResponse)
```

### Search Response Format

```json
{
  "traces": [{
    "traceID": "abc123...",
    "rootServiceName": "frontend",
    "rootTraceName": "GET /api",
    "startTimeUnixNano": "1234567890000000000",
    "durationMs": 150,
    "spanSets": [{ "spans": [...], "matched": 5 }],
    "serviceStats": { "frontend": { "spanCount": 3, "errorCount": 0 } }
  }],
  "metrics": { "inspectedTraces": 100, "inspectedSpans": 500 }
}
```

## Tag Handler Queries

| Endpoint | SQL Strategy |
|----------|-------------|
| Tags (resource scope) | `SELECT DISTINCT arrayJoin(mapKeys(ResourceAttributes)) FROM otel_traces` |
| Tags (span scope) | `SELECT DISTINCT arrayJoin(mapKeys(SpanAttributes)) FROM otel_traces` |
| Tags (intrinsic) | Hardcoded list |
| Tags V2 | Returns all scopes (intrinsic + resource + span) |
| Tag values (status) | Hardcoded: error, ok, unset |
| Tag values (kind) | Hardcoded: unspecified, internal, client, server, producer, consumer |
| Tag values (service.name) | `SELECT DISTINCT ServiceName FROM otel_traces` |
| Tag values (other) | `SpanAttributes['tag'] UNION ALL ResourceAttributes['tag']` |
| Tag values V2 | Same as V1 but wraps values with type info |

## Key Design Decisions

- Default time range: last 1 hour if not specified
- Default limit: from config (20), max from config (100)
- Default spss: from config (3)
- Empty query `{ }` matches all traces
- Tag queries limited to 1000 results
- Intrinsic tags returned from hardcoded list (no ClickHouse query)

## What's Next (Phase 6)

Build metrics API endpoints:
1. `internal/api/metrics.go` -- GET /api/metrics/query_range, /api/metrics/query, /api/metrics/summary
2. Add time-bucketed query support to transpiler
3. Wire into server routes
