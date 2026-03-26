# Phase 8 Reference — Optimization & Production Readiness

## Overview

Phase 8 focused on implementing the deferred structural operators, adding materialized views for query optimization, PREWHERE/SAMPLE support, Prometheus metrics instrumentation, and enhanced structured logging.

## 8.1 Structural Operators

### Implemented Operators

All 5 structural operators plus negated and union variants:

| Operator | TraceQL | SQL Strategy |
|----------|---------|-------------|
| Child | `{ LHS } > { RHS }` | JOIN on `p.SpanId = c.ParentSpanId` |
| Parent | `{ LHS } < { RHS }` | JOIN on `c.ParentSpanId = p.SpanId` (reversed) |
| Descendant | `{ LHS } >> { RHS }` | Recursive CTE walking the ancestor chain |
| Ancestor | `{ LHS } << { RHS }` | Recursive CTE walking the descendant chain |
| Sibling | `{ LHS } ~ { RHS }` | JOIN on `s1.ParentSpanId = s2.ParentSpanId AND s1.SpanId != s2.SpanId` |

### Negated Variants (`!>`, `!<`, `!>>`, `!<<`, `!~`)

Negated structural operators use `EXCEPT` to subtract the structural match from the LHS:

```sql
-- { .a = "x" } !> { .b = "y" }
<LHS query>
EXCEPT
<positive structural query>
```

**Note**: `!~` (not-sibling) cannot be parsed from TraceQL text because `!~` is the regex not-equal operator. It works at the AST level via `OpSpansetNotSibling`.

### Union Variants (`&>`, `&<`, `&>>`, `&<<`, `&~`)

Union structural operators use `UNION` to combine the LHS trace set with the structural match:

```sql
-- { .a = "x" } &> { .b = "y" }
<LHS query>
UNION
<positive structural query>
```

### Column Aliasing for JOINs

Structural operators require both sides of a JOIN to reference the same table. The `replaceColumnsWithAlias` function prefixes known ClickHouse column names with table aliases:

```go
func (t *transpiler) replaceColumnsWithAlias(sql, alias string) string
```

This handles: `SpanAttributes`, `ResourceAttributes`, `ServiceName`, `SpanName`, `Duration`, `StatusCode`, `StatusMessage`, `SpanKind`, `TraceId`, `SpanId`, `ParentSpanId`, `ScopeName`, `ScopeVersion`, `Timestamp`.

### Files Modified

- `internal/transpiler/transpiler.go` — Added `transpileStructuralChild`, `transpileStructuralParent`, `transpileStructuralDescendant`, `transpileStructuralAncestor`, `transpileStructuralSibling`, `transpileStructuralNot`, `transpileStructuralUnion`, `aliasedTimeFilter`, `replaceColumnsWithAlias`, `extractFilterCondition`, `transpileSetOperation`. Refactored `transpileSpansetOperation` to dispatch to all variants.

### Example SQL Output

**Child (`>`):**
```sql
SELECT DISTINCT p.TraceId FROM otel_traces p
JOIN otel_traces c ON p.TraceId = c.TraceId AND p.SpanId = c.ParentSpanId
WHERE p.SpanAttributes['a'] = 'x' AND c.SpanAttributes['b'] = 'y'
  AND p.Timestamp >= ... AND c.Timestamp >= ...
LIMIT 20
```

**Descendant (`>>`):**
```sql
WITH RECURSIVE ancestors AS (
  SELECT TraceId, SpanId FROM otel_traces WHERE <LHS condition> AND <time>
  UNION ALL
  SELECT t.TraceId, t.SpanId FROM otel_traces t
  JOIN ancestors a ON t.ParentSpanId = a.SpanId AND t.TraceId = a.TraceId
)
SELECT DISTINCT d.TraceId FROM otel_traces d
JOIN ancestors a ON d.TraceId = a.TraceId AND d.ParentSpanId = a.SpanId
WHERE <RHS condition> AND <time>
LIMIT 20
```

**Sibling (`~`):**
```sql
SELECT DISTINCT s1.TraceId FROM otel_traces s1
JOIN otel_traces s2 ON s1.TraceId = s2.TraceId
  AND s1.ParentSpanId = s2.ParentSpanId AND s1.SpanId != s2.SpanId
WHERE <LHS on s1> AND <RHS on s2>
  AND s1.ParentSpanId != ''
  AND s1.Timestamp >= ... AND s2.Timestamp >= ...
LIMIT 20
```

---

## 8.2 Materialized Views

### DDL File

`deploy/clickhouse/materialized_views.sql` — creates 4 materialized views:

| Table | Engine | Purpose |
|-------|--------|---------|
| `otel_trace_metadata` | AggregatingMergeTree | Pre-computed root service/span, start/end time, span/error counts per trace |
| `otel_span_tag_names` | AggregatingMergeTree | Pre-computed distinct span attribute keys |
| `otel_resource_tag_names` | AggregatingMergeTree | Pre-computed distinct resource attribute keys |
| `otel_service_names` | AggregatingMergeTree | Pre-computed distinct service names |

### Configuration

Enable materialized views via `config.yaml`:

```yaml
clickhouse:
  use_materialized_views: true
  trace_metadata_table: otel_trace_metadata
  span_tag_names_table: otel_span_tag_names
  resource_tag_names_table: otel_resource_tag_names
  service_names_table: otel_service_names
```

### How It Works

When `use_materialized_views: true`:

- **Search** (`/api/search`): Trace metadata (root service/name, start time, wall-clock duration, span count, error count) is read from `otel_trace_metadata` instead of fetching and re-aggregating all raw spans. Service stats come from a lightweight `GROUP BY (TraceId, ServiceName)` query on the raw table. Raw span rows are only fetched when `spss > 0` for SpanSet construction. This dramatically reduces I/O for search result enrichment.
- **Tag discovery** (`/api/search/tags`, `/api/v2/search/tags`): Tag name lists come from 5-minute bucketed `otel_span_tag_names` / `otel_resource_tag_names` tables. The requested time window is snapped outward to the nearest 5-minute boundaries before querying, so results may include tags that appeared up to ~5 minutes outside the exact window. This is intentional for fast UI discovery on long-running traces.
- **Service name values** (`/api/search/tag/service.name/values`): Service names come from the bucketed `otel_service_names` table with the same snapped-window semantics.
- **Other tag values**: Arbitrary attribute values (e.g., `/api/search/tag/http.method/values`) still query the raw `otel_traces` table since there is no value-cache materialized table.

### Trace Metadata Schema

The `otel_trace_metadata` table stores `MaxEndNano` (the latest nanosecond epoch at which any span in the trace ended) rather than a single-span max duration. Wall-clock trace duration is computed as `MaxEndNano - StartTime.UnixNano()`, matching the search API's existing semantics.

### Tag/Service Bucketed Schema

The tag name and service name tables use a 5-minute bucketed design. Each row represents the presence of a key within a specific 5-minute interval (`BucketStart`). This replaces the earlier `FirstSeen`/`LastSeen` lifetime design, which produced false positives for long-running ML pipeline traces where a tag or service could appear at the start and end of a multi-day trace with gaps in between. The bucketed schema eliminates those false positives while keeping discovery queries fast by avoiding raw span scans entirely.

### Files Modified

- `deploy/clickhouse/materialized_views.sql` — DDL for all 4 materialized tables
- `internal/config/config.go` — Added `UseMatViews`, `TraceMetadataTable`, `SpanTagNamesTable`, `ResourceTagNamesTable`, `ServiceNamesTable` fields
- `internal/clickhouse/trace.go` — Added `QueryTraceMetadataByTraceIDs()`, `QueryServiceStatsByTraceIDs()`, bucketed `QueryTagNamesFromBuckets()`, bucketed `QueryServiceNamesFromBuckets()`
- `internal/tracequery/search.go` — Hybrid search path using trace metadata MV when enabled
- `internal/tracequery/tags.go` — Bucket-snapped MV reads for tag/service discovery with `snapTo5m()` helper
- `Makefile` — `matviews` target

### Running

```bash
make matviews  # Create materialized views (ClickHouse must be running)
```

---

## 8.3 Query Optimizations

### PREWHERE

PREWHERE separates time range conditions from filter conditions. ClickHouse reads only indexed columns for the PREWHERE clause first, then reads full columns only for matching rows.

Enable via `TranspileOptions.UsePrewhere = true` or the `with(prewhere=true)` hint.

```sql
-- Without PREWHERE:
SELECT DISTINCT TraceId FROM otel_traces
WHERE Timestamp >= ... AND Timestamp <= ... AND SpanAttributes['http.method'] = 'GET'

-- With PREWHERE:
SELECT DISTINCT TraceId FROM otel_traces
PREWHERE Timestamp >= ... AND Timestamp <= ...
WHERE SpanAttributes['http.method'] = 'GET'
```

### SAMPLE

SAMPLE enables ClickHouse's built-in sampling for approximate query results on large datasets.

Enable via `TranspileOptions.SampleRate = 0.1` or the `with(sample=0.1)` hint.

```sql
SELECT DISTINCT TraceId FROM otel_traces SAMPLE 0.1
WHERE ...
```

### Query Hints (applyHints)

The `applyHints` function reads hints from the TraceQL AST (e.g., `with(sample=0.1, prewhere=true)`) and applies them to `TranspileOptions`:

```go
func applyHints(root *traceql.RootExpr, opts *TranspileOptions)
```

Supported hints:
- `sample` (float or int) — Sets `SampleRate`. Int values are interpreted as percentages (e.g., `10` → `0.1`).
- `prewhere` (bool) — Sets `UsePrewhere`.

### Query Timeout

The ClickHouse client applies a configurable read timeout to queries via `context.WithTimeout`. Configured via `clickhouse.read_timeout` in config.

### Files Modified

- `internal/transpiler/transpiler.go` — Added `UsePrewhere`, `SampleRate` to `TranspileOptions`, added `sampleClause()`, `applyHints()`, refactored `transpileSpansetFilter` to support PREWHERE.
- `internal/clickhouse/client.go` — Added query timeout enforcement via context deadlines.

---

## 8.4 Prometheus Metrics

### New Package

`internal/metrics/metrics.go` — provides Prometheus metrics instrumentation.

### Metrics Exposed

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `opex_query_duration_seconds` | Histogram | endpoint, method, status_code | HTTP request duration |
| `opex_clickhouse_query_duration_seconds` | Histogram | query_type | ClickHouse query execution time |
| `opex_active_queries` | Gauge | — | In-flight queries |
| `opex_query_errors_total` | Counter | error_type | Query error count |
| `opex_traces_searched_total` | Counter | — | Total traces inspected |
| `opex_spans_searched_total` | Counter | — | Total spans inspected |

### Query Type Classification

The `classifyQuery` function in `internal/clickhouse/client.go` labels ClickHouse queries by type:
- `structural` — Recursive CTE queries
- `pipeline` — CTE-based pipeline queries
- `spanset_op` — INTERSECT/UNION queries
- `aggregate` — GROUP BY queries
- `tag_discovery` — arrayJoin(mapKeys) queries
- `tag_values` — mapContains queries
- `query` — Default

### Endpoint Normalization

`normalizeEndpoint` collapses variable path segments for Prometheus label cardinality:
- `/api/traces/abc123` → `/api/traces/{traceID}`
- `/api/search/tag/http.method/values` → `/api/search/tag/{tagName}/values`

### Integration

- `internal/server/server.go` — Registered `/metrics` endpoint and `metrics.Middleware`
- `internal/clickhouse/client.go` — Instrumented `Query()` with `ObserveClickHouseQuery` and `RecordQueryError`

---

## 8.5 Enhanced Structured Logging

### Improvements

1. **Response-aware logging middleware** — Captures status code and response size via `loggingResponseWriter` wrapper.

2. **Adaptive log levels** — Request logging uses:
   - `ERROR` for 5xx responses
   - `WARN` for slow queries (>5s)
   - `INFO` for normal requests

3. **Rich log attributes** — Each request log includes:
   - `method`, `path`, `status`, `duration`, `duration_ms`, `response_bytes`, `remote`
   - `query` (when present in URL params)
   - `user_agent`

4. **ClickHouse query logging** — Each query logs:
   - `query_type`, `sql_length`, truncated `sql` (max 500 chars)
   - On completion: `duration_ms`
   - On error: full error message

5. **Startup logging** — `main.go` logs all configuration on startup: version, listen addr, table, limits, timeouts, matview status, log level/format.

### Files Modified

- `internal/server/server.go` — Added `loggingResponseWriter`, enhanced `loggingMiddleware` with status codes, response size, adaptive log levels, query params
- `internal/clickhouse/client.go` — Enhanced `Query()` with structured debug/error logging, added `truncateSQL` helper
- `cmd/opex/main.go` — Added startup configuration logging

---

## 8.6 Test Coverage

### New Tests Added

#### Transpiler Tests (`internal/transpiler/transpiler_test.go`)

**Structural operator tests:**
- `TestTranspileStructuralChild` — Child JOIN with unscoped attributes
- `TestTranspileStructuralParent` — Parent JOIN (reversed)
- `TestTranspileStructuralDescendant` — Recursive CTE for descendants
- `TestTranspileStructuralAncestor` — Recursive CTE for ancestors
- `TestTranspileStructuralSibling` — Sibling JOIN with ParentSpanId match
- `TestTranspileStructuralChildWithIntrinsics` — Intrinsics (SpanName, StatusCode) in structural ops
- `TestTranspileStructuralSiblingWithDuration` — Duration intrinsic in sibling query
- `TestTranspileStructuralChildWithScopedAttrs` — resource.service.name + span.http.method
- `TestTranspileStructuralChildHasTimeFilter` — Time filter aliasing verification
- `TestTranspileStructuralSiblingHasTimeFilter` — Time filter aliasing for sibling
- `TestTranspileStructuralChildEmptyFilters` — Empty `{ }` filters on both sides
- `TestTranspileStructuralNonFilterError` — Error on non-SpansetFilter elements
- `TestTranspileStructuralNotChild` — Negated child (EXCEPT)
- `TestTranspileStructuralNotDescendant` — Negated descendant (EXCEPT + WITH RECURSIVE)
- `TestTranspileStructuralNotSibling` — Negated sibling (AST-level, !~ parser conflict)
- `TestTranspileStructuralUnionChild` — Union child (UNION + JOIN)

**Optimization tests:**
- `TestSampleClause` — 5 cases: no sampling, 10%, 50%, 100% (disabled), negative
- `TestTranspileWithSampling` — SAMPLE in generated SQL
- `TestTranspileWithoutSampling` — No SAMPLE when rate is 0
- `TestTranspileWithPrewhere` — PREWHERE with time range
- `TestTranspilePrewhereWithPrevCTE` — PREWHERE in pipeline queries
- `TestTranspilePrewhereNoTimeRange` — PREWHERE disabled without time range
- `TestTranspilePrewhereAndSample` — Combined PREWHERE + SAMPLE

**Hint tests:**
- `TestApplyHintsSample` — Float sample rate from hint
- `TestApplyHintsSampleInt` — Int sample rate (percentage) from hint
- `TestApplyHintsPrewhere` — Boolean prewhere hint
- `TestApplyHintsNilHints` — No hints (nil safety)
- `TestApplyHintsUnknownHint` — Unknown hints ignored

**Helper function tests:**
- `TestAliasedTimeFilter` — Full time range aliased
- `TestAliasedTimeFilterNoRange` — Returns "1=1"
- `TestAliasedTimeFilterStartOnly` — Start-only alias
- `TestAliasedTimeFilterEndOnly` — End-only alias
- `TestReplaceColumnsWithAlias` — 6 column replacement cases
- `TestExtractFilterConditionEmptyFilter` — Empty filter returns "1=1"

#### Metrics Tests (`internal/metrics/metrics_test.go`)

- `TestNormalizeEndpoint` — 14 endpoint normalization cases
- `TestMetricsHandler` — Prometheus handler returns 200 with content
- `TestMiddleware` — Status code capture on 200
- `TestMiddlewareCaptures500` — Status code capture on 500
- `TestResponseWriterCapturesStatusCode` — WriteHeader capture
- `TestObserveClickHouseQuery` — No panic
- `TestRecordQueryError` — No panic

### Coverage Summary

| Package | Coverage | Notes |
|---------|----------|-------|
| `internal/response` | **100%** | Unchanged from Phase 7 |
| `internal/metrics` | **100%** | All functions covered |
| `internal/transpiler` | **88.1%** | Up from 90% (denominator grew with new code; ~same uncovered lines) |
| `internal/traceql` | **57%** | Unchanged from Phase 7 |
| `internal/api` | **25.6%** | Unchanged (HTTP handlers need ClickHouse) |

### Total Test Count

~370+ individual test cases (up from ~160 in Phase 7).

---

## Configuration Reference

Updated `config.yaml` with all Phase 8 options:

```yaml
listen_addr: ":8080"

clickhouse:
  dsn: "clickhouse://localhost:9000/default"
  traces_table: "otel_traces"
  max_open_conns: 10
  max_idle_conns: 5
  conn_max_lifetime: 5m
  dial_timeout: 5s
  read_timeout: 30s
  # Materialized views (Phase 8)
  use_materialized_views: true
  trace_metadata_table: "otel_trace_metadata"
  span_tag_names_table: "otel_span_tag_names"
  resource_tag_names_table: "otel_resource_tag_names"
  service_names_table: "otel_service_names"

query:
  max_limit: 100
  default_limit: 20
  default_spss: 3
  max_duration: 168h
  timeout: 30s
  max_concurrent: 20

logging:
  level: info     # debug, info, warn, error
  format: json    # json, text
```

---

## New Files

| File | Purpose |
|------|---------|
| `internal/metrics/metrics.go` | Prometheus metrics definitions + middleware |
| `internal/metrics/metrics_test.go` | Metrics package tests |
| `deploy/clickhouse/materialized_views.sql` | Materialized view DDL |
| `docs/phase8_reference.md` | This document |

## Modified Files

| File | Changes |
|------|---------|
| `internal/transpiler/transpiler.go` | Structural operators, PREWHERE, SAMPLE, hints |
| `internal/transpiler/field.go` | No changes |
| `internal/transpiler/transpiler_test.go` | ~40 new tests for Phase 8 features |
| `internal/config/config.go` | Materialized view config fields |
| `internal/clickhouse/client.go` | Query instrumentation, timeout, structured logging |
| `internal/clickhouse/trace.go` | Materialized view query methods |
| `internal/api/tags.go` | Materialized view integration |
| `internal/server/server.go` | Metrics endpoint, enhanced logging middleware |
| `cmd/opex/main.go` | Startup config logging |
| `Makefile` | `matviews` target |
| `go.mod` / `go.sum` | prometheus/client_golang dependency |
