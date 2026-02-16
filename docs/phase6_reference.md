# Phase 6 Reference - Metrics APIs

**Status:** COMPLETE

## Files Created/Modified

```
internal/api/metrics.go       # QueryRange, QueryInstant, MetricsSummary handlers
internal/server/server.go     # Updated: registers metrics routes
```

## All Registered Routes

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
| `/api/metrics/query_range` | GET | QueryRange | P6 |
| `/api/metrics/query` | GET | QueryInstant | P6 |
| `/api/metrics/summary` | GET | MetricsSummary | P6 |

## Metrics Query Range

**Endpoint:** `GET /api/metrics/query_range?q={traceql}&start=&end=&step=`

**Flow:**
1. Parse TraceQL query
2. Extract MetricsAggregate from pipeline (last element)
3. Separate filter pipeline (everything before metrics aggregate)
4. Build time-bucketed SQL with `toStartOfInterval(Timestamp, INTERVAL {step} SECOND)`
5. Execute and parse into TimeSeries

**SQL Pattern:**
```sql
SELECT
    toStartOfInterval(Timestamp, INTERVAL {step} SECOND) AS ts,
    count(*) / {step} AS value,                    -- for rate()
    ServiceName AS label_resource_service_name     -- for by()
FROM otel_traces
WHERE <time_filter> [AND TraceId IN (<filter_subquery>)]
GROUP BY ts [, ServiceName]
ORDER BY ts
```

**Supported metrics aggregates:**
- `rate()` -> `count(*) / step_seconds`
- `count_over_time()` -> `count(*)`
- `min_over_time()` -> `min(Duration)`
- `max_over_time()` -> `max(Duration)`
- `avg_over_time()` -> `avg(Duration)`
- `sum_over_time()` -> `sum(Duration)`
- `quantile_over_time(attr, q)` -> `quantile(q)(Duration)`

## Metrics Query Instant

**Endpoint:** `GET /api/metrics/query?q={traceql}&start=&end=`

Uses the transpiler directly (no time bucketing). Returns scalar values per series.

## Metrics Summary

**Endpoint:** `GET /api/metrics/summary?q=&groupBy=&start=&end=&limit=`

**SQL:**
```sql
SELECT
    count(*) AS span_count,
    countIf(StatusCode = 'STATUS_CODE_ERROR') AS error_span_count,
    quantile(0.99)(Duration) AS p99,
    quantile(0.95)(Duration) AS p95,
    quantile(0.90)(Duration) AS p90,
    quantile(0.50)(Duration) AS p50,
    ServiceName AS label_service_name
FROM otel_traces
WHERE <time_filter>
GROUP BY ServiceName
LIMIT 10
```

## Key Functions

```go
func extractMetricsAggregate(root) (*MetricsAggregate, *Pipeline)
func (h) buildMetricsRangeSQL(m, filterSQL, start, end, step) string
func (h) parseRangeRows(rows, numLabels, labelNames) []TimeSeries
func parseStep(stepStr, start, end) time.Duration
func attributeToColumn(attr) string
func groupByToColumn(g) string
```

## Step Auto-Calculation

If `step` is not provided, auto-calculate: `(end - start) / 100` with a minimum of 1 second.

## What's Next (Phase 7)

Testing & Grafana Integration:
1. Add unit tests for API handlers (httptest)
2. Verify all endpoints end-to-end with ClickHouse
3. Test Grafana Tempo datasource connection
