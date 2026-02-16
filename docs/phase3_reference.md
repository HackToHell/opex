# Phase 3 Reference - SQL Transpiler Core

**Status:** COMPLETE

## Files

```
internal/transpiler/
├── transpiler.go       # Entry point: Transpile(root, opts) -> TranspileResult
├── field.go            # Field expressions, operators, attributes, statics -> SQL
└── transpiler_test.go  # 30+ test cases
```

## Package: `internal/transpiler`

### Entry Point

```go
func Transpile(root *traceql.RootExpr, opts TranspileOptions) (*TranspileResult, error)

type TranspileResult struct {
    SQL  string
    Args []any
}

type TranspileOptions struct {
    Table string        // default: "otel_traces"
    Start time.Time     // time range start
    End   time.Time     // time range end
    Limit int           // default: 20
}
```

### SQL Generation Patterns

#### 1. Simple filter: `{ expr }`
```sql
SELECT DISTINCT TraceId FROM otel_traces
WHERE <time_filter> AND <expr_sql>
LIMIT 20
```

#### 2. Spanset AND: `{ a } && { b }`
```sql
<left_sql> INTERSECT <right_sql>
```

#### 3. Spanset UNION: `{ a } || { b }`
```sql
<left_sql> UNION <right_sql>
```

#### 4. Pipeline: `{ a } | { b }`
```sql
WITH stage1 AS (
  SELECT DISTINCT TraceId FROM otel_traces WHERE <time> AND <a_expr> LIMIT 20
)
SELECT DISTINCT TraceId FROM otel_traces
WHERE TraceId IN (SELECT TraceId FROM stage1)
  AND <time> AND <b_expr> LIMIT 20
```

#### 5. Scalar filter: `{ } | count() > 5`
```sql
WITH stage1 AS (
  SELECT DISTINCT TraceId FROM otel_traces WHERE <time> LIMIT 20
)
SELECT TraceId FROM stage1 WHERE 1=1
GROUP BY TraceId HAVING count(*) > 5 LIMIT 20
```

#### 6. Metrics aggregate: `{ } | rate() by(resource.service.name)`
```sql
SELECT count(*) AS value, ServiceName AS label_resource_service_name
FROM otel_traces WHERE <time>
GROUP BY ServiceName LIMIT 20
```

### Attribute-to-SQL Mapping

| TraceQL | SQL |
|---------|-----|
| `.attr` (unscoped, string) | `(SpanAttributes['attr'] = 'val' OR ResourceAttributes['attr'] = 'val')` |
| `.attr` (unscoped, int) | `(toInt64OrZero(SpanAttributes['attr']) > 400 OR toInt64OrZero(ResourceAttributes['attr']) > 400)` |
| `span.attr` | `SpanAttributes['attr']` |
| `resource.attr` | `ResourceAttributes['attr']` |
| `resource.service.name` | `ServiceName` (first-class column) |
| `duration` | `Duration` |
| `name` | `SpanName` |
| `status` | `StatusCode` |
| `kind` | `SpanKind` |
| `statusMessage` | `StatusMessage` |
| `span:id` | `SpanId` |
| `trace:id` | `TraceId` |
| `span:parentID` | `ParentSpanId` |
| `instrumentation:name` | `ScopeName` |

### Type Coercion for Map Values

| Static Type | Map Access SQL |
|------------|----------------|
| TypeString | `SpanAttributes['key']` (no coercion) |
| TypeInt | `toInt64OrZero(SpanAttributes['key'])` |
| TypeFloat | `toFloat64OrZero(SpanAttributes['key'])` |
| TypeBoolean | `SpanAttributes['key']` (stored as 'true'/'false') |

### Status/Kind Value Mapping

| TraceQL | ClickHouse |
|---------|------------|
| `error` | `STATUS_CODE_ERROR` |
| `ok` | `STATUS_CODE_OK` |
| `unset` | `STATUS_CODE_UNSET` |
| `server` | `SPAN_KIND_SERVER` |
| `client` | `SPAN_KIND_CLIENT` |
| `internal` | `SPAN_KIND_INTERNAL` |
| `producer` | `SPAN_KIND_PRODUCER` |
| `consumer` | `SPAN_KIND_CONSUMER` |

### Key Functions

| Function | Purpose |
|----------|---------|
| `transpileFieldExpr(expr)` | FieldExpression -> SQL string |
| `transpileAttributeComparison(attr, op, static)` | Attr op Literal with type coercion |
| `transpileSpansetFilter(f, prevCTE)` | SpansetFilter -> SELECT with WHERE |
| `transpileSpansetOperation(op)` | SpansetOp -> INTERSECT/UNION |
| `transpileScalarFilter(f, prevCTE)` | ScalarFilter -> GROUP BY + HAVING |
| `transpileMetricsAggregate(m, prevCTE)` | MetricsAggregate -> aggregation SQL |
| `transpilePipeline(p)` | Pipeline -> CTE-chained SQL |
| `attributeToSQL(attr)` | Attribute -> column reference |
| `intrinsicColumnSQL(i)` | Intrinsic -> column name |
| `staticToSQL(s)` | Static -> SQL literal |
| `operatorToSQL(op)` | Operator -> SQL operator |
| `aggregateToSQL(a)` | Aggregate -> SQL function |
| `mapAccessSQL(col, key, type)` | Map access with coercion |

### Not Yet Supported
- Structural operators (>, >>, <, <<, ~) -- returns error
- `rootServiceName` / `rootName` / `traceDuration` -- simplified to ServiceName/SpanName/Duration (need subquery)
- `event:*` and `link:*` attributes (array access)
- `parent.*` attributes (need self-join)

### Test Results
30+ tests passing: simple filters, scoped attributes, existence checks, regex, compound expressions, spanset operations, pipelines, aggregates, metrics, intrinsics, error cases.

## What's Next (Phase 4)

Build the Tempo-compatible HTTP API layer:
1. `internal/clickhouse/client.go` -- ClickHouse connection + query execution
2. `internal/clickhouse/result.go` -- Row scanning to protobuf types
3. `internal/response/marshal.go` -- Content negotiation (JSON + protobuf)
4. `internal/api/trace.go` -- GET /api/traces/{traceID}
5. Wire everything together in the server
