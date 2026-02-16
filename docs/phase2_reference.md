# Phase 2 Reference - TraceQL Parser

**Status:** COMPLETE

## Decision: Custom Parser

We built our own TraceQL parser instead of importing Tempo's because:
- Tempo v1.5.0 (available from Go proxy) has all unexported AST fields
- Local tmp/tempo requires go 1.26.0 (we have 1.25.5)
- Our parser has all fields exported, making the transpiler straightforward

## Files Created

```
internal/traceql/
├── ast.go          # All AST types, enums, constants (all fields exported)
├── lexer.go        # Tokenizer
├── parser.go       # Recursive-descent parser
└── parser_test.go  # 40+ test cases
```

## Package: `internal/traceql`

### Entry Point

```go
func Parse(input string) (*RootExpr, error)
```

### AST Types (all fields exported)

```go
type RootExpr struct {
    Pipeline Pipeline
    Hints    *Hints
}

type Pipeline struct {
    Elements []PipelineElement
}
// Pipeline also implements PipelineElement

// Interface: PipelineElement (marker method: pipelineElement())
// Interface: FieldExpression (marker method: fieldExpression())
// Interface: ScalarExpression (marker method: scalarExpression())
```

### Pipeline Element Types

| Type | Implements | Key Fields |
|------|-----------|------------|
| `SpansetFilter` | PipelineElement | `Expression FieldExpression` (nil for `{ }`) |
| `SpansetOperation` | PipelineElement | `Op Operator`, `LHS PipelineElement`, `RHS PipelineElement` |
| `ScalarFilter` | PipelineElement | `Op Operator`, `LHS ScalarExpression`, `RHS ScalarExpression` |
| `Aggregate` | PipelineElement, ScalarExpression | `Op AggregateOp`, `Expression FieldExpression` |
| `GroupOperation` | PipelineElement | `Expression FieldExpression` |
| `CoalesceOperation` | PipelineElement | (empty) |
| `SelectOperation` | PipelineElement | `Attrs []Attribute` |
| `MetricsAggregate` | PipelineElement | `Op MetricsAggregateOp`, `By []Attribute`, `Attr FieldExpression`, `Floats []float64` |
| `Pipeline` | PipelineElement | `Elements []PipelineElement` |

### Field Expression Types

| Type | Key Fields |
|------|------------|
| `BinaryOperation` | `Op Operator`, `LHS FieldExpression`, `RHS FieldExpression` |
| `UnaryOperation` | `Op Operator`, `Expression FieldExpression` |
| `Static` | `Type StaticType`, `IntVal int64`, `FloatVal float64`, `StringVal string`, `BoolVal bool`, `DurationVal time.Duration`, `StatusVal Status`, `KindVal Kind` |
| `Attribute` | `Name string`, `Scope AttributeScope`, `Parent bool`, `Intrinsic Intrinsic` |

### Enums

**Operator** (all exported constants):
- Arithmetic: `OpAdd`, `OpSub`, `OpDiv`, `OpMod`, `OpMult`, `OpPower`
- Comparison: `OpEqual`, `OpNotEqual`, `OpRegex`, `OpNotRegex`, `OpGreater`, `OpGreaterEqual`, `OpLess`, `OpLessEqual`
- Logical: `OpAnd`, `OpOr`, `OpNot`
- Spanset: `OpSpansetAnd`, `OpSpansetUnion`
- Structural: `OpSpansetChild`, `OpSpansetParent`, `OpSpansetDescendant`, `OpSpansetAncestor`, `OpSpansetSibling` (+ Not/Union variants)
- Internal: `OpExists`, `OpNotExists`, `OpIn`, `OpNotIn`

**StaticType**: `TypeNil`, `TypeInt`, `TypeFloat`, `TypeString`, `TypeBoolean`, `TypeDuration`, `TypeStatus`, `TypeKind`

**Status**: `StatusError`, `StatusOk`, `StatusUnset`

**Kind**: `KindUnspecified`, `KindInternal`, `KindClient`, `KindServer`, `KindProducer`, `KindConsumer`

**AttributeScope**: `AttributeScopeNone`, `AttributeScopeResource`, `AttributeScopeSpan`, `AttributeScopeEvent`, `AttributeScopeLink`, `AttributeScopeInstrumentation`

**Intrinsic**: `IntrinsicNone`, `IntrinsicDuration`, `IntrinsicName`, `IntrinsicStatus`, `IntrinsicStatusMessage`, `IntrinsicKind`, `IntrinsicChildCount`, `IntrinsicTraceRootService`, `IntrinsicTraceRootSpan`, `IntrinsicTraceDuration`, `IntrinsicTraceID`, `IntrinsicSpanID`, `IntrinsicParentID`, `IntrinsicEventName`, `IntrinsicEventTimeSinceStart`, `IntrinsicLinkSpanID`, `IntrinsicLinkTraceID`, `IntrinsicInstrumentationName`, `IntrinsicInstrumentationVersion`, `IntrinsicParent`

**AggregateOp**: `AggregateCount`, `AggregateMax`, `AggregateMin`, `AggregateSum`, `AggregateAvg`

**MetricsAggregateOp**: `MetricsAggregateRate`, `MetricsAggregateCountOverTime`, `MetricsAggregateMinOverTime`, `MetricsAggregateMaxOverTime`, `MetricsAggregateAvgOverTime`, `MetricsAggregateSumOverTime`, `MetricsAggregateQuantileOverTime`, `MetricsAggregateHistogramOverTime`

### Parsing Behavior

- `status`, `kind` are lexed as identifiers and resolved by the parser
- Status values (`error`, `ok`, `unset`) and kind values (`server`, `client`, etc.) are resolved contextually
- `parent.resource.X` correctly parses as parent-scoped resource attribute
- `{ }` (empty filter) has `Expression = nil`
- Nil checks: `{ .x != nil }` -> `UnaryOperation{Op: OpExists}`, `{ .x = nil }` -> `UnaryOperation{Op: OpNotExists}`
- Negative numbers are folded into Static at parse time
- Duration supports: ns, us, ms, s, m, h, d (day = 24h)
- Dotted attribute names: `.http.status_code` -> `Attribute{Name: "http.status_code"}`

### Helper Function

```go
func LookupIntrinsic(s string) Intrinsic  // returns IntrinsicNone if not found
```

### Test Coverage

40+ test cases covering:
- Empty filter, string/int/float/bool/duration comparisons
- All comparison operators (=, !=, >, >=, <, <=, =~, !~)
- Intrinsics (duration, name, status, kind, trace:id, span:id)
- Scoped attributes (resource.X, span.X)
- Parent-scoped attributes (parent.resource.X)
- Existence checks (nil, not nil)
- Logical operators (&&, ||, !)
- Arithmetic (+, -)
- Spanset operations (&&, ||)
- Structural operators (>>, <<, ~)
- Pipelines with multiple stages
- Aggregates (count, min, max, sum, avg)
- ScalarFilter (count() > 5)
- GroupOperation (by())
- CoalesceOperation (coalesce())
- SelectOperation (select())
- Metrics aggregates (rate, count_over_time, quantile_over_time, avg_over_time)
- Metrics with by() clause
- Negative numbers
- Error cases (7 tests)

## What's Next (Phase 3)

Build `internal/transpiler/` to translate the AST into ClickHouse SQL:
1. `attribute.go` -- Attribute/Intrinsic -> column/map access SQL
2. `field.go` -- FieldExpression -> SQL WHERE clause fragments
3. `filter.go` -- SpansetFilter -> complete WHERE clause
4. `spanset_op.go` -- SpansetOperation -> INTERSECT/UNION
5. `aggregate.go` -- Aggregate -> SQL aggregation
6. `scalar.go` -- ScalarFilter -> HAVING clause
7. `pipeline.go` -- Pipeline -> CTE chaining
8. `transpiler.go` -- Main entry point: RootExpr -> TranspileResult{SQL, Args}

The transpiler should type-switch on AST nodes (all fields are exported and accessible).
