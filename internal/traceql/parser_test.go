package traceql

import (
	"testing"
	"time"
)

// helper to assert parse succeeds and run a validation function on the result.
func mustParse(t *testing.T, input string) *RootExpr {
	t.Helper()
	root, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse(%q) failed: %v", input, err)
	}
	if root == nil {
		t.Fatalf("Parse(%q) returned nil", input)
	}
	return root
}

func mustFail(t *testing.T, input string) {
	t.Helper()
	_, err := Parse(input)
	if err == nil {
		t.Fatalf("Parse(%q) should have failed but succeeded", input)
	}
}

// ---------------------------------------------------------------------------
// Basic spanset filters
// ---------------------------------------------------------------------------

func TestParseEmptyFilter(t *testing.T) {
	root := mustParse(t, "{ }")
	if len(root.Pipeline.Elements) != 1 {
		t.Fatalf("expected 1 element, got %d", len(root.Pipeline.Elements))
	}
	sf, ok := root.Pipeline.Elements[0].(*SpansetFilter)
	if !ok {
		t.Fatalf("expected SpansetFilter, got %T", root.Pipeline.Elements[0])
	}
	if sf.Expression != nil {
		t.Fatalf("expected nil expression for empty filter")
	}
}

func TestParseSimpleStringComparison(t *testing.T) {
	root := mustParse(t, `{ .http.method = "GET" }`)
	sf := root.Pipeline.Elements[0].(*SpansetFilter)
	bin := sf.Expression.(*BinaryOperation)
	if bin.Op != OpEqual {
		t.Fatalf("expected OpEqual, got %v", bin.Op)
	}
	attr := bin.LHS.(*Attribute)
	if attr.Name != "http.method" {
		t.Fatalf("expected http.method, got %s", attr.Name)
	}
	if attr.Scope != AttributeScopeNone {
		t.Fatalf("expected unscoped, got %s", attr.Scope)
	}
	static := bin.RHS.(*Static)
	if static.Type != TypeString || static.StringVal != "GET" {
		t.Fatalf("expected string 'GET', got %v %q", static.Type, static.StringVal)
	}
}

func TestParseIntrinsicDuration(t *testing.T) {
	root := mustParse(t, "{ duration > 1s }")
	sf := root.Pipeline.Elements[0].(*SpansetFilter)
	bin := sf.Expression.(*BinaryOperation)
	if bin.Op != OpGreater {
		t.Fatalf("expected OpGreater, got %v", bin.Op)
	}
	attr := bin.LHS.(*Attribute)
	if attr.Intrinsic != IntrinsicDuration {
		t.Fatalf("expected IntrinsicDuration, got %v", attr.Intrinsic)
	}
	static := bin.RHS.(*Static)
	if static.Type != TypeDuration || static.DurationVal != time.Second {
		t.Fatalf("expected 1s duration, got %v", static.DurationVal)
	}
}

func TestParseIntrinsicStatus(t *testing.T) {
	root := mustParse(t, "{ status = error }")
	sf := root.Pipeline.Elements[0].(*SpansetFilter)
	bin := sf.Expression.(*BinaryOperation)
	attr := bin.LHS.(*Attribute)
	if attr.Intrinsic != IntrinsicStatus {
		t.Fatalf("expected IntrinsicStatus, got %v", attr.Intrinsic)
	}
	static := bin.RHS.(*Static)
	if static.Type != TypeStatus || static.StatusVal != StatusError {
		t.Fatalf("expected status=error, got %v %v", static.Type, static.StatusVal)
	}
}

func TestParseIntrinsicKind(t *testing.T) {
	root := mustParse(t, "{ kind = server }")
	sf := root.Pipeline.Elements[0].(*SpansetFilter)
	bin := sf.Expression.(*BinaryOperation)
	static := bin.RHS.(*Static)
	if static.Type != TypeKind || static.KindVal != KindServer {
		t.Fatalf("expected kind=server, got %v %v", static.Type, static.KindVal)
	}
}

func TestParseScopedAttribute(t *testing.T) {
	root := mustParse(t, `{ resource.service.name = "frontend" }`)
	sf := root.Pipeline.Elements[0].(*SpansetFilter)
	bin := sf.Expression.(*BinaryOperation)
	attr := bin.LHS.(*Attribute)
	if attr.Scope != AttributeScopeResource {
		t.Fatalf("expected resource scope, got %v", attr.Scope)
	}
	if attr.Name != "service.name" {
		t.Fatalf("expected service.name, got %s", attr.Name)
	}
}

func TestParseSpanScopedAttribute(t *testing.T) {
	root := mustParse(t, `{ span.http.status_code = 200 }`)
	sf := root.Pipeline.Elements[0].(*SpansetFilter)
	bin := sf.Expression.(*BinaryOperation)
	attr := bin.LHS.(*Attribute)
	if attr.Scope != AttributeScopeSpan {
		t.Fatalf("expected span scope, got %v", attr.Scope)
	}
	if attr.Name != "http.status_code" {
		t.Fatalf("expected http.status_code, got %s", attr.Name)
	}
}

func TestParseScopedIntrinsic(t *testing.T) {
	root := mustParse(t, "{ span:id = \"abc\" }")
	sf := root.Pipeline.Elements[0].(*SpansetFilter)
	bin := sf.Expression.(*BinaryOperation)
	attr := bin.LHS.(*Attribute)
	if attr.Intrinsic != IntrinsicSpanID {
		t.Fatalf("expected IntrinsicSpanID, got %v", attr.Intrinsic)
	}
}

func TestParseTraceID(t *testing.T) {
	root := mustParse(t, `{ trace:id = "abc123" }`)
	sf := root.Pipeline.Elements[0].(*SpansetFilter)
	bin := sf.Expression.(*BinaryOperation)
	attr := bin.LHS.(*Attribute)
	if attr.Intrinsic != IntrinsicTraceID {
		t.Fatalf("expected IntrinsicTraceID, got %v", attr.Intrinsic)
	}
}

// ---------------------------------------------------------------------------
// Existence checks (nil)
// ---------------------------------------------------------------------------

func TestParseExistsCheck(t *testing.T) {
	root := mustParse(t, "{ .http.method != nil }")
	sf := root.Pipeline.Elements[0].(*SpansetFilter)
	unary := sf.Expression.(*UnaryOperation)
	if unary.Op != OpExists {
		t.Fatalf("expected OpExists, got %v", unary.Op)
	}
	attr := unary.Expression.(*Attribute)
	if attr.Name != "http.method" {
		t.Fatalf("expected http.method, got %s", attr.Name)
	}
}

func TestParseNotExistsCheck(t *testing.T) {
	root := mustParse(t, "{ .http.method = nil }")
	sf := root.Pipeline.Elements[0].(*SpansetFilter)
	unary := sf.Expression.(*UnaryOperation)
	if unary.Op != OpNotExists {
		t.Fatalf("expected OpNotExists, got %v", unary.Op)
	}
}

// ---------------------------------------------------------------------------
// Compound expressions
// ---------------------------------------------------------------------------

func TestParseAndExpression(t *testing.T) {
	root := mustParse(t, `{ .http.method = "GET" && duration > 1s }`)
	sf := root.Pipeline.Elements[0].(*SpansetFilter)
	bin := sf.Expression.(*BinaryOperation)
	if bin.Op != OpAnd {
		t.Fatalf("expected OpAnd, got %v", bin.Op)
	}
	// LHS: .http.method = "GET"
	lhsBin := bin.LHS.(*BinaryOperation)
	if lhsBin.Op != OpEqual {
		t.Fatalf("expected OpEqual on LHS, got %v", lhsBin.Op)
	}
	// RHS: duration > 1s
	rhsBin := bin.RHS.(*BinaryOperation)
	if rhsBin.Op != OpGreater {
		t.Fatalf("expected OpGreater on RHS, got %v", rhsBin.Op)
	}
}

func TestParseOrExpression(t *testing.T) {
	root := mustParse(t, `{ .http.method = "GET" || .http.method = "POST" }`)
	sf := root.Pipeline.Elements[0].(*SpansetFilter)
	bin := sf.Expression.(*BinaryOperation)
	if bin.Op != OpOr {
		t.Fatalf("expected OpOr, got %v", bin.Op)
	}
}

func TestParseNotExpression(t *testing.T) {
	root := mustParse(t, `{ !(status = error) }`)
	sf := root.Pipeline.Elements[0].(*SpansetFilter)
	unary := sf.Expression.(*UnaryOperation)
	if unary.Op != OpNot {
		t.Fatalf("expected OpNot, got %v", unary.Op)
	}
}

func TestParseRegex(t *testing.T) {
	root := mustParse(t, `{ name =~ "GET.*" }`)
	sf := root.Pipeline.Elements[0].(*SpansetFilter)
	bin := sf.Expression.(*BinaryOperation)
	if bin.Op != OpRegex {
		t.Fatalf("expected OpRegex, got %v", bin.Op)
	}
}

func TestParseNotRegex(t *testing.T) {
	root := mustParse(t, `{ name !~ "internal.*" }`)
	sf := root.Pipeline.Elements[0].(*SpansetFilter)
	bin := sf.Expression.(*BinaryOperation)
	if bin.Op != OpNotRegex {
		t.Fatalf("expected OpNotRegex, got %v", bin.Op)
	}
}

// ---------------------------------------------------------------------------
// Arithmetic
// ---------------------------------------------------------------------------

func TestParseArithmetic(t *testing.T) {
	root := mustParse(t, "{ duration > 1s + 500ms }")
	sf := root.Pipeline.Elements[0].(*SpansetFilter)
	bin := sf.Expression.(*BinaryOperation)
	if bin.Op != OpGreater {
		t.Fatalf("expected OpGreater, got %v", bin.Op)
	}
	addOp := bin.RHS.(*BinaryOperation)
	if addOp.Op != OpAdd {
		t.Fatalf("expected OpAdd, got %v", addOp.Op)
	}
}

func TestParseIntegers(t *testing.T) {
	root := mustParse(t, "{ .count > 42 }")
	sf := root.Pipeline.Elements[0].(*SpansetFilter)
	bin := sf.Expression.(*BinaryOperation)
	static := bin.RHS.(*Static)
	if static.Type != TypeInt || static.IntVal != 42 {
		t.Fatalf("expected int 42, got %v %d", static.Type, static.IntVal)
	}
}

func TestParseFloats(t *testing.T) {
	root := mustParse(t, "{ .ratio >= 0.95 }")
	sf := root.Pipeline.Elements[0].(*SpansetFilter)
	bin := sf.Expression.(*BinaryOperation)
	static := bin.RHS.(*Static)
	if static.Type != TypeFloat || static.FloatVal != 0.95 {
		t.Fatalf("expected float 0.95, got %v %f", static.Type, static.FloatVal)
	}
}

func TestParseBooleans(t *testing.T) {
	root := mustParse(t, "{ .cache.hit = true }")
	sf := root.Pipeline.Elements[0].(*SpansetFilter)
	bin := sf.Expression.(*BinaryOperation)
	static := bin.RHS.(*Static)
	if static.Type != TypeBoolean || !static.BoolVal {
		t.Fatalf("expected bool true, got %v %v", static.Type, static.BoolVal)
	}
}

func TestParseDurations(t *testing.T) {
	tests := []struct {
		input    string
		expected time.Duration
	}{
		{"{ duration > 1s }", time.Second},
		{"{ duration > 500ms }", 500 * time.Millisecond},
		{"{ duration > 1h }", time.Hour},
		{"{ duration > 1m }", time.Minute},
		{"{ duration > 100us }", 100 * time.Microsecond},
		{"{ duration > 100ns }", 100 * time.Nanosecond},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			root := mustParse(t, tc.input)
			sf := root.Pipeline.Elements[0].(*SpansetFilter)
			bin := sf.Expression.(*BinaryOperation)
			static := bin.RHS.(*Static)
			if static.Type != TypeDuration || static.DurationVal != tc.expected {
				t.Fatalf("expected duration %v, got %v", tc.expected, static.DurationVal)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Spanset operations
// ---------------------------------------------------------------------------

func TestParseSpansetAnd(t *testing.T) {
	root := mustParse(t, `{ .a = "x" } && { .b = "y" }`)
	if len(root.Pipeline.Elements) != 1 {
		t.Fatalf("expected 1 element (SpansetOperation), got %d", len(root.Pipeline.Elements))
	}
	sop := root.Pipeline.Elements[0].(*SpansetOperation)
	if sop.Op != OpSpansetAnd {
		t.Fatalf("expected OpSpansetAnd, got %v", sop.Op)
	}
	_, ok := sop.LHS.(*SpansetFilter)
	if !ok {
		t.Fatalf("expected SpansetFilter LHS, got %T", sop.LHS)
	}
	_, ok = sop.RHS.(*SpansetFilter)
	if !ok {
		t.Fatalf("expected SpansetFilter RHS, got %T", sop.RHS)
	}
}

func TestParseSpansetUnion(t *testing.T) {
	root := mustParse(t, `{ .a = "x" } || { .b = "y" }`)
	sop := root.Pipeline.Elements[0].(*SpansetOperation)
	if sop.Op != OpSpansetUnion {
		t.Fatalf("expected OpSpansetUnion, got %v", sop.Op)
	}
}

// ---------------------------------------------------------------------------
// Structural operators
// ---------------------------------------------------------------------------

func TestParseStructuralDescendant(t *testing.T) {
	root := mustParse(t, `{ .a = "x" } >> { .b = "y" }`)
	sop := root.Pipeline.Elements[0].(*SpansetOperation)
	if sop.Op != OpSpansetDescendant {
		t.Fatalf("expected OpSpansetDescendant, got %v", sop.Op)
	}
}

func TestParseStructuralAncestor(t *testing.T) {
	root := mustParse(t, `{ .a = "x" } << { .b = "y" }`)
	sop := root.Pipeline.Elements[0].(*SpansetOperation)
	if sop.Op != OpSpansetAncestor {
		t.Fatalf("expected OpSpansetAncestor, got %v", sop.Op)
	}
}

func TestParseStructuralSibling(t *testing.T) {
	root := mustParse(t, `{ .a = "x" } ~ { .b = "y" }`)
	sop := root.Pipeline.Elements[0].(*SpansetOperation)
	if sop.Op != OpSpansetSibling {
		t.Fatalf("expected OpSpansetSibling, got %v", sop.Op)
	}
}

// ---------------------------------------------------------------------------
// Pipelines
// ---------------------------------------------------------------------------

func TestParsePipelineWithFilter(t *testing.T) {
	root := mustParse(t, `{ .http.method = "GET" } | { status = error }`)
	if len(root.Pipeline.Elements) != 2 {
		t.Fatalf("expected 2 pipeline elements, got %d", len(root.Pipeline.Elements))
	}
	_, ok := root.Pipeline.Elements[0].(*SpansetFilter)
	if !ok {
		t.Fatalf("expected SpansetFilter at position 0, got %T", root.Pipeline.Elements[0])
	}
	_, ok = root.Pipeline.Elements[1].(*SpansetFilter)
	if !ok {
		t.Fatalf("expected SpansetFilter at position 1, got %T", root.Pipeline.Elements[1])
	}
}

func TestParsePipelineWithAggregate(t *testing.T) {
	root := mustParse(t, `{ } | count() > 5`)
	if len(root.Pipeline.Elements) != 2 {
		t.Fatalf("expected 2 pipeline elements, got %d", len(root.Pipeline.Elements))
	}
	sf, ok := root.Pipeline.Elements[1].(*ScalarFilter)
	if !ok {
		t.Fatalf("expected ScalarFilter at position 1, got %T", root.Pipeline.Elements[1])
	}
	agg := sf.LHS.(*Aggregate)
	if agg.Op != AggregateCount {
		t.Fatalf("expected AggregateCount, got %v", agg.Op)
	}
	static := sf.RHS.(*Static)
	if static.IntVal != 5 {
		t.Fatalf("expected 5, got %d", static.IntVal)
	}
}

func TestParsePipelineWithAvgDuration(t *testing.T) {
	root := mustParse(t, `{ .http.method = "GET" } | avg(duration) > 1s`)
	sf := root.Pipeline.Elements[1].(*ScalarFilter)
	agg := sf.LHS.(*Aggregate)
	if agg.Op != AggregateAvg {
		t.Fatalf("expected AggregateAvg, got %v", agg.Op)
	}
	attr := agg.Expression.(*Attribute)
	if attr.Intrinsic != IntrinsicDuration {
		t.Fatalf("expected IntrinsicDuration, got %v", attr.Intrinsic)
	}
}

func TestParsePipelineWithGroupBy(t *testing.T) {
	root := mustParse(t, `{ } | by(.http.method)`)
	if len(root.Pipeline.Elements) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(root.Pipeline.Elements))
	}
	group, ok := root.Pipeline.Elements[1].(*GroupOperation)
	if !ok {
		t.Fatalf("expected GroupOperation, got %T", root.Pipeline.Elements[1])
	}
	attr := group.Expression.(*Attribute)
	if attr.Name != "http.method" {
		t.Fatalf("expected http.method, got %s", attr.Name)
	}
}

func TestParsePipelineWithCoalesce(t *testing.T) {
	root := mustParse(t, `{ } | coalesce()`)
	_, ok := root.Pipeline.Elements[1].(*CoalesceOperation)
	if !ok {
		t.Fatalf("expected CoalesceOperation, got %T", root.Pipeline.Elements[1])
	}
}

func TestParsePipelineWithSelect(t *testing.T) {
	root := mustParse(t, `{ } | select(.http.method, .http.status_code)`)
	sel, ok := root.Pipeline.Elements[1].(*SelectOperation)
	if !ok {
		t.Fatalf("expected SelectOperation, got %T", root.Pipeline.Elements[1])
	}
	if len(sel.Attrs) != 2 {
		t.Fatalf("expected 2 attrs, got %d", len(sel.Attrs))
	}
	if sel.Attrs[0].Name != "http.method" {
		t.Fatalf("expected http.method, got %s", sel.Attrs[0].Name)
	}
	if sel.Attrs[1].Name != "http.status_code" {
		t.Fatalf("expected http.status_code, got %s", sel.Attrs[1].Name)
	}
}

// ---------------------------------------------------------------------------
// Metrics aggregates
// ---------------------------------------------------------------------------

func TestParseRate(t *testing.T) {
	root := mustParse(t, `{ } | rate()`)
	ma, ok := root.Pipeline.Elements[1].(*MetricsAggregate)
	if !ok {
		t.Fatalf("expected MetricsAggregate, got %T", root.Pipeline.Elements[1])
	}
	if ma.Op != MetricsAggregateRate {
		t.Fatalf("expected Rate, got %v", ma.Op)
	}
}

func TestParseCountOverTimeBy(t *testing.T) {
	root := mustParse(t, `{ status = error } | count_over_time() by(resource.service.name)`)
	ma := root.Pipeline.Elements[1].(*MetricsAggregate)
	if ma.Op != MetricsAggregateCountOverTime {
		t.Fatalf("expected CountOverTime, got %v", ma.Op)
	}
	if len(ma.By) != 1 {
		t.Fatalf("expected 1 by attr, got %d", len(ma.By))
	}
	if ma.By[0].Scope != AttributeScopeResource || ma.By[0].Name != "service.name" {
		t.Fatalf("expected resource.service.name, got %v %s", ma.By[0].Scope, ma.By[0].Name)
	}
}

func TestParseQuantileOverTime(t *testing.T) {
	root := mustParse(t, `{ } | quantile_over_time(duration, 0.95)`)
	ma := root.Pipeline.Elements[1].(*MetricsAggregate)
	if ma.Op != MetricsAggregateQuantileOverTime {
		t.Fatalf("expected QuantileOverTime, got %v", ma.Op)
	}
	attr := ma.Attr.(*Attribute)
	if attr.Intrinsic != IntrinsicDuration {
		t.Fatalf("expected duration attr, got %v", attr.Intrinsic)
	}
	if len(ma.Floats) != 1 || ma.Floats[0] != 0.95 {
		t.Fatalf("expected [0.95], got %v", ma.Floats)
	}
}

// ---------------------------------------------------------------------------
// Comparison operators
// ---------------------------------------------------------------------------

func TestParseAllComparisonOps(t *testing.T) {
	tests := []struct {
		query    string
		expected Operator
	}{
		{`{ .x = 1 }`, OpEqual},
		{`{ .x != 1 }`, OpNotEqual},
		{`{ .x > 1 }`, OpGreater},
		{`{ .x >= 1 }`, OpGreaterEqual},
		{`{ .x < 1 }`, OpLess},
		{`{ .x <= 1 }`, OpLessEqual},
		{`{ .x =~ "a" }`, OpRegex},
		{`{ .x !~ "a" }`, OpNotRegex},
	}
	for _, tc := range tests {
		t.Run(tc.query, func(t *testing.T) {
			root := mustParse(t, tc.query)
			sf := root.Pipeline.Elements[0].(*SpansetFilter)
			bin := sf.Expression.(*BinaryOperation)
			if bin.Op != tc.expected {
				t.Fatalf("expected %v, got %v", tc.expected, bin.Op)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Parent-scoped attributes
// ---------------------------------------------------------------------------

func TestParseParentAttribute(t *testing.T) {
	root := mustParse(t, `{ parent.resource.service.name = "frontend" }`)
	sf := root.Pipeline.Elements[0].(*SpansetFilter)
	bin := sf.Expression.(*BinaryOperation)
	attr := bin.LHS.(*Attribute)
	if !attr.Parent {
		t.Fatalf("expected parent=true")
	}
	if attr.Scope != AttributeScopeResource {
		t.Fatalf("expected resource scope, got %v", attr.Scope)
	}
	if attr.Name != "service.name" {
		t.Fatalf("expected service.name, got %s", attr.Name)
	}
}

// ---------------------------------------------------------------------------
// Error cases
// ---------------------------------------------------------------------------

func TestParseErrors(t *testing.T) {
	tests := []string{
		"",
		"{ ",
		"{ .x = }",
		`{ .x = "unterminated`,
		"not a query",
		"{ } |",
		"{ } | unknownfunc()",
	}
	for _, tc := range tests {
		t.Run(tc, func(t *testing.T) {
			mustFail(t, tc)
		})
	}
}

// ---------------------------------------------------------------------------
// Complex queries
// ---------------------------------------------------------------------------

func TestParseComplexPipeline(t *testing.T) {
	root := mustParse(t, `{ .http.method = "GET" && status = error } | { duration > 500ms } | count() > 3`)
	if len(root.Pipeline.Elements) != 3 {
		t.Fatalf("expected 3 pipeline elements, got %d", len(root.Pipeline.Elements))
	}
	// First: SpansetFilter with AND
	sf0 := root.Pipeline.Elements[0].(*SpansetFilter)
	and := sf0.Expression.(*BinaryOperation)
	if and.Op != OpAnd {
		t.Fatalf("expected AND, got %v", and.Op)
	}
	// Second: SpansetFilter
	_, ok := root.Pipeline.Elements[1].(*SpansetFilter)
	if !ok {
		t.Fatalf("expected SpansetFilter, got %T", root.Pipeline.Elements[1])
	}
	// Third: ScalarFilter
	scf, ok := root.Pipeline.Elements[2].(*ScalarFilter)
	if !ok {
		t.Fatalf("expected ScalarFilter, got %T", root.Pipeline.Elements[2])
	}
	if scf.Op != OpGreater {
		t.Fatalf("expected >, got %v", scf.Op)
	}
}

func TestParseSpansetAndThenPipeline(t *testing.T) {
	root := mustParse(t, `{ .a = "x" } && { .b = "y" } | count() > 2`)
	// This should parse as: (SpansetOperation) | ScalarFilter
	if len(root.Pipeline.Elements) != 2 {
		t.Fatalf("expected 2 pipeline elements, got %d", len(root.Pipeline.Elements))
	}
}

func TestParseString(t *testing.T) {
	// Verify the AST String() methods produce readable output
	root := mustParse(t, `{ .http.method = "GET" } | count() > 5`)
	s := root.Pipeline.String()
	if s == "" {
		t.Fatal("expected non-empty string")
	}
	t.Logf("AST string: %s", s)
}

// ---------------------------------------------------------------------------
// Metrics with by() clause
// ---------------------------------------------------------------------------

func TestParseMetricsWithMultipleBy(t *testing.T) {
	root := mustParse(t, `{ } | avg_over_time(duration) by(resource.service.name, .http.method)`)
	ma := root.Pipeline.Elements[1].(*MetricsAggregate)
	if ma.Op != MetricsAggregateAvgOverTime {
		t.Fatalf("expected AvgOverTime, got %v", ma.Op)
	}
	if len(ma.By) != 2 {
		t.Fatalf("expected 2 by attrs, got %d", len(ma.By))
	}
}

// ---------------------------------------------------------------------------
// Edge cases
// ---------------------------------------------------------------------------

func TestParseNegativeNumber(t *testing.T) {
	root := mustParse(t, "{ .temp < -10 }")
	sf := root.Pipeline.Elements[0].(*SpansetFilter)
	bin := sf.Expression.(*BinaryOperation)
	static := bin.RHS.(*Static)
	if static.Type != TypeInt || static.IntVal != -10 {
		t.Fatalf("expected int -10, got %v %d", static.Type, static.IntVal)
	}
}

func TestParseMultipleAggregates(t *testing.T) {
	root := mustParse(t, `{ } | min(duration) > 100ms`)
	sf := root.Pipeline.Elements[1].(*ScalarFilter)
	agg := sf.LHS.(*Aggregate)
	if agg.Op != AggregateMin {
		t.Fatalf("expected AggregateMin, got %v", agg.Op)
	}
}

func TestParseSumAggregate(t *testing.T) {
	root := mustParse(t, `{ } | sum(duration) > 10s`)
	sf := root.Pipeline.Elements[1].(*ScalarFilter)
	agg := sf.LHS.(*Aggregate)
	if agg.Op != AggregateSum {
		t.Fatalf("expected AggregateSum, got %v", agg.Op)
	}
}

func TestParseMaxAggregate(t *testing.T) {
	root := mustParse(t, `{ } | max(duration) > 5s`)
	sf := root.Pipeline.Elements[1].(*ScalarFilter)
	agg := sf.LHS.(*Aggregate)
	if agg.Op != AggregateMax {
		t.Fatalf("expected AggregateMax, got %v", agg.Op)
	}
}
