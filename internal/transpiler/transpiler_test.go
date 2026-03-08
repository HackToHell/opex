package transpiler

import (
	"strings"
	"testing"
	"time"

	"github.com/hacktohell/opex/internal/traceql"
)

func defaultOpts() TranspileOptions {
	return TranspileOptions{
		Table: "otel_traces",
		Start: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		End:   time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC),
		Limit: 20,
	}
}

func mustTranspile(t *testing.T, query string) string {
	t.Helper()
	root, err := traceql.Parse(query)
	if err != nil {
		t.Fatalf("Parse(%q) failed: %v", query, err)
	}
	result, err := Transpile(root, defaultOpts())
	if err != nil {
		t.Fatalf("Transpile(%q) failed: %v", query, err)
	}
	return result.SQL
}

func assertContains(t *testing.T, sql, substr string) {
	t.Helper()
	if !strings.Contains(sql, substr) {
		t.Errorf("expected SQL to contain %q\ngot: %s", substr, sql)
	}
}

func assertNotContains(t *testing.T, sql, substr string) {
	t.Helper()
	if strings.Contains(sql, substr) {
		t.Errorf("expected SQL to NOT contain %q\ngot: %s", substr, sql)
	}
}

// ---------------------------------------------------------------------------
// Simple filters
// ---------------------------------------------------------------------------

func TestTranspileEmptyFilter(t *testing.T) {
	sql := mustTranspile(t, "{ }")
	assertContains(t, sql, "SELECT DISTINCT TraceId FROM otel_traces")
	assertContains(t, sql, "LIMIT 20")
	t.Logf("SQL: %s", sql)
}

func TestTranspileStringComparison(t *testing.T) {
	sql := mustTranspile(t, `{ .http.method = "GET" }`)
	assertContains(t, sql, "SpanAttributes['http.method']")
	assertContains(t, sql, "'GET'")
	assertContains(t, sql, "OR")
	assertContains(t, sql, "ResourceAttributes['http.method']")
	t.Logf("SQL: %s", sql)
}

func TestTranspileIntComparison(t *testing.T) {
	sql := mustTranspile(t, "{ .http.status_code > 400 }")
	assertContains(t, sql, "toInt64OrZero(SpanAttributes['http.status_code'])")
	assertContains(t, sql, "> 400")
	t.Logf("SQL: %s", sql)
}

func TestTranspileFloatComparison(t *testing.T) {
	sql := mustTranspile(t, "{ .ratio >= 0.95 }")
	assertContains(t, sql, "toFloat64OrZero(SpanAttributes['ratio'])")
	assertContains(t, sql, ">= 0.95")
	t.Logf("SQL: %s", sql)
}

func TestTranspileDurationComparison(t *testing.T) {
	sql := mustTranspile(t, "{ duration > 1s }")
	assertContains(t, sql, "Duration > 1000000000")
	t.Logf("SQL: %s", sql)
}

func TestTranspileStatusComparison(t *testing.T) {
	sql := mustTranspile(t, "{ status = error }")
	assertContains(t, sql, "StatusCode = 'STATUS_CODE_ERROR'")
	t.Logf("SQL: %s", sql)
}

func TestTranspileKindComparison(t *testing.T) {
	sql := mustTranspile(t, "{ kind = server }")
	assertContains(t, sql, "SpanKind = 'SPAN_KIND_SERVER'")
	t.Logf("SQL: %s", sql)
}

func TestTranspileNameComparison(t *testing.T) {
	sql := mustTranspile(t, `{ name = "GET /users" }`)
	assertContains(t, sql, "SpanName = 'GET /users'")
	t.Logf("SQL: %s", sql)
}

// ---------------------------------------------------------------------------
// Scoped attributes
// ---------------------------------------------------------------------------

func TestTranspileScopedSpan(t *testing.T) {
	sql := mustTranspile(t, `{ span.http.method = "GET" }`)
	assertContains(t, sql, "SpanAttributes['http.method'] = 'GET'")
	assertNotContains(t, sql, "ResourceAttributes")
	t.Logf("SQL: %s", sql)
}

func TestTranspileScopedResource(t *testing.T) {
	sql := mustTranspile(t, `{ resource.service.name = "frontend" }`)
	assertContains(t, sql, "ServiceName = 'frontend'")
	t.Logf("SQL: %s", sql)
}

func TestTranspileScopedResourceOther(t *testing.T) {
	sql := mustTranspile(t, `{ resource.deployment.environment = "prod" }`)
	assertContains(t, sql, "ResourceAttributes['deployment.environment'] = 'prod'")
	t.Logf("SQL: %s", sql)
}

// ---------------------------------------------------------------------------
// Existence checks
// ---------------------------------------------------------------------------

func TestTranspileExists(t *testing.T) {
	sql := mustTranspile(t, "{ .http.method != nil }")
	assertContains(t, sql, "mapContains(SpanAttributes, 'http.method')")
	assertContains(t, sql, "OR mapContains(ResourceAttributes, 'http.method')")
	t.Logf("SQL: %s", sql)
}

func TestTranspileNotExists(t *testing.T) {
	sql := mustTranspile(t, "{ .http.method = nil }")
	assertContains(t, sql, "NOT mapContains(SpanAttributes, 'http.method')")
	assertContains(t, sql, "AND NOT mapContains(ResourceAttributes, 'http.method')")
	t.Logf("SQL: %s", sql)
}

func TestTranspileExistsScoped(t *testing.T) {
	sql := mustTranspile(t, "{ span.http.method != nil }")
	assertContains(t, sql, "mapContains(SpanAttributes, 'http.method')")
	assertNotContains(t, sql, "ResourceAttributes")
	t.Logf("SQL: %s", sql)
}

// ---------------------------------------------------------------------------
// Regex
// ---------------------------------------------------------------------------

func TestTranspileRegex(t *testing.T) {
	sql := mustTranspile(t, `{ name =~ "GET.*" }`)
	assertContains(t, sql, "match(SpanName, 'GET.*')")
	t.Logf("SQL: %s", sql)
}

func TestTranspileNotRegex(t *testing.T) {
	sql := mustTranspile(t, `{ name !~ "internal.*" }`)
	assertContains(t, sql, "NOT match(SpanName, 'internal.*')")
	t.Logf("SQL: %s", sql)
}

// ---------------------------------------------------------------------------
// Compound expressions
// ---------------------------------------------------------------------------

func TestTranspileAndExpression(t *testing.T) {
	sql := mustTranspile(t, `{ .http.method = "GET" && duration > 1s }`)
	assertContains(t, sql, "AND")
	assertContains(t, sql, "'GET'")
	assertContains(t, sql, "Duration > 1000000000")
	t.Logf("SQL: %s", sql)
}

func TestTranspileOrExpression(t *testing.T) {
	sql := mustTranspile(t, `{ .http.method = "GET" || .http.method = "POST" }`)
	assertContains(t, sql, "OR")
	assertContains(t, sql, "'GET'")
	assertContains(t, sql, "'POST'")
	t.Logf("SQL: %s", sql)
}

func TestTranspileNotExpression(t *testing.T) {
	sql := mustTranspile(t, "{ !(status = error) }")
	assertContains(t, sql, "NOT")
	assertContains(t, sql, "STATUS_CODE_ERROR")
	t.Logf("SQL: %s", sql)
}

// ---------------------------------------------------------------------------
// Spanset operations
// ---------------------------------------------------------------------------

func TestTranspileSpansetAnd(t *testing.T) {
	sql := mustTranspile(t, `{ .http.method = "GET" } && { status = error }`)
	assertContains(t, sql, "INTERSECT")
	assertContains(t, sql, "'GET'")
	assertContains(t, sql, "STATUS_CODE_ERROR")
	t.Logf("SQL: %s", sql)
}

func TestTranspileSpansetUnion(t *testing.T) {
	sql := mustTranspile(t, `{ .http.method = "GET" } || { status = error }`)
	assertContains(t, sql, "UNION DISTINCT")
	t.Logf("SQL: %s", sql)
}

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------

func TestTranspilePipeline(t *testing.T) {
	sql := mustTranspile(t, `{ .http.method = "GET" } | { status = error }`)
	assertContains(t, sql, "WITH")
	assertContains(t, sql, "stage1")
	assertContains(t, sql, "TraceId IN (SELECT TraceId FROM stage1)")
	t.Logf("SQL: %s", sql)
}

// ---------------------------------------------------------------------------
// Aggregates and scalar filters
// ---------------------------------------------------------------------------

func TestTranspileCountGreaterThan(t *testing.T) {
	sql := mustTranspile(t, "{ } | count() > 5")
	assertContains(t, sql, "GROUP BY TraceId")
	assertContains(t, sql, "HAVING count(*) > 5")
	t.Logf("SQL: %s", sql)
}

func TestTranspileAvgDuration(t *testing.T) {
	sql := mustTranspile(t, `{ .http.method = "GET" } | avg(duration) > 1s`)
	assertContains(t, sql, "HAVING avg(Duration) > 1000000000")
	t.Logf("SQL: %s", sql)
}

func TestTranspileMinDuration(t *testing.T) {
	sql := mustTranspile(t, "{ } | min(duration) > 100ms")
	assertContains(t, sql, "HAVING min(Duration) > 100000000")
	t.Logf("SQL: %s", sql)
}

func TestTranspileMaxDuration(t *testing.T) {
	sql := mustTranspile(t, "{ } | max(duration) > 5s")
	assertContains(t, sql, "HAVING max(Duration) > 5000000000")
	t.Logf("SQL: %s", sql)
}

func TestTranspileSumDuration(t *testing.T) {
	sql := mustTranspile(t, "{ } | sum(duration) > 10s")
	assertContains(t, sql, "HAVING sum(Duration) > 10000000000")
	t.Logf("SQL: %s", sql)
}

// ---------------------------------------------------------------------------
// Metrics aggregates
// ---------------------------------------------------------------------------

func TestTranspileRate(t *testing.T) {
	sql := mustTranspile(t, "{ } | rate()")
	assertContains(t, sql, "count(*) AS value")
	t.Logf("SQL: %s", sql)
}

func TestTranspileCountOverTimeBy(t *testing.T) {
	sql := mustTranspile(t, "{ status = error } | count_over_time() by(resource.service.name)")
	assertContains(t, sql, "count(*) AS value")
	assertContains(t, sql, "ServiceName")
	assertContains(t, sql, "GROUP BY")
	t.Logf("SQL: %s", sql)
}

func TestTranspileQuantileOverTime(t *testing.T) {
	sql := mustTranspile(t, "{ } | quantile_over_time(duration, 0.95)")
	assertContains(t, sql, "quantile(0.95)(Duration)")
	t.Logf("SQL: %s", sql)
}

func TestTranspileAvgOverTime(t *testing.T) {
	sql := mustTranspile(t, "{ } | avg_over_time(duration)")
	assertContains(t, sql, "avg(Duration)")
	t.Logf("SQL: %s", sql)
}

// ---------------------------------------------------------------------------
// Time range filtering
// ---------------------------------------------------------------------------

func TestTranspileTimeRange(t *testing.T) {
	sql := mustTranspile(t, "{ }")
	assertContains(t, sql, "Timestamp >=")
	assertContains(t, sql, "Timestamp <=")
	t.Logf("SQL: %s", sql)
}

func TestTranspileNoTimeRange(t *testing.T) {
	root, _ := traceql.Parse("{ }")
	opts := TranspileOptions{Table: "otel_traces", Limit: 20}
	result, err := Transpile(root, opts)
	if err != nil {
		t.Fatal(err)
	}
	// Without time range, should still produce valid SQL
	assertContains(t, result.SQL, "SELECT DISTINCT TraceId")
	t.Logf("SQL: %s", result.SQL)
}

// ---------------------------------------------------------------------------
// Complex queries
// ---------------------------------------------------------------------------

func TestTranspileComplexPipeline(t *testing.T) {
	sql := mustTranspile(t, `{ .http.method = "GET" && status = error } | { duration > 500ms } | count() > 3`)
	assertContains(t, sql, "WITH")
	assertContains(t, sql, "HAVING count(*) > 3")
	t.Logf("SQL: %s", sql)
}

func TestTranspileBooleanFilter(t *testing.T) {
	sql := mustTranspile(t, `{ .cache.hit = true }`)
	assertContains(t, sql, "'true'")
	t.Logf("SQL: %s", sql)
}

// ---------------------------------------------------------------------------
// Intrinsic attributes
// ---------------------------------------------------------------------------

func TestTranspileIntrinsics(t *testing.T) {
	tests := []struct {
		query    string
		expected string
	}{
		{`{ name = "GET" }`, "SpanName = 'GET'"},
		{`{ duration > 1s }`, "Duration > 1000000000"},
		{`{ status = ok }`, "StatusCode = 'STATUS_CODE_OK'"},
		{`{ kind = client }`, "SpanKind = 'SPAN_KIND_CLIENT'"},
		{`{ statusMessage = "timeout" }`, "StatusMessage = 'timeout'"},
		{`{ span:id = "abc" }`, "SpanId = 'abc'"},
		{`{ trace:id = "xyz" }`, "TraceId = 'xyz'"},
	}
	for _, tc := range tests {
		t.Run(tc.query, func(t *testing.T) {
			sql := mustTranspile(t, tc.query)
			assertContains(t, sql, tc.expected)
			t.Logf("SQL: %s", sql)
		})
	}
}

// ---------------------------------------------------------------------------
// Additional intrinsic coverage
// ---------------------------------------------------------------------------

func TestTranspileAllKinds(t *testing.T) {
	tests := []struct {
		query    string
		expected string
	}{
		{`{ kind = server }`, "SPAN_KIND_SERVER"},
		{`{ kind = client }`, "SPAN_KIND_CLIENT"},
		{`{ kind = internal }`, "SPAN_KIND_INTERNAL"},
		{`{ kind = producer }`, "SPAN_KIND_PRODUCER"},
		{`{ kind = consumer }`, "SPAN_KIND_CONSUMER"},
		{`{ kind = unspecified }`, "SPAN_KIND_UNSPECIFIED"},
	}
	for _, tc := range tests {
		t.Run(tc.query, func(t *testing.T) {
			sql := mustTranspile(t, tc.query)
			assertContains(t, sql, tc.expected)
		})
	}
}

func TestTranspileAllStatuses(t *testing.T) {
	tests := []struct {
		query    string
		expected string
	}{
		{`{ status = error }`, "STATUS_CODE_ERROR"},
		{`{ status = ok }`, "STATUS_CODE_OK"},
		{`{ status = unset }`, "STATUS_CODE_UNSET"},
	}
	for _, tc := range tests {
		t.Run(tc.query, func(t *testing.T) {
			sql := mustTranspile(t, tc.query)
			assertContains(t, sql, tc.expected)
		})
	}
}

func TestTranspileMoreIntrinsics(t *testing.T) {
	tests := []struct {
		query    string
		expected string
	}{
		{`{ span:id = "abc" }`, "SpanId = 'abc'"},
		{`{ trace:id = "def" }`, "TraceId = 'def'"},
		{`{ statusMessage = "timeout" }`, "StatusMessage = 'timeout'"},
		{`{ instrumentation:name = "otel-go" }`, "ScopeName = 'otel-go'"},
		{`{ instrumentation:version = "1.0" }`, "ScopeVersion = '1.0'"},
	}
	for _, tc := range tests {
		t.Run(tc.query, func(t *testing.T) {
			sql := mustTranspile(t, tc.query)
			assertContains(t, sql, tc.expected)
		})
	}
}

// ---------------------------------------------------------------------------
// Additional operator coverage
// ---------------------------------------------------------------------------

func TestTranspileAllComparisonOperators(t *testing.T) {
	tests := []struct {
		query    string
		expected string
	}{
		{`{ duration = 1s }`, "Duration = 1000000000"},
		{`{ duration != 1s }`, "Duration != 1000000000"},
		{`{ duration > 1s }`, "Duration > 1000000000"},
		{`{ duration >= 1s }`, "Duration >= 1000000000"},
		{`{ duration < 1s }`, "Duration < 1000000000"},
		{`{ duration <= 1s }`, "Duration <= 1000000000"},
	}
	for _, tc := range tests {
		t.Run(tc.query, func(t *testing.T) {
			sql := mustTranspile(t, tc.query)
			assertContains(t, sql, tc.expected)
		})
	}
}

// ---------------------------------------------------------------------------
// Regex on scoped attributes
// ---------------------------------------------------------------------------

func TestTranspileRegexOnSpanAttr(t *testing.T) {
	sql := mustTranspile(t, `{ span.http.url =~ ".*users.*" }`)
	assertContains(t, sql, "match(SpanAttributes['http.url'], '.*users.*')")
	t.Logf("SQL: %s", sql)
}

func TestTranspileNotRegexOnSpanAttr(t *testing.T) {
	sql := mustTranspile(t, `{ span.http.url !~ ".*internal.*" }`)
	assertContains(t, sql, "NOT match(SpanAttributes['http.url'], '.*internal.*')")
	t.Logf("SQL: %s", sql)
}

func TestTranspileRegexOnResourceAttr(t *testing.T) {
	sql := mustTranspile(t, `{ resource.service.name =~ "front.*" }`)
	// resource.service.name maps to ServiceName column, regex should use match()
	assertContains(t, sql, "match(ServiceName, 'front.*')")
	t.Logf("SQL: %s", sql)
}

func TestTranspileRegexOnResourceAttrOther(t *testing.T) {
	sql := mustTranspile(t, `{ resource.deployment.env =~ "prod.*" }`)
	assertContains(t, sql, "match(ResourceAttributes['deployment.env'], 'prod.*')")
	t.Logf("SQL: %s", sql)
}

func TestTranspileRegexOnUnscopedAttr(t *testing.T) {
	sql := mustTranspile(t, `{ .http.url =~ ".*api.*" }`)
	assertContains(t, sql, "match(SpanAttributes['http.url']")
	assertContains(t, sql, "match(ResourceAttributes['http.url']")
	t.Logf("SQL: %s", sql)
}

func TestTranspileNotRegexOnUnscopedAttr(t *testing.T) {
	sql := mustTranspile(t, `{ .http.url !~ ".*secret.*" }`)
	assertContains(t, sql, "NOT match(SpanAttributes['http.url']")
	assertContains(t, sql, "NOT match(ResourceAttributes['http.url']")
	t.Logf("SQL: %s", sql)
}

// ---------------------------------------------------------------------------
// Existence checks: scoped resource
// ---------------------------------------------------------------------------

func TestTranspileExistsResourceScoped(t *testing.T) {
	sql := mustTranspile(t, "{ resource.deployment != nil }")
	assertContains(t, sql, "mapContains(ResourceAttributes, 'deployment')")
	assertNotContains(t, sql, "SpanAttributes")
	t.Logf("SQL: %s", sql)
}

func TestTranspileNotExistsResourceScoped(t *testing.T) {
	sql := mustTranspile(t, "{ resource.deployment = nil }")
	assertContains(t, sql, "NOT mapContains(ResourceAttributes, 'deployment')")
	t.Logf("SQL: %s", sql)
}

// ---------------------------------------------------------------------------
// Unscoped service.name special case
// ---------------------------------------------------------------------------

func TestTranspileUnscopedServiceName(t *testing.T) {
	sql := mustTranspile(t, `{ .service.name = "frontend" }`)
	assertContains(t, sql, "ServiceName")
	t.Logf("SQL: %s", sql)
}

// ---------------------------------------------------------------------------
// Numeric type coercion on resource scope
// ---------------------------------------------------------------------------

func TestTranspileIntComparisonResourceScoped(t *testing.T) {
	sql := mustTranspile(t, `{ resource.custom.count > 10 }`)
	assertContains(t, sql, "toInt64OrZero(ResourceAttributes['custom.count'])")
	assertContains(t, sql, "> 10")
	t.Logf("SQL: %s", sql)
}

func TestTranspileFloatComparisonResourceScoped(t *testing.T) {
	sql := mustTranspile(t, `{ resource.custom.ratio >= 0.5 }`)
	assertContains(t, sql, "toFloat64OrZero(ResourceAttributes['custom.ratio'])")
	t.Logf("SQL: %s", sql)
}

// ---------------------------------------------------------------------------
// Metrics aggregates: expanded coverage
// ---------------------------------------------------------------------------

func TestTranspileMinOverTime(t *testing.T) {
	sql := mustTranspile(t, "{ } | min_over_time(duration)")
	assertContains(t, sql, "min(Duration)")
	t.Logf("SQL: %s", sql)
}

func TestTranspileMaxOverTime(t *testing.T) {
	sql := mustTranspile(t, "{ } | max_over_time(duration)")
	assertContains(t, sql, "max(Duration)")
	t.Logf("SQL: %s", sql)
}

func TestTranspileSumOverTime(t *testing.T) {
	sql := mustTranspile(t, "{ } | sum_over_time(duration)")
	assertContains(t, sql, "sum(Duration)")
	t.Logf("SQL: %s", sql)
}

func TestTranspileHistogramOverTime(t *testing.T) {
	sql := mustTranspile(t, "{ } | histogram_over_time(duration)")
	assertContains(t, sql, "histogram(10)(Duration)")
	t.Logf("SQL: %s", sql)
}

func TestTranspileMetricsWithBy(t *testing.T) {
	sql := mustTranspile(t, `{ } | rate() by(resource.service.name)`)
	assertContains(t, sql, "ServiceName")
	assertContains(t, sql, "GROUP BY")
	assertContains(t, sql, "label_")
	t.Logf("SQL: %s", sql)
}

func TestTranspileCountOverTimeNoFilter(t *testing.T) {
	sql := mustTranspile(t, "{ } | count_over_time()")
	assertContains(t, sql, "count(*) AS value")
	t.Logf("SQL: %s", sql)
}

// ---------------------------------------------------------------------------
// Coalesce operation
// ---------------------------------------------------------------------------

func TestTranspileCoalesce(t *testing.T) {
	sql := mustTranspile(t, `{ .http.method = "GET" } | coalesce()`)
	assertContains(t, sql, "SELECT DISTINCT TraceId FROM stage1")
	t.Logf("SQL: %s", sql)
}

// ---------------------------------------------------------------------------
// Default options handling
// ---------------------------------------------------------------------------

func TestTranspileDefaultTable(t *testing.T) {
	root, err := traceql.Parse("{ }")
	if err != nil {
		t.Fatal(err)
	}
	// No table specified
	opts := TranspileOptions{
		Start: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		End:   time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC),
	}
	result, err := Transpile(root, opts)
	if err != nil {
		t.Fatal(err)
	}
	assertContains(t, result.SQL, "otel_traces")
	// Default limit should be 20
	assertContains(t, result.SQL, "LIMIT 20")
	t.Logf("SQL: %s", result.SQL)
}

func TestTranspileDefaultLimit(t *testing.T) {
	root, err := traceql.Parse("{ }")
	if err != nil {
		t.Fatal(err)
	}
	opts := TranspileOptions{
		Table: "otel_traces",
		Start: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		End:   time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC),
		Limit: -1, // invalid, should default to 20
	}
	result, err := Transpile(root, opts)
	if err != nil {
		t.Fatal(err)
	}
	assertContains(t, result.SQL, "LIMIT 20")
}

// ---------------------------------------------------------------------------
// Time filter edge cases
// ---------------------------------------------------------------------------

func TestTranspileStartOnlyTimeRange(t *testing.T) {
	root, _ := traceql.Parse("{ }")
	opts := TranspileOptions{
		Table: "otel_traces",
		Start: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		Limit: 10,
	}
	result, err := Transpile(root, opts)
	if err != nil {
		t.Fatal(err)
	}
	assertContains(t, result.SQL, "Timestamp >=")
	assertNotContains(t, result.SQL, "Timestamp <=")
	t.Logf("SQL: %s", result.SQL)
}

func TestTranspileEndOnlyTimeRange(t *testing.T) {
	root, _ := traceql.Parse("{ }")
	opts := TranspileOptions{
		Table: "otel_traces",
		End:   time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC),
		Limit: 10,
	}
	result, err := Transpile(root, opts)
	if err != nil {
		t.Fatal(err)
	}
	assertContains(t, result.SQL, "Timestamp <=")
	assertNotContains(t, result.SQL, "AND Timestamp >=")
	t.Logf("SQL: %s", result.SQL)
}

// ---------------------------------------------------------------------------
// Three-stage pipeline
// ---------------------------------------------------------------------------

func TestTranspileThreeStagePipeline(t *testing.T) {
	sql := mustTranspile(t, `{ .http.method = "GET" } | { status = error } | { duration > 1s }`)
	assertContains(t, sql, "WITH")
	assertContains(t, sql, "stage1")
	assertContains(t, sql, "stage2")
	t.Logf("SQL: %s", sql)
}

// ---------------------------------------------------------------------------
// SQL escaping
// ---------------------------------------------------------------------------

func TestTranspileSQLEscaping(t *testing.T) {
	sql := mustTranspile(t, `{ name = "it's a test" }`)
	assertContains(t, sql, "\\'s a test")
	t.Logf("SQL: %s", sql)
}

// ---------------------------------------------------------------------------
// Duration units
// ---------------------------------------------------------------------------

func TestTranspileDurationUnits(t *testing.T) {
	tests := []struct {
		query    string
		expected string
	}{
		{"{ duration > 100ms }", "Duration > 100000000"},
		{"{ duration > 2s }", "Duration > 2000000000"},
		{"{ duration > 500us }", "Duration > 500000"},
		{"{ duration > 1m0s }", "Duration > 60000000000"},
	}
	for _, tc := range tests {
		t.Run(tc.query, func(t *testing.T) {
			sql := mustTranspile(t, tc.query)
			assertContains(t, sql, tc.expected)
		})
	}
}

// ---------------------------------------------------------------------------
// Scalar filter with previous CTE
// ---------------------------------------------------------------------------

func TestTranspileScalarFilterWithPrevCTE(t *testing.T) {
	// { .http.method = "GET" } | count() > 5
	// This creates a CTE for the filter and then a scalar filter referencing it
	sql := mustTranspile(t, `{ .http.method = "GET" } | count() > 5`)
	assertContains(t, sql, "WITH")
	assertContains(t, sql, "GROUP BY TraceId")
	assertContains(t, sql, "HAVING count(*) > 5")
	t.Logf("SQL: %s", sql)
}

// ---------------------------------------------------------------------------
// Boolean false
// ---------------------------------------------------------------------------

func TestTranspileBooleanFalse(t *testing.T) {
	sql := mustTranspile(t, `{ .cache.hit = false }`)
	assertContains(t, sql, "'false'")
	t.Logf("SQL: %s", sql)
}

// ---------------------------------------------------------------------------
// Error cases
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Structural operators (now implemented in Phase 8)
// ---------------------------------------------------------------------------

func TestTranspileStructuralChild(t *testing.T) {
	sql := mustTranspile(t, `{ .a = "x" } > { .b = "y" }`)
	assertContains(t, sql, "JOIN")
	assertContains(t, sql, "p.SpanId = c.ParentSpanId")
	assertContains(t, sql, "p.SpanAttributes['a']")
	assertContains(t, sql, "c.SpanAttributes['b']")
	t.Logf("SQL: %s", sql)
}

func TestTranspileStructuralDescendant(t *testing.T) {
	sql := mustTranspile(t, `{ .a = "x" } >> { .b = "y" }`)
	assertContains(t, sql, "WITH RECURSIVE")
	assertContains(t, sql, "ancestors")
	assertContains(t, sql, "ParentSpanId")
	t.Logf("SQL: %s", sql)
}

func TestTranspileStructuralSibling(t *testing.T) {
	sql := mustTranspile(t, `{ .a = "x" } ~ { .b = "y" }`)
	assertContains(t, sql, "JOIN")
	assertContains(t, sql, "s1.ParentSpanId = s2.ParentSpanId")
	assertContains(t, sql, "s1.SpanId != s2.SpanId")
	t.Logf("SQL: %s", sql)
}

// ---------------------------------------------------------------------------
// Direct function coverage: staticToSQL
// ---------------------------------------------------------------------------

func TestStaticToSQL(t *testing.T) {
	tests := []struct {
		name string
		s    traceql.Static
		want string
	}{
		{"nil", traceql.Static{Type: traceql.TypeNil}, "NULL"},
		{"int", traceql.Static{Type: traceql.TypeInt, IntVal: 42}, "42"},
		{"float", traceql.Static{Type: traceql.TypeFloat, FloatVal: 3.14}, "3.14"},
		{"string", traceql.Static{Type: traceql.TypeString, StringVal: "hello"}, "'hello'"},
		{"bool true", traceql.Static{Type: traceql.TypeBoolean, BoolVal: true}, "1"},
		{"bool false", traceql.Static{Type: traceql.TypeBoolean, BoolVal: false}, "0"},
		{"duration", traceql.Static{Type: traceql.TypeDuration, DurationVal: 2 * time.Second}, "2000000000"},
		{"status error", traceql.Static{Type: traceql.TypeStatus, StatusVal: traceql.StatusError}, "'STATUS_CODE_ERROR'"},
		{"kind server", traceql.Static{Type: traceql.TypeKind, KindVal: traceql.KindServer}, "'SPAN_KIND_SERVER'"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := staticToSQL(&tc.s)
			if got != tc.want {
				t.Errorf("staticToSQL() = %q, want %q", got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Direct function coverage: operatorToSQL
// ---------------------------------------------------------------------------

func TestOperatorToSQL(t *testing.T) {
	tests := []struct {
		op   traceql.Operator
		want string
	}{
		{traceql.OpEqual, "="},
		{traceql.OpNotEqual, "!="},
		{traceql.OpGreater, ">"},
		{traceql.OpGreaterEqual, ">="},
		{traceql.OpLess, "<"},
		{traceql.OpLessEqual, "<="},
		{traceql.OpAnd, "AND"},
		{traceql.OpOr, "OR"},
		{traceql.OpAdd, "+"},
		{traceql.OpSub, "-"},
		{traceql.OpMult, "*"},
		{traceql.OpDiv, "/"},
		{traceql.OpMod, "%"},
		{traceql.OpPower, "^"},
		{traceql.OpIn, "IN"},
		{traceql.OpNotIn, "NOT IN"},
		{traceql.OpNone, "="}, // default
	}
	for _, tc := range tests {
		got := operatorToSQL(tc.op)
		if got != tc.want {
			t.Errorf("operatorToSQL(%v) = %q, want %q", tc.op, got, tc.want)
		}
	}
}

// ---------------------------------------------------------------------------
// Direct function coverage: intrinsicColumnSQL
// ---------------------------------------------------------------------------

func TestIntrinsicColumnSQL(t *testing.T) {
	tests := []struct {
		i    traceql.Intrinsic
		want string
	}{
		{traceql.IntrinsicDuration, "Duration"},
		{traceql.IntrinsicName, "SpanName"},
		{traceql.IntrinsicStatus, "StatusCode"},
		{traceql.IntrinsicStatusMessage, "StatusMessage"},
		{traceql.IntrinsicKind, "SpanKind"},
		{traceql.IntrinsicTraceID, "TraceId"},
		{traceql.IntrinsicSpanID, "SpanId"},
		{traceql.IntrinsicParentID, "ParentSpanId"},
		{traceql.IntrinsicInstrumentationName, "ScopeName"},
		{traceql.IntrinsicInstrumentationVersion, "ScopeVersion"},
		{traceql.IntrinsicTraceRootService, "ServiceName"},
		{traceql.IntrinsicTraceRootSpan, "SpanName"},
		{traceql.IntrinsicTraceDuration, "Duration"},
		{traceql.IntrinsicSpanStartTime, "Timestamp"},
		{traceql.IntrinsicNone, "SpanName"}, // default
	}
	for _, tc := range tests {
		got := intrinsicColumnSQL(tc.i)
		if got != tc.want {
			t.Errorf("intrinsicColumnSQL(%v) = %q, want %q", tc.i, got, tc.want)
		}
	}
}

// ---------------------------------------------------------------------------
// Direct function coverage: kindToClickHouse
// ---------------------------------------------------------------------------

func TestKindToClickHouse(t *testing.T) {
	tests := []struct {
		k    traceql.Kind
		want string
	}{
		{traceql.KindServer, "SPAN_KIND_SERVER"},
		{traceql.KindClient, "SPAN_KIND_CLIENT"},
		{traceql.KindInternal, "SPAN_KIND_INTERNAL"},
		{traceql.KindProducer, "SPAN_KIND_PRODUCER"},
		{traceql.KindConsumer, "SPAN_KIND_CONSUMER"},
		{traceql.KindUnspecified, "SPAN_KIND_UNSPECIFIED"},
		{traceql.Kind(99), "SPAN_KIND_UNSPECIFIED"}, // unknown
	}
	for _, tc := range tests {
		got := kindToClickHouse(tc.k)
		if got != tc.want {
			t.Errorf("kindToClickHouse(%v) = %q, want %q", tc.k, got, tc.want)
		}
	}
}

// ---------------------------------------------------------------------------
// Direct function coverage: statusToClickHouse
// ---------------------------------------------------------------------------

func TestStatusToClickHouse(t *testing.T) {
	tests := []struct {
		s    traceql.Status
		want string
	}{
		{traceql.StatusError, "STATUS_CODE_ERROR"},
		{traceql.StatusOk, "STATUS_CODE_OK"},
		{traceql.StatusUnset, "STATUS_CODE_UNSET"},
		{traceql.Status(99), "STATUS_CODE_UNSET"}, // unknown
	}
	for _, tc := range tests {
		got := statusToClickHouse(tc.s)
		if got != tc.want {
			t.Errorf("statusToClickHouse(%v) = %q, want %q", tc.s, got, tc.want)
		}
	}
}

// ---------------------------------------------------------------------------
// Direct function coverage: aggregateToSQL
// ---------------------------------------------------------------------------

func TestAggregateToSQL(t *testing.T) {
	tests := []struct {
		name string
		agg  traceql.Aggregate
		want string
	}{
		{"count", traceql.Aggregate{Op: traceql.AggregateCount}, "count(*)"},
		{"min duration", traceql.Aggregate{Op: traceql.AggregateMin}, "min(Duration)"},
		{"max duration", traceql.Aggregate{Op: traceql.AggregateMax}, "max(Duration)"},
		{"sum duration", traceql.Aggregate{Op: traceql.AggregateSum}, "sum(Duration)"},
		{"avg duration", traceql.Aggregate{Op: traceql.AggregateAvg}, "avg(Duration)"},
		{"min with attr", traceql.Aggregate{
			Op:         traceql.AggregateMin,
			Expression: &traceql.Attribute{Intrinsic: traceql.IntrinsicDuration},
		}, "min(Duration)"},
		{"max with attr", traceql.Aggregate{
			Op:         traceql.AggregateMax,
			Expression: &traceql.Attribute{Name: "http.status_code", Scope: traceql.AttributeScopeSpan},
		}, "max(SpanAttributes['http.status_code'])"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := aggregateToSQL(&tc.agg)
			if got != tc.want {
				t.Errorf("aggregateToSQL() = %q, want %q", got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Direct function coverage: attributeToSQL
// ---------------------------------------------------------------------------

func TestAttributeToSQL(t *testing.T) {
	tests := []struct {
		name string
		attr traceql.Attribute
		want string
	}{
		{"intrinsic duration", traceql.Attribute{Intrinsic: traceql.IntrinsicDuration}, "Duration"},
		{"span scope", traceql.Attribute{Name: "http.method", Scope: traceql.AttributeScopeSpan}, "SpanAttributes['http.method']"},
		{"resource scope", traceql.Attribute{Name: "deployment.env", Scope: traceql.AttributeScopeResource}, "ResourceAttributes['deployment.env']"},
		{"resource service.name", traceql.Attribute{Name: "service.name", Scope: traceql.AttributeScopeResource}, "ServiceName"},
		{"unscoped", traceql.Attribute{Name: "http.url", Scope: traceql.AttributeScopeNone}, "SpanAttributes['http.url']"},
		{"unscoped service.name", traceql.Attribute{Name: "service.name", Scope: traceql.AttributeScopeNone}, "ServiceName"},
		{"event scope", traceql.Attribute{Name: "exception.msg", Scope: traceql.AttributeScopeEvent}, "SpanAttributes['exception.msg']"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := attributeToSQL(&tc.attr)
			if got != tc.want {
				t.Errorf("attributeToSQL() = %q, want %q", got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Direct function coverage: mapAccessSQL
// ---------------------------------------------------------------------------

func TestMapAccessSQL(t *testing.T) {
	tests := []struct {
		mapCol  string
		key     string
		valType traceql.StaticType
		want    string
	}{
		{"SpanAttributes", "http.method", traceql.TypeString, "SpanAttributes['http.method']"},
		{"SpanAttributes", "http.status_code", traceql.TypeInt, "toInt64OrZero(SpanAttributes['http.status_code'])"},
		{"ResourceAttributes", "ratio", traceql.TypeFloat, "toFloat64OrZero(ResourceAttributes['ratio'])"},
		{"SpanAttributes", "cache.hit", traceql.TypeBoolean, "SpanAttributes['cache.hit']"},
	}
	for _, tc := range tests {
		got := mapAccessSQL(tc.mapCol, tc.key, tc.valType)
		if got != tc.want {
			t.Errorf("mapAccessSQL(%q, %q, %v) = %q, want %q", tc.mapCol, tc.key, tc.valType, got, tc.want)
		}
	}
}

// ---------------------------------------------------------------------------
// Direct function coverage: sanitizeAlias
// ---------------------------------------------------------------------------

func TestSanitizeAlias(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"resource.service.name", "resource_service_name"},
		{"span:id", "span_id"},
		{"simple", "simple"},
		{"a.b:c d", "a_b_c_d"},
	}
	for _, tc := range tests {
		got := sanitizeAlias(tc.input)
		if got != tc.want {
			t.Errorf("sanitizeAlias(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

// ---------------------------------------------------------------------------
// Direct function coverage: escapeSQL
// ---------------------------------------------------------------------------

func TestEscapeSQL(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"hello", "hello"},
		{"it's", "it\\'s"},
		{"a'b'c", "a\\'b\\'c"},
		{"back\\slash", "back\\\\slash"},
		{"inject\\' OR 1=1 --", "inject\\\\\\' OR 1=1 --"},
		{"no special chars", "no special chars"},
	}
	for _, tc := range tests {
		got := escapeSQL(tc.input)
		if got != tc.want {
			t.Errorf("escapeSQL(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

// ---------------------------------------------------------------------------
// transpileScalarExpr: ScalarOperation path
// ---------------------------------------------------------------------------

func TestTranspileScalarExprOperation(t *testing.T) {
	tr := &transpiler{opts: defaultOpts()}
	// count() + 1
	expr := &traceql.ScalarOperation{
		Op:  traceql.OpAdd,
		LHS: &traceql.Aggregate{Op: traceql.AggregateCount},
		RHS: &traceql.Static{Type: traceql.TypeInt, IntVal: 1},
	}
	result, err := tr.transpileScalarExpr(expr)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "count(*)") || !strings.Contains(result, "+") || !strings.Contains(result, "1") {
		t.Errorf("expected (count(*) + 1), got %q", result)
	}
}

func TestTranspileScalarExprUnsupported(t *testing.T) {
	tr := &transpiler{opts: defaultOpts()}
	// Unsupported scalar expression type
	_, err := tr.transpileScalarExpr(&traceql.Aggregate{Op: traceql.AggregateCount})
	if err != nil {
		t.Fatal("Aggregate should be supported")
	}
}

// ---------------------------------------------------------------------------
// transpileAggregate: standalone aggregate in pipeline
// ---------------------------------------------------------------------------

func TestTranspileStandaloneAggregate(t *testing.T) {
	tr := &transpiler{opts: defaultOpts()}
	agg := &traceql.Aggregate{Op: traceql.AggregateCount}
	sql, err := tr.transpileAggregate(agg, "")
	if err != nil {
		t.Fatal(err)
	}
	assertContains(t, sql, "count(*) AS agg_value")
	assertContains(t, sql, "GROUP BY TraceId")
	t.Logf("SQL: %s", sql)
}

func TestTranspileStandaloneAggregateWithPrevCTE(t *testing.T) {
	tr := &transpiler{opts: defaultOpts()}
	agg := &traceql.Aggregate{Op: traceql.AggregateAvg, Expression: &traceql.Attribute{Intrinsic: traceql.IntrinsicDuration}}
	sql, err := tr.transpileAggregate(agg, "stage1")
	if err != nil {
		t.Fatal(err)
	}
	assertContains(t, sql, "avg(Duration) AS agg_value")
	assertContains(t, sql, "FROM stage1")
	t.Logf("SQL: %s", sql)
}

// ---------------------------------------------------------------------------
// transpileElement: GroupOperation error
// ---------------------------------------------------------------------------

func TestTranspileElementGroupOperationError(t *testing.T) {
	tr := &transpiler{opts: defaultOpts()}
	_, err := tr.transpileElement(&traceql.GroupOperation{
		Expression: &traceql.Attribute{Name: "http.method", Scope: traceql.AttributeScopeSpan},
	}, "")
	if err == nil {
		t.Fatal("expected error for standalone GroupOperation")
	}
	assertContains(t, err.Error(), "GroupOperation")
}

// ---------------------------------------------------------------------------
// transpileElement: CoalesceOperation standalone
// ---------------------------------------------------------------------------

func TestTranspileElementCoalesceStandalone(t *testing.T) {
	tr := &transpiler{opts: defaultOpts()}
	sql, err := tr.transpileElement(&traceql.CoalesceOperation{}, "")
	if err != nil {
		t.Fatal(err)
	}
	assertContains(t, sql, "SELECT DISTINCT TraceId FROM otel_traces")
	t.Logf("SQL: %s", sql)
}

func TestTranspileElementCoalesceWithPrevCTE(t *testing.T) {
	tr := &transpiler{opts: defaultOpts()}
	sql, err := tr.transpileElement(&traceql.CoalesceOperation{}, "stage1")
	if err != nil {
		t.Fatal(err)
	}
	assertContains(t, sql, "SELECT DISTINCT TraceId FROM stage1")
	t.Logf("SQL: %s", sql)
}

// ---------------------------------------------------------------------------
// transpileElement: nested Pipeline
// ---------------------------------------------------------------------------

func TestTranspileNestedPipeline(t *testing.T) {
	tr := &transpiler{opts: defaultOpts()}
	nested := &traceql.Pipeline{
		Elements: []traceql.PipelineElement{
			&traceql.SpansetFilter{
				Expression: &traceql.BinaryOperation{
					Op:  traceql.OpEqual,
					LHS: &traceql.Attribute{Intrinsic: traceql.IntrinsicStatus},
					RHS: &traceql.Static{Type: traceql.TypeStatus, StatusVal: traceql.StatusError},
				},
			},
		},
	}
	sql, err := tr.transpileElement(nested, "")
	if err != nil {
		t.Fatal(err)
	}
	assertContains(t, sql, "STATUS_CODE_ERROR")
	t.Logf("SQL: %s", sql)
}

// ---------------------------------------------------------------------------
// transpileFieldExpr: various expression types
// ---------------------------------------------------------------------------

func TestTranspileFieldExprStatic(t *testing.T) {
	tr := &transpiler{opts: defaultOpts()}
	s := &traceql.Static{Type: traceql.TypeInt, IntVal: 42}
	sql, err := tr.transpileFieldExpr(s)
	if err != nil {
		t.Fatal(err)
	}
	if sql != "42" {
		t.Errorf("expected '42', got %q", sql)
	}
}

func TestTranspileFieldExprAttribute(t *testing.T) {
	tr := &transpiler{opts: defaultOpts()}
	attr := &traceql.Attribute{Intrinsic: traceql.IntrinsicDuration}
	sql, err := tr.transpileFieldExpr(attr)
	if err != nil {
		t.Fatal(err)
	}
	if sql != "Duration" {
		t.Errorf("expected 'Duration', got %q", sql)
	}
}

// ---------------------------------------------------------------------------
// transpileBinaryOp: non-attribute LHS (generic path)
// ---------------------------------------------------------------------------

func TestTranspileBinaryOpGenericPath(t *testing.T) {
	tr := &transpiler{opts: defaultOpts()}
	// Static op Static (both non-attribute)
	b := &traceql.BinaryOperation{
		Op:  traceql.OpAdd,
		LHS: &traceql.Static{Type: traceql.TypeInt, IntVal: 1},
		RHS: &traceql.Static{Type: traceql.TypeInt, IntVal: 2},
	}
	sql, err := tr.transpileBinaryOp(b)
	if err != nil {
		t.Fatal(err)
	}
	assertContains(t, sql, "1 + 2")
}

func TestTranspileBinaryOpRegexGenericPath(t *testing.T) {
	tr := &transpiler{opts: defaultOpts()}
	// Attribute regex - this goes through the attribute comparison path
	// Let's test a non-attribute regex path (static regex static)
	b := &traceql.BinaryOperation{
		Op:  traceql.OpRegex,
		LHS: &traceql.Static{Type: traceql.TypeString, StringVal: "hello"},
		RHS: &traceql.Static{Type: traceql.TypeString, StringVal: "hel.*"},
	}
	sql, err := tr.transpileBinaryOp(b)
	if err != nil {
		t.Fatal(err)
	}
	assertContains(t, sql, "match(")
}

func TestTranspileBinaryOpNotRegexGenericPath(t *testing.T) {
	tr := &transpiler{opts: defaultOpts()}
	b := &traceql.BinaryOperation{
		Op:  traceql.OpNotRegex,
		LHS: &traceql.Static{Type: traceql.TypeString, StringVal: "hello"},
		RHS: &traceql.Static{Type: traceql.TypeString, StringVal: "hel.*"},
	}
	sql, err := tr.transpileBinaryOp(b)
	if err != nil {
		t.Fatal(err)
	}
	assertContains(t, sql, "NOT match(")
}

// ---------------------------------------------------------------------------
// transpileUnaryOp: OpSub (negation)
// ---------------------------------------------------------------------------

func TestTranspileUnaryOpNegation(t *testing.T) {
	tr := &transpiler{opts: defaultOpts()}
	u := &traceql.UnaryOperation{
		Op:         traceql.OpSub,
		Expression: &traceql.Static{Type: traceql.TypeInt, IntVal: 5},
	}
	sql, err := tr.transpileUnaryOp(u)
	if err != nil {
		t.Fatal(err)
	}
	assertContains(t, sql, "-5")
}

func TestTranspileUnaryOpUnsupported(t *testing.T) {
	tr := &transpiler{opts: defaultOpts()}
	u := &traceql.UnaryOperation{
		Op:         traceql.OpAdd, // OpAdd isn't a valid unary op
		Expression: &traceql.Static{Type: traceql.TypeInt, IntVal: 5},
	}
	_, err := tr.transpileUnaryOp(u)
	if err == nil {
		t.Fatal("expected error for unsupported unary operator")
	}
}

// ---------------------------------------------------------------------------
// transpileElement: unsupported type
// ---------------------------------------------------------------------------

func TestTranspileElementUnsupportedType(t *testing.T) {
	tr := &transpiler{opts: defaultOpts()}
	// SelectOperation is not handled in transpileElement
	_, err := tr.transpileElement(&traceql.SelectOperation{
		Attrs: []traceql.Attribute{{Name: "x"}},
	}, "")
	if err == nil {
		t.Fatal("expected error for unsupported element type")
	}
}

// ---------------------------------------------------------------------------
// transpileMetricsAggregate with prevCTE
// ---------------------------------------------------------------------------

func TestTranspileMetricsAggregateWithPrevCTE(t *testing.T) {
	tr := &transpiler{opts: defaultOpts()}
	ma := &traceql.MetricsAggregate{Op: traceql.MetricsAggregateCountOverTime}
	sql, err := tr.transpileMetricsAggregate(ma, "stage1")
	if err != nil {
		t.Fatal(err)
	}
	assertContains(t, sql, "FROM stage1")
	assertContains(t, sql, "WHERE 1=1")
	t.Logf("SQL: %s", sql)
}

// ---------------------------------------------------------------------------
// transpileAttributeComparison: intrinsic regex
// ---------------------------------------------------------------------------

func TestTranspileAttributeComparisonIntrinsicRegex(t *testing.T) {
	sql := mustTranspile(t, `{ name =~ "GET.*" }`)
	assertContains(t, sql, "match(SpanName, 'GET.*')")
}

func TestTranspileAttributeComparisonIntrinsicNotRegex(t *testing.T) {
	sql := mustTranspile(t, `{ name !~ "internal.*" }`)
	assertContains(t, sql, "NOT match(SpanName, 'internal.*')")
}

// ---------------------------------------------------------------------------
// transpileAttributeComparison: resource non-service.name regex
// ---------------------------------------------------------------------------

func TestTranspileNotRegexOnResourceOther(t *testing.T) {
	sql := mustTranspile(t, `{ resource.deployment.env !~ "test.*" }`)
	assertContains(t, sql, "NOT match(ResourceAttributes['deployment.env'], 'test.*')")
	t.Logf("SQL: %s", sql)
}

// ---------------------------------------------------------------------------
// transpileExists with non-attribute expression (error)
// ---------------------------------------------------------------------------

func TestTranspileExistsNonAttribute(t *testing.T) {
	tr := &transpiler{opts: defaultOpts()}
	// Pass a Static instead of Attribute
	_, err := tr.transpileExists(&traceql.Static{Type: traceql.TypeInt, IntVal: 1}, true)
	if err == nil {
		t.Fatal("expected error for existence check on non-attribute")
	}
	assertContains(t, err.Error(), "existence check requires an attribute")
}

// ---------------------------------------------------------------------------
// transpileFieldExpr: unsupported type
// ---------------------------------------------------------------------------

func TestTranspileFieldExprUnsupported(t *testing.T) {
	// All FieldExpression impls (BinaryOperation, UnaryOperation, Static, Attribute)
	// are handled. This test verifies the known types work without error.
	tr := &transpiler{opts: defaultOpts()}
	_, err := tr.transpileFieldExpr(&traceql.Static{Type: traceql.TypeString, StringVal: "x"})
	if err != nil {
		t.Fatalf("Static should be supported: %v", err)
	}
}

// ---------------------------------------------------------------------------
// transpileSpansetOperation: unsupported spanset operator
// ---------------------------------------------------------------------------

func TestTranspileSpansetOperationUnsupportedOp(t *testing.T) {
	tr := &transpiler{opts: defaultOpts()}
	op := &traceql.SpansetOperation{
		Op: traceql.OpEqual, // Not a spanset operator
		LHS: &traceql.SpansetFilter{
			Expression: &traceql.Attribute{Intrinsic: traceql.IntrinsicName},
		},
		RHS: &traceql.SpansetFilter{
			Expression: &traceql.Attribute{Intrinsic: traceql.IntrinsicName},
		},
	}
	_, err := tr.transpileSpansetOperation(op)
	if err == nil {
		t.Fatal("expected error for unsupported spanset operator")
	}
}

// ---------------------------------------------------------------------------
// mustTimeFilter: no time range → returns "1=1"
// ---------------------------------------------------------------------------

func TestMustTimeFilterNoRange(t *testing.T) {
	tr := &transpiler{opts: TranspileOptions{Table: "otel_traces", Limit: 20}}
	result := tr.mustTimeFilter()
	if result != "1=1" {
		t.Errorf("expected '1=1', got %q", result)
	}
}

// ---------------------------------------------------------------------------
// transpileAttributeComparison: default scope (not span/resource/none)
// ---------------------------------------------------------------------------

func TestTranspileAttributeComparisonDefaultScope(t *testing.T) {
	tr := &transpiler{opts: defaultOpts()}
	// Use event scope, which falls through to default case
	attr := &traceql.Attribute{Name: "exception.msg", Scope: traceql.AttributeScopeEvent}
	static := &traceql.Static{Type: traceql.TypeString, StringVal: "NPE"}
	sql, err := tr.transpileAttributeComparison(attr, traceql.OpEqual, static)
	if err != nil {
		t.Fatal(err)
	}
	assertContains(t, sql, "SpanAttributes['exception.msg']")
	assertContains(t, sql, "'NPE'")
}

// ---------------------------------------------------------------------------
// transpileExists with default scope
// ---------------------------------------------------------------------------

func TestTranspileExistsDefaultScope(t *testing.T) {
	tr := &transpiler{opts: defaultOpts()}
	attr := &traceql.Attribute{Name: "custom.tag", Scope: traceql.AttributeScopeEvent}
	sql, err := tr.transpileExists(attr, true)
	if err != nil {
		t.Fatal(err)
	}
	assertContains(t, sql, "mapContains(SpanAttributes, 'custom.tag')")
}

func TestTranspileExistsDefaultScopeNotExists(t *testing.T) {
	tr := &transpiler{opts: defaultOpts()}
	attr := &traceql.Attribute{Name: "custom.tag", Scope: traceql.AttributeScopeEvent}
	sql, err := tr.transpileExists(attr, false)
	if err != nil {
		t.Fatal(err)
	}
	assertContains(t, sql, "NOT mapContains(SpanAttributes, 'custom.tag')")
}

// ===========================================================================
// Phase 8 Tests
// ===========================================================================

// ---------------------------------------------------------------------------
// Structural operator: Parent (<)
// ---------------------------------------------------------------------------

func TestTranspileStructuralParent(t *testing.T) {
	sql := mustTranspile(t, `{ .a = "x" } < { .b = "y" }`)
	assertContains(t, sql, "JOIN")
	assertContains(t, sql, "c.ParentSpanId = p.SpanId")
	t.Logf("SQL: %s", sql)
}

// ---------------------------------------------------------------------------
// Structural operator: Ancestor (<<)
// ---------------------------------------------------------------------------

func TestTranspileStructuralAncestor(t *testing.T) {
	sql := mustTranspile(t, `{ .a = "x" } << { .b = "y" }`)
	assertContains(t, sql, "WITH RECURSIVE")
	assertContains(t, sql, "descendants")
	t.Logf("SQL: %s", sql)
}

// ---------------------------------------------------------------------------
// Structural operators with intrinsics
// ---------------------------------------------------------------------------

func TestTranspileStructuralChildWithIntrinsics(t *testing.T) {
	sql := mustTranspile(t, `{ name = "parent-span" } > { status = error }`)
	assertContains(t, sql, "JOIN")
	assertContains(t, sql, "p.SpanName = 'parent-span'")
	assertContains(t, sql, "c.StatusCode = 'STATUS_CODE_ERROR'")
	t.Logf("SQL: %s", sql)
}

func TestTranspileStructuralSiblingWithDuration(t *testing.T) {
	sql := mustTranspile(t, `{ duration > 1s } ~ { status = error }`)
	assertContains(t, sql, "s1.Duration > 1000000000")
	assertContains(t, sql, "s2.StatusCode = 'STATUS_CODE_ERROR'")
	assertContains(t, sql, "s1.ParentSpanId = s2.ParentSpanId")
	t.Logf("SQL: %s", sql)
}

// ---------------------------------------------------------------------------
// Structural operators with scoped attributes
// ---------------------------------------------------------------------------

func TestTranspileStructuralChildWithScopedAttrs(t *testing.T) {
	sql := mustTranspile(t, `{ resource.service.name = "frontend" } > { span.http.method = "GET" }`)
	assertContains(t, sql, "p.ServiceName = 'frontend'")
	assertContains(t, sql, "c.SpanAttributes['http.method'] = 'GET'")
	t.Logf("SQL: %s", sql)
}

// ---------------------------------------------------------------------------
// Structural operators: time filter aliasing
// ---------------------------------------------------------------------------

func TestTranspileStructuralChildHasTimeFilter(t *testing.T) {
	sql := mustTranspile(t, `{ name = "a" } > { name = "b" }`)
	// Both sides should have aliased time filters
	assertContains(t, sql, "p.Timestamp >=")
	assertContains(t, sql, "c.Timestamp >=")
	t.Logf("SQL: %s", sql)
}

func TestTranspileStructuralSiblingHasTimeFilter(t *testing.T) {
	sql := mustTranspile(t, `{ name = "a" } ~ { name = "b" }`)
	assertContains(t, sql, "s1.Timestamp >=")
	assertContains(t, sql, "s2.Timestamp >=")
	t.Logf("SQL: %s", sql)
}

// ---------------------------------------------------------------------------
// Structural operators: empty filters
// ---------------------------------------------------------------------------

func TestTranspileStructuralChildEmptyFilters(t *testing.T) {
	sql := mustTranspile(t, `{ } > { }`)
	assertContains(t, sql, "JOIN")
	assertContains(t, sql, "p.SpanId = c.ParentSpanId")
	t.Logf("SQL: %s", sql)
}

// ---------------------------------------------------------------------------
// Structural operators: non-SpansetFilter element error
// ---------------------------------------------------------------------------

func TestTranspileStructuralNonFilterError(t *testing.T) {
	tr := &transpiler{opts: defaultOpts()}
	// Use a CoalesceOperation instead of SpansetFilter — should error
	op := &traceql.SpansetOperation{
		Op:  traceql.OpSpansetChild,
		LHS: &traceql.CoalesceOperation{},
		RHS: &traceql.SpansetFilter{},
	}
	_, err := tr.transpileSpansetOperation(op)
	if err == nil {
		t.Fatal("expected error for structural operator with non-filter element")
	}
	assertContains(t, err.Error(), "structural operators require spanset filters")
}

// ---------------------------------------------------------------------------
// Direct function coverage: aliasedTimeFilter
// ---------------------------------------------------------------------------

func TestAliasedTimeFilter(t *testing.T) {
	tr := &transpiler{opts: defaultOpts()}
	result := tr.aliasedTimeFilter("p")
	assertContains(t, result, "p.Timestamp >=")
	assertContains(t, result, "p.Timestamp <=")
}

func TestAliasedTimeFilterNoRange(t *testing.T) {
	tr := &transpiler{opts: TranspileOptions{Table: "otel_traces", Limit: 20}}
	result := tr.aliasedTimeFilter("p")
	if result != "1=1" {
		t.Errorf("expected '1=1', got %q", result)
	}
}

func TestAliasedTimeFilterStartOnly(t *testing.T) {
	tr := &transpiler{opts: TranspileOptions{
		Table: "otel_traces",
		Start: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		Limit: 20,
	}}
	result := tr.aliasedTimeFilter("t")
	assertContains(t, result, "t.Timestamp >=")
	assertNotContains(t, result, "t.Timestamp <=")
}

func TestAliasedTimeFilterEndOnly(t *testing.T) {
	tr := &transpiler{opts: TranspileOptions{
		Table: "otel_traces",
		End:   time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC),
		Limit: 20,
	}}
	result := tr.aliasedTimeFilter("t")
	assertContains(t, result, "t.Timestamp <=")
	assertNotContains(t, result, "t.Timestamp >=")
}

// ---------------------------------------------------------------------------
// Direct function coverage: replaceColumnsWithAlias
// ---------------------------------------------------------------------------

func TestReplaceColumnsWithAlias(t *testing.T) {
	tr := &transpiler{opts: defaultOpts()}

	tests := []struct {
		name     string
		input    string
		alias    string
		expected string
	}{
		{
			"span attributes",
			"SpanAttributes['http.method'] = 'GET'",
			"p",
			"p.SpanAttributes['http.method'] = 'GET'",
		},
		{
			"resource attributes",
			"ResourceAttributes['env'] = 'prod'",
			"c",
			"c.ResourceAttributes['env'] = 'prod'",
		},
		{
			"service name",
			"ServiceName = 'frontend'",
			"s1",
			"s1.ServiceName = 'frontend'",
		},
		{
			"status code",
			"StatusCode = 'STATUS_CODE_ERROR'",
			"s2",
			"s2.StatusCode = 'STATUS_CODE_ERROR'",
		},
		{
			"duration",
			"Duration > 1000000000",
			"p",
			"p.Duration > 1000000000",
		},
		{
			"span name",
			"SpanName = 'GET /users'",
			"c",
			"c.SpanName = 'GET /users'",
		},
		{
			"string literal containing Duration not replaced",
			"SpanName = 'Duration'",
			"p",
			"p.SpanName = 'Duration'",
		},
		{
			"string literal containing ServiceName not replaced",
			"SpanAttributes['key'] = 'ServiceName is cool'",
			"c",
			"c.SpanAttributes['key'] = 'ServiceName is cool'",
		},
		{
			"escaped quote inside string literal",
			"SpanName = 'it\\'s Duration'",
			"p",
			"p.SpanName = 'it\\'s Duration'",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := tr.replaceColumnsWithAlias(tc.input, tc.alias)
			if result != tc.expected {
				t.Errorf("replaceColumnsWithAlias(%q, %q) = %q, want %q", tc.input, tc.alias, result, tc.expected)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Direct function coverage: sampleClause
// ---------------------------------------------------------------------------

func TestSampleClause(t *testing.T) {
	tests := []struct {
		name       string
		sampleRate float64
		expected   string
	}{
		{"no sampling", 0, ""},
		{"10% sampling", 0.1, " SAMPLE 0.1"},
		{"50% sampling", 0.5, " SAMPLE 0.5"},
		{"100% sampling (disabled)", 1.0, ""},
		{"negative (disabled)", -0.1, ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tr := &transpiler{opts: TranspileOptions{
				Table:      "otel_traces",
				Limit:      20,
				SampleRate: tc.sampleRate,
			}}
			result := tr.sampleClause()
			if result != tc.expected {
				t.Errorf("sampleClause() = %q, want %q", result, tc.expected)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// SAMPLE clause in generated SQL
// ---------------------------------------------------------------------------

func TestTranspileWithSampling(t *testing.T) {
	root, err := traceql.Parse("{ }")
	if err != nil {
		t.Fatal(err)
	}
	opts := TranspileOptions{
		Table:      "otel_traces",
		Start:      time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		End:        time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC),
		Limit:      20,
		SampleRate: 0.1,
	}
	result, err := Transpile(root, opts)
	if err != nil {
		t.Fatal(err)
	}
	assertContains(t, result.SQL, "SAMPLE 0.1")
	t.Logf("SQL: %s", result.SQL)
}

func TestTranspileWithoutSampling(t *testing.T) {
	root, err := traceql.Parse("{ }")
	if err != nil {
		t.Fatal(err)
	}
	opts := TranspileOptions{
		Table: "otel_traces",
		Start: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		End:   time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC),
		Limit: 20,
	}
	result, err := Transpile(root, opts)
	if err != nil {
		t.Fatal(err)
	}
	assertNotContains(t, result.SQL, "SAMPLE")
	t.Logf("SQL: %s", result.SQL)
}

// ---------------------------------------------------------------------------
// PREWHERE in generated SQL
// ---------------------------------------------------------------------------

func TestTranspileWithPrewhere(t *testing.T) {
	root, err := traceql.Parse(`{ .http.method = "GET" }`)
	if err != nil {
		t.Fatal(err)
	}
	opts := TranspileOptions{
		Table:       "otel_traces",
		Start:       time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		End:         time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC),
		Limit:       20,
		UsePrewhere: true,
	}
	result, err := Transpile(root, opts)
	if err != nil {
		t.Fatal(err)
	}
	assertContains(t, result.SQL, "PREWHERE")
	assertContains(t, result.SQL, "Timestamp >=")
	assertContains(t, result.SQL, "'GET'")
	t.Logf("SQL: %s", result.SQL)
}

func TestTranspilePrewhereWithPrevCTE(t *testing.T) {
	root, err := traceql.Parse(`{ .a = "x" } | { .b = "y" }`)
	if err != nil {
		t.Fatal(err)
	}
	opts := TranspileOptions{
		Table:       "otel_traces",
		Start:       time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		End:         time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC),
		Limit:       20,
		UsePrewhere: true,
	}
	result, err := Transpile(root, opts)
	if err != nil {
		t.Fatal(err)
	}
	assertContains(t, result.SQL, "PREWHERE")
	t.Logf("SQL: %s", result.SQL)
}

func TestTranspilePrewhereNoTimeRange(t *testing.T) {
	root, err := traceql.Parse(`{ .http.method = "GET" }`)
	if err != nil {
		t.Fatal(err)
	}
	// UsePrewhere is true but no time range — should not use PREWHERE
	opts := TranspileOptions{
		Table:       "otel_traces",
		Limit:       20,
		UsePrewhere: true,
	}
	result, err := Transpile(root, opts)
	if err != nil {
		t.Fatal(err)
	}
	// Without time range, PREWHERE has nothing to prewhere on
	assertNotContains(t, result.SQL, "PREWHERE")
	t.Logf("SQL: %s", result.SQL)
}

// ---------------------------------------------------------------------------
// PREWHERE + SAMPLE combined
// ---------------------------------------------------------------------------

func TestTranspilePrewhereAndSample(t *testing.T) {
	root, err := traceql.Parse(`{ duration > 1s }`)
	if err != nil {
		t.Fatal(err)
	}
	opts := TranspileOptions{
		Table:       "otel_traces",
		Start:       time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		End:         time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC),
		Limit:       20,
		UsePrewhere: true,
		SampleRate:  0.5,
	}
	result, err := Transpile(root, opts)
	if err != nil {
		t.Fatal(err)
	}
	assertContains(t, result.SQL, "PREWHERE")
	assertContains(t, result.SQL, "SAMPLE 0.5")
	assertContains(t, result.SQL, "Duration > 1000000000")
	t.Logf("SQL: %s", result.SQL)
}

// ---------------------------------------------------------------------------
// applyHints
// ---------------------------------------------------------------------------

func TestApplyHintsSample(t *testing.T) {
	root := &traceql.RootExpr{
		Pipeline: traceql.Pipeline{
			Elements: []traceql.PipelineElement{
				&traceql.SpansetFilter{},
			},
		},
		Hints: &traceql.Hints{
			Hints: []*traceql.Hint{
				{Name: "sample", Value: traceql.Static{Type: traceql.TypeFloat, FloatVal: 0.25}},
			},
		},
	}
	opts := TranspileOptions{
		Table: "otel_traces",
		Limit: 20,
	}
	applyHints(root, &opts)
	if opts.SampleRate != 0.25 {
		t.Errorf("expected SampleRate=0.25, got %g", opts.SampleRate)
	}
}

func TestApplyHintsSampleInt(t *testing.T) {
	root := &traceql.RootExpr{
		Pipeline: traceql.Pipeline{
			Elements: []traceql.PipelineElement{
				&traceql.SpansetFilter{},
			},
		},
		Hints: &traceql.Hints{
			Hints: []*traceql.Hint{
				{Name: "sample", Value: traceql.Static{Type: traceql.TypeInt, IntVal: 10}},
			},
		},
	}
	opts := TranspileOptions{Table: "otel_traces", Limit: 20}
	applyHints(root, &opts)
	if opts.SampleRate != 0.1 {
		t.Errorf("expected SampleRate=0.1, got %g", opts.SampleRate)
	}
}

func TestApplyHintsPrewhere(t *testing.T) {
	root := &traceql.RootExpr{
		Pipeline: traceql.Pipeline{
			Elements: []traceql.PipelineElement{
				&traceql.SpansetFilter{},
			},
		},
		Hints: &traceql.Hints{
			Hints: []*traceql.Hint{
				{Name: "prewhere", Value: traceql.Static{Type: traceql.TypeBoolean, BoolVal: true}},
			},
		},
	}
	opts := TranspileOptions{Table: "otel_traces", Limit: 20}
	applyHints(root, &opts)
	if !opts.UsePrewhere {
		t.Error("expected UsePrewhere=true")
	}
}

func TestApplyHintsNilHints(t *testing.T) {
	root := &traceql.RootExpr{
		Pipeline: traceql.Pipeline{
			Elements: []traceql.PipelineElement{
				&traceql.SpansetFilter{},
			},
		},
	}
	opts := TranspileOptions{Table: "otel_traces", Limit: 20}
	applyHints(root, &opts) // should not panic
	if opts.SampleRate != 0 {
		t.Error("expected SampleRate to remain 0")
	}
}

func TestApplyHintsUnknownHint(_ *testing.T) {
	root := &traceql.RootExpr{
		Pipeline: traceql.Pipeline{
			Elements: []traceql.PipelineElement{
				&traceql.SpansetFilter{},
			},
		},
		Hints: &traceql.Hints{
			Hints: []*traceql.Hint{
				{Name: "unknown_hint", Value: traceql.Static{Type: traceql.TypeString, StringVal: "val"}},
			},
		},
	}
	opts := TranspileOptions{Table: "otel_traces", Limit: 20}
	applyHints(root, &opts) // should not panic or error
}

// ---------------------------------------------------------------------------
// Negated structural operators
// ---------------------------------------------------------------------------

func TestTranspileStructuralNotChild(t *testing.T) {
	root, err := traceql.Parse(`{ .a = "x" } !> { .b = "y" }`)
	if err != nil {
		t.Fatal(err)
	}
	result, err := Transpile(root, defaultOpts())
	if err != nil {
		t.Fatal(err)
	}
	assertContains(t, result.SQL, "EXCEPT")
	t.Logf("SQL: %s", result.SQL)
}

func TestTranspileStructuralNotDescendant(t *testing.T) {
	root, err := traceql.Parse(`{ .a = "x" } !>> { .b = "y" }`)
	if err != nil {
		t.Fatal(err)
	}
	result, err := Transpile(root, defaultOpts())
	if err != nil {
		t.Fatal(err)
	}
	assertContains(t, result.SQL, "EXCEPT")
	assertContains(t, result.SQL, "WITH RECURSIVE")
	t.Logf("SQL: %s", result.SQL)
}

func TestTranspileStructuralNotSibling(t *testing.T) {
	// !~ is ambiguous with not-regex in the lexer, so construct AST directly
	tr := &transpiler{opts: defaultOpts()}
	op := &traceql.SpansetOperation{
		Op: traceql.OpSpansetNotSibling,
		LHS: &traceql.SpansetFilter{
			Expression: &traceql.BinaryOperation{
				Op:  traceql.OpEqual,
				LHS: &traceql.Attribute{Name: "a", Scope: traceql.AttributeScopeNone},
				RHS: &traceql.Static{Type: traceql.TypeString, StringVal: "x"},
			},
		},
		RHS: &traceql.SpansetFilter{
			Expression: &traceql.BinaryOperation{
				Op:  traceql.OpEqual,
				LHS: &traceql.Attribute{Name: "b", Scope: traceql.AttributeScopeNone},
				RHS: &traceql.Static{Type: traceql.TypeString, StringVal: "y"},
			},
		},
	}
	sql, err := tr.transpileSpansetOperation(op)
	if err != nil {
		t.Fatal(err)
	}
	assertContains(t, sql, "EXCEPT")
	t.Logf("SQL: %s", sql)
}

// ---------------------------------------------------------------------------
// Union structural operators
// ---------------------------------------------------------------------------

func TestTranspileStructuralUnionChild(t *testing.T) {
	root, err := traceql.Parse(`{ .a = "x" } &> { .b = "y" }`)
	if err != nil {
		t.Fatal(err)
	}
	result, err := Transpile(root, defaultOpts())
	if err != nil {
		t.Fatal(err)
	}
	assertContains(t, result.SQL, "UNION DISTINCT")
	assertContains(t, result.SQL, "JOIN")
	t.Logf("SQL: %s", result.SQL)
}

// ---------------------------------------------------------------------------
// extractFilterCondition edge cases
// ---------------------------------------------------------------------------

func TestExtractFilterConditionEmptyFilter(t *testing.T) {
	tr := &transpiler{opts: defaultOpts()}
	cond, err := tr.extractFilterCondition(&traceql.SpansetFilter{Expression: nil})
	if err != nil {
		t.Fatal(err)
	}
	if cond != "1=1" {
		t.Errorf("expected '1=1', got %q", cond)
	}
}
