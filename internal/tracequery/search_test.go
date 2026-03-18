package tracequery

import (
	"testing"
	"time"

	"github.com/hacktohell/opex/internal/clickhouse"
	"github.com/hacktohell/opex/internal/response"
)

func TestBuildSearchResponse_Empty(t *testing.T) {
	resp := buildSearchResponse(nil, nil, 3, 0, 0)
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
	if len(resp.Traces) != 0 {
		t.Errorf("expected 0 traces, got %d", len(resp.Traces))
	}
}

func TestBuildSearchResponse_Basic(t *testing.T) {
	ts := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	spans := []clickhouse.SpanRow{
		{
			Timestamp: ts, TraceID: "trace1", SpanID: "span1",
			ParentSpanID: "", SpanName: "GET /api", ServiceName: "frontend",
			Duration: 500_000_000, StatusCode: "STATUS_CODE_OK",
		},
		{
			Timestamp: ts.Add(100 * time.Millisecond), TraceID: "trace1", SpanID: "span2",
			ParentSpanID: "span1", SpanName: "db.query", ServiceName: "backend",
			Duration: 200_000_000, StatusCode: "STATUS_CODE_OK",
		},
	}

	resp := buildSearchResponse(spans, []string{"trace1"}, 3, 0, 0)
	if len(resp.Traces) != 1 {
		t.Fatalf("expected 1 trace, got %d", len(resp.Traces))
	}
	meta := resp.Traces[0]
	if meta.TraceID != "trace1" {
		t.Errorf("traceID = %q, want trace1", meta.TraceID)
	}
	if meta.RootServiceName != "frontend" {
		t.Errorf("rootServiceName = %q, want frontend", meta.RootServiceName)
	}
	if meta.DurationMs <= 0 {
		t.Errorf("expected positive duration, got %d", meta.DurationMs)
	}
}

func TestBuildSearchResponse_DurationFilter(t *testing.T) {
	ts := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	spans := []clickhouse.SpanRow{
		{Timestamp: ts, TraceID: "t1", SpanID: "s1", SpanName: "op",
			ServiceName: "svc", Duration: 2_000_000_000, StatusCode: "STATUS_CODE_OK"},
	}

	// minDuration=3s should filter out 2s trace
	resp := buildSearchResponse(spans, []string{"t1"}, 3, 3*time.Second, 0)
	if len(resp.Traces) != 0 {
		t.Errorf("expected 0 traces with minDuration=3s, got %d", len(resp.Traces))
	}

	// maxDuration=1s should filter out 2s trace
	resp = buildSearchResponse(spans, []string{"t1"}, 3, 0, 1*time.Second)
	if len(resp.Traces) != 0 {
		t.Errorf("expected 0 traces with maxDuration=1s, got %d", len(resp.Traces))
	}

	// minDuration=1s should include 2s trace
	resp = buildSearchResponse(spans, []string{"t1"}, 3, 1*time.Second, 0)
	if len(resp.Traces) != 1 {
		t.Errorf("expected 1 trace with minDuration=1s, got %d", len(resp.Traces))
	}
}

func TestBuildSearchResponse_SpanSets(t *testing.T) {
	ts := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	var spans []clickhouse.SpanRow
	for i := 0; i < 5; i++ {
		spans = append(spans, clickhouse.SpanRow{
			Timestamp: ts.Add(time.Duration(i) * time.Millisecond),
			TraceID: "t1", SpanID: string(rune('a' + i)),
			SpanName: "op", ServiceName: "svc", Duration: 1_000_000,
			StatusCode: "STATUS_CODE_OK",
		})
	}

	// spss=2 should limit spans
	resp := buildSearchResponse(spans, []string{"t1"}, 2, 0, 0)
	if len(resp.Traces[0].SpanSets) != 1 {
		t.Fatalf("expected 1 spanset, got %d", len(resp.Traces[0].SpanSets))
	}
	if len(resp.Traces[0].SpanSets[0].Spans) != 2 {
		t.Errorf("expected 2 spans (spss=2), got %d", len(resp.Traces[0].SpanSets[0].Spans))
	}
	if resp.Traces[0].SpanSets[0].Matched != 5 {
		t.Errorf("expected matched=5, got %d", resp.Traces[0].SpanSets[0].Matched)
	}

	// spss=0 should not include spansets
	resp = buildSearchResponse(spans, []string{"t1"}, 0, 0, 0)
	if len(resp.Traces[0].SpanSets) != 0 {
		t.Errorf("expected 0 spansets with spss=0, got %d", len(resp.Traces[0].SpanSets))
	}
}

func TestBuildSearchResponse_ServiceStats(t *testing.T) {
	ts := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	spans := []clickhouse.SpanRow{
		{Timestamp: ts, TraceID: "t1", SpanID: "s1", ParentSpanID: "",
			SpanName: "root", ServiceName: "svc-a", Duration: 1_000_000, StatusCode: "STATUS_CODE_OK"},
		{Timestamp: ts, TraceID: "t1", SpanID: "s2", ParentSpanID: "s1",
			SpanName: "child", ServiceName: "svc-b", Duration: 500_000, StatusCode: "STATUS_CODE_ERROR"},
		{Timestamp: ts, TraceID: "t1", SpanID: "s3", ParentSpanID: "s1",
			SpanName: "child2", ServiceName: "svc-a", Duration: 300_000, StatusCode: "STATUS_CODE_OK"},
	}

	resp := buildSearchResponse(spans, []string{"t1"}, 3, 0, 0)
	meta := resp.Traces[0]
	if meta.ServiceStats["svc-a"].SpanCount != 2 {
		t.Errorf("svc-a spanCount = %d, want 2", meta.ServiceStats["svc-a"].SpanCount)
	}
	if meta.ServiceStats["svc-b"].ErrorCount != 1 {
		t.Errorf("svc-b errorCount = %d, want 1", meta.ServiceStats["svc-b"].ErrorCount)
	}
}

func TestBuildSearchResponse_Metrics(t *testing.T) {
	ts := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	spans := []clickhouse.SpanRow{
		{Timestamp: ts, TraceID: "t1", SpanID: "s1", SpanName: "root",
			ServiceName: "svc", Duration: 1_000_000, StatusCode: "STATUS_CODE_OK"},
	}
	resp := buildSearchResponse(spans, []string{"t1"}, 3, 0, 0)
	if resp.Metrics.InspectedTraces != 1 {
		t.Errorf("InspectedTraces = %d, want 1", resp.Metrics.InspectedTraces)
	}
	if resp.Metrics.InspectedSpans != 1 {
		t.Errorf("InspectedSpans = %d, want 1", resp.Metrics.InspectedSpans)
	}
}

// Verify response package types are used correctly.
var _ = response.SearchResponse{}
