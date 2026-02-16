package api

import (
	"testing"
	"time"

	"github.com/hacktohell/opex/internal/clickhouse"
)

func TestBuildSearchResponse_Empty(t *testing.T) {
	resp := buildSearchResponse(nil, nil, "{}", 3, "", "")
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
			Timestamp:    ts,
			TraceId:      "trace1",
			SpanId:       "span1",
			ParentSpanId: "",
			SpanName:     "GET /api",
			SpanKind:     "SPAN_KIND_SERVER",
			ServiceName:  "frontend",
			Duration:     500000000, // 500ms
			StatusCode:   "STATUS_CODE_OK",
		},
		{
			Timestamp:    ts.Add(100 * time.Millisecond),
			TraceId:      "trace1",
			SpanId:       "span2",
			ParentSpanId: "span1",
			SpanName:     "db.query",
			SpanKind:     "SPAN_KIND_CLIENT",
			ServiceName:  "backend",
			Duration:     200000000, // 200ms
			StatusCode:   "STATUS_CODE_OK",
		},
	}
	traceIDs := []string{"trace1"}

	resp := buildSearchResponse(spans, traceIDs, "{}", 3, "", "")
	if len(resp.Traces) != 1 {
		t.Fatalf("expected 1 trace, got %d", len(resp.Traces))
	}

	meta := resp.Traces[0]
	if meta.TraceID != "trace1" {
		t.Errorf("expected traceID 'trace1', got %q", meta.TraceID)
	}
	if meta.RootServiceName != "frontend" {
		t.Errorf("expected rootServiceName 'frontend', got %q", meta.RootServiceName)
	}
	if meta.RootTraceName != "GET /api" {
		t.Errorf("expected rootTraceName 'GET /api', got %q", meta.RootTraceName)
	}
	if meta.DurationMs <= 0 {
		t.Errorf("expected positive duration, got %d", meta.DurationMs)
	}
}

func TestBuildSearchResponse_DurationFilter(t *testing.T) {
	ts := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	spans := []clickhouse.SpanRow{
		{
			Timestamp:    ts,
			TraceId:      "trace1",
			SpanId:       "span1",
			ParentSpanId: "",
			SpanName:     "slow",
			ServiceName:  "svc",
			Duration:     2000000000, // 2s
			StatusCode:   "STATUS_CODE_OK",
		},
	}

	// minDuration=3s should filter out a 2s trace
	resp := buildSearchResponse(spans, []string{"trace1"}, "{}", 3, "3s", "")
	if len(resp.Traces) != 0 {
		t.Errorf("expected 0 traces with minDuration=3s, got %d", len(resp.Traces))
	}

	// maxDuration=1s should filter out a 2s trace
	resp = buildSearchResponse(spans, []string{"trace1"}, "{}", 3, "", "1s")
	if len(resp.Traces) != 0 {
		t.Errorf("expected 0 traces with maxDuration=1s, got %d", len(resp.Traces))
	}

	// minDuration=1s should include a 2s trace
	resp = buildSearchResponse(spans, []string{"trace1"}, "{}", 3, "1s", "")
	if len(resp.Traces) != 1 {
		t.Errorf("expected 1 trace with minDuration=1s, got %d", len(resp.Traces))
	}
}

func TestBuildSearchResponse_SpanSets(t *testing.T) {
	ts := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	var spans []clickhouse.SpanRow
	for i := 0; i < 5; i++ {
		spans = append(spans, clickhouse.SpanRow{
			Timestamp:   ts.Add(time.Duration(i) * time.Millisecond),
			TraceId:     "trace1",
			SpanId:      "span" + string(rune('a'+i)),
			SpanName:    "op",
			ServiceName: "svc",
			Duration:    1000000,
			StatusCode:  "STATUS_CODE_OK",
		})
	}

	// spss=2 should limit spans in spanset to 2
	resp := buildSearchResponse(spans, []string{"trace1"}, "{}", 2, "", "")
	if len(resp.Traces) != 1 {
		t.Fatalf("expected 1 trace, got %d", len(resp.Traces))
	}
	if len(resp.Traces[0].SpanSets) != 1 {
		t.Fatalf("expected 1 spanset, got %d", len(resp.Traces[0].SpanSets))
	}
	ss := resp.Traces[0].SpanSets[0]
	if len(ss.Spans) != 2 {
		t.Errorf("expected 2 spans in spanset (spss=2), got %d", len(ss.Spans))
	}
	if ss.Matched != 5 {
		t.Errorf("expected matched=5, got %d", ss.Matched)
	}

	// spss=0 should not include spansets
	resp = buildSearchResponse(spans, []string{"trace1"}, "{}", 0, "", "")
	if len(resp.Traces[0].SpanSets) != 0 {
		t.Errorf("expected 0 spansets with spss=0, got %d", len(resp.Traces[0].SpanSets))
	}
}

func TestBuildTraceMetadata_ServiceStats(t *testing.T) {
	ts := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	spans := []clickhouse.SpanRow{
		{
			Timestamp:    ts,
			TraceId:      "trace1",
			SpanId:       "span1",
			ParentSpanId: "",
			SpanName:     "root",
			ServiceName:  "svc-a",
			Duration:     1000000,
			StatusCode:   "STATUS_CODE_OK",
		},
		{
			Timestamp:    ts,
			TraceId:      "trace1",
			SpanId:       "span2",
			ParentSpanId: "span1",
			SpanName:     "child",
			ServiceName:  "svc-b",
			Duration:     500000,
			StatusCode:   "STATUS_CODE_ERROR",
		},
		{
			Timestamp:    ts,
			TraceId:      "trace1",
			SpanId:       "span3",
			ParentSpanId: "span1",
			SpanName:     "child2",
			ServiceName:  "svc-a",
			Duration:     300000,
			StatusCode:   "STATUS_CODE_OK",
		},
	}

	meta := buildTraceMetadata(spans, 3)

	if meta.ServiceStats == nil {
		t.Fatal("expected non-nil ServiceStats")
	}
	if meta.ServiceStats["svc-a"].SpanCount != 2 {
		t.Errorf("expected svc-a spanCount=2, got %d", meta.ServiceStats["svc-a"].SpanCount)
	}
	if meta.ServiceStats["svc-b"].SpanCount != 1 {
		t.Errorf("expected svc-b spanCount=1, got %d", meta.ServiceStats["svc-b"].SpanCount)
	}
	if meta.ServiceStats["svc-b"].ErrorCount != 1 {
		t.Errorf("expected svc-b errorCount=1, got %d", meta.ServiceStats["svc-b"].ErrorCount)
	}
}

func TestBuildSearchResponse_MultipleTraces(t *testing.T) {
	ts := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	spans := []clickhouse.SpanRow{
		{
			Timestamp:    ts,
			TraceId:      "trace1",
			SpanId:       "span1",
			ParentSpanId: "",
			SpanName:     "root1",
			ServiceName:  "svc",
			Duration:     1000000,
			StatusCode:   "STATUS_CODE_OK",
		},
		{
			Timestamp:    ts,
			TraceId:      "trace2",
			SpanId:       "span2",
			ParentSpanId: "",
			SpanName:     "root2",
			ServiceName:  "svc",
			Duration:     2000000,
			StatusCode:   "STATUS_CODE_OK",
		},
	}
	traceIDs := []string{"trace1", "trace2"}

	resp := buildSearchResponse(spans, traceIDs, "{}", 3, "", "")
	if len(resp.Traces) != 2 {
		t.Fatalf("expected 2 traces, got %d", len(resp.Traces))
	}

	// Verify order matches traceIDs order
	if resp.Traces[0].TraceID != "trace1" {
		t.Errorf("expected first trace to be trace1, got %q", resp.Traces[0].TraceID)
	}
	if resp.Traces[1].TraceID != "trace2" {
		t.Errorf("expected second trace to be trace2, got %q", resp.Traces[1].TraceID)
	}
}

func TestBuildSearchResponse_Metrics(t *testing.T) {
	ts := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	spans := []clickhouse.SpanRow{
		{
			Timestamp:    ts,
			TraceId:      "trace1",
			SpanId:       "span1",
			ParentSpanId: "",
			SpanName:     "root",
			ServiceName:  "svc",
			Duration:     1000000,
			StatusCode:   "STATUS_CODE_OK",
		},
	}

	resp := buildSearchResponse(spans, []string{"trace1"}, "{}", 3, "", "")
	if resp.Metrics.InspectedTraces != 1 {
		t.Errorf("expected InspectedTraces=1, got %d", resp.Metrics.InspectedTraces)
	}
	if resp.Metrics.InspectedSpans != 1 {
		t.Errorf("expected InspectedSpans=1, got %d", resp.Metrics.InspectedSpans)
	}
}
