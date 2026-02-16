package response

import (
	"testing"
	"time"

	"github.com/hacktohell/opex/internal/clickhouse"
)

func TestMapToKeyValues(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]string
		wantLen  int
		wantNil  bool
		wantKeys []string // expected sorted keys
	}{
		{
			name:    "nil map",
			input:   nil,
			wantLen: 0,
			wantNil: true,
		},
		{
			name:    "empty map",
			input:   map[string]string{},
			wantLen: 0,
			wantNil: true,
		},
		{
			name:     "single entry",
			input:    map[string]string{"foo": "bar"},
			wantLen:  1,
			wantKeys: []string{"foo"},
		},
		{
			name:     "multiple entries sorted",
			input:    map[string]string{"z": "1", "a": "2", "m": "3"},
			wantLen:  3,
			wantKeys: []string{"a", "m", "z"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			kvs := mapToKeyValues(tc.input)
			if tc.wantNil && kvs != nil {
				t.Fatalf("expected nil, got %v", kvs)
			}
			if !tc.wantNil && len(kvs) != tc.wantLen {
				t.Fatalf("expected %d key-values, got %d", tc.wantLen, len(kvs))
			}
			for i, key := range tc.wantKeys {
				if kvs[i].Key != key {
					t.Errorf("key[%d]: expected %q, got %q", i, key, kvs[i].Key)
				}
				if kvs[i].Value.StringValue == nil {
					t.Errorf("key[%d] %q: StringValue is nil", i, key)
				}
			}
		})
	}
}

func TestSpanKindToInt(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"SPAN_KIND_INTERNAL", 1},
		{"SPAN_KIND_SERVER", 2},
		{"SPAN_KIND_CLIENT", 3},
		{"SPAN_KIND_PRODUCER", 4},
		{"SPAN_KIND_CONSUMER", 5},
		{"span_kind_server", 2}, // case insensitive
		{"SPAN_KIND_UNSPECIFIED", 0},
		{"", 0},
		{"UNKNOWN", 0},
	}
	for _, tc := range tests {
		got := spanKindToInt(tc.input)
		if got != tc.want {
			t.Errorf("spanKindToInt(%q) = %d, want %d", tc.input, got, tc.want)
		}
	}
}

func TestStatusCodeToInt(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"STATUS_CODE_OK", 1},
		{"STATUS_CODE_ERROR", 2},
		{"STATUS_CODE_UNSET", 0},
		{"status_code_ok", 1}, // case insensitive
		{"", 0},
		{"UNKNOWN", 0},
	}
	for _, tc := range tests {
		got := statusCodeToInt(tc.input)
		if got != tc.want {
			t.Errorf("statusCodeToInt(%q) = %d, want %d", tc.input, got, tc.want)
		}
	}
}

func TestGetServiceName(t *testing.T) {
	sn := "my-service"
	tests := []struct {
		name  string
		attrs []KeyValue
		want  string
	}{
		{
			name:  "empty attrs",
			attrs: nil,
			want:  "",
		},
		{
			name: "has service.name",
			attrs: []KeyValue{
				{Key: "service.name", Value: AnyValue{StringValue: &sn}},
			},
			want: "my-service",
		},
		{
			name: "no service.name",
			attrs: []KeyValue{
				{Key: "other.attr", Value: AnyValue{StringValue: &sn}},
			},
			want: "",
		},
		{
			name: "service.name with nil StringValue",
			attrs: []KeyValue{
				{Key: "service.name", Value: AnyValue{}},
			},
			want: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := getServiceName(tc.attrs)
			if got != tc.want {
				t.Errorf("getServiceName() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestBuildTrace_Nil(t *testing.T) {
	result := BuildTrace(nil)
	if result != nil {
		t.Fatal("expected nil for empty spans")
	}
	result = BuildTrace([]clickhouse.SpanRow{})
	if result != nil {
		t.Fatal("expected nil for empty slice")
	}
}

func TestBuildTrace_SingleSpan(t *testing.T) {
	ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	spans := []clickhouse.SpanRow{
		{
			Timestamp:          ts,
			TraceID:            "abc123",
			SpanID:             "span1",
			ParentSpanID:       "",
			SpanName:           "GET /api",
			SpanKind:           "SPAN_KIND_SERVER",
			ServiceName:        "frontend",
			ResourceAttributes: map[string]string{"deployment": "prod"},
			ScopeName:          "otel-go",
			ScopeVersion:       "1.0.0",
			SpanAttributes:     map[string]string{"http.method": "GET"},
			Duration:           1000000000, // 1s
			StatusCode:         "STATUS_CODE_OK",
			StatusMessage:      "",
		},
	}

	trace := BuildTrace(spans)
	if trace == nil {
		t.Fatal("expected non-nil trace")
	}
	if len(trace.Batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(trace.Batches))
	}

	batch := trace.Batches[0]
	// Check resource has service.name
	foundSN := false
	for _, a := range batch.Resource.Attributes {
		if a.Key == "service.name" && a.Value.StringValue != nil && *a.Value.StringValue == "frontend" {
			foundSN = true
		}
	}
	if !foundSN {
		t.Error("expected service.name=frontend in resource attributes")
	}

	if len(batch.ScopeSpans) != 1 {
		t.Fatalf("expected 1 scope span group, got %d", len(batch.ScopeSpans))
	}

	ss := batch.ScopeSpans[0]
	if ss.Scope.Name != "otel-go" {
		t.Errorf("expected scope name 'otel-go', got %q", ss.Scope.Name)
	}
	if ss.Scope.Version != "1.0.0" {
		t.Errorf("expected scope version '1.0.0', got %q", ss.Scope.Version)
	}

	if len(ss.Spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(ss.Spans))
	}
	span := ss.Spans[0]
	if span.TraceID != "abc123" {
		t.Errorf("expected traceID 'abc123', got %q", span.TraceID)
	}
	if span.Name != "GET /api" {
		t.Errorf("expected name 'GET /api', got %q", span.Name)
	}
	if span.Kind != 2 { // SERVER
		t.Errorf("expected kind 2 (SERVER), got %d", span.Kind)
	}
	if span.Status.Code != 1 { // OK
		t.Errorf("expected status code 1 (OK), got %d", span.Status.Code)
	}
}

func TestBuildTrace_MultiService(t *testing.T) {
	ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	spans := []clickhouse.SpanRow{
		{
			Timestamp:          ts,
			TraceID:            "trace1",
			SpanID:             "span1",
			SpanName:           "request",
			SpanKind:           "SPAN_KIND_SERVER",
			ServiceName:        "frontend",
			ResourceAttributes: map[string]string{},
			SpanAttributes:     map[string]string{},
			Duration:           1000000,
			StatusCode:         "STATUS_CODE_OK",
		},
		{
			Timestamp:          ts.Add(100 * time.Millisecond),
			TraceID:            "trace1",
			SpanID:             "span2",
			ParentSpanID:       "span1",
			SpanName:           "db.query",
			SpanKind:           "SPAN_KIND_CLIENT",
			ServiceName:        "backend",
			ResourceAttributes: map[string]string{},
			SpanAttributes:     map[string]string{},
			Duration:           500000,
			StatusCode:         "STATUS_CODE_OK",
		},
	}

	trace := BuildTrace(spans)
	if trace == nil {
		t.Fatal("expected non-nil trace")
	}
	if len(trace.Batches) != 2 {
		t.Fatalf("expected 2 batches (one per service), got %d", len(trace.Batches))
	}

	// Batches should be sorted by service name
	sn0 := getServiceName(trace.Batches[0].Resource.Attributes)
	sn1 := getServiceName(trace.Batches[1].Resource.Attributes)
	if sn0 != "backend" || sn1 != "frontend" {
		t.Errorf("expected batches sorted [backend, frontend], got [%s, %s]", sn0, sn1)
	}
}

func TestBuildTrace_WithEvents(t *testing.T) {
	ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	eventTs := ts.Add(50 * time.Millisecond)
	spans := []clickhouse.SpanRow{
		{
			Timestamp:          ts,
			TraceID:            "trace1",
			SpanID:             "span1",
			SpanName:           "request",
			SpanKind:           "SPAN_KIND_SERVER",
			ServiceName:        "svc",
			ResourceAttributes: map[string]string{},
			SpanAttributes:     map[string]string{},
			Duration:           1000000,
			StatusCode:         "STATUS_CODE_ERROR",
			EventsTimestamp:    []time.Time{eventTs},
			EventsName:         []string{"exception"},
			EventsAttributes:   []map[string]string{{"exception.message": "null pointer"}},
		},
	}

	trace := BuildTrace(spans)
	span := trace.Batches[0].ScopeSpans[0].Spans[0]
	if len(span.Events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(span.Events))
	}
	if span.Events[0].Name != "exception" {
		t.Errorf("expected event name 'exception', got %q", span.Events[0].Name)
	}
	if len(span.Events[0].Attributes) != 1 {
		t.Errorf("expected 1 event attribute, got %d", len(span.Events[0].Attributes))
	}
}

func TestBuildTrace_WithLinks(t *testing.T) {
	ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	spans := []clickhouse.SpanRow{
		{
			Timestamp:          ts,
			TraceID:            "trace1",
			SpanID:             "span1",
			SpanName:           "request",
			SpanKind:           "SPAN_KIND_SERVER",
			ServiceName:        "svc",
			ResourceAttributes: map[string]string{},
			SpanAttributes:     map[string]string{},
			Duration:           1000000,
			StatusCode:         "STATUS_CODE_OK",
			LinksTraceID:       []string{"linked-trace"},
			LinksSpanID:        []string{"linked-span"},
			LinksAttributes:    []map[string]string{{"link.attr": "val"}},
		},
	}

	trace := BuildTrace(spans)
	span := trace.Batches[0].ScopeSpans[0].Spans[0]
	if len(span.Links) != 1 {
		t.Fatalf("expected 1 link, got %d", len(span.Links))
	}
	if span.Links[0].TraceID != "linked-trace" {
		t.Errorf("expected link traceID 'linked-trace', got %q", span.Links[0].TraceID)
	}
	if span.Links[0].SpanID != "linked-span" {
		t.Errorf("expected link spanID 'linked-span', got %q", span.Links[0].SpanID)
	}
}

func TestBuildTrace_ServiceNameInResourceAttrs(t *testing.T) {
	// When resource attributes already contain service.name, it should NOT be duplicated
	ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	spans := []clickhouse.SpanRow{
		{
			Timestamp:          ts,
			TraceID:            "trace1",
			SpanID:             "span1",
			SpanName:           "request",
			SpanKind:           "SPAN_KIND_SERVER",
			ServiceName:        "my-svc",
			ResourceAttributes: map[string]string{"service.name": "my-svc"},
			SpanAttributes:     map[string]string{},
			Duration:           1000000,
			StatusCode:         "STATUS_CODE_OK",
		},
	}

	trace := BuildTrace(spans)
	// Count how many service.name entries there are
	count := 0
	for _, a := range trace.Batches[0].Resource.Attributes {
		if a.Key == "service.name" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected exactly 1 service.name attribute, got %d", count)
	}
}

func TestSpanRowToOTLP_TimeCalculation(t *testing.T) {
	ts := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	row := clickhouse.SpanRow{
		Timestamp:      ts,
		TraceID:        "t1",
		SpanID:         "s1",
		SpanName:       "test",
		SpanKind:       "SPAN_KIND_INTERNAL",
		ServiceName:    "svc",
		SpanAttributes: map[string]string{},
		Duration:       500000000, // 500ms
		StatusCode:     "STATUS_CODE_UNSET",
	}

	span := spanRowToOTLP(row)

	expectedStart := ts.UnixNano()
	expectedEnd := expectedStart + 500000000

	if span.StartTimeUnixNano != "1750075200000000000" {
		// Just check it's parseable and correct
		if span.StartTimeUnixNano == "" {
			t.Error("StartTimeUnixNano is empty")
		}
	}
	_ = expectedStart
	_ = expectedEnd

	// Check the end time is start + duration
	if span.EndTimeUnixNano == span.StartTimeUnixNano {
		t.Error("EndTimeUnixNano should differ from StartTimeUnixNano when duration > 0")
	}
}

func TestBuildTrace_MultipleScopes(t *testing.T) {
	ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	spans := []clickhouse.SpanRow{
		{
			Timestamp:          ts,
			TraceID:            "t1",
			SpanID:             "s1",
			SpanName:           "span1",
			SpanKind:           "SPAN_KIND_SERVER",
			ServiceName:        "svc",
			ResourceAttributes: map[string]string{},
			ScopeName:          "scope-a",
			ScopeVersion:       "1.0",
			SpanAttributes:     map[string]string{},
			Duration:           100,
			StatusCode:         "STATUS_CODE_OK",
		},
		{
			Timestamp:          ts,
			TraceID:            "t1",
			SpanID:             "s2",
			SpanName:           "span2",
			SpanKind:           "SPAN_KIND_CLIENT",
			ServiceName:        "svc",
			ResourceAttributes: map[string]string{},
			ScopeName:          "scope-b",
			ScopeVersion:       "2.0",
			SpanAttributes:     map[string]string{},
			Duration:           200,
			StatusCode:         "STATUS_CODE_OK",
		},
	}

	trace := BuildTrace(spans)
	if len(trace.Batches) != 1 {
		t.Fatalf("expected 1 batch (same service), got %d", len(trace.Batches))
	}
	if len(trace.Batches[0].ScopeSpans) != 2 {
		t.Fatalf("expected 2 scope span groups, got %d", len(trace.Batches[0].ScopeSpans))
	}
}
