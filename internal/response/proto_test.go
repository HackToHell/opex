package response

import (
	"bytes"
	"testing"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/hacktohell/opex/internal/clickhouse"
)

func TestHexToBytes(t *testing.T) {
	tests := []struct {
		name string
		hex  string
		size int
		want []byte
	}{
		{
			name: "empty string",
			hex:  "",
			size: 16,
			want: nil,
		},
		{
			name: "valid 32-char trace ID",
			hex:  "0102030405060708090a0b0c0d0e0f10",
			size: 16,
			want: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		},
		{
			name: "valid 16-char span ID",
			hex:  "0102030405060708",
			size: 8,
			want: []byte{1, 2, 3, 4, 5, 6, 7, 8},
		},
		{
			name: "short hex gets left-padded",
			hex:  "0a0b",
			size: 8,
			want: []byte{0, 0, 0, 0, 0, 0, 10, 11},
		},
		{
			name: "invalid hex returns nil",
			hex:  "zzzz",
			size: 8,
			want: nil,
		},
		{
			name: "odd-length hex returns nil",
			hex:  "abc",
			size: 8,
			want: nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := hexToBytes(tc.hex, tc.size)
			if !bytes.Equal(got, tc.want) {
				t.Errorf("hexToBytes(%q, %d) = %v, want %v", tc.hex, tc.size, got, tc.want)
			}
		})
	}
}

func TestConvertAnyValue(t *testing.T) {
	str := func(s string) *string { return &s }
	bl := func(b bool) *bool { return &b }

	tests := []struct {
		name      string
		input     AnyValue
		wantType  string
		wantStr   string
		wantInt   int64
		wantBool  bool
	}{
		{
			name:     "string value",
			input:    AnyValue{StringValue: str("hello")},
			wantType: "string",
			wantStr:  "hello",
		},
		{
			name:     "valid int value",
			input:    AnyValue{IntValue: str("42")},
			wantType: "int",
			wantInt:  42,
		},
		{
			name:     "invalid int falls back to string",
			input:    AnyValue{IntValue: str("not-a-number")},
			wantType: "string",
			wantStr:  "not-a-number",
		},
		{
			name:     "bool true",
			input:    AnyValue{BoolValue: bl(true)},
			wantType: "bool",
			wantBool: true,
		},
		{
			name:     "bool false",
			input:    AnyValue{BoolValue: bl(false)},
			wantType: "bool",
			wantBool: false,
		},
		{
			name:     "nil fields default to empty string",
			input:    AnyValue{},
			wantType: "string",
			wantStr:  "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := convertAnyValue(tc.input)
			switch tc.wantType {
			case "string":
				sv, ok := got.Value.(*commonpb.AnyValue_StringValue)
				if !ok {
					t.Fatalf("expected StringValue, got %T", got.Value)
				}
				if sv.StringValue != tc.wantStr {
					t.Errorf("StringValue = %q, want %q", sv.StringValue, tc.wantStr)
				}
			case "int":
				iv, ok := got.Value.(*commonpb.AnyValue_IntValue)
				if !ok {
					t.Fatalf("expected IntValue, got %T", got.Value)
				}
				if iv.IntValue != tc.wantInt {
					t.Errorf("IntValue = %d, want %d", iv.IntValue, tc.wantInt)
				}
			case "bool":
				bv, ok := got.Value.(*commonpb.AnyValue_BoolValue)
				if !ok {
					t.Fatalf("expected BoolValue, got %T", got.Value)
				}
				if bv.BoolValue != tc.wantBool {
					t.Errorf("BoolValue = %v, want %v", bv.BoolValue, tc.wantBool)
				}
			}
		})
	}
}

func TestParseUint64(t *testing.T) {
	tests := []struct {
		input string
		want  uint64
	}{
		{"0", 0},
		{"1234567890", 1234567890},
		{"", 0},
		{"not-a-number", 0},
	}
	for _, tc := range tests {
		got := parseUint64(tc.input)
		if got != tc.want {
			t.Errorf("parseUint64(%q) = %d, want %d", tc.input, got, tc.want)
		}
	}
}

func TestMarshalTraceProto_RoundTrip(t *testing.T) {
	ts := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	eventTs := ts.Add(50 * time.Millisecond)
	spans := []clickhouse.SpanRow{
		{
			Timestamp:          ts,
			TraceID:            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			SpanID:             "1111111111111111",
			SpanName:           "GET /api",
			SpanKind:           "SPAN_KIND_SERVER",
			ServiceName:        "frontend",
			ResourceAttributes: map[string]string{"service.name": "frontend"},
			ScopeName:          "otel-go",
			ScopeVersion:       "1.0.0",
			SpanAttributes:     map[string]string{"http.method": "GET"},
			Duration:           500000000,
			StatusCode:         "STATUS_CODE_OK",
			EventsTimestamp:    []time.Time{eventTs},
			EventsName:         []string{"log"},
			EventsAttributes:   []map[string]string{{"msg": "handled request"}},
		},
		{
			Timestamp:          ts.Add(10 * time.Millisecond),
			TraceID:            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			SpanID:             "2222222222222222",
			ParentSpanID:       "1111111111111111",
			SpanName:           "db.query",
			SpanKind:           "SPAN_KIND_CLIENT",
			ServiceName:        "backend",
			ResourceAttributes: map[string]string{"service.name": "backend"},
			ScopeName:          "otel-go",
			ScopeVersion:       "1.0.0",
			SpanAttributes:     map[string]string{"db.system": "postgres"},
			Duration:           100000000,
			StatusCode:         "STATUS_CODE_ERROR",
			StatusMessage:      "timeout",
		},
	}

	trace := BuildTrace(spans)
	data, err := MarshalTraceProto(trace)
	if err != nil {
		t.Fatalf("MarshalTraceProto() error: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("expected non-empty protobuf bytes")
	}

	var td tracepb.TracesData
	if err := proto.Unmarshal(data, &td); err != nil {
		t.Fatalf("proto.Unmarshal() error: %v", err)
	}

	if len(td.ResourceSpans) != 2 {
		t.Fatalf("expected 2 ResourceSpans, got %d", len(td.ResourceSpans))
	}

	// Batches sorted alphabetically: backend, frontend
	assertResourceService(t, td.ResourceSpans[0], "backend")
	assertResourceService(t, td.ResourceSpans[1], "frontend")

	// Verify frontend span details
	frontendRS := td.ResourceSpans[1]
	if len(frontendRS.ScopeSpans) != 1 {
		t.Fatalf("expected 1 ScopeSpans for frontend, got %d", len(frontendRS.ScopeSpans))
	}
	ss := frontendRS.ScopeSpans[0]
	if ss.Scope.Name != "otel-go" || ss.Scope.Version != "1.0.0" {
		t.Errorf("scope = %q/%q, want otel-go/1.0.0", ss.Scope.Name, ss.Scope.Version)
	}
	if len(ss.Spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(ss.Spans))
	}

	span := ss.Spans[0]
	wantTraceID := []byte{0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa}
	if !bytes.Equal(span.TraceId, wantTraceID) {
		t.Errorf("TraceId = %x, want %x", span.TraceId, wantTraceID)
	}
	if span.Name != "GET /api" {
		t.Errorf("Name = %q, want %q", span.Name, "GET /api")
	}
	if span.Kind != tracepb.Span_SPAN_KIND_SERVER {
		t.Errorf("Kind = %v, want SERVER", span.Kind)
	}
	if span.Status.Code != tracepb.Status_STATUS_CODE_OK {
		t.Errorf("Status.Code = %v, want OK", span.Status.Code)
	}
	if span.StartTimeUnixNano == 0 {
		t.Error("StartTimeUnixNano should not be zero")
	}
	if span.EndTimeUnixNano <= span.StartTimeUnixNano {
		t.Error("EndTimeUnixNano should be after StartTimeUnixNano")
	}

	// Verify event
	if len(span.Events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(span.Events))
	}
	if span.Events[0].Name != "log" {
		t.Errorf("event name = %q, want %q", span.Events[0].Name, "log")
	}

	// Verify backend span has error status
	backendSpan := td.ResourceSpans[0].ScopeSpans[0].Spans[0]
	if backendSpan.Status.Code != tracepb.Status_STATUS_CODE_ERROR {
		t.Errorf("backend status = %v, want ERROR", backendSpan.Status.Code)
	}
	if backendSpan.Status.Message != "timeout" {
		t.Errorf("backend status message = %q, want %q", backendSpan.Status.Message, "timeout")
	}
	if len(backendSpan.ParentSpanId) == 0 {
		t.Error("backend ParentSpanId should not be empty")
	}
}

func TestMarshalTraceProto_EmptyTrace(t *testing.T) {
	trace := &Trace{Batches: nil}
	data, err := MarshalTraceProto(trace)
	if err != nil {
		t.Fatalf("MarshalTraceProto() error: %v", err)
	}

	var td tracepb.TracesData
	if err := proto.Unmarshal(data, &td); err != nil {
		t.Fatalf("proto.Unmarshal() error: %v", err)
	}
	if len(td.ResourceSpans) != 0 {
		t.Errorf("expected 0 ResourceSpans, got %d", len(td.ResourceSpans))
	}
}

func TestMarshalTraceByIDResponseProto_Envelope(t *testing.T) {
	trace := &Trace{
		Batches: []ResourceSpans{
			{
				Resource: Resource{Attributes: []KeyValue{
					{Key: "service.name", Value: AnyValue{StringValue: strPtr("svc")}},
				}},
				ScopeSpans: []ScopeSpans{{
					Scope: InstrumentationScope{Name: "test"},
					Spans: []Span{{
						TraceID:           "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
						SpanID:            "1111111111111111",
						Name:              "op",
						Kind:              2,
						StartTimeUnixNano: "1000",
						EndTimeUnixNano:   "2000",
						Status:            SpanStatus{Code: 1},
					}},
				}},
			},
		},
	}

	data, err := MarshalTraceByIDResponseProto(trace)
	if err != nil {
		t.Fatalf("MarshalTraceByIDResponseProto() error: %v", err)
	}

	// The envelope should be: field 1 (LEN) = inner trace bytes.
	// Unwrap field 1 to get the inner trace, then decode as TracesData.
	fieldNum, wireType, n := protowire.ConsumeTag(data)
	if n < 0 {
		t.Fatal("failed to read tag")
	}
	if fieldNum != 1 || wireType != protowire.BytesType {
		t.Fatalf("expected field 1 LEN, got field %d wire type %d", fieldNum, wireType)
	}

	innerBytes, n2 := protowire.ConsumeBytes(data[n:])
	if n2 < 0 {
		t.Fatal("failed to read inner bytes")
	}

	var td tracepb.TracesData
	if err := proto.Unmarshal(innerBytes, &td); err != nil {
		t.Fatalf("inner trace unmarshal error: %v", err)
	}
	if len(td.ResourceSpans) != 1 {
		t.Fatalf("expected 1 ResourceSpans, got %d", len(td.ResourceSpans))
	}
	if td.ResourceSpans[0].ScopeSpans[0].Spans[0].Name != "op" {
		t.Errorf("span name = %q, want %q", td.ResourceSpans[0].ScopeSpans[0].Spans[0].Name, "op")
	}

	// Verify the entire response has only field 1 (no trailing garbage)
	if n+n2 != len(data) {
		t.Errorf("expected total consumed %d bytes, got %d", len(data), n+n2)
	}
}

func strPtr(s string) *string { return &s }

func TestConvertKeyValues_Empty(t *testing.T) {
	got := convertKeyValues(nil)
	if got != nil {
		t.Errorf("convertKeyValues(nil) = %v, want nil", got)
	}
	got = convertKeyValues([]KeyValue{})
	if got != nil {
		t.Errorf("convertKeyValues([]) = %v, want nil", got)
	}
}

func assertResourceService(t *testing.T, rs *tracepb.ResourceSpans, wantService string) {
	t.Helper()
	if rs.Resource == nil {
		t.Fatalf("resource is nil for expected service %q", wantService)
	}
	for _, kv := range rs.Resource.Attributes {
		if kv.Key == "service.name" {
			sv, ok := kv.Value.Value.(*commonpb.AnyValue_StringValue)
			if !ok {
				t.Fatalf("service.name is not a string")
			}
			if sv.StringValue != wantService {
				t.Errorf("service.name = %q, want %q", sv.StringValue, wantService)
			}
			return
		}
	}
	t.Errorf("service.name not found in resource attributes, want %q", wantService)
}
