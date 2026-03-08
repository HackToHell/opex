package response

import (
	"encoding/hex"
	"strconv"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// MarshalTraceProto converts a Trace to its protobuf wire representation.
// The output is wire-compatible with Tempo's tempopb.Trace (both use field 1
// for the repeated ResourceSpans).
func MarshalTraceProto(t *Trace) ([]byte, error) {
	td := &tracepb.TracesData{
		ResourceSpans: make([]*tracepb.ResourceSpans, 0, len(t.Batches)),
	}
	for _, batch := range t.Batches {
		td.ResourceSpans = append(td.ResourceSpans, convertResourceSpans(batch))
	}
	return proto.Marshal(td)
}

// MarshalTraceByIDResponseProto produces a wire-compatible
// tempopb.TraceByIDResponse:
//
//	message TraceByIDResponse {
//	    Trace trace = 1;                // field 1
//	    TraceByIDMetrics metrics = 2;   // field 2 (omitted)
//	    PartialStatus status = 3;       // field 3: 0=COMPLETE (proto3 default, omitted)
//	    string message = 4;             // field 4 (omitted)
//	}
func MarshalTraceByIDResponseProto(t *Trace) ([]byte, error) {
	traceBytes, err := MarshalTraceProto(t)
	if err != nil {
		return nil, err
	}
	var buf []byte
	buf = protowire.AppendTag(buf, 1, protowire.BytesType)
	buf = protowire.AppendBytes(buf, traceBytes)
	return buf, nil
}

func convertResourceSpans(rs ResourceSpans) *tracepb.ResourceSpans {
	pbRS := &tracepb.ResourceSpans{
		Resource: &resourcepb.Resource{
			Attributes: convertKeyValues(rs.Resource.Attributes),
		},
		ScopeSpans: make([]*tracepb.ScopeSpans, 0, len(rs.ScopeSpans)),
	}
	for _, ss := range rs.ScopeSpans {
		pbRS.ScopeSpans = append(pbRS.ScopeSpans, convertScopeSpans(ss))
	}
	return pbRS
}

func convertScopeSpans(ss ScopeSpans) *tracepb.ScopeSpans {
	pbSS := &tracepb.ScopeSpans{
		Scope: &commonpb.InstrumentationScope{
			Name:    ss.Scope.Name,
			Version: ss.Scope.Version,
		},
		Spans: make([]*tracepb.Span, 0, len(ss.Spans)),
	}
	for i := range ss.Spans {
		pbSS.Spans = append(pbSS.Spans, convertSpan(&ss.Spans[i]))
	}
	return pbSS
}

func convertSpan(s *Span) *tracepb.Span {
	pbSpan := &tracepb.Span{
		TraceId:           hexToBytes(s.TraceID, 16),
		SpanId:            hexToBytes(s.SpanID, 8),
		ParentSpanId:      hexToBytes(s.ParentSpanID, 8),
		Name:              s.Name,
		Kind:              tracepb.Span_SpanKind(s.Kind),
		StartTimeUnixNano: parseUint64(s.StartTimeUnixNano),
		EndTimeUnixNano:   parseUint64(s.EndTimeUnixNano),
		Attributes:        convertKeyValues(s.Attributes),
		Status: &tracepb.Status{
			Code:    tracepb.Status_StatusCode(s.Status.Code),
			Message: s.Status.Message,
		},
	}
	for i := range s.Events {
		pbSpan.Events = append(pbSpan.Events, convertEvent(&s.Events[i]))
	}
	for i := range s.Links {
		pbSpan.Links = append(pbSpan.Links, convertLink(&s.Links[i]))
	}
	return pbSpan
}

func convertEvent(e *Event) *tracepb.Span_Event {
	return &tracepb.Span_Event{
		TimeUnixNano: parseUint64(e.TimeUnixNano),
		Name:         e.Name,
		Attributes:   convertKeyValues(e.Attributes),
	}
}

func convertLink(l *Link) *tracepb.Span_Link {
	return &tracepb.Span_Link{
		TraceId:    hexToBytes(l.TraceID, 16),
		SpanId:     hexToBytes(l.SpanID, 8),
		Attributes: convertKeyValues(l.Attributes),
	}
}

func convertKeyValues(kvs []KeyValue) []*commonpb.KeyValue {
	if len(kvs) == 0 {
		return nil
	}
	out := make([]*commonpb.KeyValue, 0, len(kvs))
	for _, kv := range kvs {
		out = append(out, &commonpb.KeyValue{
			Key:   kv.Key,
			Value: convertAnyValue(kv.Value),
		})
	}
	return out
}

func convertAnyValue(v AnyValue) *commonpb.AnyValue {
	switch {
	case v.StringValue != nil:
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: *v.StringValue}}
	case v.IntValue != nil:
		n, err := strconv.ParseInt(*v.IntValue, 10, 64)
		if err != nil {
			return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: *v.IntValue}}
		}
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: n}}
	case v.BoolValue != nil:
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: *v.BoolValue}}
	default:
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: ""}}
	}
}

func hexToBytes(s string, size int) []byte {
	if s == "" {
		return nil
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return nil
	}
	if len(b) < size {
		padded := make([]byte, size)
		copy(padded[size-len(b):], b)
		return padded
	}
	return b
}

func parseUint64(s string) uint64 {
	n, _ := strconv.ParseUint(s, 10, 64)
	return n
}
