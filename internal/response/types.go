// Package response provides Tempo-compatible response types and marshaling.
//
// These types mirror Tempo's protobuf/JSON response format exactly so that
// Grafana's Tempo datasource plugin can consume them directly.
package response

// ---------------------------------------------------------------------------
// Trace types (OTLP-compatible JSON format)
// ---------------------------------------------------------------------------

// Trace is the top-level trace response (OTLP format).
type Trace struct {
	Batches []ResourceSpans `json:"batches,omitempty"`
}

// ResourceSpans groups spans by resource.
type ResourceSpans struct {
	Resource   Resource     `json:"resource"`
	ScopeSpans []ScopeSpans `json:"scopeSpans,omitempty"`
}

// Resource holds resource attributes.
type Resource struct {
	Attributes []KeyValue `json:"attributes,omitempty"`
}

// ScopeSpans groups spans by instrumentation scope.
type ScopeSpans struct {
	Scope InstrumentationScope `json:"scope"`
	Spans []Span               `json:"spans,omitempty"`
}

// InstrumentationScope identifies the instrumentation library.
type InstrumentationScope struct {
	Name    string `json:"name,omitempty"`
	Version string `json:"version,omitempty"`
}

// Span is an OTLP span.
type Span struct {
	TraceID           string     `json:"traceId"`
	SpanID            string     `json:"spanId"`
	ParentSpanID      string     `json:"parentSpanId,omitempty"`
	Name              string     `json:"name"`
	Kind              int        `json:"kind"`
	StartTimeUnixNano string     `json:"startTimeUnixNano"`
	EndTimeUnixNano   string     `json:"endTimeUnixNano"`
	Attributes        []KeyValue `json:"attributes,omitempty"`
	Status            SpanStatus `json:"status"`
	Events            []Event    `json:"events,omitempty"`
	Links             []Link     `json:"links,omitempty"`
}

// SpanStatus holds the span status.
type SpanStatus struct {
	Code    int    `json:"code"`
	Message string `json:"message,omitempty"`
}

// Event is a span event.
type Event struct {
	TimeUnixNano string     `json:"timeUnixNano"`
	Name         string     `json:"name"`
	Attributes   []KeyValue `json:"attributes,omitempty"`
}

// Link is a span link.
type Link struct {
	TraceID    string     `json:"traceId"`
	SpanID     string     `json:"spanId"`
	Attributes []KeyValue `json:"attributes,omitempty"`
}

// KeyValue is an OTLP attribute key-value pair.
type KeyValue struct {
	Key   string   `json:"key"`
	Value AnyValue `json:"value"`
}

// AnyValue holds a typed value.
type AnyValue struct {
	StringValue *string `json:"stringValue,omitempty"`
	IntValue    *string `json:"intValue,omitempty"`
	BoolValue   *bool   `json:"boolValue,omitempty"`
}

// ---------------------------------------------------------------------------
// Search response types
// ---------------------------------------------------------------------------

// SearchResponse is the response for /api/search.
type SearchResponse struct {
	Traces  []TraceSearchMetadata `json:"traces"`
	Metrics SearchMetrics         `json:"metrics,omitempty"`
}

// TraceSearchMetadata is metadata for a single trace in search results.
type TraceSearchMetadata struct {
	TraceID           string                  `json:"traceID"`
	RootServiceName   string                  `json:"rootServiceName"`
	RootTraceName     string                  `json:"rootTraceName"`
	StartTimeUnixNano string                  `json:"startTimeUnixNano"`
	DurationMs        int                     `json:"durationMs"`
	SpanSets          []SpanSet               `json:"spanSets,omitempty"`
	ServiceStats      map[string]ServiceStats `json:"serviceStats,omitempty"`
}

// SpanSet is a set of matching spans within a trace.
type SpanSet struct {
	Spans      []SpanSetSpan   `json:"spans,omitempty"`
	Matched    int             `json:"matched"`
	Attributes []SpanAttribute `json:"attributes,omitempty"`
}

// SpanSetSpan is a span within a SpanSet.
type SpanSetSpan struct {
	SpanID            string          `json:"spanID"`
	Name              string          `json:"name,omitempty"`
	StartTimeUnixNano string          `json:"startTimeUnixNano"`
	DurationNanos     string          `json:"durationNanos"`
	Attributes        []SpanAttribute `json:"attributes,omitempty"`
}

// SpanAttribute is a key-value pair for span attributes in search results.
type SpanAttribute struct {
	Key   string         `json:"key"`
	Value AttributeValue `json:"value"`
}

// AttributeValue holds a typed value for search result attributes.
type AttributeValue struct {
	Type      string  `json:"type"`
	StringVal string  `json:"stringValue,omitempty"`
	IntVal    int64   `json:"intValue,omitempty"`
	FloatVal  float64 `json:"doubleValue,omitempty"`
	BoolVal   bool    `json:"boolValue,omitempty"`
}

// ServiceStats holds per-service statistics for a trace.
type ServiceStats struct {
	SpanCount  int `json:"spanCount"`
	ErrorCount int `json:"errorCount,omitempty"`
}

// SearchMetrics holds query execution metrics.
type SearchMetrics struct {
	InspectedTraces uint32 `json:"inspectedTraces,omitempty"`
	InspectedBytes  uint64 `json:"inspectedBytes,omitempty"`
	TotalBlocks     uint32 `json:"totalBlocks,omitempty"`
	InspectedSpans  uint64 `json:"inspectedSpans,omitempty"`
}

// ---------------------------------------------------------------------------
// Tag responses
// ---------------------------------------------------------------------------

// SearchTagsResponse is the response for /api/search/tags.
type SearchTagsResponse struct {
	TagNames []string `json:"tagNames"`
}

// SearchTagsV2Response is the response for /api/v2/search/tags.
type SearchTagsV2Response struct {
	Scopes []SearchTagsV2Scope `json:"scopes"`
}

// SearchTagsV2Scope holds tags for a single scope.
type SearchTagsV2Scope struct {
	Name string   `json:"name"`
	Tags []string `json:"tags"`
}

// SearchTagValuesResponse is the response for /api/search/tag/{tagName}/values.
type SearchTagValuesResponse struct {
	TagValues []string `json:"tagValues"`
}

// SearchTagValuesV2Response is the response for /api/v2/search/tag/{tagName}/values.
type SearchTagValuesV2Response struct {
	TagValues []TagValue `json:"tagValues"`
}

// TagValue holds a typed tag value.
type TagValue struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

// ---------------------------------------------------------------------------
// TraceByIDResponse (V2)
// ---------------------------------------------------------------------------

// TraceByIDResponse wraps a trace with metadata (V2 endpoint).
type TraceByIDResponse struct {
	Trace   *Trace `json:"trace,omitempty"`
	Status  string `json:"status,omitempty"`
	Message string `json:"message,omitempty"`
}

// ---------------------------------------------------------------------------
// Metrics responses
// ---------------------------------------------------------------------------

// QueryRangeResponse is the response for /api/metrics/query_range.
type QueryRangeResponse struct {
	Series  []TimeSeries  `json:"series"`
	Metrics SearchMetrics `json:"metrics,omitempty"`
}

// TimeSeries is a single time series.
type TimeSeries struct {
	Labels  []Label  `json:"labels"`
	Samples []Sample `json:"samples"`
}

// Label is a key-value label.
type Label struct {
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// Sample is a single data point.
type Sample struct {
	TimestampMs int64   `json:"timestampMs"`
	Value       float64 `json:"value"`
}

// QueryInstantResponse is the response for /api/metrics/query.
type QueryInstantResponse struct {
	Series  []InstantSeries `json:"series"`
	Metrics SearchMetrics   `json:"metrics,omitempty"`
}

// InstantSeries is a single instant result.
type InstantSeries struct {
	Labels []Label `json:"labels"`
	Value  float64 `json:"value"`
}

// SpanMetricsSummaryResponse is the response for /api/metrics/summary.
type SpanMetricsSummaryResponse struct {
	Summaries []SpanMetricsSummary `json:"summaries"`
}

// SpanMetricsSummary holds summary metrics for a group.
type SpanMetricsSummary struct {
	SpanCount      int     `json:"spanCount"`
	ErrorSpanCount int     `json:"errorSpanCount"`
	P99            float64 `json:"p99"`
	P95            float64 `json:"p95"`
	P90            float64 `json:"p90"`
	P50            float64 `json:"p50"`
	Series         []Label `json:"series,omitempty"`
}
