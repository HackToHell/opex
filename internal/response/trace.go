package response

import (
	"fmt"
	"sort"
	"strings"

	"github.com/hacktohell/opex/internal/clickhouse"
)

// BuildTrace converts ClickHouse span rows into an OTLP-compatible Trace.
func BuildTrace(spans []clickhouse.SpanRow) *Trace {
	if len(spans) == 0 {
		return nil
	}

	// Group spans by (ServiceName, ResourceAttributes) -> (ScopeName, ScopeVersion)
	type resourceKey struct {
		serviceName string
		resAttrsKey string
	}
	type scopeKey struct {
		name    string
		version string
	}

	// Build nested structure
	type scopeEntry struct {
		scope InstrumentationScope
		spans []Span
	}
	type resourceEntry struct {
		resource Resource
		scopes   map[scopeKey]*scopeEntry
	}

	resources := make(map[string]*resourceEntry) // keyed by serviceName

	for _, row := range spans {
		rk := row.ServiceName

		re, ok := resources[rk]
		if !ok {
			attrs := mapToKeyValues(row.ResourceAttributes)
			// Add service.name to resource attributes if not already present
			hasServiceName := false
			for _, a := range attrs {
				if a.Key == "service.name" {
					hasServiceName = true
					break
				}
			}
			if !hasServiceName {
				sn := row.ServiceName
				attrs = append([]KeyValue{{Key: "service.name", Value: AnyValue{StringValue: &sn}}}, attrs...)
			}
			re = &resourceEntry{
				resource: Resource{Attributes: attrs},
				scopes:   make(map[scopeKey]*scopeEntry),
			}
			resources[rk] = re
		}

		sk := scopeKey{name: row.ScopeName, version: row.ScopeVersion}
		se, ok := re.scopes[sk]
		if !ok {
			se = &scopeEntry{
				scope: InstrumentationScope{Name: row.ScopeName, Version: row.ScopeVersion},
			}
			re.scopes[sk] = se
		}

		span := spanRowToOTLP(row)
		se.spans = append(se.spans, span)
	}

	// Convert to output format
	var batches []ResourceSpans
	for _, re := range resources {
		var scopeSpans []ScopeSpans
		for _, se := range re.scopes {
			scopeSpans = append(scopeSpans, ScopeSpans{
				Scope: se.scope,
				Spans: se.spans,
			})
		}
		batches = append(batches, ResourceSpans{
			Resource:   re.resource,
			ScopeSpans: scopeSpans,
		})
	}

	// Sort batches by service name for deterministic output
	sort.Slice(batches, func(i, j int) bool {
		iName := getServiceName(batches[i].Resource.Attributes)
		jName := getServiceName(batches[j].Resource.Attributes)
		return iName < jName
	})

	return &Trace{Batches: batches}
}

func spanRowToOTLP(row clickhouse.SpanRow) Span {
	startNano := row.Timestamp.UnixNano()
	endNano := startNano + int64(row.Duration)

	span := Span{
		TraceID:           row.TraceId,
		SpanID:            row.SpanId,
		ParentSpanID:      row.ParentSpanId,
		Name:              row.SpanName,
		Kind:              spanKindToInt(row.SpanKind),
		StartTimeUnixNano: fmt.Sprintf("%d", startNano),
		EndTimeUnixNano:   fmt.Sprintf("%d", endNano),
		Attributes:        mapToKeyValues(row.SpanAttributes),
		Status:            SpanStatus{Code: statusCodeToInt(row.StatusCode), Message: row.StatusMessage},
	}

	// Events
	for i := 0; i < len(row.EventsName) && i < len(row.EventsTimestamp); i++ {
		ev := Event{
			TimeUnixNano: fmt.Sprintf("%d", row.EventsTimestamp[i].UnixNano()),
			Name:         row.EventsName[i],
		}
		if i < len(row.EventsAttributes) {
			ev.Attributes = mapToKeyValues(row.EventsAttributes[i])
		}
		span.Events = append(span.Events, ev)
	}

	// Links
	for i := 0; i < len(row.LinksTraceId) && i < len(row.LinksSpanId); i++ {
		lnk := Link{
			TraceID: row.LinksTraceId[i],
			SpanID:  row.LinksSpanId[i],
		}
		if i < len(row.LinksAttributes) {
			lnk.Attributes = mapToKeyValues(row.LinksAttributes[i])
		}
		span.Links = append(span.Links, lnk)
	}

	return span
}

func mapToKeyValues(m map[string]string) []KeyValue {
	if len(m) == 0 {
		return nil
	}
	kvs := make([]KeyValue, 0, len(m))
	for k, v := range m {
		val := v
		kvs = append(kvs, KeyValue{Key: k, Value: AnyValue{StringValue: &val}})
	}
	sort.Slice(kvs, func(i, j int) bool { return kvs[i].Key < kvs[j].Key })
	return kvs
}

func spanKindToInt(kind string) int {
	switch strings.ToUpper(kind) {
	case "SPAN_KIND_INTERNAL":
		return 1
	case "SPAN_KIND_SERVER":
		return 2
	case "SPAN_KIND_CLIENT":
		return 3
	case "SPAN_KIND_PRODUCER":
		return 4
	case "SPAN_KIND_CONSUMER":
		return 5
	default:
		return 0
	}
}

func statusCodeToInt(code string) int {
	switch strings.ToUpper(code) {
	case "STATUS_CODE_OK":
		return 1
	case "STATUS_CODE_ERROR":
		return 2
	default:
		return 0 // UNSET
	}
}

func getServiceName(attrs []KeyValue) string {
	for _, a := range attrs {
		if a.Key == "service.name" && a.Value.StringValue != nil {
			return *a.Value.StringValue
		}
	}
	return ""
}
