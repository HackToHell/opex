// Package tracequery provides shared service functions for querying trace data.
// Both the HTTP handlers and the MCP server call these functions, eliminating
// logic duplication.
package tracequery

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hacktohell/opex/internal/clickhouse"
	"github.com/hacktohell/opex/internal/config"
	"github.com/hacktohell/opex/internal/response"
	"github.com/hacktohell/opex/internal/traceql"
	"github.com/hacktohell/opex/internal/transpiler"
)

// InputError represents a user input error (invalid query, bad parameters).
// HTTP handlers should return 400 for these rather than 500.
type InputError struct {
	Err error
}

func (e *InputError) Error() string { return e.Err.Error() }
func (e *InputError) Unwrap() error { return e.Err }

// newInputError wraps err as an InputError.
func newInputError(err error) error {
	return &InputError{Err: err}
}

// escapeSQL escapes single quotes and backslashes in a string for safe
// interpolation into ClickHouse SQL. Used for attribute names and values.
func escapeSQL(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	return strings.ReplaceAll(s, "'", "\\'")
}

// IsInputError reports whether err (or any error in its chain) is an InputError.
func IsInputError(err error) bool {
	var ie *InputError
	return errors.As(err, &ie)
}

// SearchTraces parses a TraceQL query, transpiles it to SQL, executes against
// ClickHouse, and returns a Tempo-compatible search response.
//
// When materialized views are enabled, metadata (root service/name, duration,
// error count) is read from the pre-aggregated trace metadata table and
// service stats come from a lightweight GROUP BY query, avoiding a full span
// fetch. Raw spans are only retrieved when spss > 0 (for SpanSets).
func SearchTraces(ctx context.Context, ch *clickhouse.Client, cfg config.QueryConfig,
	query string, start, end time.Time, limit, spss int,
	minDuration, maxDuration time.Duration,
) (*response.SearchResponse, error) {
	root, err := traceql.Parse(query)
	if err != nil {
		return nil, newInputError(fmt.Errorf("invalid TraceQL query: %w", err))
	}

	opts := transpiler.TranspileOptions{
		Table: ch.Table(),
		Start: start,
		End:   end,
		Limit: limit,
	}
	result, err := transpiler.Transpile(root, opts)
	if err != nil {
		return nil, newInputError(fmt.Errorf("transpile error: %w", err))
	}

	traceIDs, err := ch.QueryTraceIDs(ctx, result.SQL)
	if err != nil {
		return nil, fmt.Errorf("search query failed: %w", err)
	}

	if len(traceIDs) == 0 {
		return &response.SearchResponse{
			Traces:  []response.TraceSearchMetadata{},
			Metrics: response.SearchMetrics{},
		}, nil
	}

	if ch.UseMatViews() {
		return searchWithMatViews(ctx, ch, traceIDs, spss, minDuration, maxDuration)
	}

	spans, err := ch.QuerySpansByTraceIDs(ctx, traceIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch trace details: %w", err)
	}

	resp := buildSearchResponse(spans, traceIDs, spss, minDuration, maxDuration)
	return resp, nil
}

// searchWithMatViews builds the search response using the trace metadata
// materialized table for core fields and a lightweight GROUP BY for
// service stats. Raw spans are fetched only when spss > 0.
func searchWithMatViews(ctx context.Context, ch *clickhouse.Client,
	traceIDs []string, spss int, minDur, maxDur time.Duration,
) (*response.SearchResponse, error) {
	metaRows, err := ch.QueryTraceMetadataByTraceIDs(ctx, traceIDs)
	if err != nil {
		return nil, fmt.Errorf("trace metadata query failed: %w", err)
	}

	svcRows, err := ch.QueryServiceStatsByTraceIDs(ctx, traceIDs)
	if err != nil {
		return nil, fmt.Errorf("service stats query failed: %w", err)
	}

	// Index service stats by TraceId.
	type perTraceSvc = map[string]response.ServiceStats
	svcByTrace := make(map[string]perTraceSvc, len(traceIDs))
	for _, s := range svcRows {
		m, ok := svcByTrace[s.TraceID]
		if !ok {
			m = make(perTraceSvc)
			svcByTrace[s.TraceID] = m
		}
		m[s.ServiceName] = response.ServiceStats{
			SpanCount:  int(s.SpanCount),
			ErrorCount: int(s.ErrorCount),
		}
	}

	// Optionally fetch spans for SpanSets.
	var spansByTrace map[string][]clickhouse.SpanRow
	if spss > 0 {
		spans, err := ch.QuerySpansByTraceIDs(ctx, traceIDs)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch spans for spansets: %w", err)
		}
		spansByTrace = make(map[string][]clickhouse.SpanRow, len(traceIDs))
		for _, s := range spans {
			spansByTrace[s.TraceID] = append(spansByTrace[s.TraceID], s)
		}
	}

	// Index metadata by TraceId so we can iterate traceIDs in the order
	// returned by the upstream TraceQL query, matching the non-MV path.
	metaByTrace := make(map[string]*clickhouse.TraceMetadataRow, len(metaRows))
	var totalSpans uint64
	for i := range metaRows {
		metaByTrace[metaRows[i].TraceID] = &metaRows[i]
		totalSpans += metaRows[i].SpanCount
	}

	var traces []response.TraceSearchMetadata
	for _, tid := range traceIDs {
		m, ok := metaByTrace[tid]
		if !ok {
			continue
		}

		startNano := m.StartTime.UnixNano()
		durationMs := int((m.MaxEndNano - startNano) / 1_000_000)
		if durationMs < 0 {
			durationMs = 0
		}

		if minDur > 0 && time.Duration(durationMs)*time.Millisecond < minDur {
			continue
		}
		if maxDur > 0 && time.Duration(durationMs)*time.Millisecond > maxDur {
			continue
		}

		meta := response.TraceSearchMetadata{
			TraceID:           m.TraceID,
			RootServiceName:   m.RootServiceName,
			RootTraceName:     m.RootSpanName,
			StartTimeUnixNano: fmt.Sprintf("%d", startNano),
			DurationMs:        durationMs,
		}

		if ss := svcByTrace[m.TraceID]; len(ss) > 0 {
			meta.ServiceStats = ss
		}

		if spss > 0 {
			if rows := spansByTrace[m.TraceID]; len(rows) > 0 {
				meta.SpanSets = buildSpanSets(rows, spss)
			}
		}

		traces = append(traces, meta)
	}

	if traces == nil {
		traces = []response.TraceSearchMetadata{}
	}

	return &response.SearchResponse{
		Traces: traces,
		Metrics: response.SearchMetrics{
			InspectedTraces: uint32(len(traceIDs)),
			InspectedSpans:  totalSpans,
		},
	}, nil
}

// buildSpanSets constructs SpanSets from raw span rows, limited to spss spans.
func buildSpanSets(spans []clickhouse.SpanRow, spss int) []response.SpanSet {
	limit := spss
	if limit > len(spans) {
		limit = len(spans)
	}
	var setSpans []response.SpanSetSpan
	for i := 0; i < limit; i++ {
		s := spans[i]
		setSpans = append(setSpans, response.SpanSetSpan{
			SpanID:            s.SpanID,
			Name:              s.SpanName,
			StartTimeUnixNano: fmt.Sprintf("%d", s.Timestamp.UnixNano()),
			DurationNanos:     fmt.Sprintf("%d", s.Duration),
		})
	}
	return []response.SpanSet{{Spans: setSpans, Matched: len(spans)}}
}

// BuildSearchResponseFromSpans builds a SearchResponse from pre-fetched spans.
// Exported for use by tests.
func BuildSearchResponseFromSpans(spans []clickhouse.SpanRow, traceIDs []string, spss int, minDur, maxDur time.Duration) *response.SearchResponse {
	return buildSearchResponse(spans, traceIDs, spss, minDur, maxDur)
}

func buildSearchResponse(spans []clickhouse.SpanRow, traceIDs []string, spss int, minDur, maxDur time.Duration) *response.SearchResponse {
	// Group spans by TraceID
	traceSpans := make(map[string][]clickhouse.SpanRow)
	for _, s := range spans {
		traceSpans[s.TraceID] = append(traceSpans[s.TraceID], s)
	}

	var traces []response.TraceSearchMetadata
	for _, tid := range traceIDs {
		spanRows, ok := traceSpans[tid]
		if !ok || len(spanRows) == 0 {
			continue
		}

		meta := buildTraceMetadata(spanRows, spss)

		// Apply duration filters
		if minDur > 0 && time.Duration(meta.DurationMs)*time.Millisecond < minDur {
			continue
		}
		if maxDur > 0 && time.Duration(meta.DurationMs)*time.Millisecond > maxDur {
			continue
		}

		traces = append(traces, meta)
	}

	if traces == nil {
		traces = []response.TraceSearchMetadata{}
	}

	return &response.SearchResponse{
		Traces: traces,
		Metrics: response.SearchMetrics{
			InspectedTraces: uint32(len(traceIDs)),
			InspectedSpans:  uint64(len(spans)),
		},
	}
}

func buildTraceMetadata(spans []clickhouse.SpanRow, spss int) response.TraceSearchMetadata {
	meta := response.TraceSearchMetadata{
		TraceID: spans[0].TraceID,
	}

	var minTime, maxTime time.Time
	serviceStats := make(map[string]*response.ServiceStats)

	for _, s := range spans {
		// Track root span
		if s.ParentSpanID == "" {
			meta.RootServiceName = s.ServiceName
			meta.RootTraceName = s.SpanName
		}

		// Track time range
		if minTime.IsZero() || s.Timestamp.Before(minTime) {
			minTime = s.Timestamp
		}
		endTime := s.Timestamp.Add(time.Duration(s.Duration))
		if maxTime.IsZero() || endTime.After(maxTime) {
			maxTime = endTime
		}

		// Track service stats
		ss, ok := serviceStats[s.ServiceName]
		if !ok {
			ss = &response.ServiceStats{}
			serviceStats[s.ServiceName] = ss
		}
		ss.SpanCount++
		if s.StatusCode == "STATUS_CODE_ERROR" {
			ss.ErrorCount++
		}
	}

	meta.StartTimeUnixNano = fmt.Sprintf("%d", minTime.UnixNano())
	meta.DurationMs = int(maxTime.Sub(minTime).Milliseconds())

	// Build service stats map
	if len(serviceStats) > 0 {
		meta.ServiceStats = make(map[string]response.ServiceStats)
		for svc, ss := range serviceStats {
			meta.ServiceStats[svc] = *ss
		}
	}

	// Build span sets (limit by spss)
	if spss > 0 {
		var spanSetSpans []response.SpanSetSpan
		limit := spss
		if limit > len(spans) {
			limit = len(spans)
		}
		for i := 0; i < limit; i++ {
			s := spans[i]
			spanSetSpans = append(spanSetSpans, response.SpanSetSpan{
				SpanID:            s.SpanID,
				Name:              s.SpanName,
				StartTimeUnixNano: fmt.Sprintf("%d", s.Timestamp.UnixNano()),
				DurationNanos:     fmt.Sprintf("%d", s.Duration),
			})
		}
		meta.SpanSets = []response.SpanSet{
			{
				Spans:   spanSetSpans,
				Matched: len(spans),
			},
		}
	}

	return meta
}
