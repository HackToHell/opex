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
func SearchTraces(ctx context.Context, ch *clickhouse.Client, cfg config.QueryConfig,
	query string, start, end time.Time, limit, spss int,
	minDuration, maxDuration time.Duration,
) (*response.SearchResponse, error) {
	// Parse TraceQL
	root, err := traceql.Parse(query)
	if err != nil {
		return nil, newInputError(fmt.Errorf("invalid TraceQL query: %w", err))
	}

	// Transpile to SQL
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

	// Execute query to get matching TraceIds
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

	// Fetch spans for the matched trace IDs
	spans, err := ch.QuerySpansByTraceIDs(ctx, traceIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch trace details: %w", err)
	}

	// Build search response
	resp := buildSearchResponse(spans, traceIDs, spss, minDuration, maxDuration)
	return resp, nil
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
