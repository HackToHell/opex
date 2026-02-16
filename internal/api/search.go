package api

import (
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/hacktohell/opex/internal/clickhouse"
	"github.com/hacktohell/opex/internal/config"
	"github.com/hacktohell/opex/internal/response"
	"github.com/hacktohell/opex/internal/traceql"
	"github.com/hacktohell/opex/internal/transpiler"
)

// SearchHandlers holds handlers for search endpoints.
type SearchHandlers struct {
	ch     *clickhouse.Client
	cfg    config.QueryConfig
	logger *slog.Logger
}

// NewSearchHandlers creates new SearchHandlers.
func NewSearchHandlers(ch *clickhouse.Client, cfg config.QueryConfig, logger *slog.Logger) *SearchHandlers {
	return &SearchHandlers{ch: ch, cfg: cfg, logger: logger}
}

// Search handles GET /api/search.
func (h *SearchHandlers) Search(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	limitStr := r.URL.Query().Get("limit")
	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")
	minDurStr := r.URL.Query().Get("minDuration")
	maxDurStr := r.URL.Query().Get("maxDuration")
	spssStr := r.URL.Query().Get("spss")

	// Parse limit
	limit := h.cfg.DefaultLimit
	if limitStr != "" {
		if v, err := strconv.Atoi(limitStr); err == nil && v > 0 {
			limit = v
		}
	}
	if limit > h.cfg.MaxLimit {
		limit = h.cfg.MaxLimit
	}

	// Parse SPSS
	spss := h.cfg.DefaultSpss
	if spssStr != "" {
		if v, err := strconv.Atoi(spssStr); err == nil && v >= 0 {
			spss = v
		}
	}

	// Parse time range
	start, end := parseTimeRange(startStr, endStr)

	// If no query provided, default to empty filter
	if q == "" {
		q = "{ }"
	}

	// Parse TraceQL
	root, err := traceql.Parse(q)
	if err != nil {
		response.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid TraceQL query: %v", err))
		return
	}

	// Transpile to SQL
	opts := transpiler.TranspileOptions{
		Table: h.ch.Table(),
		Start: start,
		End:   end,
		Limit: limit,
	}
	result, err := transpiler.Transpile(root, opts)
	if err != nil {
		response.WriteError(w, http.StatusBadRequest, fmt.Sprintf("transpile error: %v", err))
		return
	}

	// Execute query to get matching TraceIds
	traceIDs, err := h.ch.QueryTraceIDs(r.Context(), result.SQL)
	if err != nil {
		h.logger.Error("search query failed", "sql", result.SQL, "error", err)
		response.WriteError(w, http.StatusInternalServerError, "query execution failed")
		return
	}

	if len(traceIDs) == 0 {
		response.WriteJSON(w, http.StatusOK, &response.SearchResponse{
			Traces:  []response.TraceSearchMetadata{},
			Metrics: response.SearchMetrics{},
		})
		return
	}

	// Fetch spans for the matched trace IDs
	spans, err := h.ch.QuerySpansByTraceIDs(r.Context(), traceIDs)
	if err != nil {
		h.logger.Error("fetch spans failed", "error", err)
		response.WriteError(w, http.StatusInternalServerError, "failed to fetch trace details")
		return
	}

	// Build search response
	resp := buildSearchResponse(spans, traceIDs, q, spss, minDurStr, maxDurStr)
	response.WriteJSON(w, http.StatusOK, resp)
}

func buildSearchResponse(spans []clickhouse.SpanRow, traceIDs []string, _ string, spss int, minDurStr, maxDurStr string) *response.SearchResponse {
	// Group spans by TraceID
	traceSpans := make(map[string][]clickhouse.SpanRow)
	for _, s := range spans {
		traceSpans[s.TraceID] = append(traceSpans[s.TraceID], s)
	}

	// Parse duration filters
	var minDur, maxDur time.Duration
	if minDurStr != "" {
		minDur, _ = time.ParseDuration(minDurStr)
	}
	if maxDurStr != "" {
		maxDur, _ = time.ParseDuration(maxDurStr)
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

func parseTimeRange(startStr, endStr string) (time.Time, time.Time) {
	var start, end time.Time

	if startStr != "" {
		if v, err := strconv.ParseInt(startStr, 10, 64); err == nil {
			start = time.Unix(v, 0)
		}
	}
	if endStr != "" {
		if v, err := strconv.ParseInt(endStr, 10, 64); err == nil {
			end = time.Unix(v, 0)
		}
	}

	// Default: last 1 hour
	switch {
	case start.IsZero() && end.IsZero():
		end = time.Now()
		start = end.Add(-1 * time.Hour)
	case start.IsZero():
		start = end.Add(-1 * time.Hour)
	case end.IsZero():
		end = time.Now()
	}

	return start, end
}
