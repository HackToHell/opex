package api

import (
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/hacktohell/opex/internal/clickhouse"
	"github.com/hacktohell/opex/internal/config"
	"github.com/hacktohell/opex/internal/response"
	"github.com/hacktohell/opex/internal/tracequery"
)

// MetricsHandlers holds handlers for metrics endpoints.
type MetricsHandlers struct {
	ch     *clickhouse.Client
	cfg    config.QueryConfig
	logger *slog.Logger
}

// NewMetricsHandlers creates new MetricsHandlers.
func NewMetricsHandlers(ch *clickhouse.Client, cfg config.QueryConfig, logger *slog.Logger) *MetricsHandlers {
	return &MetricsHandlers{ch: ch, cfg: cfg, logger: logger}
}

// QueryRange handles GET /api/metrics/query_range.
func (h *MetricsHandlers) QueryRange(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	if q == "" {
		q = r.URL.Query().Get("query")
	}
	if q == "" {
		response.WriteError(w, http.StatusBadRequest, "query parameter 'q' is required")
		return
	}

	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")
	stepStr := r.URL.Query().Get("step")

	start, end := parseTimeRange(startStr, endStr)
	step := parseStep(stepStr, start, end)

	result, err := tracequery.MetricsQueryRange(r.Context(), h.ch, h.cfg,
		q, start, end, step)
	if err != nil {
		h.logger.Error("metrics query failed", "query", q, "error", err)
		writeDBError(w, err, "query execution failed")
		return
	}

	response.WriteJSON(w, http.StatusOK, result)
}

// QueryInstant handles GET /api/metrics/query.
func (h *MetricsHandlers) QueryInstant(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	if q == "" {
		q = r.URL.Query().Get("query")
	}
	if q == "" {
		response.WriteError(w, http.StatusBadRequest, "query parameter 'q' is required")
		return
	}

	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")
	start, end := parseTimeRange(startStr, endStr)

	result, err := tracequery.MetricsQueryInstant(r.Context(), h.ch, h.cfg,
		q, start, end)
	if err != nil {
		h.logger.Error("instant query failed", "query", q, "error", err)
		writeDBError(w, err, "query execution failed")
		return
	}

	response.WriteJSON(w, http.StatusOK, result)
}

// MetricsSummary handles GET /api/metrics/summary.
func (h *MetricsHandlers) MetricsSummary(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	groupBy := r.URL.Query().Get("groupBy")
	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")
	limitStr := r.URL.Query().Get("limit")

	start, end := parseTimeRange(startStr, endStr)
	limit := 10
	if limitStr != "" {
		if v, err := strconv.Atoi(limitStr); err == nil && v > 0 {
			limit = v
		}
	}

	var groupBySlice []string
	if groupBy != "" {
		for _, g := range strings.Split(groupBy, ",") {
			g = strings.TrimSpace(g)
			if g != "" {
				groupBySlice = append(groupBySlice, g)
			}
		}
	}

	result, err := tracequery.MetricsSummary(r.Context(), h.ch,
		q, groupBySlice, start, end, limit)
	if err != nil {
		h.logger.Error("summary query failed", "query", q, "error", err)
		writeDBError(w, err, "query execution failed")
		return
	}

	response.WriteJSON(w, http.StatusOK, result)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func parseStep(stepStr string, start, end time.Time) time.Duration {
	if stepStr != "" {
		// Try as duration
		if d, err := time.ParseDuration(stepStr); err == nil {
			return d
		}
		// Try as seconds
		if v, err := strconv.ParseInt(stepStr, 10, 64); err == nil {
			return time.Duration(v) * time.Second
		}
	}
	// Auto-calculate: aim for ~100 data points
	totalDur := end.Sub(start)
	step := totalDur / 100
	if step < time.Second {
		step = time.Second
	}
	return step
}
