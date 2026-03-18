package api

import (
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/hacktohell/opex/internal/clickhouse"
	"github.com/hacktohell/opex/internal/config"
	"github.com/hacktohell/opex/internal/response"
	"github.com/hacktohell/opex/internal/tracequery"
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

	// Parse duration filters
	var minDur, maxDur time.Duration
	if minDurStr != "" {
		minDur, _ = time.ParseDuration(minDurStr)
	}
	if maxDurStr != "" {
		maxDur, _ = time.ParseDuration(maxDurStr)
	}

	// Call shared service
	result, err := tracequery.SearchTraces(r.Context(), h.ch, h.cfg,
		q, start, end, limit, spss, minDur, maxDur)
	if err != nil {
		h.logger.Error("search query failed", "query", q, "error", err)
		writeDBError(w, err, "search query failed")
		return
	}

	response.WriteJSON(w, http.StatusOK, result)
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
