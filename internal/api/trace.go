package api

import (
	"encoding/hex"
	"log/slog"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	"github.com/hacktohell/opex/internal/clickhouse"
	"github.com/hacktohell/opex/internal/response"
)

// TraceHandlers holds handlers for trace-by-ID endpoints.
type TraceHandlers struct {
	ch     *clickhouse.Client
	logger *slog.Logger
}

// NewTraceHandlers creates new TraceHandlers.
func NewTraceHandlers(ch *clickhouse.Client, logger *slog.Logger) *TraceHandlers {
	return &TraceHandlers{ch: ch, logger: logger}
}

// TraceByID handles GET /api/traces/{traceID}.
func (h *TraceHandlers) TraceByID(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	traceID := vars["traceID"]

	if traceID == "" {
		response.WriteError(w, http.StatusBadRequest, "traceID is required")
		return
	}

	// Validate and normalize trace ID (hex-encoded, 16 or 32 bytes)
	traceID = normalizeTraceID(traceID)
	if !isValidHexTraceID(traceID) {
		response.WriteError(w, http.StatusBadRequest, "invalid traceID format")
		return
	}

	spans, err := h.ch.QueryTraceByID(r.Context(), traceID)
	if err != nil {
		h.logger.Error("failed to query trace", "traceID", traceID, "error", err)
		response.WriteError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	if len(spans) == 0 {
		response.WriteError(w, http.StatusNotFound, "trace not found")
		return
	}

	trace := response.BuildTrace(spans)
	if err := response.WriteTrace(w, r, http.StatusOK, trace); err != nil {
		h.logger.Error("failed to marshal trace response", "traceID", traceID, "error", err)
		response.WriteError(w, http.StatusInternalServerError, "internal server error")
	}
}

// TraceByIDV2 handles GET /api/v2/traces/{traceID}.
func (h *TraceHandlers) TraceByIDV2(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	traceID := vars["traceID"]

	if traceID == "" {
		response.WriteError(w, http.StatusBadRequest, "traceID is required")
		return
	}

	traceID = normalizeTraceID(traceID)
	if !isValidHexTraceID(traceID) {
		response.WriteError(w, http.StatusBadRequest, "invalid traceID format")
		return
	}

	spans, err := h.ch.QueryTraceByID(r.Context(), traceID)
	if err != nil {
		h.logger.Error("failed to query trace", "traceID", traceID, "error", err)
		response.WriteError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	var trace *response.Trace
	if len(spans) > 0 {
		trace = response.BuildTrace(spans)
	}

	resp := &response.TraceByIDResponse{
		Trace:  trace,
		Status: "complete",
	}
	if err := response.WriteTraceByIDResponse(w, r, http.StatusOK, resp); err != nil {
		h.logger.Error("failed to marshal trace-by-id response", "traceID", traceID, "error", err)
		response.WriteError(w, http.StatusInternalServerError, "internal server error")
	}
}

// normalizeTraceID strips hyphens and lowercases the trace ID.
func normalizeTraceID(id string) string {
	id = strings.ReplaceAll(id, "-", "")
	return strings.ToLower(id)
}

// isValidHexTraceID checks if a trace ID is valid hex (16 or 32 bytes).
func isValidHexTraceID(id string) bool {
	if len(id) != 32 && len(id) != 16 {
		return false
	}
	_, err := hex.DecodeString(id)
	return err == nil
}
