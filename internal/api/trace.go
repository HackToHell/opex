package api

import (
	"log/slog"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/hacktohell/opex/internal/clickhouse"
	"github.com/hacktohell/opex/internal/response"
	"github.com/hacktohell/opex/internal/tracequery"
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
	traceID = tracequery.NormalizeTraceID(traceID)
	if !tracequery.IsValidHexTraceID(traceID) {
		response.WriteError(w, http.StatusBadRequest, "invalid traceID format")
		return
	}

	// Call shared service
	trace, err := tracequery.GetTraceByID(r.Context(), h.ch, traceID)
	if err != nil {
		h.logger.Error("failed to query trace", "traceID", traceID, "error", err)
		writeDBError(w, err, "internal server error")
		return
	}

	if trace == nil {
		response.WriteError(w, http.StatusNotFound, "trace not found")
		return
	}

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

	traceID = tracequery.NormalizeTraceID(traceID)
	if !tracequery.IsValidHexTraceID(traceID) {
		response.WriteError(w, http.StatusBadRequest, "invalid traceID format")
		return
	}

	// Call shared service
	trace, err := tracequery.GetTraceByID(r.Context(), h.ch, traceID)
	if err != nil {
		h.logger.Error("failed to query trace", "traceID", traceID, "error", err)
		writeDBError(w, err, "internal server error")
		return
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
