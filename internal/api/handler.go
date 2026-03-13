// Package api provides HTTP handlers for Tempo-compatible trace query endpoints.
package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"runtime"

	"github.com/hacktohell/opex/internal/clickhouse"
	"github.com/hacktohell/opex/internal/response"
)

// Build-time variables, injected via -ldflags.
var (
	Version   = "dev"
	Revision  = "unknown"
	Branch    = "unknown"
	BuildDate = "unknown"
)

// Handlers holds the HTTP handler functions for the API.
type Handlers struct {
	logger *slog.Logger
}

// NewHandlers creates a new Handlers instance.
func NewHandlers(logger *slog.Logger) *Handlers {
	return &Handlers{logger: logger}
}

// Echo is a health-check endpoint that returns "echo".
func (h *Handlers) Echo(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("echo"))
}

// Ready returns 200 if the server is ready to serve traffic.
func (h *Handlers) Ready(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready"))
}

// writeDBError inspects a ClickHouse error and writes the appropriate HTTP
// response. Connection/circuit-breaker errors return 503; everything else
// returns 500 with a generic message.
func writeDBError(w http.ResponseWriter, err error, genericMsg string) {
	if errors.Is(err, clickhouse.ErrNotConnected) || errors.Is(err, clickhouse.ErrCircuitOpen) {
		response.WriteError(w, http.StatusServiceUnavailable, "clickhouse unavailable, please retry later")
		return
	}
	response.WriteError(w, http.StatusInternalServerError, genericMsg)
}

// BuildInfo returns build metadata as JSON.
func (h *Handlers) BuildInfo(w http.ResponseWriter, _ *http.Request) {
	info := map[string]string{
		"version":   Version,
		"revision":  Revision,
		"branch":    Branch,
		"buildDate": BuildDate,
		"goVersion": runtime.Version(),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(info); err != nil {
		h.logger.Error("failed to encode build info", "error", err)
	}
}
