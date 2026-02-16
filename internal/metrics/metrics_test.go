package metrics

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNormalizeEndpoint(t *testing.T) {
	tests := []struct {
		path     string
		expected string
	}{
		{"/api/echo", "/api/echo"},
		{"/api/search", "/api/search"},
		{"/api/search/tags", "/api/search/tags"},
		{"/api/v2/search/tags", "/api/v2/search/tags"},
		{"/api/traces/abc123def456", "/api/traces/{traceID}"},
		{"/api/v2/traces/abc123def456", "/api/v2/traces/{traceID}"},
		{"/api/search/tag/http.method/values", "/api/search/tag/{tagName}/values"},
		{"/api/v2/search/tag/service.name/values", "/api/v2/search/tag/{tagName}/values"},
		{"/api/metrics/query_range", "/api/metrics/query_range"},
		{"/api/metrics/query", "/api/metrics/query"},
		{"/api/metrics/summary", "/api/metrics/summary"},
		{"/api/status/buildinfo", "/api/status/buildinfo"},
		{"/ready", "/ready"},
		{"/metrics", "/metrics"},
	}

	for _, tc := range tests {
		t.Run(tc.path, func(t *testing.T) {
			result := normalizeEndpoint(tc.path)
			if result != tc.expected {
				t.Errorf("normalizeEndpoint(%q) = %q, want %q", tc.path, result, tc.expected)
			}
		})
	}
}

func TestMetricsHandler(t *testing.T) {
	handler := Handler()
	if handler == nil {
		t.Fatal("Handler() returned nil")
	}

	// Verify it responds with Prometheus metrics format
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	body := w.Body.String()
	// Should contain at least the Go runtime metrics
	if len(body) == 0 {
		t.Error("expected non-empty metrics response")
	}
}

func TestMiddleware(t *testing.T) {
	// Create a simple handler that returns 200
	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	handler := Middleware(inner)

	req := httptest.NewRequest(http.MethodGet, "/api/search", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}
	if w.Body.String() != "ok" {
		t.Errorf("expected body 'ok', got %q", w.Body.String())
	}
}

func TestMiddlewareCaptures500(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("error"))
	})

	handler := Middleware(inner)

	req := httptest.NewRequest(http.MethodGet, "/api/search", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", w.Code)
	}
}

func TestResponseWriterCapturesStatusCode(t *testing.T) {
	rw := &responseWriter{
		ResponseWriter: httptest.NewRecorder(),
		statusCode:     http.StatusOK,
	}

	rw.WriteHeader(http.StatusNotFound)
	if rw.statusCode != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", rw.statusCode)
	}
}

func TestObserveClickHouseQuery(_ *testing.T) {
	// Should not panic
	ObserveClickHouseQuery("test_query", 100)
}

func TestRecordQueryError(_ *testing.T) {
	// Should not panic
	RecordQueryError("parse_error")
	RecordQueryError("clickhouse")
}
