package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestEcho(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	h := NewHandlers(logger)

	req := httptest.NewRequest(http.MethodGet, "/api/echo", nil)
	w := httptest.NewRecorder()
	h.Echo(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if w.Body.String() != "echo" {
		t.Fatalf("expected body 'echo', got %q", w.Body.String())
	}
}

func TestReady(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	h := NewHandlers(logger)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	w := httptest.NewRecorder()
	h.Ready(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestBuildInfo(t *testing.T) {
	// Set build variables
	Version = "test-1.0.0"
	Revision = "abc123"
	Branch = "main"
	BuildDate = "2025-01-01"

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	h := NewHandlers(logger)

	req := httptest.NewRequest(http.MethodGet, "/api/status/buildinfo", nil)
	w := httptest.NewRecorder()
	h.BuildInfo(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if resp.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("expected json content type, got %q", resp.Header.Get("Content-Type"))
	}

	var info map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		t.Fatalf("failed to decode JSON: %v", err)
	}
	if info["version"] != "test-1.0.0" {
		t.Fatalf("expected version test-1.0.0, got %q", info["version"])
	}
	if info["revision"] != "abc123" {
		t.Fatalf("expected revision abc123, got %q", info["revision"])
	}
	if info["branch"] != "main" {
		t.Fatalf("expected branch main, got %q", info["branch"])
	}
	if info["goVersion"] == "" {
		t.Fatal("expected goVersion to be set")
	}
}

func TestNormalizeTraceID(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"AABBCCDD", "aabbccdd"},
		{"aa-bb-cc-dd", "aabbccdd"},
		{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
	}
	for _, tc := range tests {
		result := normalizeTraceID(tc.input)
		if result != tc.expected {
			t.Errorf("normalizeTraceID(%q) = %q, want %q", tc.input, result, tc.expected)
		}
	}
}

func TestIsValidHexTraceID(t *testing.T) {
	tests := []struct {
		input string
		valid bool
	}{
		{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", true}, // 32 hex chars
		{"aaaaaaaaaaaaaaaa", true},                 // 16 hex chars
		{"abc", false},                             // too short
		{"gggggggggggggggg", false},                // invalid hex
		{"aabbccddeeff00112233445566778899", true}, // 32 hex chars
	}
	for _, tc := range tests {
		result := isValidHexTraceID(tc.input)
		if result != tc.valid {
			t.Errorf("isValidHexTraceID(%q) = %v, want %v", tc.input, result, tc.valid)
		}
	}
}

func TestParseTimeRange(t *testing.T) {
	start, end := parseTimeRange("1704067200", "1704153600")
	if start.Unix() != 1704067200 {
		t.Errorf("expected start 1704067200, got %d", start.Unix())
	}
	if end.Unix() != 1704153600 {
		t.Errorf("expected end 1704153600, got %d", end.Unix())
	}

	// Empty strings should default to last hour
	start2, end2 := parseTimeRange("", "")
	if start2.IsZero() || end2.IsZero() {
		t.Error("expected non-zero times for empty strings")
	}
	if end2.Sub(start2) != 1*60*60*1000000000 { // 1 hour in nanoseconds
		t.Errorf("expected 1 hour duration, got %v", end2.Sub(start2))
	}
}
