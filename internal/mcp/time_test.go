package mcp

import (
	"testing"
	"time"

	mcplib "github.com/mark3labs/mcp-go/mcp"
)

func makeToolRequest(args map[string]any) mcplib.CallToolRequest {
	return mcplib.CallToolRequest{
		Params: mcplib.CallToolParams{
			Name:      "test",
			Arguments: args,
		},
	}
}

func TestParseTimeParam_RFC3339(t *testing.T) {
	req := makeToolRequest(map[string]any{
		"start": "2025-03-17T10:00:00Z",
	})

	got, err := parseTimeParam(req, "start", time.Time{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := time.Date(2025, 3, 17, 10, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("parseTimeParam() = %v, want %v", got, want)
	}
}

func TestParseTimeParam_UnixEpoch(t *testing.T) {
	req := makeToolRequest(map[string]any{
		"start": "1710669600",
	})

	got, err := parseTimeParam(req, "start", time.Time{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := time.Unix(1710669600, 0)
	if !got.Equal(want) {
		t.Errorf("parseTimeParam() = %v, want %v", got, want)
	}
}

func TestParseTimeParam_Missing(t *testing.T) {
	req := makeToolRequest(map[string]any{})

	defaultVal := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	got, err := parseTimeParam(req, "start", defaultVal)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !got.Equal(defaultVal) {
		t.Errorf("parseTimeParam() = %v, want default %v", got, defaultVal)
	}
}

func TestParseTimeParam_Invalid(t *testing.T) {
	req := makeToolRequest(map[string]any{
		"start": "not-a-time",
	})

	_, err := parseTimeParam(req, "start", time.Time{})
	if err == nil {
		t.Fatal("expected error for invalid time format")
	}
}

func TestParseTimeRange_Defaults(t *testing.T) {
	req := makeToolRequest(map[string]any{})

	before := time.Now()
	start, end, err := parseTimeRange(req)
	after := time.Now()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// end should be approximately now
	if end.Before(before) || end.After(after) {
		t.Errorf("end should be approximately now, got %v", end)
	}

	// start should be approximately 1 hour before end
	diff := end.Sub(start)
	if diff < 59*time.Minute || diff > 61*time.Minute {
		t.Errorf("start-end diff should be ~1h, got %v", diff)
	}
}

func TestParseTimeRange_Explicit(t *testing.T) {
	req := makeToolRequest(map[string]any{
		"start": "2025-03-17T10:00:00Z",
		"end":   "2025-03-17T16:00:00Z",
	})

	start, end, err := parseTimeRange(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	wantStart := time.Date(2025, 3, 17, 10, 0, 0, 0, time.UTC)
	wantEnd := time.Date(2025, 3, 17, 16, 0, 0, 0, time.UTC)

	if !start.Equal(wantStart) {
		t.Errorf("start = %v, want %v", start, wantStart)
	}
	if !end.Equal(wantEnd) {
		t.Errorf("end = %v, want %v", end, wantEnd)
	}
}
