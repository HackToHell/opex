package mcp

import (
	"fmt"
	"strconv"
	"time"

	mcplib "github.com/mark3labs/mcp-go/mcp"
)

// parseTimeParam parses a time parameter from a CallToolRequest.
// It accepts RFC3339 strings (e.g., "2025-03-17T10:00:00Z") or Unix epoch
// seconds (e.g., "1710669600"). If the parameter is missing, it returns
// the provided default value.
func parseTimeParam(req mcplib.CallToolRequest, key string, defaultVal time.Time) (time.Time, error) {
	raw := req.GetString(key, "")
	if raw == "" {
		return defaultVal, nil
	}

	// Try RFC3339 first (LLMs generate this naturally)
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return t, nil
	}

	// Try Unix epoch seconds
	if secs, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return time.Unix(secs, 0), nil
	}

	return time.Time{}, fmt.Errorf("invalid time format for %q: expected RFC3339 or Unix epoch seconds, got %q", key, raw)
}

// parseTimeRange extracts start/end time from a CallToolRequest, using
// defaults of 1-hour-ago and now.
func parseTimeRange(req mcplib.CallToolRequest) (time.Time, time.Time, error) {
	now := time.Now()
	defaultStart := now.Add(-1 * time.Hour)

	start, err := parseTimeParam(req, "start", defaultStart)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}

	end, err := parseTimeParam(req, "end", now)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}

	return start, end, nil
}
