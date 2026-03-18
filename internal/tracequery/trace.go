package tracequery

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/hacktohell/opex/internal/clickhouse"
	"github.com/hacktohell/opex/internal/response"
)

// NormalizeTraceID strips hyphens and lowercases the trace ID.
func NormalizeTraceID(id string) string {
	id = strings.ReplaceAll(id, "-", "")
	return strings.ToLower(id)
}

// IsValidHexTraceID checks if a trace ID is valid hex (16 or 32 characters).
func IsValidHexTraceID(id string) bool {
	if len(id) != 32 && len(id) != 16 {
		return false
	}
	_, err := hex.DecodeString(id)
	return err == nil
}

// GetTraceByID retrieves all spans for a trace and builds an OTLP Trace response.
// Returns nil with no error if the trace is not found.
func GetTraceByID(ctx context.Context, ch *clickhouse.Client,
	traceID string,
) (*response.Trace, error) {
	spans, err := ch.QueryTraceByID(ctx, traceID)
	if err != nil {
		return nil, fmt.Errorf("query trace %s: %w", traceID, err)
	}

	if len(spans) == 0 {
		return nil, nil
	}

	return response.BuildTrace(spans), nil
}
