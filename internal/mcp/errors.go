// Package mcp provides the MCP (Model Context Protocol) server for Opex.
package mcp

import (
	"context"
	"errors"

	"github.com/hacktohell/opex/internal/clickhouse"
	"github.com/hacktohell/opex/internal/tracequery"
)

// classifyError translates internal errors into LLM-friendly messages.
func classifyError(err error) string {
	switch {
	case tracequery.IsInputError(err):
		return "Invalid query: " + err.Error()
	case errors.Is(err, clickhouse.ErrNotConnected):
		return "ClickHouse is not connected. The database may be starting up. Please retry in a few seconds."
	case errors.Is(err, clickhouse.ErrCircuitOpen):
		return "ClickHouse is temporarily unavailable due to repeated failures. Please retry in 10-15 seconds."
	case errors.Is(err, context.DeadlineExceeded):
		return "Query timed out. Try narrowing the time range or simplifying the query."
	case errors.Is(err, context.Canceled):
		return "Query was cancelled."
	default:
		return "Query failed due to an internal error. Please retry or simplify your query."
	}
}
