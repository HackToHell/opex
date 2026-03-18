package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	mcplib "github.com/mark3labs/mcp-go/mcp"

	"github.com/hacktohell/opex/internal/mcp/docs"
	"github.com/hacktohell/opex/internal/metrics"
	"github.com/hacktohell/opex/internal/response"
	"github.com/hacktohell/opex/internal/tracequery"
)

// attributeNameRegex validates attribute names to prevent injection.
var attributeNameRegex = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_.]*$`)

// validScopes lists valid attribute scopes for the get-attribute-names tool.
var validScopes = map[string]bool{
	"":          true,
	"span":      true,
	"resource":  true,
	"intrinsic": true,
}

// setupTools registers all 7 MCP tools with the server.
func (s *Server) setupTools() {
	s.mcpServer.AddTool(
		mcplib.NewTool("traceql-search",
			mcplib.WithDescription("Search for traces using a TraceQL query. Returns matching traces with metadata."),
			mcplib.WithString("query",
				mcplib.Required(),
				mcplib.Description("TraceQL query string (e.g., `{ resource.service.name = \"api\" && status = error }`)")),
			mcplib.WithString("start",
				mcplib.Description("Start time (RFC3339 or Unix epoch seconds). Default: 1 hour ago")),
			mcplib.WithString("end",
				mcplib.Description("End time (RFC3339 or Unix epoch seconds). Default: now")),
			mcplib.WithNumber("limit",
				mcplib.Description("Max traces to return. Default: 10")),
		),
		s.handleSearch,
	)

	s.mcpServer.AddTool(
		mcplib.NewTool("traceql-metrics-instant",
			mcplib.WithDescription("Retrieve a single metric value from a TraceQL metrics query. Best for answering 'what is the current value of X?' questions."),
			mcplib.WithString("query",
				mcplib.Required(),
				mcplib.Description("TraceQL metrics query (e.g., `{ resource.service.name = \"api\" } | rate()`)")),
			mcplib.WithString("start",
				mcplib.Description("Start time (RFC3339 or Unix epoch seconds). Default: 1 hour ago")),
			mcplib.WithString("end",
				mcplib.Description("End time (RFC3339 or Unix epoch seconds). Default: now")),
		),
		s.handleMetricsInstant,
	)

	s.mcpServer.AddTool(
		mcplib.NewTool("traceql-metrics-range",
			mcplib.WithDescription("Retrieve a time series from a TraceQL metrics query. For understanding trends over time. Prefer instant queries when a single value suffices."),
			mcplib.WithString("query",
				mcplib.Required(),
				mcplib.Description("TraceQL metrics query")),
			mcplib.WithString("start",
				mcplib.Description("Start time (RFC3339 or Unix epoch seconds). Default: 1 hour ago")),
			mcplib.WithString("end",
				mcplib.Description("End time (RFC3339 or Unix epoch seconds). Default: now")),
			mcplib.WithString("step",
				mcplib.Description("Step size (e.g., '60s', '5m'). Auto-calculated if omitted (~100 data points)")),
		),
		s.handleMetricsRange,
	)

	s.mcpServer.AddTool(
		mcplib.NewTool("get-trace",
			mcplib.WithDescription("Retrieve a specific trace by ID. Returns OTLP trace data with spans."),
			mcplib.WithString("trace_id",
				mcplib.Required(),
				mcplib.Description("Hex trace ID (16 or 32 characters)")),
		),
		s.handleGetTrace,
	)

	s.mcpServer.AddTool(
		mcplib.NewTool("get-attribute-names",
			mcplib.WithDescription("List available attribute names for use in TraceQL queries."),
			mcplib.WithString("scope",
				mcplib.Description("Filter by scope: 'span', 'resource', 'intrinsic', or empty for all")),
		),
		s.handleGetAttributeNames,
	)

	s.mcpServer.AddTool(
		mcplib.NewTool("get-attribute-values",
			mcplib.WithDescription("Get values for a specific attribute. Useful for discovering what services, endpoints, or status codes exist in the data."),
			mcplib.WithString("name",
				mcplib.Required(),
				mcplib.Description("Fully scoped attribute name (e.g., 'resource.service.name', 'span.http.method')")),
			mcplib.WithString("filter_query",
				mcplib.Description("TraceQL filter to narrow results (e.g., `{ resource.service.name = \"api\" }`)")),
			mcplib.WithString("start",
				mcplib.Description("Start time (RFC3339 or Unix epoch seconds). Default: 1 hour ago")),
			mcplib.WithString("end",
				mcplib.Description("End time (RFC3339 or Unix epoch seconds). Default: now")),
		),
		s.handleGetAttributeValues,
	)

	s.mcpServer.AddTool(
		mcplib.NewTool("docs-traceql",
			mcplib.WithDescription("Retrieve TraceQL documentation. Use this to learn TraceQL syntax before writing queries."),
			mcplib.WithString("name",
				mcplib.Required(),
				mcplib.Description("Doc type: 'basic', 'aggregates', 'structural', 'metrics'")),
		),
		s.handleDocs,
	)
}

// ---------------------------------------------------------------------------
// Tool handlers
// ---------------------------------------------------------------------------

func (s *Server) handleSearch(ctx context.Context, req mcplib.CallToolRequest) (*mcplib.CallToolResult, error) {
	metrics.MCPToolCalls.WithLabelValues("traceql-search").Inc()
	timer := prometheus.NewTimer(metrics.MCPToolDuration.WithLabelValues("traceql-search"))
	defer timer.ObserveDuration()

	query, err := req.RequireString("query")
	if err != nil {
		return mcplib.NewToolResultError("query parameter is required"), nil
	}

	start, end, err := parseTimeRange(req)
	if err != nil {
		return mcplib.NewToolResultError(err.Error()), nil
	}

	limit := req.GetInt("limit", s.mcpCfg.MaxResults)
	spss := s.mcpCfg.DefaultSpss

	if err := s.acquire(ctx); err != nil {
		return mcplib.NewToolResultError("server is busy, please retry"), nil
	}
	defer s.release()

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	s.logger.Info("mcp tool invoked",
		"tool", "traceql-search",
		"query", query,
		"start", start,
		"end", end,
		"limit", limit,
	)

	result, err := tracequery.SearchTraces(ctx, s.ch, s.queryCfg,
		query, start, end, limit, spss, 0, 0)
	if err != nil {
		s.logger.Error("mcp tool failed", "tool", "traceql-search", "error", err, "query", query)
		metrics.MCPToolErrors.WithLabelValues("traceql-search", "query").Inc()
		return mcplib.NewToolResultError(classifyError(err)), nil
	}

	data, _ := json.Marshal(result)
	return toolResult(data), nil
}

func (s *Server) handleMetricsInstant(ctx context.Context, req mcplib.CallToolRequest) (*mcplib.CallToolResult, error) {
	metrics.MCPToolCalls.WithLabelValues("traceql-metrics-instant").Inc()
	timer := prometheus.NewTimer(metrics.MCPToolDuration.WithLabelValues("traceql-metrics-instant"))
	defer timer.ObserveDuration()

	query, err := req.RequireString("query")
	if err != nil {
		return mcplib.NewToolResultError("query parameter is required"), nil
	}

	start, end, err := parseTimeRange(req)
	if err != nil {
		return mcplib.NewToolResultError(err.Error()), nil
	}

	if err := s.acquire(ctx); err != nil {
		return mcplib.NewToolResultError("server is busy, please retry"), nil
	}
	defer s.release()

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	s.logger.Info("mcp tool invoked",
		"tool", "traceql-metrics-instant",
		"query", query,
		"start", start,
		"end", end,
	)

	result, err := tracequery.MetricsQueryInstant(ctx, s.ch, s.queryCfg,
		query, start, end)
	if err != nil {
		s.logger.Error("mcp tool failed", "tool", "traceql-metrics-instant", "error", err, "query", query)
		metrics.MCPToolErrors.WithLabelValues("traceql-metrics-instant", "query").Inc()
		return mcplib.NewToolResultError(classifyError(err)), nil
	}

	data, _ := json.Marshal(result)
	return toolResult(data), nil
}

func (s *Server) handleMetricsRange(ctx context.Context, req mcplib.CallToolRequest) (*mcplib.CallToolResult, error) {
	metrics.MCPToolCalls.WithLabelValues("traceql-metrics-range").Inc()
	timer := prometheus.NewTimer(metrics.MCPToolDuration.WithLabelValues("traceql-metrics-range"))
	defer timer.ObserveDuration()

	query, err := req.RequireString("query")
	if err != nil {
		return mcplib.NewToolResultError("query parameter is required"), nil
	}

	start, end, err := parseTimeRange(req)
	if err != nil {
		return mcplib.NewToolResultError(err.Error()), nil
	}

	stepStr := req.GetString("step", "")
	var step time.Duration
	if stepStr != "" {
		step, err = time.ParseDuration(stepStr)
		if err != nil {
			return mcplib.NewToolResultError(fmt.Sprintf("invalid step format: %v", err)), nil
		}
	}

	if err := s.acquire(ctx); err != nil {
		return mcplib.NewToolResultError("server is busy, please retry"), nil
	}
	defer s.release()

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	s.logger.Info("mcp tool invoked",
		"tool", "traceql-metrics-range",
		"query", query,
		"start", start,
		"end", end,
		"step", step,
	)

	result, err := tracequery.MetricsQueryRange(ctx, s.ch, s.queryCfg,
		query, start, end, step)
	if err != nil {
		s.logger.Error("mcp tool failed", "tool", "traceql-metrics-range", "error", err, "query", query)
		metrics.MCPToolErrors.WithLabelValues("traceql-metrics-range", "query").Inc()
		return mcplib.NewToolResultError(classifyError(err)), nil
	}

	data, _ := json.Marshal(result)
	return toolResult(data), nil
}

func (s *Server) handleGetTrace(ctx context.Context, req mcplib.CallToolRequest) (*mcplib.CallToolResult, error) {
	metrics.MCPToolCalls.WithLabelValues("get-trace").Inc()
	timer := prometheus.NewTimer(metrics.MCPToolDuration.WithLabelValues("get-trace"))
	defer timer.ObserveDuration()

	traceID, err := req.RequireString("trace_id")
	if err != nil {
		return mcplib.NewToolResultError("trace_id parameter is required"), nil
	}

	traceID = tracequery.NormalizeTraceID(traceID)
	if !tracequery.IsValidHexTraceID(traceID) {
		return mcplib.NewToolResultError("invalid trace_id format: must be 16 or 32 hex characters"), nil
	}

	if err := s.acquire(ctx); err != nil {
		return mcplib.NewToolResultError("server is busy, please retry"), nil
	}
	defer s.release()

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	s.logger.Info("mcp tool invoked",
		"tool", "get-trace",
		"trace_id", traceID,
	)

	trace, err := tracequery.GetTraceByID(ctx, s.ch, traceID)
	if err != nil {
		s.logger.Error("mcp tool failed", "tool", "get-trace", "error", err, "trace_id", traceID)
		metrics.MCPToolErrors.WithLabelValues("get-trace", "query").Inc()
		return mcplib.NewToolResultError(classifyError(err)), nil
	}

	if trace == nil {
		return mcplib.NewToolResultError("trace not found"), nil
	}

	// Truncate if too many spans
	traceResp := s.buildTraceMCPResponse(trace, traceID)
	data, _ := json.Marshal(traceResp)
	return toolResult(data), nil
}

func (s *Server) handleGetAttributeNames(ctx context.Context, req mcplib.CallToolRequest) (*mcplib.CallToolResult, error) {
	metrics.MCPToolCalls.WithLabelValues("get-attribute-names").Inc()
	timer := prometheus.NewTimer(metrics.MCPToolDuration.WithLabelValues("get-attribute-names"))
	defer timer.ObserveDuration()

	scope := req.GetString("scope", "")

	if !validScopes[scope] {
		return mcplib.NewToolResultError(fmt.Sprintf("invalid scope %q: must be 'span', 'resource', 'intrinsic', or empty", scope)), nil
	}

	if err := s.acquire(ctx); err != nil {
		return mcplib.NewToolResultError("server is busy, please retry"), nil
	}
	defer s.release()

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	now := time.Now()
	start := now.Add(-1 * time.Hour)

	s.logger.Info("mcp tool invoked",
		"tool", "get-attribute-names",
		"scope", scope,
	)

	result, err := tracequery.GetTagNames(ctx, s.ch, scope, start, now)
	if err != nil {
		s.logger.Error("mcp tool failed", "tool", "get-attribute-names", "error", err)
		metrics.MCPToolErrors.WithLabelValues("get-attribute-names", "query").Inc()
		return mcplib.NewToolResultError(classifyError(err)), nil
	}

	data, _ := json.Marshal(result)
	return toolResult(data), nil
}

func (s *Server) handleGetAttributeValues(ctx context.Context, req mcplib.CallToolRequest) (*mcplib.CallToolResult, error) {
	metrics.MCPToolCalls.WithLabelValues("get-attribute-values").Inc()
	timer := prometheus.NewTimer(metrics.MCPToolDuration.WithLabelValues("get-attribute-values"))
	defer timer.ObserveDuration()

	name, err := req.RequireString("name")
	if err != nil {
		return mcplib.NewToolResultError("name parameter is required"), nil
	}

	if !attributeNameRegex.MatchString(name) {
		return mcplib.NewToolResultError(fmt.Sprintf("invalid attribute name %q: must match %s", name, attributeNameRegex.String())), nil
	}

	filterQuery := req.GetString("filter_query", "")

	start, end, err := parseTimeRange(req)
	if err != nil {
		return mcplib.NewToolResultError(err.Error()), nil
	}

	if err := s.acquire(ctx); err != nil {
		return mcplib.NewToolResultError("server is busy, please retry"), nil
	}
	defer s.release()

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	s.logger.Info("mcp tool invoked",
		"tool", "get-attribute-values",
		"name", name,
		"filter_query", filterQuery,
		"start", start,
		"end", end,
	)

	result, err := tracequery.GetTagValues(ctx, s.ch, name, filterQuery, start, end)
	if err != nil {
		s.logger.Error("mcp tool failed", "tool", "get-attribute-values", "error", err, "name", name)
		metrics.MCPToolErrors.WithLabelValues("get-attribute-values", "query").Inc()
		return mcplib.NewToolResultError(classifyError(err)), nil
	}

	data, _ := json.Marshal(result)
	return toolResult(data), nil
}

func (s *Server) handleDocs(_ context.Context, req mcplib.CallToolRequest) (*mcplib.CallToolResult, error) {
	metrics.MCPToolCalls.WithLabelValues("docs-traceql").Inc()

	name, err := req.RequireString("name")
	if err != nil {
		return mcplib.NewToolResultError("name parameter is required"), nil
	}

	if !docs.IsValidDocType(name) {
		return mcplib.NewToolResultError(fmt.Sprintf(
			"unknown doc type %q. Valid types: basic, aggregates, structural, metrics", name)), nil
	}

	content := docs.GetContent(name)
	return mcplib.NewToolResultText(content), nil
}

// ---------------------------------------------------------------------------
// Trace response truncation
// ---------------------------------------------------------------------------

// mcpTraceResponse is a truncation-aware trace response for LLM consumption.
type mcpTraceResponse struct {
	TraceID     string          `json:"trace_id"`
	TotalSpans  int             `json:"total_spans"`
	ShownSpans  int             `json:"shown_spans"`
	Truncated   bool            `json:"truncated"`
	RootService string          `json:"root_service,omitempty"`
	RootSpan    string          `json:"root_span,omitempty"`
	DurationMs  int64           `json:"duration_ms,omitempty"`
	Services    []string        `json:"services,omitempty"`
	ErrorCount  int             `json:"error_count"`
	Trace       *response.Trace `json:"trace,omitempty"`
}

func (s *Server) buildTraceMCPResponse(trace *response.Trace, traceID string) *mcpTraceResponse {
	totalSpans := countSpans(trace)
	maxSpans := s.mcpCfg.MaxTraceSpans

	resp := &mcpTraceResponse{
		TraceID:    traceID,
		TotalSpans: totalSpans,
	}

	// Extract metadata from the full trace
	svcSet := make(map[string]bool)
	var errorCount int
	for _, batch := range trace.Batches {
		svcName := getServiceNameFromResource(batch.Resource)
		if svcName != "" {
			svcSet[svcName] = true
		}
		for _, ss := range batch.ScopeSpans {
			for _, span := range ss.Spans {
				if span.Status.Code == 2 { // ERROR
					errorCount++
				}
				if span.ParentSpanID == "" {
					resp.RootService = svcName
					resp.RootSpan = span.Name
				}
			}
		}
	}

	for svc := range svcSet {
		resp.Services = append(resp.Services, svc)
	}
	resp.ErrorCount = errorCount

	if totalSpans <= maxSpans {
		resp.ShownSpans = totalSpans
		resp.Truncated = false
		resp.Trace = trace
	} else {
		// Truncate: keep only maxSpans spans
		truncated := truncateTrace(trace, maxSpans)
		resp.ShownSpans = maxSpans
		resp.Truncated = true
		resp.Trace = truncated
	}

	return resp
}

func countSpans(trace *response.Trace) int {
	count := 0
	for _, batch := range trace.Batches {
		for _, ss := range batch.ScopeSpans {
			count += len(ss.Spans)
		}
	}
	return count
}

func truncateTrace(trace *response.Trace, maxSpans int) *response.Trace {
	result := &response.Trace{}
	remaining := maxSpans
	for _, batch := range trace.Batches {
		if remaining <= 0 {
			break
		}
		newBatch := response.ResourceSpans{Resource: batch.Resource}
		for _, ss := range batch.ScopeSpans {
			if remaining <= 0 {
				break
			}
			newSS := response.ScopeSpans{Scope: ss.Scope}
			for _, span := range ss.Spans {
				if remaining <= 0 {
					break
				}
				newSS.Spans = append(newSS.Spans, span)
				remaining--
			}
			if len(newSS.Spans) > 0 {
				newBatch.ScopeSpans = append(newBatch.ScopeSpans, newSS)
			}
		}
		if len(newBatch.ScopeSpans) > 0 {
			result.Batches = append(result.Batches, newBatch)
		}
	}
	return result
}

func getServiceNameFromResource(r response.Resource) string {
	for _, attr := range r.Attributes {
		if attr.Key == "service.name" && attr.Value.StringValue != nil {
			return *attr.Value.StringValue
		}
	}
	return ""
}
