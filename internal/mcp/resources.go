package mcp

import (
	"context"

	mcplib "github.com/mark3labs/mcp-go/mcp"

	"github.com/hacktohell/opex/internal/mcp/docs"
)

// setupResources registers the embedded documentation as MCP resources.
func (s *Server) setupResources() {
	docResources := []struct {
		uri     string
		name    string
		desc    string
		docType string
	}{
		{"docs://traceql/basic", "TraceQL Basic Docs", "Intrinsics, operators, attribute syntax, simple filters", docs.DocsTypeBasic},
		{"docs://traceql/aggregates", "TraceQL Aggregates Docs", "count, sum, avg, min, max, pipeline usage", docs.DocsTypeAggregates},
		{"docs://traceql/structural", "TraceQL Structural Docs", "Parent/child, descendant, ancestor, sibling operators", docs.DocsTypeStructural},
		{"docs://traceql/metrics", "TraceQL Metrics Docs", "rate, count_over_time, quantile_over_time, histogram_over_time", docs.DocsTypeMetrics},
	}

	for _, dr := range docResources {
		docType := dr.docType
		s.mcpServer.AddResource(
			mcplib.Resource{
				URI:         dr.uri,
				Name:        dr.name,
				Description: dr.desc,
				MIMEType:    "text/markdown",
			},
			func(_ context.Context, req mcplib.ReadResourceRequest) ([]mcplib.ResourceContents, error) {
				return []mcplib.ResourceContents{
					mcplib.TextResourceContents{
						URI:      req.Params.URI,
						MIMEType: "text/markdown",
						Text:     docs.GetContent(docType),
					},
				}, nil
			},
		)
	}
}
