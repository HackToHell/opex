// Package docs provides embedded TraceQL documentation for the MCP server.
package docs

import "embed"

//go:embed basic.md aggregates.md structural.md metrics.md
var docsFS embed.FS

// Documentation type constants.
const (
	DocsTypeBasic      = "basic"
	DocsTypeAggregates = "aggregates"
	DocsTypeStructural = "structural"
	DocsTypeMetrics    = "metrics"
)

// ValidDocTypes lists all valid documentation types.
var ValidDocTypes = []string{
	DocsTypeBasic,
	DocsTypeAggregates,
	DocsTypeStructural,
	DocsTypeMetrics,
}

// GetContent returns the documentation content for the given type.
func GetContent(docType string) string {
	filename := docType + ".md"
	data, err := docsFS.ReadFile(filename)
	if err != nil {
		return "Documentation not found for type: " + docType
	}
	return string(data)
}

// IsValidDocType returns true if the given type is a known documentation type.
func IsValidDocType(docType string) bool {
	for _, dt := range ValidDocTypes {
		if dt == docType {
			return true
		}
	}
	return false
}
