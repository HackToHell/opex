package mcp

import (
	"strings"
	"testing"

	"github.com/hacktohell/opex/internal/mcp/docs"
	"github.com/hacktohell/opex/internal/response"
)

func TestAttributeNameRegex(t *testing.T) {
	tests := []struct {
		input string
		valid bool
	}{
		{"resource.service.name", true},
		{"span.http.method", true},
		{"http.status_code", true},
		{"_private", true},
		{"a", true},
		{"a1.b2.c3", true},
		{"123", false},
		{"", false},
		{"foo bar", false},
		{"foo;DROP TABLE", false},
		{"foo'bar", false},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got := attributeNameRegex.MatchString(tc.input)
			if got != tc.valid {
				t.Errorf("attributeNameRegex.MatchString(%q) = %v, want %v", tc.input, got, tc.valid)
			}
		})
	}
}

func TestValidScopes(t *testing.T) {
	valid := []string{"", "span", "resource", "intrinsic"}
	for _, s := range valid {
		if !validScopes[s] {
			t.Errorf("expected scope %q to be valid", s)
		}
	}

	invalid := []string{"unknown", "global", "all"}
	for _, s := range invalid {
		if validScopes[s] {
			t.Errorf("expected scope %q to be invalid", s)
		}
	}
}

func TestHandleDocs(t *testing.T) {
	tests := []struct {
		name      string
		docName   string
		wantError bool
		errSubstr string
	}{
		{"basic docs", "basic", false, ""},
		{"aggregates docs", "aggregates", false, ""},
		{"structural docs", "structural", false, ""},
		{"metrics docs", "metrics", false, ""},
		{"unknown type", "unknown", true, "unknown doc type"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.wantError {
				if docs.IsValidDocType(tc.docName) {
					t.Errorf("expected invalid doc type %q", tc.docName)
				}
			} else {
				if !docs.IsValidDocType(tc.docName) {
					t.Errorf("expected valid doc type %q", tc.docName)
				}
				content := docs.GetContent(tc.docName)
				if content == "" {
					t.Errorf("expected non-empty content for %q", tc.docName)
				}
			}
		})
	}
}

func TestCountSpans(t *testing.T) {
	svcName := "test-svc"
	trace := &response.Trace{
		Batches: []response.ResourceSpans{
			{
				Resource: response.Resource{
					Attributes: []response.KeyValue{
						{Key: "service.name", Value: response.AnyValue{StringValue: &svcName}},
					},
				},
				ScopeSpans: []response.ScopeSpans{
					{
						Spans: []response.Span{
							{SpanID: "span1", Name: "op1"},
							{SpanID: "span2", Name: "op2"},
						},
					},
				},
			},
			{
				ScopeSpans: []response.ScopeSpans{
					{
						Spans: []response.Span{
							{SpanID: "span3", Name: "op3"},
						},
					},
				},
			},
		},
	}

	got := countSpans(trace)
	if got != 3 {
		t.Errorf("countSpans() = %d, want 3", got)
	}
}

func TestTruncateTrace(t *testing.T) {
	trace := &response.Trace{
		Batches: []response.ResourceSpans{
			{
				ScopeSpans: []response.ScopeSpans{
					{
						Spans: []response.Span{
							{SpanID: "span1"},
							{SpanID: "span2"},
							{SpanID: "span3"},
							{SpanID: "span4"},
							{SpanID: "span5"},
						},
					},
				},
			},
		},
	}

	truncated := truncateTrace(trace, 3)
	got := countSpans(truncated)
	if got != 3 {
		t.Errorf("truncateTrace(5, 3) = %d spans, want 3", got)
	}
}

func TestGetServiceNameFromResource(t *testing.T) {
	svcName := "my-service"
	r := response.Resource{
		Attributes: []response.KeyValue{
			{Key: "service.name", Value: response.AnyValue{StringValue: &svcName}},
			{Key: "other.attr", Value: response.AnyValue{StringValue: &svcName}},
		},
	}

	got := getServiceNameFromResource(r)
	if got != "my-service" {
		t.Errorf("getServiceNameFromResource() = %q, want %q", got, "my-service")
	}

	// Empty resource
	empty := response.Resource{}
	got = getServiceNameFromResource(empty)
	if got != "" {
		t.Errorf("getServiceNameFromResource(empty) = %q, want empty", got)
	}
}

func TestDocsContent(t *testing.T) {
	for _, dt := range docs.ValidDocTypes {
		content := docs.GetContent(dt)
		if content == "" {
			t.Errorf("docs.GetContent(%q) returned empty", dt)
		}
		if strings.Contains(content, "Documentation not found") {
			t.Errorf("docs.GetContent(%q) returned not found message", dt)
		}
	}

	// Unknown type
	content := docs.GetContent("nonexistent")
	if !strings.Contains(content, "Documentation not found") {
		t.Errorf("expected 'not found' for nonexistent doc type, got %q", content)
	}
}
