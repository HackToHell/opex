package tracequery

import (
	"testing"

	"github.com/hacktohell/opex/internal/traceql"
)

func TestExtractMetricsAggregate(t *testing.T) {
	tests := []struct {
		name       string
		root       *traceql.RootExpr
		wantOp     traceql.MetricsAggregateOp
		wantFilter bool
		wantNil    bool
	}{
		{
			name: "rate with filter",
			root: &traceql.RootExpr{
				Pipeline: traceql.Pipeline{
					Elements: []traceql.PipelineElement{
						&traceql.SpansetFilter{
							Expression: &traceql.BinaryOperation{
								Op:  traceql.OpEqual,
								LHS: &traceql.Attribute{Name: "service.name", Scope: traceql.AttributeScopeResource},
								RHS: &traceql.Static{Type: traceql.TypeString, StringVal: "frontend"},
							},
						},
						&traceql.MetricsAggregate{Op: traceql.MetricsAggregateRate},
					},
				},
			},
			wantOp:     traceql.MetricsAggregateRate,
			wantFilter: true,
		},
		{
			name: "count_over_time no filter",
			root: &traceql.RootExpr{
				Pipeline: traceql.Pipeline{
					Elements: []traceql.PipelineElement{
						&traceql.MetricsAggregate{Op: traceql.MetricsAggregateCountOverTime},
					},
				},
			},
			wantOp:     traceql.MetricsAggregateCountOverTime,
			wantFilter: false,
		},
		{
			name:    "empty pipeline",
			root:    &traceql.RootExpr{Pipeline: traceql.Pipeline{Elements: nil}},
			wantNil: true,
		},
		{
			name: "no metrics aggregate",
			root: &traceql.RootExpr{
				Pipeline: traceql.Pipeline{
					Elements: []traceql.PipelineElement{
						&traceql.SpansetFilter{
							Expression: &traceql.Attribute{Intrinsic: traceql.IntrinsicName},
						},
					},
				},
			},
			wantNil: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ma, fp := extractMetricsAggregate(tc.root)
			if tc.wantNil {
				if ma != nil {
					t.Fatalf("expected nil MetricsAggregate, got %v", ma)
				}
				return
			}
			if ma == nil {
				t.Fatal("expected non-nil MetricsAggregate")
			}
			if ma.Op != tc.wantOp {
				t.Errorf("op = %v, want %v", ma.Op, tc.wantOp)
			}
			if tc.wantFilter && fp == nil {
				t.Error("expected non-nil filter pipeline")
			}
			if !tc.wantFilter && fp != nil {
				t.Error("expected nil filter pipeline")
			}
		})
	}
}

func TestAttributeToColumn(t *testing.T) {
	tests := []struct {
		name string
		attr traceql.Attribute
		want string
	}{
		{"intrinsic duration", traceql.Attribute{Intrinsic: traceql.IntrinsicDuration}, "Duration"},
		{"intrinsic name", traceql.Attribute{Intrinsic: traceql.IntrinsicName}, "SpanName"},
		{"intrinsic status", traceql.Attribute{Intrinsic: traceql.IntrinsicStatus}, "StatusCode"},
		{"intrinsic kind", traceql.Attribute{Intrinsic: traceql.IntrinsicKind}, "SpanKind"},
		{"resource service.name", traceql.Attribute{Name: "service.name", Scope: traceql.AttributeScopeResource}, "ServiceName"},
		{"resource other", traceql.Attribute{Name: "deployment.env", Scope: traceql.AttributeScopeResource}, "ResourceAttributes['deployment.env']"},
		{"span attr", traceql.Attribute{Name: "http.method", Scope: traceql.AttributeScopeSpan}, "SpanAttributes['http.method']"},
		{"unscoped service.name", traceql.Attribute{Name: "service.name"}, "ServiceName"},
		{"unscoped other", traceql.Attribute{Name: "http.url"}, "SpanAttributes['http.url']"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := attributeToColumn(&tc.attr)
			if got != tc.want {
				t.Errorf("attributeToColumn() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestAttributeToColumn_SQLInjection(t *testing.T) {
	// Verify that attribute names with single quotes are escaped
	attr := traceql.Attribute{Name: "foo'] OR 1=1--", Scope: traceql.AttributeScopeSpan}
	got := attributeToColumn(&attr)
	want := "SpanAttributes['foo\\'] OR 1=1--']"
	if got != want {
		t.Errorf("attributeToColumn with injection = %q, want %q", got, want)
	}
}

func TestGroupByToColumn(t *testing.T) {
	tests := []struct {
		input, want string
	}{
		{"resource.service.name", "ServiceName"},
		{"service.name", "ServiceName"},
		{"resource.deployment.env", "ResourceAttributes['deployment.env']"},
		{"span.http.method", "SpanAttributes['http.method']"},
		{".http.url", "SpanAttributes['http.url']"},
		{"custom.attr", "SpanAttributes['custom.attr']"},
	}

	for _, tc := range tests {
		got := groupByToColumn(tc.input)
		if got != tc.want {
			t.Errorf("groupByToColumn(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestGroupByToColumn_SQLInjection(t *testing.T) {
	got := groupByToColumn("'; DROP TABLE--")
	want := "SpanAttributes['\\'; DROP TABLE--']"
	if got != want {
		t.Errorf("groupByToColumn with injection = %q, want %q", got, want)
	}
}
