package api

import (
	"math"
	"testing"
	"time"

	"github.com/hacktohell/opex/internal/traceql"
)

func TestParseStep(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC) // 1 hour later

	tests := []struct {
		name    string
		stepStr string
		want    time.Duration
	}{
		{
			name:    "duration string",
			stepStr: "30s",
			want:    30 * time.Second,
		},
		{
			name:    "duration minutes",
			stepStr: "5m",
			want:    5 * time.Minute,
		},
		{
			name:    "integer seconds",
			stepStr: "60",
			want:    60 * time.Second,
		},
		{
			name:    "auto-calculate ~100 points",
			stepStr: "",
			want:    36 * time.Second, // 3600s / 100 = 36s
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := parseStep(tc.stepStr, start, end)
			if got != tc.want {
				t.Errorf("parseStep(%q) = %v, want %v", tc.stepStr, got, tc.want)
			}
		})
	}
}

func TestParseStep_SmallRange(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(10 * time.Millisecond) // very small range

	// Step should be at least 1 second
	got := parseStep("", start, end)
	if got < time.Second {
		t.Errorf("expected step >= 1s for small range, got %v", got)
	}
}

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
			name: "empty pipeline",
			root: &traceql.RootExpr{
				Pipeline: traceql.Pipeline{Elements: nil},
			},
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
				t.Errorf("expected op %v, got %v", tc.wantOp, ma.Op)
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
		{
			name: "intrinsic duration",
			attr: traceql.Attribute{Intrinsic: traceql.IntrinsicDuration},
			want: "Duration",
		},
		{
			name: "intrinsic name",
			attr: traceql.Attribute{Intrinsic: traceql.IntrinsicName},
			want: "SpanName",
		},
		{
			name: "intrinsic status",
			attr: traceql.Attribute{Intrinsic: traceql.IntrinsicStatus},
			want: "StatusCode",
		},
		{
			name: "intrinsic kind",
			attr: traceql.Attribute{Intrinsic: traceql.IntrinsicKind},
			want: "SpanKind",
		},
		{
			name: "resource service.name",
			attr: traceql.Attribute{Name: "service.name", Scope: traceql.AttributeScopeResource},
			want: "ServiceName",
		},
		{
			name: "resource other attr",
			attr: traceql.Attribute{Name: "deployment.env", Scope: traceql.AttributeScopeResource},
			want: "ResourceAttributes['deployment.env']",
		},
		{
			name: "span attr",
			attr: traceql.Attribute{Name: "http.method", Scope: traceql.AttributeScopeSpan},
			want: "SpanAttributes['http.method']",
		},
		{
			name: "unscoped service.name",
			attr: traceql.Attribute{Name: "service.name"},
			want: "ServiceName",
		},
		{
			name: "unscoped other",
			attr: traceql.Attribute{Name: "http.url"},
			want: "SpanAttributes['http.url']",
		},
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

func TestGroupByToColumn(t *testing.T) {
	tests := []struct {
		input string
		want  string
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

func TestToFloat64(t *testing.T) {
	tests := []struct {
		name  string
		input any
		want  float64
		isNaN bool
	}{
		{"float64", float64(3.14), 3.14, false},
		{"float32", float32(2.5), 2.5, false},
		{"int64", int64(42), 42.0, false},
		{"int32", int32(10), 10.0, false},
		{"uint64", uint64(100), 100.0, false},
		{"int", int(7), 7.0, false},
		{"string", "hello", 0, true}, // should be NaN
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := toFloat64(tc.input)
			if tc.isNaN {
				if !math.IsNaN(got) {
					t.Errorf("expected NaN, got %f", got)
				}
			} else {
				if got != tc.want {
					t.Errorf("toFloat64() = %f, want %f", got, tc.want)
				}
			}
		})
	}
}
