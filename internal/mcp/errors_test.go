package mcp

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/hacktohell/opex/internal/clickhouse"
)

func TestClassifyError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "not connected",
			err:  clickhouse.ErrNotConnected,
			want: "ClickHouse is not connected",
		},
		{
			name: "circuit open",
			err:  clickhouse.ErrCircuitOpen,
			want: "ClickHouse is temporarily unavailable",
		},
		{
			name: "deadline exceeded",
			err:  context.DeadlineExceeded,
			want: "Query timed out",
		},
		{
			name: "canceled",
			err:  context.Canceled,
			want: "Query was cancelled",
		},
		{
			name: "wrapped not connected",
			err:  fmt.Errorf("query failed: %w", clickhouse.ErrNotConnected),
			want: "ClickHouse is not connected",
		},
		{
			name: "generic error",
			err:  fmt.Errorf("something went wrong"),
			want: "Query failed due to an internal error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyError(tc.err)
			if !strings.Contains(got, tc.want) {
				t.Errorf("classifyError() = %q, want to contain %q", got, tc.want)
			}
		})
	}
}
