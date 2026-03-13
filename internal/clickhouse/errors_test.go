package clickhouse

import (
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"
)

func TestIsTransient(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		// Nil error
		{"nil error", nil, false},

		// io errors
		{"io.EOF", io.EOF, true},
		{"io.ErrUnexpectedEOF", io.ErrUnexpectedEOF, true},

		// Wrapped io errors
		{"wrapped io.EOF", fmt.Errorf("query trace abc: %w", io.EOF), true},
		{"wrapped io.ErrUnexpectedEOF", fmt.Errorf("scan: %w", io.ErrUnexpectedEOF), true},

		// syscall errors
		{"ECONNRESET", syscall.ECONNRESET, true},
		{"ECONNREFUSED", syscall.ECONNREFUSED, true},
		{"ECONNABORTED", syscall.ECONNABORTED, true},
		{"EPIPE", syscall.EPIPE, true},
		{"wrapped ECONNRESET", fmt.Errorf("query: %w", syscall.ECONNRESET), true},

		// net.OpError
		{"net.OpError dial", &net.OpError{Op: "dial", Err: errors.New("connection refused")}, true},
		{"wrapped net.OpError", fmt.Errorf("query: %w", &net.OpError{Op: "read", Err: errors.New("reset")}), true},

		// String patterns
		{"broken pipe string", errors.New("write tcp: broken pipe"), true},
		{"connection refused string", errors.New("dial tcp 127.0.0.1:9000: connection refused"), true},
		{"connection reset string", errors.New("read: connection reset by peer"), true},
		{"i/o timeout string", errors.New("i/o timeout"), true},
		{"unexpected EOF string", errors.New("unexpected EOF"), true},
		{"closed connection string", errors.New("use of closed network connection"), true},
		{"transport closing string", errors.New("transport is closing"), true},
		{"dial tcp string", errors.New("dial tcp 10.0.0.1:9000: connect: no route to host"), true},

		// Permanent errors (should NOT be transient)
		{"bad SQL", errors.New("code: 62, message: Syntax error"), false},
		{"missing table", errors.New("code: 60, message: Table default.nonexistent doesn't exist"), false},
		{"auth failure", errors.New("code: 516, message: Authentication failed"), false},
		{"generic error", errors.New("something went wrong"), false},
		{"type mismatch", errors.New("code: 53, message: Type mismatch"), false},

		// ErrNotConnected / ErrCircuitOpen (these are checked separately by handlers,
		// but should not be retried by the query loop)
		{"ErrNotConnected", ErrNotConnected, false},
		{"ErrCircuitOpen", ErrCircuitOpen, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := isTransient(tc.err)
			if got != tc.expected {
				t.Errorf("isTransient(%v) = %v, want %v", tc.err, got, tc.expected)
			}
		})
	}
}
