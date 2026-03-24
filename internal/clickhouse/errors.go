package clickhouse

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"syscall"
)

// ErrNotConnected is returned when a query is attempted while the client
// has no active ClickHouse connection.
var ErrNotConnected = errors.New("clickhouse: not connected")

// ErrCircuitOpen is returned when the circuit breaker is open and rejecting
// queries to protect a failing ClickHouse instance.
var ErrCircuitOpen = errors.New("clickhouse: circuit breaker open")

// MigrationError wraps a schema migration failure that occurs while bringing
// up a ClickHouse connection.
type MigrationError struct {
	Err error
}

func (e *MigrationError) Error() string {
	return fmt.Sprintf("clickhouse migrations: %v", e.Err)
}

func (e *MigrationError) Unwrap() error {
	return e.Err
}

// isTransient returns true if the error represents a transient failure that
// may succeed on retry (network issues, connection resets, timeouts).
// Permanent errors (bad SQL, missing tables, auth failures) return false.
func isTransient(err error) bool {
	if err == nil {
		return false
	}

	// io errors indicate broken connections
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	// syscall-level connection errors
	if errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, syscall.EPIPE) {
		return true
	}

	// net.Error covers timeouts and temporary network failures
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	// net.OpError wraps lower-level connection errors
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}

	// String-based fallback for driver errors that don't use typed errors.
	// The clickhouse-go driver sometimes wraps errors in ways that lose
	// the original type, so we check common substrings.
	msg := err.Error()
	transientPatterns := []string{
		"broken pipe",
		"connection refused",
		"connection reset",
		"connection was refused",
		"no such host",
		"i/o timeout",
		"unexpected eof",
		"use of closed network connection",
		"transport is closing",
		"write: connection reset by peer",
		"read: connection reset by peer",
		"dial tcp",
	}
	for _, pattern := range transientPatterns {
		if strings.Contains(strings.ToLower(msg), pattern) {
			return true
		}
	}

	return false
}
