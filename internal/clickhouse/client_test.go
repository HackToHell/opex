package clickhouse

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/hacktohell/opex/internal/config"
)

func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestNewLazy_CreatesDisconnectedClient(t *testing.T) {
	cfg := config.DefaultConfig().ClickHouse
	logger := newTestLogger()

	c := NewLazy(cfg, logger)

	if c.Connected() {
		t.Error("NewLazy client should not be connected")
	}
}

func TestClient_QueryReturnsErrNotConnected_WhenDisconnected(t *testing.T) {
	cfg := config.DefaultConfig().ClickHouse
	logger := newTestLogger()

	c := NewLazy(cfg, logger)

	_, err := c.Query(context.Background(), "SELECT 1")
	if err != ErrNotConnected {
		t.Errorf("Query() error = %v, want ErrNotConnected", err)
	}
}

func TestClient_ExecReturnsErrNotConnected_WhenDisconnected(t *testing.T) {
	cfg := config.DefaultConfig().ClickHouse
	logger := newTestLogger()

	c := NewLazy(cfg, logger)

	err := c.Exec(context.Background(), "CREATE TABLE test (id Int64) ENGINE = Memory")
	if err != ErrNotConnected {
		t.Errorf("Exec() error = %v, want ErrNotConnected", err)
	}
}

func TestClient_PingReturnsErrNotConnected_WhenDisconnected(t *testing.T) {
	cfg := config.DefaultConfig().ClickHouse
	logger := newTestLogger()

	c := NewLazy(cfg, logger)

	err := c.Ping(context.Background())
	if err != ErrNotConnected {
		t.Errorf("Ping() error = %v, want ErrNotConnected", err)
	}
}

func TestClient_QueryReturnsErrCircuitOpen_WhenCircuitOpen(t *testing.T) {
	cfg := config.DefaultConfig().ClickHouse
	cfg.CircuitBreakerThreshold = 1
	logger := newTestLogger()

	c := NewLazy(cfg, logger)
	// Force the circuit breaker open
	c.cb.RecordFailure()

	_, err := c.Query(context.Background(), "SELECT 1")
	if err != ErrCircuitOpen {
		t.Errorf("Query() error = %v, want ErrCircuitOpen", err)
	}
}

func TestClient_CloseOnDisconnected(t *testing.T) {
	cfg := config.DefaultConfig().ClickHouse
	logger := newTestLogger()

	c := NewLazy(cfg, logger)

	err := c.Close()
	if err != nil {
		t.Errorf("Close() on disconnected client error = %v, want nil", err)
	}

	if c.Connected() {
		t.Error("Connected() = true after Close, want false")
	}
}

func TestClient_HealthCheckStopsOnContextCancel(t *testing.T) {
	cfg := config.DefaultConfig().ClickHouse
	cfg.HealthCheckInterval = 50 * time.Millisecond
	// Use a bad DSN so reconnection fails fast
	cfg.DSN = "clickhouse://localhost:19999/nonexistent"
	cfg.DialTimeout = 100 * time.Millisecond
	logger := newTestLogger()

	c := NewLazy(cfg, logger)

	ctx, cancel := context.WithCancel(context.Background())
	c.StartHealthCheck(ctx)

	// Let it run briefly
	time.Sleep(200 * time.Millisecond)

	// Cancel the context — health loop should stop
	cancel()

	// Give it time to stop
	time.Sleep(100 * time.Millisecond)

	// Client should still be disconnected (couldn't reach the bad DSN)
	if c.Connected() {
		t.Error("Connected() = true, want false (bad DSN)")
	}
}

func TestClient_Table(t *testing.T) {
	cfg := config.DefaultConfig().ClickHouse
	cfg.TracesTable = "custom_traces"
	logger := newTestLogger()

	c := NewLazy(cfg, logger)

	if c.Table() != "custom_traces" {
		t.Errorf("Table() = %q, want %q", c.Table(), "custom_traces")
	}
}
