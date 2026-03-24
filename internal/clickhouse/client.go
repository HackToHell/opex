// Package clickhouse provides a ClickHouse client for executing trace queries.
package clickhouse

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/hacktohell/opex/internal/config"
	"github.com/hacktohell/opex/internal/metrics"
	"github.com/hacktohell/opex/internal/migrations"
)

// Client wraps a ClickHouse connection with resilience features including
// automatic reconnection, query retry with exponential backoff, and a
// circuit breaker to protect against cascading failures.
type Client struct {
	mu     sync.RWMutex
	conn   driver.Conn
	cfg    config.ClickHouseConfig
	logger *slog.Logger
	cb     *CircuitBreaker

	// connected is an atomic flag for fast-path health checks without locking.
	connected atomic.Bool
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

// New creates a new ClickHouse client from config and verifies connectivity.
// If the connection cannot be established, New returns an error. Use
// NewLazy to create a client that connects in the background.
func New(cfg config.ClickHouseConfig, logger *slog.Logger) (*Client, error) {
	c := newClient(cfg, logger)

	conn, err := c.dial()
	if err != nil {
		return nil, err
	}
	if err := c.runMigrations(); err != nil {
		_ = conn.Close()
		return nil, err
	}

	c.mu.Lock()
	c.conn = conn
	c.connected.Store(true)
	c.mu.Unlock()

	metrics.SetClickHouseConnected(true)
	logger.Info("connected to ClickHouse", "dsn", cfg.DSN)

	return c, nil
}

// NewLazy creates a ClickHouse client in a disconnected state. The caller
// should start the background health-check loop via StartHealthCheck, which
// will establish the connection asynchronously. This allows the application
// to start and serve infrastructure endpoints while ClickHouse is unavailable.
func NewLazy(cfg config.ClickHouseConfig, logger *slog.Logger) *Client {
	c := newClient(cfg, logger)
	metrics.SetClickHouseConnected(false)
	logger.Warn("created lazy ClickHouse client, connection will be established in background")
	return c
}

func newClient(cfg config.ClickHouseConfig, logger *slog.Logger) *Client {
	cbThreshold := cfg.CircuitBreakerThreshold
	if cbThreshold <= 0 {
		cbThreshold = 5
	}
	cbTimeout := cfg.CircuitBreakerTimeout
	if cbTimeout <= 0 {
		cbTimeout = 10 * time.Second
	}

	return &Client{
		cfg:    cfg,
		logger: logger,
		cb:     NewCircuitBreaker(cbThreshold, cbTimeout),
	}
}

// dial creates a new ClickHouse connection and verifies it with a ping.
func (c *Client) dial() (driver.Conn, error) {
	opts, err := ch.ParseDSN(c.cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("parsing ClickHouse DSN: %w", err)
	}

	if c.cfg.MaxOpenConns > 0 {
		opts.MaxOpenConns = c.cfg.MaxOpenConns
	}
	if c.cfg.MaxIdleConns > 0 {
		opts.MaxIdleConns = c.cfg.MaxIdleConns
	}
	if c.cfg.ConnMaxLifetime > 0 {
		opts.ConnMaxLifetime = c.cfg.ConnMaxLifetime
	}
	if c.cfg.DialTimeout > 0 {
		opts.DialTimeout = c.cfg.DialTimeout
	}
	if c.cfg.ReadTimeout > 0 {
		opts.ReadTimeout = c.cfg.ReadTimeout
	}

	conn, err := ch.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("opening ClickHouse connection: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("pinging ClickHouse: %w", err)
	}

	return conn, nil
}

// ---------------------------------------------------------------------------
// Connection lifecycle
// ---------------------------------------------------------------------------

// Close closes the ClickHouse connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.connected.Store(false)
	metrics.SetClickHouseConnected(false)
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// Connected returns true if the client has an active ClickHouse connection.
func (c *Client) Connected() bool {
	return c.connected.Load()
}

// Ping checks if the ClickHouse connection is alive.
func (c *Client) Ping(ctx context.Context) error {
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()

	if conn == nil {
		return ErrNotConnected
	}
	return conn.Ping(ctx)
}

// Reconnect attempts to establish a new ClickHouse connection, replacing
// the current one. On success it resets the circuit breaker. Safe for
// concurrent use.
func (c *Client) Reconnect() error {
	metrics.RecordReconnectAttempt()

	conn, err := c.dial()
	if err != nil {
		c.logger.Error("reconnection failed", "error", err)
		return fmt.Errorf("reconnecting to ClickHouse: %w", err)
	}
	if err := c.runMigrations(); err != nil {
		_ = conn.Close()
		c.logger.Error("reconnection failed", "error", err)
		return fmt.Errorf("reconnecting to ClickHouse: %w", err)
	}

	c.mu.Lock()
	old := c.conn
	c.conn = conn
	c.connected.Store(true)
	c.mu.Unlock()

	// Close the old connection if it existed
	if old != nil {
		_ = old.Close()
	}

	c.cb.Reset()
	metrics.SetClickHouseConnected(true)
	metrics.SetCircuitBreakerState(float64(CircuitClosed))
	c.logger.Info("reconnected to ClickHouse")

	return nil
}

func (c *Client) runMigrations() error {
	if !c.cfg.RunMigrations {
		return nil
	}
	if err := migrations.Run(c.cfg, c.logger); err != nil {
		return &MigrationError{Err: err}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Background health check
// ---------------------------------------------------------------------------

// StartHealthCheck runs a background goroutine that periodically pings
// ClickHouse and attempts reconnection when the connection is lost.
// It also handles the initial connection for lazy-created clients.
// Cancel the context to stop the health-check loop.
func (c *Client) StartHealthCheck(ctx context.Context) {
	interval := c.cfg.HealthCheckInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}

	go c.healthLoop(ctx, interval)
}

func (c *Client) healthLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// backoff state for reconnection attempts
	backoff := interval
	maxBackoff := 60 * time.Second

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !c.connected.Load() {
				// Not connected — attempt to connect/reconnect
				if err := c.Reconnect(); err != nil {
					c.logger.Warn("health check: reconnection failed, retrying",
						"error", err,
						"next_attempt_in", backoff.String(),
					)
					// Apply exponential backoff to the ticker
					backoff = backoff * 2
					if backoff > maxBackoff {
						backoff = maxBackoff
					}
					ticker.Reset(backoff)
					continue
				}
				// Connected successfully — reset backoff
				backoff = interval
				ticker.Reset(interval)
				continue
			}

			// Already connected — do a health ping
			pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			err := c.Ping(pingCtx)
			cancel()

			if err != nil {
				c.logger.Warn("health check: ping failed, marking disconnected", "error", err)
				c.connected.Store(false)
				metrics.SetClickHouseConnected(false)
				// Will attempt reconnection on next tick
				continue
			}

			// Update circuit breaker metric on each successful ping
			metrics.SetCircuitBreakerState(float64(c.cb.State()))
		}
	}
}

// ---------------------------------------------------------------------------
// Query execution
// ---------------------------------------------------------------------------

// Table returns the configured traces table name.
func (c *Client) Table() string {
	return c.cfg.TracesTable
}

// Exec executes a statement that does not return rows (DDL, INSERT, etc.).
func (c *Client) Exec(ctx context.Context, sql string, args ...any) error {
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()

	if conn == nil {
		return ErrNotConnected
	}
	return conn.Exec(ctx, sql, args...)
}

// Query executes a query and returns rows. It integrates the circuit breaker,
// transient error retry with exponential backoff, and connection state tracking.
//
// If the config has a ReadTimeout, a deadline is applied to the context when
// no existing deadline is set. The cancel function is intentionally NOT
// deferred because the returned driver.Rows outlives this function — callers
// iterate rows after Query() returns. The timer goroutine is cleaned up when
// the timeout fires or the parent context is cancelled.
func (c *Client) Query(ctx context.Context, sql string, args ...any) (driver.Rows, error) {
	// Circuit breaker check
	if !c.cb.Allow() {
		metrics.SetCircuitBreakerState(float64(CircuitOpen))
		return nil, ErrCircuitOpen
	}

	// Apply query timeout if configured and context has no existing deadline.
	if c.cfg.ReadTimeout > 0 {
		if _, ok := ctx.Deadline(); !ok {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, c.cfg.ReadTimeout)
			_ = cancel
		}
	}

	queryType := classifyQuery(sql)

	c.logger.Debug("executing query",
		"query_type", queryType,
		"sql_length", len(sql),
		"sql", truncateSQL(sql, 500),
	)

	maxRetries := c.cfg.MaxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	baseDelay := c.cfg.RetryBaseDelay
	if baseDelay <= 0 {
		baseDelay = 50 * time.Millisecond
	}

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			// Check if context is already done before retrying
			if ctx.Err() != nil {
				break
			}
			delay := baseDelay * (1 << (attempt - 1)) // 50ms, 100ms, 200ms, ...
			c.logger.Warn("retrying query",
				"query_type", queryType,
				"attempt", attempt+1,
				"delay", delay.String(),
			)
			metrics.RecordQueryRetry()

			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				break
			case <-timer.C:
			}
		}

		rows, err := c.queryOnce(ctx, sql, args...)
		if err == nil {
			c.cb.RecordSuccess()
			metrics.SetCircuitBreakerState(float64(c.cb.State()))
			return rows, nil
		}

		lastErr = err

		if !isTransient(err) {
			// Permanent error — don't retry
			break
		}

		if isTransient(err) {
			c.cb.RecordFailure()
			metrics.SetCircuitBreakerState(float64(c.cb.State()))
		}
	}

	// All attempts exhausted or permanent error
	start := time.Now()
	metrics.ObserveClickHouseQuery(queryType, time.Since(start))
	metrics.RecordQueryError("clickhouse")

	c.logger.Error("query failed",
		"query_type", queryType,
		"error", lastErr,
		"sql", truncateSQL(sql, 500),
	)

	return nil, lastErr
}

// queryOnce executes a single query attempt against the current connection.
func (c *Client) queryOnce(ctx context.Context, sql string, args ...any) (driver.Rows, error) {
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()

	if conn == nil {
		return nil, ErrNotConnected
	}

	queryType := classifyQuery(sql)
	start := time.Now()
	rows, err := conn.Query(ctx, sql, args...)
	duration := time.Since(start)

	metrics.ObserveClickHouseQuery(queryType, duration)

	if err != nil {
		c.logger.Debug("query attempt failed",
			"query_type", queryType,
			"duration_ms", duration.Milliseconds(),
			"error", err,
			"transient", isTransient(err),
		)
		// Mark disconnected on transient errors so health loop picks it up
		if isTransient(err) {
			c.connected.Store(false)
			metrics.SetClickHouseConnected(false)
		}
	} else {
		c.logger.Debug("query completed",
			"query_type", queryType,
			"duration_ms", duration.Milliseconds(),
		)
	}

	return rows, err
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// truncateSQL truncates a SQL string to maxLen characters for logging.
func truncateSQL(sql string, maxLen int) string {
	if len(sql) <= maxLen {
		return sql
	}
	return sql[:maxLen] + "..."
}

// classifyQuery returns a short label for the query type based on SQL content.
func classifyQuery(sql string) string {
	switch {
	case len(sql) > 14 && sql[:14] == "WITH RECURSIVE":
		return "structural"
	case len(sql) > 4 && sql[:4] == "WITH":
		return "pipeline"
	case containsStr(sql, "INTERSECT") || containsStr(sql, "UNION"):
		return "spanset_op"
	case containsStr(sql, "GROUP BY"):
		return "aggregate"
	case containsStr(sql, "arrayJoin(mapKeys"):
		return "tag_discovery"
	case containsStr(sql, "mapContains"):
		return "tag_values"
	default:
		return "query"
	}
}

func containsStr(s, substr string) bool {
	return len(s) >= len(substr) && searchSubstr(s, substr)
}

func searchSubstr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
