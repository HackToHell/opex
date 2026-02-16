// Package clickhouse provides a ClickHouse client for executing trace queries.
package clickhouse

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/hacktohell/opex/internal/config"
	"github.com/hacktohell/opex/internal/metrics"
)

// Client wraps a ClickHouse connection.
type Client struct {
	conn   driver.Conn
	cfg    config.ClickHouseConfig
	logger *slog.Logger
}

// New creates a new ClickHouse client from config.
func New(cfg config.ClickHouseConfig, logger *slog.Logger) (*Client, error) {
	opts, err := ch.ParseDSN(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("parsing ClickHouse DSN: %w", err)
	}

	if cfg.MaxOpenConns > 0 {
		opts.MaxOpenConns = cfg.MaxOpenConns
	}
	if cfg.MaxIdleConns > 0 {
		opts.MaxIdleConns = cfg.MaxIdleConns
	}
	if cfg.ConnMaxLifetime > 0 {
		opts.ConnMaxLifetime = cfg.ConnMaxLifetime
	}
	if cfg.DialTimeout > 0 {
		opts.DialTimeout = cfg.DialTimeout
	}
	if cfg.ReadTimeout > 0 {
		opts.ReadTimeout = cfg.ReadTimeout
	}

	conn, err := ch.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("opening ClickHouse connection: %w", err)
	}

	// Test the connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("pinging ClickHouse: %w", err)
	}

	logger.Info("connected to ClickHouse", "dsn", cfg.DSN)

	return &Client{
		conn:   conn,
		cfg:    cfg,
		logger: logger,
	}, nil
}

// Close closes the ClickHouse connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// Ping checks if the ClickHouse connection is alive.
func (c *Client) Ping(ctx context.Context) error {
	return c.conn.Ping(ctx)
}

// Table returns the configured traces table name.
func (c *Client) Table() string {
	return c.cfg.TracesTable
}

// Query executes a query and returns rows.
// If the config has a ReadTimeout, a deadline is applied to the context.
func (c *Client) Query(ctx context.Context, sql string, args ...any) (driver.Rows, error) {
	// Apply query timeout if configured and context has no existing deadline
	if c.cfg.ReadTimeout > 0 {
		if _, ok := ctx.Deadline(); !ok {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, c.cfg.ReadTimeout)
			_ = cancel // caller is responsible for cancel via rows.Close or context
		}
	}

	queryType := classifyQuery(sql)

	c.logger.Debug("executing query",
		"query_type", queryType,
		"sql_length", len(sql),
		"sql", truncateSQL(sql, 500),
	)

	start := time.Now()
	rows, err := c.conn.Query(ctx, sql, args...)
	duration := time.Since(start)

	metrics.ObserveClickHouseQuery(queryType, duration)

	if err != nil {
		metrics.RecordQueryError("clickhouse")
		c.logger.Error("query failed",
			"query_type", queryType,
			"duration_ms", duration.Milliseconds(),
			"error", err,
			"sql", truncateSQL(sql, 500),
		)
	} else {
		c.logger.Debug("query completed",
			"query_type", queryType,
			"duration_ms", duration.Milliseconds(),
		)
	}

	return rows, err
}

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
