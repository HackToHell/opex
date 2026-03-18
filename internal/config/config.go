// Package config provides YAML-based configuration loading with sane defaults.
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config holds the application configuration.
type Config struct {
	ListenAddr string           `yaml:"listen_addr"`
	ClickHouse ClickHouseConfig `yaml:"clickhouse"`
	Query      QueryConfig      `yaml:"query"`
	Logging    LoggingConfig    `yaml:"logging"`
	MCP        MCPConfig        `yaml:"mcp"`
}

// ClickHouseConfig holds ClickHouse connection settings.
type ClickHouseConfig struct {
	DSN             string        `yaml:"dsn"`
	TracesTable     string        `yaml:"traces_table"`
	MaxOpenConns    int           `yaml:"max_open_conns"`
	MaxIdleConns    int           `yaml:"max_idle_conns"`
	ConnMaxLifetime time.Duration `yaml:"conn_max_lifetime"`
	DialTimeout     time.Duration `yaml:"dial_timeout"`
	ReadTimeout     time.Duration `yaml:"read_timeout"`
	// UseMatViews enables use of materialized views for tag discovery queries.
	UseMatViews bool `yaml:"use_materialized_views"`
	// TraceMetadataTable is the table name for pre-computed trace metadata.
	TraceMetadataTable string `yaml:"trace_metadata_table"`
	// SpanTagNamesTable is the table name for cached span attribute keys.
	SpanTagNamesTable string `yaml:"span_tag_names_table"`
	// ResourceTagNamesTable is the table name for cached resource attribute keys.
	ResourceTagNamesTable string `yaml:"resource_tag_names_table"`
	// ServiceNamesTable is the table name for cached service names.
	ServiceNamesTable string `yaml:"service_names_table"`
	// HealthCheckInterval is the interval between background health checks.
	HealthCheckInterval time.Duration `yaml:"health_check_interval"`
	// MaxRetries is the maximum number of retries for transient query errors.
	MaxRetries int `yaml:"max_retries"`
	// RetryBaseDelay is the base delay for exponential backoff on retries.
	RetryBaseDelay time.Duration `yaml:"retry_base_delay"`
	// CircuitBreakerThreshold is the number of consecutive failures before opening the circuit.
	CircuitBreakerThreshold int `yaml:"circuit_breaker_threshold"`
	// CircuitBreakerTimeout is the time to wait before transitioning from open to half-open.
	CircuitBreakerTimeout time.Duration `yaml:"circuit_breaker_timeout"`
}

// QueryConfig holds query execution settings.
type QueryConfig struct {
	MaxLimit      int           `yaml:"max_limit"`
	DefaultLimit  int           `yaml:"default_limit"`
	DefaultSpss   int           `yaml:"default_spss"`
	MaxDuration   time.Duration `yaml:"max_duration"`
	Timeout       time.Duration `yaml:"timeout"`
	MaxConcurrent int           `yaml:"max_concurrent"`
}

// LoggingConfig holds logging settings.
type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

// MCPConfig holds MCP server settings.
type MCPConfig struct {
	// Enabled controls whether the MCP server is started.
	Enabled bool `yaml:"enabled"`
	// MaxConcurrent limits the number of concurrent MCP tool invocations.
	MaxConcurrent int `yaml:"max_concurrent"`
	// QueryTimeout is the per-tool-invocation timeout.
	QueryTimeout time.Duration `yaml:"query_timeout"`
	// MaxResults is the default limit for trace search results.
	MaxResults int `yaml:"max_results"`
	// DefaultSpss is the default spans-per-spanset for MCP search results.
	DefaultSpss int `yaml:"default_spss"`
	// MaxTraceSpans is the maximum number of spans returned by get-trace.
	MaxTraceSpans int `yaml:"max_trace_spans"`
}

// DefaultConfig returns a Config with sane defaults.
func DefaultConfig() *Config {
	return &Config{
		ListenAddr: ":8080",
		ClickHouse: ClickHouseConfig{
			DSN:                     "clickhouse://localhost:9000/default",
			TracesTable:             "otel_traces",
			MaxOpenConns:            10,
			MaxIdleConns:            5,
			ConnMaxLifetime:         5 * time.Minute,
			DialTimeout:             5 * time.Second,
			ReadTimeout:             30 * time.Second,
			UseMatViews:             false,
			TraceMetadataTable:      "otel_trace_metadata",
			SpanTagNamesTable:       "otel_span_tag_names",
			ResourceTagNamesTable:   "otel_resource_tag_names",
			ServiceNamesTable:       "otel_service_names",
			HealthCheckInterval:     5 * time.Second,
			MaxRetries:              2,
			RetryBaseDelay:          50 * time.Millisecond,
			CircuitBreakerThreshold: 5,
			CircuitBreakerTimeout:   10 * time.Second,
		},
		Query: QueryConfig{
			MaxLimit:      100,
			DefaultLimit:  20,
			DefaultSpss:   3,
			MaxDuration:   168 * time.Hour,
			Timeout:       30 * time.Second,
			MaxConcurrent: 20,
		},
		Logging: LoggingConfig{
			Level:  "info",
			Format: "json",
		},
		MCP: MCPConfig{
			Enabled:       false,
			MaxConcurrent: 5,
			QueryTimeout:  30 * time.Second,
			MaxResults:    10,
			DefaultSpss:   1,
			MaxTraceSpans: 50,
		},
	}
}

// Validate checks the configuration for invalid values.
func (c *Config) Validate() error {
	if c.MCP.Enabled {
		if c.MCP.MaxConcurrent <= 0 {
			return fmt.Errorf("mcp.max_concurrent must be > 0, got %d", c.MCP.MaxConcurrent)
		}
		if c.MCP.QueryTimeout <= 0 {
			return fmt.Errorf("mcp.query_timeout must be positive, got %v", c.MCP.QueryTimeout)
		}
		if c.MCP.MaxResults <= 0 {
			return fmt.Errorf("mcp.max_results must be > 0, got %d", c.MCP.MaxResults)
		}
		if c.MCP.MaxTraceSpans <= 0 {
			return fmt.Errorf("mcp.max_trace_spans must be > 0, got %d", c.MCP.MaxTraceSpans)
		}
	}
	return nil
}

// LoadFromFile reads a YAML config file and merges it with defaults.
func LoadFromFile(path string) (*Config, error) {
	cfg := DefaultConfig()

	if path == "" {
		return cfg, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parsing config file: %w", err)
	}

	return cfg, nil
}
