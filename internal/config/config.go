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

// DefaultConfig returns a Config with sane defaults.
func DefaultConfig() *Config {
	return &Config{
		ListenAddr: ":8080",
		ClickHouse: ClickHouseConfig{
			DSN:                   "clickhouse://localhost:9000/default",
			TracesTable:           "otel_traces",
			MaxOpenConns:          10,
			MaxIdleConns:          5,
			ConnMaxLifetime:       5 * time.Minute,
			DialTimeout:           5 * time.Second,
			ReadTimeout:           30 * time.Second,
			UseMatViews:           false,
			TraceMetadataTable:    "otel_trace_metadata",
			SpanTagNamesTable:     "otel_span_tag_names",
			ResourceTagNamesTable: "otel_resource_tag_names",
			ServiceNamesTable:     "otel_service_names",
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
	}
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
