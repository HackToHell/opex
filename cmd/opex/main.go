// Package main is the entry point for the opex TraceQL-to-ClickHouse transpiler.
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/hacktohell/opex/internal/api"
	"github.com/hacktohell/opex/internal/clickhouse"
	"github.com/hacktohell/opex/internal/config"
	"github.com/hacktohell/opex/internal/server"
)

func main() {
	configPath := flag.String("config", "", "path to config file (YAML)")
	flag.Parse()

	cfg, err := config.LoadFromFile(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "invalid config: %v\n", err)
		os.Exit(1)
	}

	// Setup logger
	var logLevel slog.Level
	switch cfg.Logging.Level {
	case "debug":
		logLevel = slog.LevelDebug
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}

	var handler slog.Handler
	opts := &slog.HandlerOptions{Level: logLevel}
	if cfg.Logging.Format == "json" {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}
	logger := slog.New(handler)

	logger.Info("starting opex",
		"version", api.Version,
		"listen_addr", cfg.ListenAddr,
		"traces_table", cfg.ClickHouse.TracesTable,
		"max_limit", cfg.Query.MaxLimit,
		"default_limit", cfg.Query.DefaultLimit,
		"query_timeout", cfg.Query.Timeout.String(),
		"max_concurrent", cfg.Query.MaxConcurrent,
		"use_materialized_views", cfg.ClickHouse.UseMatViews,
		"log_level", cfg.Logging.Level,
		"log_format", cfg.Logging.Format,
	)

	// Connect to ClickHouse. If the initial connection fails, create a lazy
	// client that will connect in the background. All routes are always
	// registered — queries return 503 while disconnected.
	var ch *clickhouse.Client
	ch, err = clickhouse.New(cfg.ClickHouse, logger)
	if err != nil {
		logger.Warn("failed to connect to ClickHouse, will retry in background", "error", err)
		ch = clickhouse.NewLazy(cfg.ClickHouse, logger)
	}

	srv := server.New(cfg, ch, logger)
	if err := srv.Run(); err != nil {
		logger.Error("server failed", "error", err)
		os.Exit(1)
	}
}
