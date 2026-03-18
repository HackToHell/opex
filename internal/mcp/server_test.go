package mcp

import (
	"log/slog"
	"os"
	"testing"

	"github.com/hacktohell/opex/internal/config"
)

func TestNew(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	mcpCfg := config.MCPConfig{
		Enabled:       true,
		MaxConcurrent: 5,
		QueryTimeout:  30e9, // 30s
		MaxResults:    10,
		DefaultSpss:   1,
		MaxTraceSpans: 50,
	}
	queryCfg := config.QueryConfig{
		MaxLimit:     100,
		DefaultLimit: 20,
		DefaultSpss:  3,
	}

	// nil ClickHouse client is fine for construction (queries will fail at runtime)
	s := New(nil, queryCfg, mcpCfg, logger)

	if s == nil {
		t.Fatal("expected non-nil server")
	}
	if s.mcpServer == nil {
		t.Fatal("expected non-nil mcpServer")
	}
	if s.httpServer == nil {
		t.Fatal("expected non-nil httpServer")
	}
	if cap(s.semaphore) != 5 {
		t.Errorf("expected semaphore capacity 5, got %d", cap(s.semaphore))
	}
}

func TestAcquireRelease(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	mcpCfg := config.MCPConfig{
		MaxConcurrent: 2,
		QueryTimeout:  30e9,
		MaxResults:    10,
		DefaultSpss:   1,
		MaxTraceSpans: 50,
	}

	s := New(nil, config.QueryConfig{}, mcpCfg, logger)

	// Should be able to acquire up to MaxConcurrent slots
	ctx := t.Context()
	if err := s.acquire(ctx); err != nil {
		t.Fatalf("first acquire failed: %v", err)
	}
	if err := s.acquire(ctx); err != nil {
		t.Fatalf("second acquire failed: %v", err)
	}

	// Release one and acquire again
	s.release()
	if err := s.acquire(ctx); err != nil {
		t.Fatalf("third acquire after release failed: %v", err)
	}

	// Clean up
	s.release()
	s.release()
}
