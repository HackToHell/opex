package config

import (
	"testing"
	"time"
)

func TestDefaultConfig_Valid(t *testing.T) {
	cfg := DefaultConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("default config should be valid: %v", err)
	}
}

func TestValidate_MCPDisabled(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MCP.Enabled = false
	cfg.MCP.MaxConcurrent = 0 // would be invalid if enabled
	if err := cfg.Validate(); err != nil {
		t.Fatalf("disabled MCP should skip validation: %v", err)
	}
}

func TestValidate_MCPEnabled_InvalidFields(t *testing.T) {
	tests := []struct {
		name   string
		modify func(*MCPConfig)
	}{
		{"zero MaxConcurrent", func(c *MCPConfig) { c.MaxConcurrent = 0 }},
		{"negative MaxConcurrent", func(c *MCPConfig) { c.MaxConcurrent = -1 }},
		{"zero QueryTimeout", func(c *MCPConfig) { c.QueryTimeout = 0 }},
		{"negative QueryTimeout", func(c *MCPConfig) { c.QueryTimeout = -1 * time.Second }},
		{"zero MaxResults", func(c *MCPConfig) { c.MaxResults = 0 }},
		{"zero MaxTraceSpans", func(c *MCPConfig) { c.MaxTraceSpans = 0 }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.MCP.Enabled = true
			tc.modify(&cfg.MCP)
			if err := cfg.Validate(); err == nil {
				t.Error("expected validation error")
			}
		})
	}
}

func TestValidate_MCPEnabled_ValidConfig(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MCP.Enabled = true
	if err := cfg.Validate(); err != nil {
		t.Fatalf("default MCP config should be valid when enabled: %v", err)
	}
}

func TestLoadFromFile_Empty(t *testing.T) {
	cfg, err := LoadFromFile("")
	if err != nil {
		t.Fatalf("empty path should return defaults: %v", err)
	}
	if cfg.ListenAddr != ":8080" {
		t.Errorf("expected default listen addr :8080, got %q", cfg.ListenAddr)
	}
}

func TestLoadFromFile_NonExistent(t *testing.T) {
	_, err := LoadFromFile("/nonexistent/path/config.yaml")
	if err == nil {
		t.Fatal("expected error for nonexistent file")
	}
}
