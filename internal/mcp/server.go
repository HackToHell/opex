package mcp

import (
	"context"
	"log/slog"
	"net/http"

	mcplib "github.com/mark3labs/mcp-go/mcp"
	mcpserver "github.com/mark3labs/mcp-go/server"

	"github.com/hacktohell/opex/internal/api"
	"github.com/hacktohell/opex/internal/clickhouse"
	"github.com/hacktohell/opex/internal/config"
)

// Server wraps the MCP protocol server with Opex-specific configuration.
type Server struct {
	ch         *clickhouse.Client
	queryCfg   config.QueryConfig
	mcpCfg     config.MCPConfig
	logger     *slog.Logger
	mcpServer  *mcpserver.MCPServer
	httpServer *mcpserver.StreamableHTTPServer
	semaphore  chan struct{}
}

// New creates a new MCP Server.
func New(ch *clickhouse.Client, queryCfg config.QueryConfig, mcpCfg config.MCPConfig,
	logger *slog.Logger) *Server {

	mcpSrv := mcpserver.NewMCPServer(
		"opex",
		api.Version,
		mcpserver.WithToolCapabilities(false),
		mcpserver.WithResourceCapabilities(false, false),
	)

	httpSrv := mcpserver.NewStreamableHTTPServer(mcpSrv,
		mcpserver.WithStateLess(true),
	)

	s := &Server{
		ch:         ch,
		queryCfg:   queryCfg,
		mcpCfg:     mcpCfg,
		logger:     logger,
		mcpServer:  mcpSrv,
		httpServer: httpSrv,
		semaphore:  make(chan struct{}, mcpCfg.MaxConcurrent),
	}

	s.setupTools()
	s.setupResources()

	return s
}

// ServeHTTP implements http.Handler, delegating to the mcp-go StreamableHTTPServer.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.httpServer.ServeHTTP(w, r)
}

// acquire obtains a concurrency slot from the bounded semaphore.
func (s *Server) acquire(ctx context.Context) error {
	select {
	case s.semaphore <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// release returns a concurrency slot to the semaphore.
func (s *Server) release() {
	<-s.semaphore
}

// withTimeout wraps the context with the configured query timeout.
func (s *Server) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.mcpCfg.QueryTimeout)
}

// toolResult is a convenience wrapper for successful text results.
func toolResult(data []byte) *mcplib.CallToolResult {
	return mcplib.NewToolResultText(string(data))
}
