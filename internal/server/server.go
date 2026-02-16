// Package server provides the main HTTP server with routing and middleware.
package server

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/hacktohell/opex/internal/api"
	"github.com/hacktohell/opex/internal/clickhouse"
	"github.com/hacktohell/opex/internal/config"
	"github.com/hacktohell/opex/internal/metrics"
)

// Server is the main Opex HTTP server.
type Server struct {
	cfg    *config.Config
	ch     *clickhouse.Client
	router *mux.Router
	logger *slog.Logger
}

// New creates a new Server with all routes registered.
// ch may be nil if ClickHouse is not available (only infra endpoints work).
func New(cfg *config.Config, ch *clickhouse.Client, logger *slog.Logger) *Server {
	s := &Server{
		cfg:    cfg,
		ch:     ch,
		router: mux.NewRouter(),
		logger: logger,
	}
	s.registerRoutes()
	return s
}

func (s *Server) registerRoutes() {
	handlers := api.NewHandlers(s.logger)

	// Infrastructure endpoints
	s.router.HandleFunc("/api/echo", handlers.Echo).Methods(http.MethodGet)
	s.router.HandleFunc("/api/status/buildinfo", handlers.BuildInfo).Methods(http.MethodGet)
	s.router.HandleFunc("/ready", s.readyHandler).Methods(http.MethodGet)

	// Trace, search, and tag endpoints (require ClickHouse)
	if s.ch != nil {
		trace := api.NewTraceHandlers(s.ch, s.logger)
		s.router.HandleFunc("/api/traces/{traceID}", trace.TraceByID).Methods(http.MethodGet)
		s.router.HandleFunc("/api/v2/traces/{traceID}", trace.TraceByIDV2).Methods(http.MethodGet)

		search := api.NewSearchHandlers(s.ch, s.cfg.Query, s.logger)
		s.router.HandleFunc("/api/search", search.Search).Methods(http.MethodGet)

		tags := api.NewTagHandlers(s.ch, s.logger)
		s.router.HandleFunc("/api/search/tags", tags.SearchTags).Methods(http.MethodGet)
		s.router.HandleFunc("/api/v2/search/tags", tags.SearchTagsV2).Methods(http.MethodGet)
		s.router.HandleFunc("/api/search/tag/{tagName:.*}/values", tags.SearchTagValues).Methods(http.MethodGet)
		s.router.HandleFunc("/api/v2/search/tag/{tagName:.*}/values", tags.SearchTagValuesV2).Methods(http.MethodGet)

		metrics := api.NewMetricsHandlers(s.ch, s.cfg.Query, s.logger)
		s.router.HandleFunc("/api/metrics/query_range", metrics.QueryRange).Methods(http.MethodGet)
		s.router.HandleFunc("/api/metrics/query", metrics.QueryInstant).Methods(http.MethodGet)
		s.router.HandleFunc("/api/metrics/summary", metrics.MetricsSummary).Methods(http.MethodGet)
	}

	// Prometheus metrics endpoint
	s.router.Handle("/metrics", metrics.Handler()).Methods(http.MethodGet)

	// Middleware: Prometheus metrics + request logging
	s.router.Use(metrics.Middleware)
	s.router.Use(s.loggingMiddleware)
}

func (s *Server) readyHandler(w http.ResponseWriter, r *http.Request) {
	if s.ch != nil {
		if err := s.ch.Ping(r.Context()); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("not ready: clickhouse unavailable"))
			return
		}
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready"))
}

// loggingResponseWriter captures the status code and response size.
type loggingResponseWriter struct {
	http.ResponseWriter
	statusCode int
	written    int64
}

func (lrw *loggingResponseWriter) WriteHeader(code int) {
	lrw.statusCode = code
	lrw.ResponseWriter.WriteHeader(code)
}

func (lrw *loggingResponseWriter) Write(b []byte) (int, error) {
	n, err := lrw.ResponseWriter.Write(b)
	lrw.written += int64(n)
	return n, err
}

func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		lrw := &loggingResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(lrw, r)

		duration := time.Since(start)

		// Use Warn level for slow queries (>5s), Error for errors, Info otherwise
		level := slog.LevelInfo
		if lrw.statusCode >= 500 {
			level = slog.LevelError
		} else if duration > 5*time.Second {
			level = slog.LevelWarn
		}

		attrs := []slog.Attr{
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Int("status", lrw.statusCode),
			slog.String("duration", duration.String()),
			slog.Int64("duration_ms", duration.Milliseconds()),
			slog.Int64("response_bytes", lrw.written),
			slog.String("remote", r.RemoteAddr),
		}

		// Add query parameter for search/metrics endpoints
		if q := r.URL.Query().Get("q"); q != "" {
			attrs = append(attrs, slog.String("query", q))
		}

		// Add user-agent for debugging
		if ua := r.Header.Get("User-Agent"); ua != "" {
			attrs = append(attrs, slog.String("user_agent", ua))
		}

		s.logger.LogAttrs(r.Context(), level, "request", attrs...)
	})
}

// Handler returns the HTTP handler (router) for use in tests.
func (s *Server) Handler() http.Handler {
	return s.router
}

// Run starts the HTTP server and blocks until shutdown.
func (s *Server) Run() error {
	srv := &http.Server{
		Addr:         s.cfg.ListenAddr,
		Handler:      s.router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Graceful shutdown
	errCh := make(chan error, 1)
	go func() {
		s.logger.Info("starting server", "addr", s.cfg.ListenAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- fmt.Errorf("server error: %w", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-quit:
		s.logger.Info("shutting down", "signal", sig.String())
	case err := <-errCh:
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		return fmt.Errorf("server shutdown: %w", err)
	}

	// Close ClickHouse connection
	if s.ch != nil {
		_ = s.ch.Close()
	}

	s.logger.Info("server stopped")
	return nil
}
