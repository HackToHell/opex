// Package metrics provides Prometheus metrics instrumentation for Opex.
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Opex Prometheus metrics.
var (
	// QueryDuration tracks HTTP request duration by endpoint and status.
	QueryDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "opex",
			Name:      "query_duration_seconds",
			Help:      "Duration of HTTP requests in seconds.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"endpoint", "method", "status_code"},
	)

	// ClickHouseQueryDuration tracks ClickHouse query execution time.
	ClickHouseQueryDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "opex",
			Name:      "clickhouse_query_duration_seconds",
			Help:      "Duration of ClickHouse query execution in seconds.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"query_type"},
	)

	// ActiveQueries tracks the number of in-flight queries.
	ActiveQueries = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "opex",
			Name:      "active_queries",
			Help:      "Number of currently in-flight queries.",
		},
	)

	// QueryErrors counts query errors by type.
	QueryErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "opex",
			Name:      "query_errors_total",
			Help:      "Total number of query errors by type.",
		},
		[]string{"error_type"},
	)

	// TracesSearched tracks the total number of traces inspected.
	TracesSearched = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "opex",
			Name:      "traces_searched_total",
			Help:      "Total number of traces inspected across all search queries.",
		},
	)

	// SpansSearched tracks the total number of spans inspected.
	SpansSearched = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "opex",
			Name:      "spans_searched_total",
			Help:      "Total number of spans inspected across all search queries.",
		},
	)

	// QueryRetries counts the number of query retries due to transient errors.
	QueryRetries = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "opex",
			Name:      "query_retries_total",
			Help:      "Total number of query retries due to transient ClickHouse errors.",
		},
	)

	// CircuitBreakerState tracks the current circuit breaker state.
	// 0 = closed (healthy), 1 = half-open (probing), 2 = open (rejecting).
	CircuitBreakerState = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "opex",
			Name:      "clickhouse_circuit_state",
			Help:      "Current circuit breaker state: 0=closed, 1=half-open, 2=open.",
		},
	)

	// ClickHouseConnected tracks whether the client has an active connection.
	// 1 = connected, 0 = disconnected.
	ClickHouseConnected = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "opex",
			Name:      "clickhouse_connected",
			Help:      "Whether the ClickHouse connection is active: 1=connected, 0=disconnected.",
		},
	)

	// ReconnectAttempts counts the number of reconnection attempts.
	ReconnectAttempts = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "opex",
			Name:      "clickhouse_reconnect_attempts_total",
			Help:      "Total number of ClickHouse reconnection attempts.",
		},
	)

	// MCPToolCalls counts the total number of MCP tool calls.
	MCPToolCalls = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "opex",
			Name:      "mcp_tool_calls_total",
			Help:      "Total number of MCP tool calls.",
		},
		[]string{"tool"},
	)

	// MCPToolDuration tracks the duration of MCP tool calls.
	MCPToolDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "opex",
			Name:      "mcp_tool_duration_seconds",
			Help:      "Duration of MCP tool calls.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"tool"},
	)

	// MCPToolErrors counts the total number of MCP tool errors.
	MCPToolErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "opex",
			Name:      "mcp_tool_errors_total",
			Help:      "Total number of MCP tool errors.",
		},
		[]string{"tool", "error_type"},
	)
)

// Handler returns the Prometheus metrics HTTP handler.
func Handler() http.Handler {
	return promhttp.Handler()
}

// responseWriter wraps http.ResponseWriter to capture the status code.
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// Middleware returns HTTP middleware that records request duration and active queries.
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ActiveQueries.Inc()
		defer ActiveQueries.Dec()

		start := time.Now()

		rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(rw, r)

		duration := time.Since(start).Seconds()
		endpoint := normalizeEndpoint(r.URL.Path)

		QueryDuration.WithLabelValues(
			endpoint,
			r.Method,
			strconv.Itoa(rw.statusCode),
		).Observe(duration)
	})
}

// ObserveClickHouseQuery records the duration of a ClickHouse query.
func ObserveClickHouseQuery(queryType string, duration time.Duration) {
	ClickHouseQueryDuration.WithLabelValues(queryType).Observe(duration.Seconds())
}

// RecordQueryError increments the error counter for a given error type.
func RecordQueryError(errorType string) {
	QueryErrors.WithLabelValues(errorType).Inc()
}

// RecordQueryRetry increments the retry counter.
func RecordQueryRetry() {
	QueryRetries.Inc()
}

// SetCircuitBreakerState sets the current circuit breaker state gauge.
func SetCircuitBreakerState(state float64) {
	CircuitBreakerState.Set(state)
}

// SetClickHouseConnected sets the connection status gauge.
func SetClickHouseConnected(connected bool) {
	if connected {
		ClickHouseConnected.Set(1)
	} else {
		ClickHouseConnected.Set(0)
	}
}

// RecordReconnectAttempt increments the reconnection attempt counter.
func RecordReconnectAttempt() {
	ReconnectAttempts.Inc()
}

// normalizeEndpoint collapses variable path segments into templates.
// E.g., /api/traces/abc123 → /api/traces/{traceID}
func normalizeEndpoint(path string) string {
	// Common Tempo API patterns
	switch {
	case len(path) > len("/api/traces/") && path[:len("/api/traces/")] == "/api/traces/":
		return "/api/traces/{traceID}"
	case len(path) > len("/api/v2/traces/") && path[:len("/api/v2/traces/")] == "/api/v2/traces/":
		return "/api/v2/traces/{traceID}"
	case len(path) > len("/api/search/tag/") && path[:len("/api/search/tag/")] == "/api/search/tag/":
		return "/api/search/tag/{tagName}/values"
	case len(path) > len("/api/v2/search/tag/") && path[:len("/api/v2/search/tag/")] == "/api/v2/search/tag/":
		return "/api/v2/search/tag/{tagName}/values"
	case path == "/api/mcp" || (len(path) > len("/api/mcp/") && path[:len("/api/mcp/")] == "/api/mcp/"):
		return "/api/mcp"
	default:
		return path
	}
}
