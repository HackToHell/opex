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
	default:
		return path
	}
}
