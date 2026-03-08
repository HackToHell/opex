package api

import (
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/hacktohell/opex/internal/clickhouse"
	"github.com/hacktohell/opex/internal/config"
	"github.com/hacktohell/opex/internal/response"
	"github.com/hacktohell/opex/internal/traceql"
	"github.com/hacktohell/opex/internal/transpiler"
)

// MetricsHandlers holds handlers for metrics endpoints.
type MetricsHandlers struct {
	ch     *clickhouse.Client
	cfg    config.QueryConfig
	logger *slog.Logger
}

// NewMetricsHandlers creates new MetricsHandlers.
func NewMetricsHandlers(ch *clickhouse.Client, cfg config.QueryConfig, logger *slog.Logger) *MetricsHandlers {
	return &MetricsHandlers{ch: ch, cfg: cfg, logger: logger}
}

// QueryRange handles GET /api/metrics/query_range.
func (h *MetricsHandlers) QueryRange(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	if q == "" {
		q = r.URL.Query().Get("query")
	}
	if q == "" {
		response.WriteError(w, http.StatusBadRequest, "query parameter 'q' is required")
		return
	}

	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")
	stepStr := r.URL.Query().Get("step")

	start, end := parseTimeRange(startStr, endStr)

	step := parseStep(stepStr, start, end)

	root, err := traceql.Parse(q)
	if err != nil {
		response.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid query: %v", err))
		return
	}

	// Find the metrics aggregate in the pipeline
	metricsElem, filterPipeline := extractMetricsAggregate(root)
	if metricsElem == nil {
		response.WriteError(w, http.StatusBadRequest, "query must contain a metrics aggregate (rate, count_over_time, etc.)")
		return
	}

	// Build the WHERE clause from the filter pipeline
	filterSQL := ""
	if filterPipeline != nil && len(filterPipeline.Elements) > 0 {
		opts := transpiler.TranspileOptions{
			Table: h.ch.Table(),
			Start: start,
			End:   end,
			Limit: 0, // no limit for metrics
		}
		result, err := transpiler.Transpile(&traceql.RootExpr{Pipeline: *filterPipeline}, opts)
		if err != nil {
			response.WriteError(w, http.StatusBadRequest, fmt.Sprintf("transpile error: %v", err))
			return
		}
		// Extract the WHERE conditions from the generated SQL
		// The result.SQL is a full SELECT, we need to extract conditions
		filterSQL = result.SQL
	}

	// Build the time-bucketed metrics query
	sql := h.buildMetricsRangeSQL(metricsElem, filterSQL, start, end, step)

	rows, err := h.ch.Query(r.Context(), sql)
	if err != nil {
		h.logger.Error("metrics query failed", "sql", sql, "error", err)
		response.WriteError(w, http.StatusInternalServerError, "query execution failed")
		return
	}
	defer func() { _ = rows.Close() }()

	// Build label names from by() clause or histogram bucket
	var labelNames []string
	if metricsElem.Op == traceql.MetricsAggregateHistogramOverTime {
		labelNames = []string{"le"}
	} else {
		for _, attr := range metricsElem.By {
			alias := strings.ReplaceAll(attr.String(), ".", "_")
			alias = strings.ReplaceAll(alias, ":", "_")
			labelNames = append(labelNames, alias)
		}
	}

	// Parse results into time series
	series := h.parseRangeRows(rows, len(labelNames), labelNames)

	response.WriteJSON(w, http.StatusOK, &response.QueryRangeResponse{
		Series: series,
	})
}

// QueryInstant handles GET /api/metrics/query.
func (h *MetricsHandlers) QueryInstant(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	if q == "" {
		q = r.URL.Query().Get("query")
	}
	if q == "" {
		response.WriteError(w, http.StatusBadRequest, "query parameter 'q' is required")
		return
	}

	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")
	start, end := parseTimeRange(startStr, endStr)

	root, err := traceql.Parse(q)
	if err != nil {
		response.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid query: %v", err))
		return
	}

	// Transpile the full query (including metrics aggregate)
	opts := transpiler.TranspileOptions{
		Table: h.ch.Table(),
		Start: start,
		End:   end,
		Limit: 100,
	}
	result, err := transpiler.Transpile(root, opts)
	if err != nil {
		response.WriteError(w, http.StatusBadRequest, fmt.Sprintf("transpile error: %v", err))
		return
	}

	rows, err := h.ch.Query(r.Context(), result.SQL)
	if err != nil {
		h.logger.Error("instant query failed", "sql", result.SQL, "error", err)
		response.WriteError(w, http.StatusInternalServerError, "query execution failed")
		return
	}
	defer func() { _ = rows.Close() }()

	var series []response.InstantSeries
	cols := rows.ColumnTypes()

	for rows.Next() {
		// Build typed scan targets based on column types
		scanArgs := make([]any, len(cols))
		valueIdx := -1
		labelIndices := map[int]string{}

		for i, col := range cols {
			name := col.Name()
			dbType := col.DatabaseTypeName()
			switch {
			case name == "value":
				valueIdx = i
				switch {
				case strings.Contains(dbType, "UInt"):
					scanArgs[i] = new(uint64)
				case strings.Contains(dbType, "Int"):
					scanArgs[i] = new(int64)
				default:
					scanArgs[i] = new(float64)
				}
			case strings.HasPrefix(name, "label_"):
				labelIndices[i] = strings.TrimPrefix(name, "label_")
				scanArgs[i] = new(string)
			default:
				scanArgs[i] = new(string)
			}
		}

		if err := rows.Scan(scanArgs...); err != nil {
			h.logger.Error("scan row failed", "error", err)
			continue
		}

		is := response.InstantSeries{}
		if valueIdx >= 0 {
			switch v := scanArgs[valueIdx].(type) {
			case *uint64:
				is.Value = float64(*v)
			case *int64:
				is.Value = float64(*v)
			case *float64:
				is.Value = *v
			}
		}
		var promParts []string
		for i, key := range labelIndices {
			val := *scanArgs[i].(*string)
			is.Labels = append(is.Labels, response.SeriesLabel{
				Key:   key,
				Value: response.SeriesLabelAnyValue{StringValue: val},
			})
			promParts = append(promParts, fmt.Sprintf("%s=%q", key, val))
		}
		if len(promParts) > 0 {
			is.PromLabels = "{" + strings.Join(promParts, ", ") + "}"
		}
		series = append(series, is)
	}

	if series == nil {
		series = []response.InstantSeries{}
	}

	response.WriteJSON(w, http.StatusOK, &response.QueryInstantResponse{Series: series})
}

// MetricsSummary handles GET /api/metrics/summary.
func (h *MetricsHandlers) MetricsSummary(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	groupBy := r.URL.Query().Get("groupBy")
	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")
	limitStr := r.URL.Query().Get("limit")

	start, end := parseTimeRange(startStr, endStr)
	limit := 10
	if limitStr != "" {
		if v, err := strconv.Atoi(limitStr); err == nil && v > 0 {
			limit = v
		}
	}

	// Build WHERE from q
	whereClause := fmt.Sprintf("Timestamp >= fromUnixTimestamp64Nano(%d) AND Timestamp <= fromUnixTimestamp64Nano(%d)",
		start.UnixNano(), end.UnixNano())
	if q != "" {
		root, err := traceql.Parse(q)
		if err != nil {
			response.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid query: %v", err))
			return
		}
		opts := transpiler.TranspileOptions{
			Table: h.ch.Table(),
			Start: start,
			End:   end,
			Limit: 0,
		}
		result, err := transpiler.Transpile(root, opts)
		if err != nil {
			response.WriteError(w, http.StatusBadRequest, fmt.Sprintf("transpile error: %v", err))
			return
		}
		_ = result // We'd extract WHERE from here in a more complete implementation
	}

	// Build GROUP BY columns from groupBy parameter
	var groupByCols []string
	var selectLabels []string
	if groupBy != "" {
		for _, g := range strings.Split(groupBy, ",") {
			g = strings.TrimSpace(g)
			if g == "" {
				continue
			}
			col := groupByToColumn(g)
			groupByCols = append(groupByCols, col)
			selectLabels = append(selectLabels, fmt.Sprintf("%s AS label_%s", col, strings.ReplaceAll(g, ".", "_")))
		}
	}

	selectParts := []string{
		"count(*) AS span_count",
		"countIf(StatusCode = 'STATUS_CODE_ERROR') AS error_span_count",
		"quantile(0.99)(Duration) AS p99",
		"quantile(0.95)(Duration) AS p95",
		"quantile(0.90)(Duration) AS p90",
		"quantile(0.50)(Duration) AS p50",
	}
	selectParts = append(selectParts, selectLabels...)

	groupByClause := ""
	if len(groupByCols) > 0 {
		groupByClause = " GROUP BY " + strings.Join(groupByCols, ", ")
	}

	sql := fmt.Sprintf("SELECT %s FROM %s WHERE %s%s LIMIT %d",
		strings.Join(selectParts, ", "), h.ch.Table(), whereClause, groupByClause, limit)

	rows, err := h.ch.Query(r.Context(), sql)
	if err != nil {
		h.logger.Error("summary query failed", "sql", sql, "error", err)
		response.WriteError(w, http.StatusInternalServerError, "query execution failed")
		return
	}
	defer func() { _ = rows.Close() }()

	var summaries []response.SpanMetricsSummary
	for rows.Next() {
		var s response.SpanMetricsSummary
		var spanCount, errorCount uint64
		var p99, p95, p90, p50 float64

		scanArgs := []any{&spanCount, &errorCount, &p99, &p95, &p90, &p50}

		// Add label scan args
		labelVals := make([]string, len(selectLabels))
		for i := range selectLabels {
			scanArgs = append(scanArgs, &labelVals[i])
		}

		if err := rows.Scan(scanArgs...); err != nil {
			h.logger.Error("scan summary row failed", "error", err)
			continue
		}

		s.SpanCount = int(spanCount)
		s.ErrorSpanCount = int(errorCount)

		// Convert durations from nanoseconds to milliseconds
		s.P99 = p99 / 1e6
		s.P95 = p95 / 1e6
		s.P90 = p90 / 1e6
		s.P50 = p50 / 1e6

		// Add labels
		for i, g := range strings.Split(groupBy, ",") {
			g = strings.TrimSpace(g)
			if g != "" && i < len(labelVals) {
				s.Series = append(s.Series, response.Label{Key: g, Value: labelVals[i]})
			}
		}

		summaries = append(summaries, s)
	}

	if summaries == nil {
		summaries = []response.SpanMetricsSummary{}
	}

	response.WriteJSON(w, http.StatusOK, &response.SpanMetricsSummaryResponse{Summaries: summaries})
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func parseStep(stepStr string, start, end time.Time) time.Duration {
	if stepStr != "" {
		// Try as duration
		if d, err := time.ParseDuration(stepStr); err == nil {
			return d
		}
		// Try as seconds
		if v, err := strconv.ParseInt(stepStr, 10, 64); err == nil {
			return time.Duration(v) * time.Second
		}
	}
	// Auto-calculate: aim for ~100 data points
	totalDur := end.Sub(start)
	step := totalDur / 100
	if step < time.Second {
		step = time.Second
	}
	return step
}

// extractMetricsAggregate separates the metrics aggregate from the filter pipeline.
func extractMetricsAggregate(root *traceql.RootExpr) (*traceql.MetricsAggregate, *traceql.Pipeline) {
	elements := root.Pipeline.Elements
	if len(elements) == 0 {
		return nil, nil
	}

	// Find the last MetricsAggregate in the pipeline
	for i := len(elements) - 1; i >= 0; i-- {
		if ma, ok := elements[i].(*traceql.MetricsAggregate); ok {
			// Everything before the metrics aggregate is the filter pipeline
			filterElements := elements[:i]
			var filterPipeline *traceql.Pipeline
			if len(filterElements) > 0 {
				filterPipeline = &traceql.Pipeline{Elements: filterElements}
			}
			return ma, filterPipeline
		}
	}

	return nil, nil
}

// histogramBucketNanos defines exponential duration bucket boundaries in nanoseconds
// for histogram_over_time. Spans are categorized by their upper-bound bucket.
var histogramBucketNanos = []struct {
	nanos int64
	label string
}{
	{2_000_000, "2ms"},
	{4_000_000, "4ms"},
	{8_000_000, "8ms"},
	{16_000_000, "16ms"},
	{32_000_000, "32ms"},
	{64_000_000, "64ms"},
	{128_000_000, "128ms"},
	{256_000_000, "256ms"},
	{512_000_000, "512ms"},
	{1_024_000_000, "1.024s"},
	{2_048_000_000, "2.048s"},
	{4_096_000_000, "4.096s"},
	{8_192_000_000, "8.192s"},
	{16_384_000_000, "16.384s"},
}

func (h *MetricsHandlers) buildMetricsRangeSQL(m *traceql.MetricsAggregate, filterSQL string, start, end time.Time, step time.Duration) string {
	stepSeconds := int(step.Seconds())
	if stepSeconds < 1 {
		stepSeconds = 1
	}

	timeFilter := fmt.Sprintf("Timestamp >= fromUnixTimestamp64Nano(%d) AND Timestamp <= fromUnixTimestamp64Nano(%d)",
		start.UnixNano(), end.UnixNano())

	where := timeFilter
	if filterSQL != "" {
		where = fmt.Sprintf("%s AND TraceId IN (%s)", timeFilter, filterSQL)
	}

	// Histogram needs special handling: group by time AND duration bucket
	if m.Op == traceql.MetricsAggregateHistogramOverTime {
		return h.buildHistogramSQL(where, stepSeconds)
	}

	var aggExpr string
	switch m.Op {
	case traceql.MetricsAggregateRate:
		aggExpr = fmt.Sprintf("count(*) / %d", stepSeconds)
	case traceql.MetricsAggregateCountOverTime:
		aggExpr = "count(*)"
	case traceql.MetricsAggregateMinOverTime:
		aggExpr = "min(Duration)"
	case traceql.MetricsAggregateMaxOverTime:
		aggExpr = "max(Duration)"
	case traceql.MetricsAggregateAvgOverTime:
		aggExpr = "avg(Duration)"
	case traceql.MetricsAggregateSumOverTime:
		aggExpr = "sum(Duration)"
	case traceql.MetricsAggregateQuantileOverTime:
		q := 0.5
		if len(m.Floats) > 0 {
			q = m.Floats[0]
		}
		aggExpr = fmt.Sprintf("quantile(%g)(Duration)", q)
	default:
		aggExpr = "count(*)"
	}

	var groupByCols []string
	var selectLabels []string
	groupByCols = append(groupByCols, "ts")
	for _, attr := range m.By {
		col := attributeToColumn(&attr)
		groupByCols = append(groupByCols, col)
		alias := strings.ReplaceAll(attr.String(), ".", "_")
		alias = strings.ReplaceAll(alias, ":", "_")
		selectLabels = append(selectLabels, fmt.Sprintf("%s AS label_%s", col, alias))
	}

	selectParts := []string{
		fmt.Sprintf("toStartOfInterval(Timestamp, INTERVAL %d SECOND) AS ts", stepSeconds),
		"toFloat64(" + aggExpr + ") AS value",
	}
	selectParts = append(selectParts, selectLabels...)

	return fmt.Sprintf("SELECT %s FROM %s WHERE %s GROUP BY %s ORDER BY ts",
		strings.Join(selectParts, ", "),
		h.ch.Table(),
		where,
		strings.Join(groupByCols, ", "),
	)
}

// buildHistogramSQL generates a query that buckets spans by duration
// to produce a histogram compatible with Grafana's heatmap visualization.
// Buckets are ordered by a numeric key so that "16ms" sorts before "128ms".
func (h *MetricsHandlers) buildHistogramSQL(where string, stepSeconds int) string {
	var labelCases, orderCases []string
	for i, b := range histogramBucketNanos {
		labelCases = append(labelCases, fmt.Sprintf("Duration <= %d, '%s'", b.nanos, b.label))
		orderCases = append(orderCases, fmt.Sprintf("Duration <= %d, %d", b.nanos, i))
	}
	labelExpr := fmt.Sprintf("multiIf(%s, '+Inf')", strings.Join(labelCases, ", "))
	orderExpr := fmt.Sprintf("multiIf(%s, %d)", strings.Join(orderCases, ", "), len(histogramBucketNanos))

	// Wrap in a subquery so the outer SELECT only exposes ts, value, label_le
	// (matching what parseRangeRows expects), while still sorting by bucket_ord.
	return fmt.Sprintf(
		"SELECT ts, value, label_le FROM ("+
			"SELECT toStartOfInterval(Timestamp, INTERVAL %d SECOND) AS ts, "+
			"toFloat64(count(*)) AS value, "+
			"%s AS label_le, "+
			"%s AS bucket_ord "+
			"FROM %s WHERE %s GROUP BY ts, label_le, bucket_ord ORDER BY ts, bucket_ord"+
			")",
		stepSeconds, labelExpr, orderExpr, h.ch.Table(), where,
	)
}

// parseRangeRows reads rows from the metrics range query and builds TimeSeries.
// Expected columns: ts (DateTime), value (Float64), label_* (optional String labels)
func (h *MetricsHandlers) parseRangeRows(rows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}, numLabels int, labelNames []string) []response.TimeSeries {
	type seriesKey string
	seriesMap := make(map[seriesKey]*response.TimeSeries)
	var order []seriesKey

	for rows.Next() {
		var ts time.Time
		var value float64
		scanArgs := []any{&ts, &value}

		labelVals := make([]string, numLabels)
		for i := range labelVals {
			scanArgs = append(scanArgs, &labelVals[i])
		}

		if err := rows.Scan(scanArgs...); err != nil {
			h.logger.Error("scan range row failed", "error", err)
			continue
		}

		// Build series key from labels
		key := seriesKey(strings.Join(labelVals, "|"))

		ts2, ok := seriesMap[key]
		if !ok {
			var labels []response.SeriesLabel
			var promParts []string
			for i, name := range labelNames {
				if i < len(labelVals) {
					labels = append(labels, response.SeriesLabel{
						Key:   name,
						Value: response.SeriesLabelAnyValue{StringValue: labelVals[i]},
					})
					promParts = append(promParts, fmt.Sprintf("%s=%q", name, labelVals[i]))
				}
			}
			promLabels := ""
			if len(promParts) > 0 {
				promLabels = "{" + strings.Join(promParts, ", ") + "}"
			}
			ts2 = &response.TimeSeries{Labels: labels, PromLabels: promLabels}
			seriesMap[key] = ts2
			order = append(order, key)
		}

		ts2.Samples = append(ts2.Samples, response.Sample{
			TimestampMs: ts.UnixMilli(),
			Value:       value,
		})
	}

	var result []response.TimeSeries
	for _, k := range order {
		result = append(result, *seriesMap[k])
	}
	if result == nil {
		result = []response.TimeSeries{}
	}
	return result
}

// attributeToColumn converts a traceql.Attribute to a SQL column reference.
func attributeToColumn(attr *traceql.Attribute) string {
	if attr.Intrinsic != traceql.IntrinsicNone {
		switch attr.Intrinsic {
		case traceql.IntrinsicDuration:
			return "Duration"
		case traceql.IntrinsicName:
			return "SpanName"
		case traceql.IntrinsicStatus:
			return "StatusCode"
		case traceql.IntrinsicKind:
			return "SpanKind"
		default:
			return "SpanName"
		}
	}
	switch attr.Scope {
	case traceql.AttributeScopeResource:
		if attr.Name == "service.name" {
			return "ServiceName"
		}
		return fmt.Sprintf("ResourceAttributes['%s']", attr.Name)
	case traceql.AttributeScopeSpan:
		return fmt.Sprintf("SpanAttributes['%s']", attr.Name)
	default:
		if attr.Name == "service.name" {
			return "ServiceName"
		}
		return fmt.Sprintf("SpanAttributes['%s']", attr.Name)
	}
}

func groupByToColumn(g string) string {
	// Handle common patterns
	if g == "resource.service.name" || g == "service.name" {
		return "ServiceName"
	}
	if strings.HasPrefix(g, "resource.") {
		attrName := strings.TrimPrefix(g, "resource.")
		return fmt.Sprintf("ResourceAttributes['%s']", attrName)
	}
	if strings.HasPrefix(g, "span.") {
		attrName := strings.TrimPrefix(g, "span.")
		return fmt.Sprintf("SpanAttributes['%s']", attrName)
	}
	if strings.HasPrefix(g, ".") {
		attrName := strings.TrimPrefix(g, ".")
		return fmt.Sprintf("SpanAttributes['%s']", attrName)
	}
	return fmt.Sprintf("SpanAttributes['%s']", g)
}

func toFloat64(v any) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int64:
		return float64(val)
	case int32:
		return float64(val)
	case uint64:
		return float64(val)
	case int:
		return float64(val)
	default:
		return math.NaN()
	}
}
