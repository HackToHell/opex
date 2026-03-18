package tracequery

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/hacktohell/opex/internal/clickhouse"
	"github.com/hacktohell/opex/internal/config"
	"github.com/hacktohell/opex/internal/response"
	"github.com/hacktohell/opex/internal/traceql"
	"github.com/hacktohell/opex/internal/transpiler"
)

// MetricsQueryRange executes a TraceQL metrics query and returns a time-bucketed
// series response. Step is auto-calculated if zero.
func MetricsQueryRange(ctx context.Context, ch *clickhouse.Client, cfg config.QueryConfig,
	query string, start, end time.Time, step time.Duration,
) (*response.QueryRangeResponse, error) {
	root, err := traceql.Parse(query)
	if err != nil {
		return nil, newInputError(fmt.Errorf("invalid query: %w", err))
	}

	metricsElem, filterPipeline := extractMetricsAggregate(root)
	if metricsElem == nil {
		return nil, newInputError(fmt.Errorf("query must contain a metrics aggregate (rate, count_over_time, etc.)"))
	}

	filterConditions := ""
	if filterPipeline != nil && len(filterPipeline.Elements) > 0 {
		opts := transpiler.TranspileOptions{
			Table: ch.Table(),
			Start: start,
			End:   end,
		}
		cond, err := transpiler.TranspileFilterConditions(filterPipeline, opts)
		if err != nil {
			return nil, newInputError(fmt.Errorf("transpile error: %w", err))
		}
		filterConditions = cond
	}

	sql := buildMetricsRangeSQL(ch.Table(), metricsElem, filterConditions, start, end, step)

	rows, err := ch.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("metrics query failed: %w", err)
	}
	defer func() { _ = rows.Close() }()

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

	series := parseRangeRows(rows, len(labelNames), labelNames)

	return &response.QueryRangeResponse{
		Series: series,
	}, nil
}

// MetricsQueryInstant executes a TraceQL metrics query and returns a single
// aggregated value for the given time range.
func MetricsQueryInstant(ctx context.Context, ch *clickhouse.Client, cfg config.QueryConfig,
	query string, start, end time.Time,
) (*response.QueryInstantResponse, error) {
	root, err := traceql.Parse(query)
	if err != nil {
		return nil, newInputError(fmt.Errorf("invalid query: %w", err))
	}

	opts := transpiler.TranspileOptions{
		Table: ch.Table(),
		Start: start,
		End:   end,
		Limit: 100,
	}
	result, err := transpiler.Transpile(root, opts)
	if err != nil {
		return nil, newInputError(fmt.Errorf("transpile error: %w", err))
	}

	rows, err := ch.Query(ctx, result.SQL)
	if err != nil {
		return nil, fmt.Errorf("instant query failed: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var series []response.InstantSeries
	cols := rows.ColumnTypes()

	for rows.Next() {
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
			slog.Default().Warn("scan instant row failed", "error", err)
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

	return &response.QueryInstantResponse{Series: series}, nil
}

// MetricsSummary computes span metrics (count, error rate, latency percentiles)
// grouped by the specified attributes.
func MetricsSummary(ctx context.Context, ch *clickhouse.Client,
	query string, groupBy []string, start, end time.Time, limit int,
) (*response.SpanMetricsSummaryResponse, error) {
	whereClause := fmt.Sprintf("Timestamp >= fromUnixTimestamp64Nano(%d) AND Timestamp <= fromUnixTimestamp64Nano(%d)",
		start.UnixNano(), end.UnixNano())
	if query != "" {
		root, err := traceql.Parse(query)
		if err != nil {
			return nil, newInputError(fmt.Errorf("invalid query: %w", err))
		}
		opts := transpiler.TranspileOptions{
			Table: ch.Table(),
			Start: start,
			End:   end,
		}
		cond, err := transpiler.TranspileFilterConditions(&root.Pipeline, opts)
		if err != nil {
			return nil, newInputError(fmt.Errorf("transpile error: %w", err))
		}
		if cond != "" {
			whereClause = whereClause + " AND " + cond
		}
	}

	var groupByCols []string
	var selectLabels []string
	for _, g := range groupBy {
		g = strings.TrimSpace(g)
		if g == "" {
			continue
		}
		col := groupByToColumn(g)
		groupByCols = append(groupByCols, col)
		selectLabels = append(selectLabels, fmt.Sprintf("%s AS label_%s", col, strings.ReplaceAll(g, ".", "_")))
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
		strings.Join(selectParts, ", "), ch.Table(), whereClause, groupByClause, limit)

	rows, err := ch.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("summary query failed: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var summaries []response.SpanMetricsSummary
	for rows.Next() {
		var s response.SpanMetricsSummary
		var spanCount, errorCount uint64
		var p99, p95, p90, p50 float64

		scanArgs := []any{&spanCount, &errorCount, &p99, &p95, &p90, &p50}

		labelVals := make([]string, len(selectLabels))
		for i := range selectLabels {
			scanArgs = append(scanArgs, &labelVals[i])
		}

		if err := rows.Scan(scanArgs...); err != nil {
			slog.Default().Warn("scan summary row failed", "error", err)
			continue
		}

		s.SpanCount = int(spanCount)
		s.ErrorSpanCount = int(errorCount)

		s.P99 = p99 / 1e6
		s.P95 = p95 / 1e6
		s.P90 = p90 / 1e6
		s.P50 = p50 / 1e6

		for i, g := range groupBy {
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

	return &response.SpanMetricsSummaryResponse{Summaries: summaries}, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// ExtractMetricsAggregate separates the metrics aggregate from the filter pipeline.
// Exported for use by tests.
func ExtractMetricsAggregate(root *traceql.RootExpr) (*traceql.MetricsAggregate, *traceql.Pipeline) {
	return extractMetricsAggregate(root)
}

func extractMetricsAggregate(root *traceql.RootExpr) (*traceql.MetricsAggregate, *traceql.Pipeline) {
	elements := root.Pipeline.Elements
	if len(elements) == 0 {
		return nil, nil
	}

	for i := len(elements) - 1; i >= 0; i-- {
		if ma, ok := elements[i].(*traceql.MetricsAggregate); ok {
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

// histogramBucketNanos defines exponential duration bucket boundaries in nanoseconds.
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

func buildMetricsRangeSQL(table string, m *traceql.MetricsAggregate, filterConditions string, start, end time.Time, step time.Duration) string {
	stepSeconds := int(step.Seconds())
	if stepSeconds < 1 {
		stepSeconds = 1
	}

	timeFilter := fmt.Sprintf("Timestamp >= fromUnixTimestamp64Nano(%d) AND Timestamp <= fromUnixTimestamp64Nano(%d)",
		start.UnixNano(), end.UnixNano())

	where := timeFilter
	if filterConditions != "" {
		where = fmt.Sprintf("%s AND %s", timeFilter, filterConditions)
	}

	if m.Op == traceql.MetricsAggregateHistogramOverTime {
		return buildHistogramSQL(table, where, stepSeconds)
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
		table,
		where,
		strings.Join(groupByCols, ", "),
	)
}

func buildHistogramSQL(table, where string, stepSeconds int) string {
	var labelCases, orderCases []string
	for i, b := range histogramBucketNanos {
		labelCases = append(labelCases, fmt.Sprintf("Duration <= %d, '%s'", b.nanos, b.label))
		orderCases = append(orderCases, fmt.Sprintf("Duration <= %d, %d", b.nanos, i))
	}
	labelExpr := fmt.Sprintf("multiIf(%s, '+Inf')", strings.Join(labelCases, ", "))
	orderExpr := fmt.Sprintf("multiIf(%s, %d)", strings.Join(orderCases, ", "), len(histogramBucketNanos))

	return fmt.Sprintf(
		"SELECT ts, value, label_le FROM ("+
			"SELECT toStartOfInterval(Timestamp, INTERVAL %d SECOND) AS ts, "+
			"toFloat64(count(*)) AS value, "+
			"%s AS label_le, "+
			"%s AS bucket_ord "+
			"FROM %s WHERE %s GROUP BY ts, label_le, bucket_ord ORDER BY ts, bucket_ord"+
			")",
		stepSeconds, labelExpr, orderExpr, table, where,
	)
}

// parseRangeRows reads rows from the metrics range query and builds TimeSeries.
func parseRangeRows(rows interface {
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
			continue
		}

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

// AttributeToColumn converts a traceql.Attribute to a SQL column reference.
// Exported for use by tests.
func AttributeToColumn(attr *traceql.Attribute) string {
	return attributeToColumn(attr)
}

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
		return fmt.Sprintf("ResourceAttributes['%s']", escapeSQL(attr.Name))
	case traceql.AttributeScopeSpan:
		return fmt.Sprintf("SpanAttributes['%s']", escapeSQL(attr.Name))
	default:
		if attr.Name == "service.name" {
			return "ServiceName"
		}
		return fmt.Sprintf("SpanAttributes['%s']", escapeSQL(attr.Name))
	}
}

// GroupByToColumn converts a group-by parameter to a SQL column reference.
// Exported for use by tests.
func GroupByToColumn(g string) string {
	return groupByToColumn(g)
}

func groupByToColumn(g string) string {
	if g == "resource.service.name" || g == "service.name" {
		return "ServiceName"
	}
	if strings.HasPrefix(g, "resource.") {
		attrName := strings.TrimPrefix(g, "resource.")
		return fmt.Sprintf("ResourceAttributes['%s']", escapeSQL(attrName))
	}
	if strings.HasPrefix(g, "span.") {
		attrName := strings.TrimPrefix(g, "span.")
		return fmt.Sprintf("SpanAttributes['%s']", escapeSQL(attrName))
	}
	if strings.HasPrefix(g, ".") {
		attrName := strings.TrimPrefix(g, ".")
		return fmt.Sprintf("SpanAttributes['%s']", escapeSQL(attrName))
	}
	return fmt.Sprintf("SpanAttributes['%s']", escapeSQL(g))
}
