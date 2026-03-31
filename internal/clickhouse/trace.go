package clickhouse

import (
	"context"
	"fmt"
	"time"
)

// TraceMetadataRow holds pre-aggregated trace-level metadata read from
// the otel_trace_metadata materialized table.
type TraceMetadataRow struct {
	TraceID         string
	RootServiceName string
	RootSpanName    string
	StartTime       time.Time
	MaxEndNano      int64
	SpanCount       uint64
	ErrorCount      uint64
}

// ServiceStatsRow holds per-service span/error counts for a single trace.
type ServiceStatsRow struct {
	TraceID     string
	ServiceName string
	SpanCount   uint64
	ErrorCount  uint64
}

// SpanRow represents a single row from the otel_traces table.
type SpanRow struct {
	Timestamp          time.Time
	TraceID            string
	SpanID             string
	ParentSpanID       string
	TraceState         string
	SpanName           string
	SpanKind           string
	ServiceName        string
	ResourceAttributes map[string]string
	ScopeName          string
	ScopeVersion       string
	SpanAttributes     map[string]string
	Duration           uint64
	StatusCode         string
	StatusMessage      string
	EventsTimestamp    []time.Time
	EventsName         []string
	EventsAttributes   []map[string]string
	LinksTraceID       []string
	LinksSpanID        []string
	LinksTraceState    []string
	LinksAttributes    []map[string]string
}

// QueryTraceByID retrieves all spans for a given trace ID.
func (c *Client) QueryTraceByID(ctx context.Context, traceID string) ([]SpanRow, error) {
	sql := fmt.Sprintf(`SELECT
		Timestamp, TraceId, SpanId, ParentSpanId, TraceState,
		SpanName, SpanKind, ServiceName, ResourceAttributes,
		ScopeName, ScopeVersion, SpanAttributes, Duration,
		StatusCode, StatusMessage,
		Events.Timestamp, Events.Name, Events.Attributes,
		Links.TraceId, Links.SpanId, Links.TraceState, Links.Attributes
	FROM %s
	WHERE TraceId = $1
	ORDER BY Timestamp ASC`, c.cfg.TracesTable)

	rows, err := c.Query(ctx, sql, traceID)
	if err != nil {
		return nil, fmt.Errorf("query trace %s: %w", traceID, err)
	}
	defer func() { _ = rows.Close() }()

	var spans []SpanRow
	for rows.Next() {
		var s SpanRow
		if err := rows.Scan(
			&s.Timestamp, &s.TraceID, &s.SpanID, &s.ParentSpanID, &s.TraceState,
			&s.SpanName, &s.SpanKind, &s.ServiceName, &s.ResourceAttributes,
			&s.ScopeName, &s.ScopeVersion, &s.SpanAttributes, &s.Duration,
			&s.StatusCode, &s.StatusMessage,
			&s.EventsTimestamp, &s.EventsName, &s.EventsAttributes,
			&s.LinksTraceID, &s.LinksSpanID, &s.LinksTraceState, &s.LinksAttributes,
		); err != nil {
			return nil, fmt.Errorf("scan span row: %w", err)
		}
		spans = append(spans, s)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}

	return spans, nil
}

// QueryTraceIDs executes a raw SQL query and returns the matching trace IDs.
func (c *Client) QueryTraceIDs(ctx context.Context, sql string) ([]string, error) {
	rows, err := c.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("query trace IDs: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var traceIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan trace ID: %w", err)
		}
		traceIDs = append(traceIDs, id)
	}

	return traceIDs, nil
}

// UseMatViews returns whether materialized views are enabled.
func (c *Client) UseMatViews() bool {
	return c.cfg.UseMatViews
}

// TraceMetadataTable returns the name of the trace metadata table.
func (c *Client) TraceMetadataTable() string {
	if c.cfg.TraceMetadataTable != "" {
		return c.cfg.TraceMetadataTable
	}
	return "otel_trace_metadata"
}

// SpanTagNamesTable returns the name of the span tag names table.
func (c *Client) SpanTagNamesTable() string {
	if c.cfg.SpanTagNamesTable != "" {
		return c.cfg.SpanTagNamesTable
	}
	return "otel_span_tag_names"
}

// ResourceTagNamesTable returns the name of the resource tag names table.
func (c *Client) ResourceTagNamesTable() string {
	if c.cfg.ResourceTagNamesTable != "" {
		return c.cfg.ResourceTagNamesTable
	}
	return "otel_resource_tag_names"
}

// ServiceNamesTable returns the name of the service names table.
func (c *Client) ServiceNamesTable() string {
	if c.cfg.ServiceNamesTable != "" {
		return c.cfg.ServiceNamesTable
	}
	return "otel_service_names"
}

// QueryTraceMetadataByTraceIDs reads pre-aggregated metadata from the trace
// metadata materialized table for the given trace IDs.
func (c *Client) QueryTraceMetadataByTraceIDs(ctx context.Context, traceIDs []string) ([]TraceMetadataRow, error) {
	if len(traceIDs) == 0 {
		return nil, nil
	}

	sql := fmt.Sprintf(`SELECT
		TraceId,
		argMinMerge(RootServiceName) AS RootServiceName,
		argMinMerge(RootSpanName) AS RootSpanName,
		min(StartTime) AS StartTime,
		max(MaxEndNano) AS MaxEndNano,
		sum(SpanCount) AS SpanCount,
		sum(ErrorCount) AS ErrorCount
	FROM %s
	WHERE TraceId IN ($1)
	GROUP BY TraceId`, c.TraceMetadataTable())

	rows, err := c.Query(ctx, sql, traceIDs)
	if err != nil {
		return nil, fmt.Errorf("query trace metadata: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var results []TraceMetadataRow
	for rows.Next() {
		var m TraceMetadataRow
		if err := rows.Scan(
			&m.TraceID, &m.RootServiceName, &m.RootSpanName,
			&m.StartTime, &m.MaxEndNano,
			&m.SpanCount, &m.ErrorCount,
		); err != nil {
			return nil, fmt.Errorf("scan trace metadata: %w", err)
		}
		results = append(results, m)
	}
	return results, rows.Err()
}

// QueryServiceStatsByTraceIDs returns per-service span/error counts grouped
// by (TraceId, ServiceName) for the given trace IDs.
func (c *Client) QueryServiceStatsByTraceIDs(ctx context.Context, traceIDs []string) ([]ServiceStatsRow, error) {
	if len(traceIDs) == 0 {
		return nil, nil
	}

	sql := fmt.Sprintf(`SELECT
		TraceId, ServiceName,
		count(*) AS SpanCount,
		countIf(StatusCode = 'STATUS_CODE_ERROR') AS ErrorCount
	FROM %s
	WHERE TraceId IN ($1)
	GROUP BY TraceId, ServiceName`, c.cfg.TracesTable)

	rows, err := c.Query(ctx, sql, traceIDs)
	if err != nil {
		return nil, fmt.Errorf("query service stats: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var results []ServiceStatsRow
	for rows.Next() {
		var s ServiceStatsRow
		if err := rows.Scan(&s.TraceID, &s.ServiceName, &s.SpanCount, &s.ErrorCount); err != nil {
			return nil, fmt.Errorf("scan service stats: %w", err)
		}
		results = append(results, s)
	}
	return results, rows.Err()
}

// QueryTagNamesFromBuckets queries distinct tag names from a bucketed
// materialized view table. The caller is responsible for snapping
// snappedStart and snappedEndExclusive to 5-minute boundaries.
func (c *Client) QueryTagNamesFromBuckets(ctx context.Context, table string, snappedStart, snappedEndExclusive time.Time) ([]string, error) {
	sql := fmt.Sprintf(
		"SELECT DISTINCT TagName FROM %s WHERE BucketStart >= fromUnixTimestamp64Nano(%d) AND BucketStart < fromUnixTimestamp64Nano(%d) ORDER BY TagName LIMIT 1000",
		table, snappedStart.UnixNano(), snappedEndExclusive.UnixNano(),
	)
	rows, err := c.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("query tag names from %s: %w", table, err)
	}
	defer func() { _ = rows.Close() }()

	var tags []string
	for rows.Next() {
		var tag string
		if err := rows.Scan(&tag); err != nil {
			return nil, fmt.Errorf("scan tag name: %w", err)
		}
		tags = append(tags, tag)
	}
	return tags, rows.Err()
}

// QueryServiceNamesFromBuckets queries distinct service names from the
// bucketed service names table. The caller is responsible for snapping
// snappedStart and snappedEndExclusive to 5-minute boundaries.
func (c *Client) QueryServiceNamesFromBuckets(ctx context.Context, snappedStart, snappedEndExclusive time.Time) ([]string, error) {
	sql := fmt.Sprintf(
		"SELECT DISTINCT ServiceName FROM %s WHERE BucketStart >= fromUnixTimestamp64Nano(%d) AND BucketStart < fromUnixTimestamp64Nano(%d) ORDER BY ServiceName LIMIT 1000",
		c.ServiceNamesTable(), snappedStart.UnixNano(), snappedEndExclusive.UnixNano(),
	)
	rows, err := c.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("query service names: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan service name: %w", err)
		}
		names = append(names, name)
	}
	return names, rows.Err()
}

// QuerySpansByTraceIDs retrieves spans for multiple trace IDs.
func (c *Client) QuerySpansByTraceIDs(ctx context.Context, traceIDs []string) ([]SpanRow, error) {
	if len(traceIDs) == 0 {
		return nil, nil
	}

	sql := fmt.Sprintf(`SELECT
		Timestamp, TraceId, SpanId, ParentSpanId, TraceState,
		SpanName, SpanKind, ServiceName, ResourceAttributes,
		ScopeName, ScopeVersion, SpanAttributes, Duration,
		StatusCode, StatusMessage,
		Events.Timestamp, Events.Name, Events.Attributes,
		Links.TraceId, Links.SpanId, Links.TraceState, Links.Attributes
	FROM %s
	WHERE TraceId IN ($1)
	ORDER BY TraceId, Timestamp ASC`, c.cfg.TracesTable)

	rows, err := c.Query(ctx, sql, traceIDs)
	if err != nil {
		return nil, fmt.Errorf("query spans: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var spans []SpanRow
	for rows.Next() {
		var s SpanRow
		if err := rows.Scan(
			&s.Timestamp, &s.TraceID, &s.SpanID, &s.ParentSpanID, &s.TraceState,
			&s.SpanName, &s.SpanKind, &s.ServiceName, &s.ResourceAttributes,
			&s.ScopeName, &s.ScopeVersion, &s.SpanAttributes, &s.Duration,
			&s.StatusCode, &s.StatusMessage,
			&s.EventsTimestamp, &s.EventsName, &s.EventsAttributes,
			&s.LinksTraceID, &s.LinksSpanID, &s.LinksTraceState, &s.LinksAttributes,
		); err != nil {
			return nil, fmt.Errorf("scan span row: %w", err)
		}
		spans = append(spans, s)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}

	return spans, nil
}
