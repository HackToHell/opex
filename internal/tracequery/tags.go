package tracequery

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"time"

	"github.com/hacktohell/opex/internal/clickhouse"
	"github.com/hacktohell/opex/internal/response"
	"github.com/hacktohell/opex/internal/traceql"
	"github.com/hacktohell/opex/internal/transpiler"
)

// validAttributeName validates attribute names to prevent SQL injection.
// Allows alphanumerics, underscores, and dots (for dotted attribute names
// like "http.method" or "resource.service.name").
var validAttributeName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_.]*$`)

// IntrinsicTags lists intrinsic tag names available in TraceQL.
var IntrinsicTags = []string{
	"duration", "name", "status", "statusMessage", "kind",
	"rootServiceName", "rootName", "traceDuration",
}

// GetTagNames returns available attribute names, optionally filtered by scope
// (span, resource, intrinsic, or empty for all).
func GetTagNames(ctx context.Context, ch *clickhouse.Client,
	scope string, start, end time.Time,
) (*response.SearchTagsV2Response, error) {
	var scopes []response.SearchTagsV2Scope

	switch scope {
	case "intrinsic":
		scopes = append(scopes, response.SearchTagsV2Scope{
			Name: "intrinsic",
			Tags: IntrinsicTags,
		})
	case "resource":
		tags, err := queryMapKeys(ctx, ch, "ResourceAttributes", start, end)
		if err != nil {
			return nil, fmt.Errorf("query resource tags: %w", err)
		}
		sort.Strings(tags)
		scopes = append(scopes, response.SearchTagsV2Scope{
			Name: "resource",
			Tags: tags,
		})
	case "span":
		tags, err := queryMapKeys(ctx, ch, "SpanAttributes", start, end)
		if err != nil {
			return nil, fmt.Errorf("query span tags: %w", err)
		}
		sort.Strings(tags)
		scopes = append(scopes, response.SearchTagsV2Scope{
			Name: "span",
			Tags: tags,
		})
	default:
		// No scope: return all
		scopes = append(scopes, response.SearchTagsV2Scope{
			Name: "intrinsic",
			Tags: IntrinsicTags,
		})

		resTags, err := queryMapKeys(ctx, ch, "ResourceAttributes", start, end)
		if err == nil {
			sort.Strings(resTags)
			scopes = append(scopes, response.SearchTagsV2Scope{
				Name: "resource",
				Tags: resTags,
			})
		}

		spanTags, err := queryMapKeys(ctx, ch, "SpanAttributes", start, end)
		if err == nil {
			sort.Strings(spanTags)
			scopes = append(scopes, response.SearchTagsV2Scope{
				Name: "span",
				Tags: spanTags,
			})
		}
	}

	return &response.SearchTagsV2Response{Scopes: scopes}, nil
}

// GetTagValues returns distinct values for a given attribute name, optionally
// filtered by a TraceQL filter expression.
func GetTagValues(ctx context.Context, ch *clickhouse.Client,
	tagName string, filterQuery string, start, end time.Time,
) (*response.SearchTagValuesV2Response, error) {
	if !validAttributeName.MatchString(tagName) {
		return nil, newInputError(fmt.Errorf("invalid attribute name %q", tagName))
	}

	values, err := queryTagValues(ctx, ch, tagName, filterQuery, start, end)
	if err != nil {
		return nil, fmt.Errorf("query tag values: %w", err)
	}

	sort.Strings(values)

	var tagValues []response.TagValue
	for _, v := range values {
		tagValues = append(tagValues, response.TagValue{Type: "string", Value: v})
	}

	if tagValues == nil {
		tagValues = []response.TagValue{}
	}

	return &response.SearchTagValuesV2Response{TagValues: tagValues}, nil
}

// ---------------------------------------------------------------------------
// Bucket snapping
// ---------------------------------------------------------------------------

const bucketInterval = 5 * time.Minute

// snapTo5m rounds a request window outward to 5-minute boundaries for
// bucketed MV queries. start is floored, end is ceiled. The returned
// snappedEnd is exclusive (suitable for BucketStart < snappedEnd).
func snapTo5m(start, end time.Time) (snappedStart, snappedEnd time.Time) {
	startNano := start.UnixNano()
	endNano := end.UnixNano()
	intervalNano := int64(bucketInterval)

	snappedStart = time.Unix(0, startNano-startNano%intervalNano).UTC()

	rem := endNano % intervalNano
	if rem == 0 {
		snappedEnd = end.UTC()
	} else {
		snappedEnd = time.Unix(0, endNano+(intervalNano-rem)).UTC()
	}
	return snappedStart, snappedEnd
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func queryMapKeys(ctx context.Context, ch *clickhouse.Client, mapCol string, start, end time.Time) ([]string, error) {
	if ch.UseMatViews() {
		var table string
		switch mapCol {
		case "SpanAttributes":
			table = ch.SpanTagNamesTable()
		case "ResourceAttributes":
			table = ch.ResourceTagNamesTable()
		}
		if table != "" {
			snappedStart, snappedEnd := snapTo5m(start, end)
			return ch.QueryTagNamesFromBuckets(ctx, table, snappedStart, snappedEnd)
		}
	}

	sql := fmt.Sprintf(
		"SELECT DISTINCT arrayJoin(mapKeys(%s)) AS tag_name FROM %s WHERE Timestamp >= fromUnixTimestamp64Nano(%d) AND Timestamp <= fromUnixTimestamp64Nano(%d) ORDER BY tag_name LIMIT 1000",
		mapCol, ch.Table(), start.UnixNano(), end.UnixNano(),
	)

	rows, err := ch.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var tags []string
	for rows.Next() {
		var tag string
		if err := rows.Scan(&tag); err != nil {
			return nil, err
		}
		tags = append(tags, tag)
	}
	return tags, rows.Err()
}

func queryTagValues(ctx context.Context, ch *clickhouse.Client, tagName string, filterQuery string, start, end time.Time) ([]string, error) {
	switch tagName {
	case "status":
		return []string{"error", "ok", "unset"}, nil
	case "kind":
		return []string{"unspecified", "internal", "client", "server", "producer", "consumer"}, nil
	case "service.name", "resource.service.name":
		return queryDistinctColumn(ctx, ch, "ServiceName", start, end)
	}

	// Build optional filter conditions from TraceQL filter query.
	extraFilter := ""
	if filterQuery != "" {
		root, err := traceql.Parse(filterQuery)
		if err != nil {
			return nil, newInputError(fmt.Errorf("invalid filter query: %w", err))
		}
		opts := transpiler.TranspileOptions{
			Table: ch.Table(),
			Start: start,
			End:   end,
		}
		cond, err := transpiler.TranspileFilterConditions(&root.Pipeline, opts)
		if err != nil {
			return nil, newInputError(fmt.Errorf("transpile filter error: %w", err))
		}
		if cond != "" {
			extraFilter = " AND " + cond
		}
	}

	escaped := escapeSQL(tagName)
	sql := fmt.Sprintf(
		`SELECT DISTINCT val FROM (
			SELECT SpanAttributes['%s'] AS val FROM %s
			WHERE mapContains(SpanAttributes, '%s')
			  AND Timestamp >= fromUnixTimestamp64Nano(%d) AND Timestamp <= fromUnixTimestamp64Nano(%d)%s
			UNION ALL
			SELECT ResourceAttributes['%s'] AS val FROM %s
			WHERE mapContains(ResourceAttributes, '%s')
			  AND Timestamp >= fromUnixTimestamp64Nano(%d) AND Timestamp <= fromUnixTimestamp64Nano(%d)%s
		) WHERE val != '' ORDER BY val LIMIT 1000`,
		escaped, ch.Table(), escaped, start.UnixNano(), end.UnixNano(), extraFilter,
		escaped, ch.Table(), escaped, start.UnixNano(), end.UnixNano(), extraFilter,
	)

	rows, err := ch.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var values []string
	for rows.Next() {
		var val string
		if err := rows.Scan(&val); err != nil {
			return nil, err
		}
		values = append(values, val)
	}
	return values, rows.Err()
}

func queryDistinctColumn(ctx context.Context, ch *clickhouse.Client, col string, start, end time.Time) ([]string, error) {
	if col == "ServiceName" && ch.UseMatViews() {
		snappedStart, snappedEnd := snapTo5m(start, end)
		return ch.QueryServiceNamesFromBuckets(ctx, snappedStart, snappedEnd)
	}

	sql := fmt.Sprintf(
		"SELECT DISTINCT %s FROM %s WHERE Timestamp >= fromUnixTimestamp64Nano(%d) AND Timestamp <= fromUnixTimestamp64Nano(%d) ORDER BY %s LIMIT 1000",
		col, ch.Table(), start.UnixNano(), end.UnixNano(), col,
	)

	rows, err := ch.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var values []string
	for rows.Next() {
		var val string
		if err := rows.Scan(&val); err != nil {
			return nil, err
		}
		values = append(values, val)
	}
	return values, rows.Err()
}
