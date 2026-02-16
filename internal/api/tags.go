package api

import (
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"

	"github.com/gorilla/mux"
	"github.com/hacktohell/opex/internal/clickhouse"
	"github.com/hacktohell/opex/internal/response"
)

// TagHandlers holds handlers for tag and tag value endpoints.
type TagHandlers struct {
	ch     *clickhouse.Client
	logger *slog.Logger
}

// NewTagHandlers creates new TagHandlers.
func NewTagHandlers(ch *clickhouse.Client, logger *slog.Logger) *TagHandlers {
	return &TagHandlers{ch: ch, logger: logger}
}

// Intrinsic tag names available in TraceQL.
var intrinsicTags = []string{
	"duration", "name", "status", "statusMessage", "kind",
	"rootServiceName", "rootName", "traceDuration",
}

// SearchTags handles GET /api/search/tags.
func (h *TagHandlers) SearchTags(w http.ResponseWriter, r *http.Request) {
	scope := r.URL.Query().Get("scope")
	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")

	start, end := parseTimeRange(startStr, endStr)

	var tags []string

	switch strings.ToLower(scope) {
	case "intrinsic":
		tags = append(tags, intrinsicTags...)
	case "resource":
		var err error
		tags, err = h.queryMapKeys(r, "ResourceAttributes", start, end)
		if err != nil {
			h.logger.Error("query resource tags failed", "error", err)
			response.WriteError(w, http.StatusInternalServerError, "failed to query tags")
			return
		}
	case "span":
		var err error
		tags, err = h.queryMapKeys(r, "SpanAttributes", start, end)
		if err != nil {
			h.logger.Error("query span tags failed", "error", err)
			response.WriteError(w, http.StatusInternalServerError, "failed to query tags")
			return
		}
	default:
		// No scope or "none": return all
		tags = append(tags, intrinsicTags...)
		resTags, err := h.queryMapKeys(r, "ResourceAttributes", start, end)
		if err == nil {
			tags = append(tags, resTags...)
		}
		spanTags, err := h.queryMapKeys(r, "SpanAttributes", start, end)
		if err == nil {
			tags = append(tags, spanTags...)
		}
	}

	sort.Strings(tags)
	// Deduplicate
	tags = dedup(tags)

	response.WriteJSON(w, http.StatusOK, &response.SearchTagsResponse{TagNames: tags})
}

// SearchTagsV2 handles GET /api/v2/search/tags.
func (h *TagHandlers) SearchTagsV2(w http.ResponseWriter, r *http.Request) {
	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")

	start, end := parseTimeRange(startStr, endStr)

	var scopes []response.SearchTagsV2Scope

	// Intrinsic scope
	scopes = append(scopes, response.SearchTagsV2Scope{
		Name: "intrinsic",
		Tags: intrinsicTags,
	})

	// Resource scope
	resTags, err := h.queryMapKeys(r, "ResourceAttributes", start, end)
	if err != nil {
		h.logger.Error("query resource tags failed", "error", err)
	} else {
		sort.Strings(resTags)
		scopes = append(scopes, response.SearchTagsV2Scope{
			Name: "resource",
			Tags: resTags,
		})
	}

	// Span scope
	spanTags, err := h.queryMapKeys(r, "SpanAttributes", start, end)
	if err != nil {
		h.logger.Error("query span tags failed", "error", err)
	} else {
		sort.Strings(spanTags)
		scopes = append(scopes, response.SearchTagsV2Scope{
			Name: "span",
			Tags: spanTags,
		})
	}

	response.WriteJSON(w, http.StatusOK, &response.SearchTagsV2Response{Scopes: scopes})
}

// SearchTagValues handles GET /api/search/tag/{tagName}/values.
func (h *TagHandlers) SearchTagValues(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	tagName := vars["tagName"]

	if tagName == "" {
		response.WriteError(w, http.StatusBadRequest, "tagName is required")
		return
	}

	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")
	start, end := parseTimeRange(startStr, endStr)

	values, err := h.queryTagValues(r, tagName, start, end)
	if err != nil {
		h.logger.Error("query tag values failed", "tag", tagName, "error", err)
		response.WriteError(w, http.StatusInternalServerError, "failed to query tag values")
		return
	}

	sort.Strings(values)

	response.WriteJSON(w, http.StatusOK, &response.SearchTagValuesResponse{TagValues: values})
}

// SearchTagValuesV2 handles GET /api/v2/search/tag/{tagName}/values.
func (h *TagHandlers) SearchTagValuesV2(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	tagName := vars["tagName"]

	if tagName == "" {
		response.WriteError(w, http.StatusBadRequest, "tagName is required")
		return
	}

	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")
	start, end := parseTimeRange(startStr, endStr)

	values, err := h.queryTagValues(r, tagName, start, end)
	if err != nil {
		h.logger.Error("query tag values failed", "tag", tagName, "error", err)
		response.WriteError(w, http.StatusInternalServerError, "failed to query tag values")
		return
	}

	sort.Strings(values)

	var tagValues []response.TagValue
	for _, v := range values {
		tagValues = append(tagValues, response.TagValue{Type: "string", Value: v})
	}

	if tagValues == nil {
		tagValues = []response.TagValue{}
	}

	response.WriteJSON(w, http.StatusOK, &response.SearchTagValuesV2Response{TagValues: tagValues})
}

// queryMapKeys queries ClickHouse for distinct map keys.
// If materialized views are enabled, queries the pre-computed tag name tables.
func (h *TagHandlers) queryMapKeys(r *http.Request, mapCol string, start, end interface{ UnixNano() int64 }) ([]string, error) {
	// Use materialized views if available
	if h.ch.UseMatViews() {
		var table string
		switch mapCol {
		case "SpanAttributes":
			table = h.ch.SpanTagNamesTable()
		case "ResourceAttributes":
			table = h.ch.ResourceTagNamesTable()
		}
		if table != "" {
			return h.ch.QueryTagNamesFromView(r.Context(), table)
		}
	}

	// Fallback: scan the traces table
	sql := fmt.Sprintf(
		"SELECT DISTINCT arrayJoin(mapKeys(%s)) AS tag_name FROM %s WHERE Timestamp >= fromUnixTimestamp64Nano(%d) AND Timestamp <= fromUnixTimestamp64Nano(%d) ORDER BY tag_name LIMIT 1000",
		mapCol, h.ch.Table(), start.UnixNano(), end.UnixNano(),
	)

	rows, err := h.ch.Query(r.Context(), sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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

// queryTagValues queries ClickHouse for distinct values of a tag.
func (h *TagHandlers) queryTagValues(r *http.Request, tagName string, start, end interface{ UnixNano() int64 }) ([]string, error) {
	// Check for intrinsic tags first
	switch tagName {
	case "status":
		return []string{"error", "ok", "unset"}, nil
	case "kind":
		return []string{"unspecified", "internal", "client", "server", "producer", "consumer"}, nil
	case "service.name", "resource.service.name":
		return h.queryDistinctColumn(r, "ServiceName", start, end)
	}

	// Check both SpanAttributes and ResourceAttributes
	sql := fmt.Sprintf(
		`SELECT DISTINCT val FROM (
			SELECT SpanAttributes['%s'] AS val FROM %s
			WHERE mapContains(SpanAttributes, '%s')
			  AND Timestamp >= fromUnixTimestamp64Nano(%d) AND Timestamp <= fromUnixTimestamp64Nano(%d)
			UNION ALL
			SELECT ResourceAttributes['%s'] AS val FROM %s
			WHERE mapContains(ResourceAttributes, '%s')
			  AND Timestamp >= fromUnixTimestamp64Nano(%d) AND Timestamp <= fromUnixTimestamp64Nano(%d)
		) WHERE val != '' ORDER BY val LIMIT 1000`,
		tagName, h.ch.Table(), tagName, start.UnixNano(), end.UnixNano(),
		tagName, h.ch.Table(), tagName, start.UnixNano(), end.UnixNano(),
	)

	rows, err := h.ch.Query(r.Context(), sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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

// queryDistinctColumn queries distinct values of a first-class column.
func (h *TagHandlers) queryDistinctColumn(r *http.Request, col string, start, end interface{ UnixNano() int64 }) ([]string, error) {
	// For ServiceName, try materialized view first
	if col == "ServiceName" && h.ch.UseMatViews() {
		return h.ch.QueryServiceNamesFromView(r.Context())
	}

	sql := fmt.Sprintf(
		"SELECT DISTINCT %s FROM %s WHERE Timestamp >= fromUnixTimestamp64Nano(%d) AND Timestamp <= fromUnixTimestamp64Nano(%d) ORDER BY %s LIMIT 1000",
		col, h.ch.Table(), start.UnixNano(), end.UnixNano(), col,
	)

	rows, err := h.ch.Query(r.Context(), sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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

func dedup(sorted []string) []string {
	if len(sorted) <= 1 {
		return sorted
	}
	result := sorted[:1]
	for i := 1; i < len(sorted); i++ {
		if sorted[i] != sorted[i-1] {
			result = append(result, sorted[i])
		}
	}
	return result
}
