package api

import (
	"log/slog"
	"net/http"
	"sort"
	"strings"

	"github.com/gorilla/mux"
	"github.com/hacktohell/opex/internal/clickhouse"
	"github.com/hacktohell/opex/internal/response"
	"github.com/hacktohell/opex/internal/tracequery"
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

// SearchTags handles GET /api/search/tags.
func (h *TagHandlers) SearchTags(w http.ResponseWriter, r *http.Request) {
	scope := r.URL.Query().Get("scope")
	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")

	start, end := parseTimeRange(startStr, endStr)

	result, err := tracequery.GetTagNames(r.Context(), h.ch, strings.ToLower(scope), start, end)
	if err != nil {
		h.logger.Error("query tags failed", "error", err)
		writeDBError(w, err, "failed to query tags")
		return
	}

	// Flatten to a single list for V1 endpoint
	var tags []string
	for _, s := range result.Scopes {
		tags = append(tags, s.Tags...)
	}
	sort.Strings(tags)
	tags = dedup(tags)

	response.WriteJSON(w, http.StatusOK, &response.SearchTagsResponse{TagNames: tags})
}

// SearchTagsV2 handles GET /api/v2/search/tags.
func (h *TagHandlers) SearchTagsV2(w http.ResponseWriter, r *http.Request) {
	scope := strings.ToLower(r.URL.Query().Get("scope"))
	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")

	start, end := parseTimeRange(startStr, endStr)

	result, err := tracequery.GetTagNames(r.Context(), h.ch, scope, start, end)
	if err != nil {
		h.logger.Error("query tags v2 failed", "error", err)
		writeDBError(w, err, "failed to query tags")
		return
	}

	response.WriteJSON(w, http.StatusOK, result)
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

	result, err := tracequery.GetTagValues(r.Context(), h.ch, tagName, "", start, end)
	if err != nil {
		h.logger.Error("query tag values failed", "tag", tagName, "error", err)
		writeDBError(w, err, "failed to query tag values")
		return
	}

	// Convert V2 response to V1 (just string values)
	var values []string
	for _, tv := range result.TagValues {
		values = append(values, tv.Value)
	}

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

	result, err := tracequery.GetTagValues(r.Context(), h.ch, tagName, "", start, end)
	if err != nil {
		h.logger.Error("query tag values failed", "tag", tagName, "error", err)
		writeDBError(w, err, "failed to query tag values")
		return
	}

	response.WriteJSON(w, http.StatusOK, result)
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
