//go:build integration

// Package integration contains end-to-end tests that spin up a real ClickHouse
// instance via Docker and exercise every Opex API endpoint.
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/hacktohell/opex/internal/clickhouse"
	"github.com/hacktohell/opex/internal/config"
	"github.com/hacktohell/opex/internal/server"
	"github.com/testcontainers/testcontainers-go"
	chmodule "github.com/testcontainers/testcontainers-go/modules/clickhouse"
)

// ---------------------------------------------------------------------------
// Package-level test state
// ---------------------------------------------------------------------------

var (
	baseURL    string       // e.g. "http://127.0.0.1:54321"
	httpServer *http.Server // the Opex HTTP server
	chClient   *clickhouse.Client
	container  *chmodule.ClickHouseContainer

	// Fixed time range covering all seed data (2025-01-15 09:00 – 13:00 UTC).
	seedStart = "1736931600" // 2025-01-15T09:00:00Z
	seedEnd   = "1736946000" // 2025-01-15T13:00:00Z
)

// ---------------------------------------------------------------------------
// TestMain — global setup / teardown
// ---------------------------------------------------------------------------

func TestMain(m *testing.M) {
	ctx := context.Background()

	// 1. Start ClickHouse container
	var err error
	container, err = chmodule.Run(ctx,
		"clickhouse/clickhouse-server:24.8",
		chmodule.WithUsername("default"),
		chmodule.WithPassword("opex_test"),
		chmodule.WithDatabase("otel"),
		chmodule.WithInitScripts(
			filepath.Join("testdata", "init.sql"),
			filepath.Join("testdata", "seed.sql"),
		),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start clickhouse container: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			fmt.Fprintf(os.Stderr, "failed to terminate container: %v\n", err)
		}
	}()

	// 2. Get connection string and create ClickHouse client
	dsn, err := container.ConnectionString(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to get connection string: %v\n", err)
		os.Exit(1)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	cfg := config.DefaultConfig()
	cfg.ClickHouse.DSN = dsn
	cfg.ClickHouse.TracesTable = "otel.otel_traces"
	cfg.ClickHouse.DialTimeout = 10 * time.Second
	cfg.ClickHouse.ReadTimeout = 30 * time.Second

	chClient, err = clickhouse.New(cfg.ClickHouse, logger)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to clickhouse: %v\n", err)
		os.Exit(1)
	}

	// 3. Start Opex HTTP server on a random port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to listen: %v\n", err)
		os.Exit(1)
	}
	baseURL = fmt.Sprintf("http://%s", listener.Addr().String())

	srv := server.New(cfg, chClient, logger)
	httpServer = &http.Server{Handler: srv.Handler()}
	go func() {
		if err := httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "server error: %v\n", err)
		}
	}()

	// Wait for server to be ready
	waitForServer(baseURL + "/ready")

	// 4. Run tests
	code := m.Run()

	// 5. Teardown
	httpServer.Close()
	chClient.Close()
	os.Exit(code)
}

func waitForServer(url string) {
	client := &http.Client{Timeout: 2 * time.Second}
	for i := 0; i < 30; i++ {
		resp, err := client.Get(url)
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(200 * time.Millisecond)
	}
	fmt.Fprintf(os.Stderr, "server did not become ready\n")
	os.Exit(1)
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

func httpGet(t *testing.T, path string) (int, []byte) {
	t.Helper()
	resp, err := http.Get(baseURL + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading body: %v", err)
	}
	return resp.StatusCode, body
}

func httpGetJSON(t *testing.T, path string, result any) int {
	t.Helper()
	status, body := httpGet(t, path)
	if err := json.Unmarshal(body, result); err != nil {
		t.Fatalf("decoding JSON from %s: %v\nbody: %s", path, err, string(body))
	}
	return status
}

// assertStatus fails the test if got != want.
func assertStatus(t *testing.T, got, want int) {
	t.Helper()
	if got != want {
		t.Errorf("status = %d, want %d", got, want)
	}
}

// assertContains fails if item is not in slice.
func assertContains(t *testing.T, slice []string, item string) {
	t.Helper()
	for _, s := range slice {
		if s == item {
			return
		}
	}
	t.Errorf("expected %v to contain %q", slice, item)
}

// assertNotEmpty fails if the slice is empty.
func assertNotEmpty(t *testing.T, name string, slice any) {
	t.Helper()
	switch v := slice.(type) {
	case []string:
		if len(v) == 0 {
			t.Errorf("expected %s to be non-empty", name)
		}
	case []any:
		if len(v) == 0 {
			t.Errorf("expected %s to be non-empty", name)
		}
	}
}

// searchPath builds a /api/search path with query parameters.
func searchPath(traceQL string, extra ...string) string {
	params := url.Values{}
	params.Set("q", traceQL)
	params.Set("start", seedStart)
	params.Set("end", seedEnd)
	for i := 0; i+1 < len(extra); i += 2 {
		params.Set(extra[i], extra[i+1])
	}
	return "/api/search?" + params.Encode()
}

// ---------------------------------------------------------------------------
// JSON response types (lightweight, just for unmarshaling in tests)
// ---------------------------------------------------------------------------

type searchResponse struct {
	Traces  []searchTrace `json:"traces"`
	Metrics searchMetrics `json:"metrics"`
}

type searchTrace struct {
	TraceID         string                  `json:"traceID"`
	RootServiceName string                  `json:"rootServiceName"`
	RootTraceName   string                  `json:"rootTraceName"`
	StartTimeUnix   string                  `json:"startTimeUnixNano"`
	DurationMs      int                     `json:"durationMs"`
	SpanSets        []spanSet               `json:"spanSets"`
	ServiceStats    map[string]serviceStats `json:"serviceStats"`
}

type spanSet struct {
	Spans   []spanSetSpan `json:"spans"`
	Matched int           `json:"matched"`
}

type spanSetSpan struct {
	SpanID            string `json:"spanID"`
	Name              string `json:"name"`
	StartTimeUnixNano string `json:"startTimeUnixNano"`
	DurationNanos     string `json:"durationNanos"`
}

type serviceStats struct {
	SpanCount  int `json:"spanCount"`
	ErrorCount int `json:"errorCount"`
}

type searchMetrics struct {
	InspectedTraces uint32 `json:"inspectedTraces"`
	InspectedSpans  uint64 `json:"inspectedSpans"`
}

type traceResponse struct {
	Batches []resourceSpans `json:"batches"`
}

type resourceSpans struct {
	Resource   resource     `json:"resource"`
	ScopeSpans []scopeSpans `json:"scopeSpans"`
}

type resource struct {
	Attributes []keyValue `json:"attributes"`
}

type scopeSpans struct {
	Scope scope  `json:"scope"`
	Spans []span `json:"spans"`
}

type scope struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type span struct {
	TraceID           string     `json:"traceId"`
	SpanID            string     `json:"spanId"`
	ParentSpanID      string     `json:"parentSpanId"`
	Name              string     `json:"name"`
	Kind              int        `json:"kind"`
	StartTimeUnixNano string     `json:"startTimeUnixNano"`
	EndTimeUnixNano   string     `json:"endTimeUnixNano"`
	Attributes        []keyValue `json:"attributes"`
	Status            spanStatus `json:"status"`
	Events            []event    `json:"events"`
	Links             []link     `json:"links"`
}

type spanStatus struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type event struct {
	TimeUnixNano string     `json:"timeUnixNano"`
	Name         string     `json:"name"`
	Attributes   []keyValue `json:"attributes"`
}

type link struct {
	TraceID    string     `json:"traceId"`
	SpanID     string     `json:"spanId"`
	Attributes []keyValue `json:"attributes"`
}

type keyValue struct {
	Key   string   `json:"key"`
	Value anyValue `json:"value"`
}

type anyValue struct {
	StringValue *string `json:"stringValue,omitempty"`
	IntValue    *string `json:"intValue,omitempty"`
	BoolValue   *bool   `json:"boolValue,omitempty"`
}

type traceByIDV2Response struct {
	Trace   *traceResponse `json:"trace"`
	Status  string         `json:"status"`
	Message string         `json:"message"`
}

type tagsResponse struct {
	TagNames []string `json:"tagNames"`
}

type tagsV2Response struct {
	Scopes []tagsV2Scope `json:"scopes"`
}

type tagsV2Scope struct {
	Name string   `json:"name"`
	Tags []string `json:"tags"`
}

type tagValuesResponse struct {
	TagValues []string `json:"tagValues"`
}

type tagValuesV2Response struct {
	TagValues []tagValue `json:"tagValues"`
}

type tagValue struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

type queryRangeResponse struct {
	Series []timeSeries `json:"series"`
}

type timeSeries struct {
	Labels  []label  `json:"labels"`
	Samples []sample `json:"samples"`
}

type label struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type sample struct {
	TimestampMs int64   `json:"timestampMs"`
	Value       float64 `json:"value"`
}

type queryInstantResponse struct {
	Series []instantSeries `json:"series"`
}

type instantSeries struct {
	Labels []label `json:"labels"`
	Value  float64 `json:"value"`
}

type metricsSummaryResponse struct {
	Summaries []spanMetricsSummary `json:"summaries"`
}

type spanMetricsSummary struct {
	SpanCount      int     `json:"spanCount"`
	ErrorSpanCount int     `json:"errorSpanCount"`
	P99            float64 `json:"p99"`
	P95            float64 `json:"p95"`
	P90            float64 `json:"p90"`
	P50            float64 `json:"p50"`
	Series         []label `json:"series"`
}

type errorResponse struct {
	Error string `json:"error"`
}

// traceIDs extracts all trace IDs from a search response.
func traceIDs(traces []searchTrace) []string {
	ids := make([]string, len(traces))
	for i, t := range traces {
		ids[i] = t.TraceID
	}
	sort.Strings(ids)
	return ids
}

// hasTraceID checks if a trace ID is in the search results.
func hasTraceID(traces []searchTrace, id string) bool {
	for _, t := range traces {
		if t.TraceID == id {
			return true
		}
	}
	return false
}

// countAllSpans counts all spans across all batches in a trace.
func countAllSpans(tr *traceResponse) int {
	n := 0
	for _, b := range tr.Batches {
		for _, ss := range b.ScopeSpans {
			n += len(ss.Spans)
		}
	}
	return n
}

// getServiceNames extracts service names from a trace response.
func getServiceNames(tr *traceResponse) []string {
	var names []string
	for _, b := range tr.Batches {
		for _, attr := range b.Resource.Attributes {
			if attr.Key == "service.name" && attr.Value.StringValue != nil {
				names = append(names, *attr.Value.StringValue)
			}
		}
	}
	sort.Strings(names)
	return names
}

// ==========================================================================
// Infrastructure Endpoint Tests
// ==========================================================================

func TestEcho(t *testing.T) {
	status, body := httpGet(t, "/api/echo")
	assertStatus(t, status, http.StatusOK)
	if string(body) != "echo" {
		t.Errorf("echo body = %q, want %q", string(body), "echo")
	}
}

func TestReady(t *testing.T) {
	status, body := httpGet(t, "/ready")
	assertStatus(t, status, http.StatusOK)
	if string(body) != "ready" {
		t.Errorf("ready body = %q, want %q", string(body), "ready")
	}
}

func TestBuildInfo(t *testing.T) {
	var result map[string]any
	status := httpGetJSON(t, "/api/status/buildinfo", &result)
	assertStatus(t, status, http.StatusOK)

	for _, key := range []string{"version", "revision", "branch", "goVersion"} {
		if _, ok := result[key]; !ok {
			t.Errorf("buildinfo missing key %q", key)
		}
	}
}

// ==========================================================================
// Trace by ID Tests
// ==========================================================================

func TestTraceByID(t *testing.T) {
	t.Run("SimpleTrace", func(t *testing.T) {
		// Trace eeee: single span health check
		var tr traceResponse
		status := httpGetJSON(t, "/api/traces/eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", &tr)
		assertStatus(t, status, http.StatusOK)

		total := countAllSpans(&tr)
		if total != 1 {
			t.Errorf("span count = %d, want 1", total)
		}
		if len(tr.Batches) != 1 {
			t.Errorf("batch count = %d, want 1", len(tr.Batches))
		}

		// Check span name
		sp := tr.Batches[0].ScopeSpans[0].Spans[0]
		if sp.Name != "GET /healthz" {
			t.Errorf("span name = %q, want %q", sp.Name, "GET /healthz")
		}
		// Kind SERVER = 2
		if sp.Kind != 2 {
			t.Errorf("span kind = %d, want 2 (SERVER)", sp.Kind)
		}
	})

	t.Run("MultiServiceTrace", func(t *testing.T) {
		// Trace aaaa: 4 spans across 3 services
		var tr traceResponse
		status := httpGetJSON(t, "/api/traces/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", &tr)
		assertStatus(t, status, http.StatusOK)

		total := countAllSpans(&tr)
		if total != 4 {
			t.Errorf("span count = %d, want 4", total)
		}

		// Batches sorted alphabetically by service name
		services := getServiceNames(&tr)
		expected := []string{"api-gateway", "frontend", "user-service"}
		if len(services) != len(expected) {
			t.Fatalf("services = %v, want %v", services, expected)
		}
		for i, s := range expected {
			if services[i] != s {
				t.Errorf("service[%d] = %q, want %q", i, services[i], s)
			}
		}
	})

	t.Run("ErrorTrace", func(t *testing.T) {
		// Trace bbbb: has error spans and events
		var tr traceResponse
		status := httpGetJSON(t, "/api/traces/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", &tr)
		assertStatus(t, status, http.StatusOK)

		total := countAllSpans(&tr)
		if total != 5 {
			t.Errorf("span count = %d, want 5", total)
		}

		// Find an error span
		foundError := false
		foundEvent := false
		for _, b := range tr.Batches {
			for _, ss := range b.ScopeSpans {
				for _, sp := range ss.Spans {
					if sp.Status.Code == 2 { // ERROR
						foundError = true
					}
					if len(sp.Events) > 0 {
						foundEvent = true
						if sp.Events[0].Name != "exception" {
							t.Errorf("event name = %q, want %q", sp.Events[0].Name, "exception")
						}
					}
				}
			}
		}
		if !foundError {
			t.Error("expected at least one span with status ERROR")
		}
		if !foundEvent {
			t.Error("expected at least one span with events")
		}
	})

	t.Run("WithLinks", func(t *testing.T) {
		// Trace ffff: root span has links
		var tr traceResponse
		status := httpGetJSON(t, "/api/traces/ffffffffffffffffffffffffffffffff", &tr)
		assertStatus(t, status, http.StatusOK)

		total := countAllSpans(&tr)
		if total != 2 {
			t.Errorf("span count = %d, want 2", total)
		}

		foundLink := false
		for _, b := range tr.Batches {
			for _, ss := range b.ScopeSpans {
				for _, sp := range ss.Spans {
					if len(sp.Links) > 0 {
						foundLink = true
						lk := sp.Links[0]
						if lk.TraceID != "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" {
							t.Errorf("link traceID = %q, want bbbb...", lk.TraceID)
						}
						if lk.SpanID != "2000000000000003" {
							t.Errorf("link spanID = %q, want 2000000000000003", lk.SpanID)
						}
					}
				}
			}
		}
		if !foundLink {
			t.Error("expected at least one span with links")
		}
	})

	t.Run("NotFound", func(t *testing.T) {
		var errResp errorResponse
		status := httpGetJSON(t, "/api/traces/00000000000000000000000000000000", &errResp)
		assertStatus(t, status, http.StatusNotFound)
	})

	t.Run("InvalidHex", func(t *testing.T) {
		var errResp errorResponse
		status := httpGetJSON(t, "/api/traces/zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", &errResp)
		assertStatus(t, status, http.StatusBadRequest)
	})
}

// ==========================================================================
// Trace by ID V2 Tests
// ==========================================================================

func TestTraceByIDV2(t *testing.T) {
	t.Run("Found", func(t *testing.T) {
		var resp traceByIDV2Response
		status := httpGetJSON(t, "/api/v2/traces/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", &resp)
		assertStatus(t, status, http.StatusOK)

		if resp.Status != "complete" {
			t.Errorf("status = %q, want %q", resp.Status, "complete")
		}
		if resp.Trace == nil {
			t.Fatal("trace is nil")
		}
		total := countAllSpans(resp.Trace)
		if total != 4 {
			t.Errorf("span count = %d, want 4", total)
		}
	})

	t.Run("NotFound", func(t *testing.T) {
		var resp traceByIDV2Response
		status := httpGetJSON(t, "/api/v2/traces/00000000000000000000000000000000", &resp)
		assertStatus(t, status, http.StatusOK)
		if resp.Trace != nil {
			t.Error("expected trace to be nil for not-found")
		}
	})
}

// ==========================================================================
// Search Tests
// ==========================================================================

func TestSearch(t *testing.T) {
	t.Run("EmptyFilter", func(t *testing.T) {
		var resp searchResponse
		status := httpGetJSON(t, searchPath("{}"), &resp)
		assertStatus(t, status, http.StatusOK)

		if len(resp.Traces) != 7 {
			t.Errorf("trace count = %d, want 7", len(resp.Traces))
		}
		// Verify every trace has root fields
		for _, tr := range resp.Traces {
			if tr.TraceID == "" {
				t.Error("traceID is empty")
			}
			if tr.RootServiceName == "" {
				t.Error("rootServiceName is empty")
			}
			if tr.RootTraceName == "" {
				t.Error("rootTraceName is empty")
			}
			if tr.DurationMs <= 0 {
				t.Errorf("durationMs = %d for trace %s, want > 0", tr.DurationMs, tr.TraceID)
			}
		}
	})

	t.Run("BySpanName", func(t *testing.T) {
		var resp searchResponse
		status := httpGetJSON(t, searchPath(`{name="GET /login"}`), &resp)
		assertStatus(t, status, http.StatusOK)

		if len(resp.Traces) != 1 {
			t.Fatalf("trace count = %d, want 1", len(resp.Traces))
		}
		if resp.Traces[0].TraceID != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
			t.Errorf("traceID = %q, want aaaa...", resp.Traces[0].TraceID)
		}
	})

	t.Run("ByStatus", func(t *testing.T) {
		var resp searchResponse
		status := httpGetJSON(t, searchPath(`{status=error}`), &resp)
		assertStatus(t, status, http.StatusOK)

		if len(resp.Traces) < 1 {
			t.Fatal("expected at least 1 trace with error status")
		}
		if !hasTraceID(resp.Traces, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb") {
			t.Error("expected trace bbbb... in error results")
		}
	})

	t.Run("ByDuration", func(t *testing.T) {
		var resp searchResponse
		status := httpGetJSON(t, searchPath(`{duration>1s}`), &resp)
		assertStatus(t, status, http.StatusOK)

		// Traces with root duration > 1s: bbbb (2.5s), dddd (4.8s), 1111 (0.8s — root, but has child 750ms)
		if len(resp.Traces) < 2 {
			t.Errorf("expected at least 2 traces with duration > 1s, got %d", len(resp.Traces))
		}
		if !hasTraceID(resp.Traces, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb") {
			t.Error("expected trace bbbb... in duration > 1s results")
		}
		if !hasTraceID(resp.Traces, "dddddddddddddddddddddddddddddd") {
			t.Error("expected trace dddd... in duration > 1s results")
		}
	})

	t.Run("BySpanAttribute", func(t *testing.T) {
		var resp searchResponse
		status := httpGetJSON(t, searchPath(`{span.http.method="POST"}`), &resp)
		assertStatus(t, status, http.StatusOK)

		if len(resp.Traces) < 1 {
			t.Fatal("expected at least 1 trace with POST spans")
		}
	})

	t.Run("ByResourceAttribute", func(t *testing.T) {
		var resp searchResponse
		status := httpGetJSON(t, searchPath(`{resource.deployment.environment="staging"}`), &resp)
		assertStatus(t, status, http.StatusOK)

		if len(resp.Traces) != 1 {
			t.Fatalf("trace count = %d, want 1", len(resp.Traces))
		}
		if resp.Traces[0].TraceID != "dddddddddddddddddddddddddddddd" {
			t.Errorf("traceID = %q, want dddd...", resp.Traces[0].TraceID)
		}
	})

	t.Run("WithLimit", func(t *testing.T) {
		var resp searchResponse
		status := httpGetJSON(t, searchPath("{}", "limit", "2"), &resp)
		assertStatus(t, status, http.StatusOK)

		if len(resp.Traces) > 2 {
			t.Errorf("trace count = %d, want <= 2", len(resp.Traces))
		}
	})

	t.Run("StatusAndDuration", func(t *testing.T) {
		var resp searchResponse
		status := httpGetJSON(t, searchPath(`{status=error && duration>1s}`), &resp)
		assertStatus(t, status, http.StatusOK)

		if len(resp.Traces) < 1 {
			t.Fatal("expected at least 1 trace")
		}
		if !hasTraceID(resp.Traces, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb") {
			t.Error("expected trace bbbb... in error + duration > 1s results")
		}
	})

	t.Run("RegexMatch", func(t *testing.T) {
		var resp searchResponse
		status := httpGetJSON(t, searchPath(`{name=~"GET.*"}`), &resp)
		assertStatus(t, status, http.StatusOK)

		if len(resp.Traces) < 1 {
			t.Fatal("expected at least 1 trace matching GET.*")
		}
	})

	t.Run("NotEqual", func(t *testing.T) {
		var resp searchResponse
		status := httpGetJSON(t, searchPath(`{status!=error}`), &resp)
		assertStatus(t, status, http.StatusOK)

		// Should include traces that have non-error spans
		if len(resp.Traces) < 1 {
			t.Fatal("expected at least 1 trace with status != error")
		}
	})

	t.Run("SpanKind", func(t *testing.T) {
		var resp searchResponse
		status := httpGetJSON(t, searchPath(`{kind=client}`), &resp)
		assertStatus(t, status, http.StatusOK)

		if len(resp.Traces) < 1 {
			t.Fatal("expected at least 1 trace with CLIENT spans")
		}
	})

	t.Run("Metrics", func(t *testing.T) {
		var resp searchResponse
		status := httpGetJSON(t, searchPath("{}"), &resp)
		assertStatus(t, status, http.StatusOK)

		if resp.Metrics.InspectedTraces == 0 {
			t.Error("inspectedTraces should be > 0")
		}
		if resp.Metrics.InspectedSpans == 0 {
			t.Error("inspectedSpans should be > 0")
		}
	})

	t.Run("EmptyResult", func(t *testing.T) {
		var resp searchResponse
		status := httpGetJSON(t, searchPath(`{name="nonexistent_span_name_xyz"}`), &resp)
		assertStatus(t, status, http.StatusOK)

		if resp.Traces == nil {
			t.Error("traces should be empty array, not null")
		}
		if len(resp.Traces) != 0 {
			t.Errorf("trace count = %d, want 0", len(resp.Traces))
		}
	})

	t.Run("MinDurationParam", func(t *testing.T) {
		var resp searchResponse
		status := httpGetJSON(t, searchPath("{}", "minDuration", "1s"), &resp)
		assertStatus(t, status, http.StatusOK)

		for _, tr := range resp.Traces {
			if tr.DurationMs < 1000 {
				t.Errorf("trace %s has duration %dms, want >= 1000ms", tr.TraceID, tr.DurationMs)
			}
		}
	})

	t.Run("MaxDurationParam", func(t *testing.T) {
		var resp searchResponse
		status := httpGetJSON(t, searchPath("{}", "maxDuration", "100ms"), &resp)
		assertStatus(t, status, http.StatusOK)

		for _, tr := range resp.Traces {
			if tr.DurationMs > 100 {
				t.Errorf("trace %s has duration %dms, want <= 100ms", tr.TraceID, tr.DurationMs)
			}
		}
	})

	t.Run("ServiceStats", func(t *testing.T) {
		var resp searchResponse
		status := httpGetJSON(t, searchPath(`{name="GET /login"}`), &resp)
		assertStatus(t, status, http.StatusOK)

		if len(resp.Traces) != 1 {
			t.Fatalf("trace count = %d, want 1", len(resp.Traces))
		}
		tr := resp.Traces[0]
		if tr.ServiceStats == nil {
			t.Fatal("serviceStats is nil")
		}
		if _, ok := tr.ServiceStats["frontend"]; !ok {
			t.Error("serviceStats missing 'frontend'")
		}
	})
}

// ==========================================================================
// TraceQL Variation Tests
// ==========================================================================

func TestTraceQL(t *testing.T) {
	t.Run("ExistenceFilter", func(t *testing.T) {
		var resp searchResponse
		status := httpGetJSON(t, searchPath(`{span.http.method != nil}`), &resp)
		assertStatus(t, status, http.StatusOK)

		if len(resp.Traces) < 1 {
			t.Fatal("expected traces with http.method attribute")
		}
	})

	t.Run("StringComparison", func(t *testing.T) {
		var resp searchResponse
		status := httpGetJSON(t, searchPath(`{span.http.status_code = "200"}`), &resp)
		assertStatus(t, status, http.StatusOK)

		if len(resp.Traces) < 1 {
			t.Fatal("expected traces with http.status_code=200")
		}
	})

	t.Run("DurationNano", func(t *testing.T) {
		var resp searchResponse
		status := httpGetJSON(t, searchPath(`{duration > 500ms}`), &resp)
		assertStatus(t, status, http.StatusOK)

		if len(resp.Traces) < 1 {
			t.Fatal("expected traces with duration > 500ms")
		}
	})

	t.Run("And", func(t *testing.T) {
		var resp searchResponse
		status := httpGetJSON(t, searchPath(`{span.http.method="GET" && status=ok}`), &resp)
		assertStatus(t, status, http.StatusOK)

		if len(resp.Traces) < 1 {
			t.Fatal("expected traces with GET + ok status")
		}
	})

	t.Run("Or", func(t *testing.T) {
		var resp searchResponse
		status := httpGetJSON(t, searchPath(`{span.http.method="GET" || span.http.method="POST"}`), &resp)
		assertStatus(t, status, http.StatusOK)

		if len(resp.Traces) < 1 {
			t.Fatal("expected traces with GET or POST")
		}
	})

	t.Run("UnscopedAttribute", func(t *testing.T) {
		var resp searchResponse
		status := httpGetJSON(t, searchPath(`{.http.method="GET"}`), &resp)
		assertStatus(t, status, http.StatusOK)

		if len(resp.Traces) < 1 {
			t.Fatal("expected traces with unscoped .http.method=GET")
		}
	})

	t.Run("SpansetIntersect", func(t *testing.T) {
		var resp searchResponse
		status := httpGetJSON(t, searchPath(`{name="GET /login"} && {name="POST /api/v1/auth"}`), &resp)
		assertStatus(t, status, http.StatusOK)

		// Trace aaaa has both these spans
		if len(resp.Traces) < 1 {
			t.Fatal("expected trace with both span names")
		}
		if !hasTraceID(resp.Traces, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa") {
			t.Error("expected trace aaaa... in spanset intersect results")
		}
	})

	t.Run("ResourceAttribute", func(t *testing.T) {
		var resp searchResponse
		status := httpGetJSON(t, searchPath(`{resource.host.name="frontend-pod-abc"}`), &resp)
		assertStatus(t, status, http.StatusOK)

		if len(resp.Traces) < 1 {
			t.Fatal("expected traces with host.name=frontend-pod-abc")
		}
	})

	t.Run("DurationRange", func(t *testing.T) {
		var resp searchResponse
		status := httpGetJSON(t, searchPath(`{duration >= 100ms && duration < 1s}`), &resp)
		assertStatus(t, status, http.StatusOK)

		if len(resp.Traces) < 1 {
			t.Fatal("expected traces with duration in [100ms, 1s)")
		}
	})

	t.Run("NegatedRegex", func(t *testing.T) {
		var resp searchResponse
		status := httpGetJSON(t, searchPath(`{name !~ "health.*"}`), &resp)
		assertStatus(t, status, http.StatusOK)

		if len(resp.Traces) < 1 {
			t.Fatal("expected traces with name not matching health.*")
		}
	})

	t.Run("ServiceName", func(t *testing.T) {
		var resp searchResponse
		status := httpGetJSON(t, searchPath(`{resource.service.name="payment-service"}`), &resp)
		assertStatus(t, status, http.StatusOK)

		if len(resp.Traces) < 1 {
			t.Fatal("expected traces with payment-service")
		}
		// Should include trace bbbb (failed payment) and ffff (retry payment)
		if !hasTraceID(resp.Traces, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb") {
			t.Error("expected trace bbbb... with payment-service")
		}
		if !hasTraceID(resp.Traces, "ffffffffffffffffffffffffffffffff") {
			t.Error("expected trace ffff... with payment-service")
		}
	})
}

// ==========================================================================
// Search Tags Tests
// ==========================================================================

func TestSearchTags(t *testing.T) {
	t.Run("All", func(t *testing.T) {
		var resp tagsResponse
		status := httpGetJSON(t, fmt.Sprintf("/api/search/tags?start=%s&end=%s", seedStart, seedEnd), &resp)
		assertStatus(t, status, http.StatusOK)

		if len(resp.TagNames) == 0 {
			t.Fatal("expected non-empty tag names")
		}
		// Should include intrinsic tags
		assertContains(t, resp.TagNames, "duration")
		assertContains(t, resp.TagNames, "name")
		assertContains(t, resp.TagNames, "status")
	})

	t.Run("ScopeIntrinsic", func(t *testing.T) {
		var resp tagsResponse
		status := httpGetJSON(t, fmt.Sprintf("/api/search/tags?scope=intrinsic&start=%s&end=%s", seedStart, seedEnd), &resp)
		assertStatus(t, status, http.StatusOK)

		assertContains(t, resp.TagNames, "duration")
		assertContains(t, resp.TagNames, "name")
		assertContains(t, resp.TagNames, "status")
		assertContains(t, resp.TagNames, "kind")
	})

	t.Run("ScopeResource", func(t *testing.T) {
		var resp tagsResponse
		status := httpGetJSON(t, fmt.Sprintf("/api/search/tags?scope=resource&start=%s&end=%s", seedStart, seedEnd), &resp)
		assertStatus(t, status, http.StatusOK)

		assertContains(t, resp.TagNames, "service.name")
		assertContains(t, resp.TagNames, "service.version")
		assertContains(t, resp.TagNames, "deployment.environment")
		assertContains(t, resp.TagNames, "host.name")
	})

	t.Run("ScopeSpan", func(t *testing.T) {
		var resp tagsResponse
		status := httpGetJSON(t, fmt.Sprintf("/api/search/tags?scope=span&start=%s&end=%s", seedStart, seedEnd), &resp)
		assertStatus(t, status, http.StatusOK)

		assertContains(t, resp.TagNames, "http.method")
		assertContains(t, resp.TagNames, "http.url")
	})

	t.Run("V2", func(t *testing.T) {
		var resp tagsV2Response
		status := httpGetJSON(t, fmt.Sprintf("/api/v2/search/tags?start=%s&end=%s", seedStart, seedEnd), &resp)
		assertStatus(t, status, http.StatusOK)

		if len(resp.Scopes) < 3 {
			t.Fatalf("scope count = %d, want >= 3", len(resp.Scopes))
		}

		foundIntrinsic := false
		foundResource := false
		foundSpan := false
		for _, scope := range resp.Scopes {
			switch scope.Name {
			case "intrinsic":
				foundIntrinsic = true
				assertContains(t, scope.Tags, "duration")
			case "resource":
				foundResource = true
				assertContains(t, scope.Tags, "service.name")
			case "span":
				foundSpan = true
				assertContains(t, scope.Tags, "http.method")
			}
		}
		if !foundIntrinsic {
			t.Error("missing intrinsic scope")
		}
		if !foundResource {
			t.Error("missing resource scope")
		}
		if !foundSpan {
			t.Error("missing span scope")
		}
	})
}

// ==========================================================================
// Search Tag Values Tests
// ==========================================================================

func TestSearchTagValues(t *testing.T) {
	t.Run("ServiceName", func(t *testing.T) {
		var resp tagValuesResponse
		status := httpGetJSON(t, fmt.Sprintf("/api/search/tag/service.name/values?start=%s&end=%s", seedStart, seedEnd), &resp)
		assertStatus(t, status, http.StatusOK)

		if len(resp.TagValues) < 5 {
			t.Errorf("service count = %d, want >= 5", len(resp.TagValues))
		}
		assertContains(t, resp.TagValues, "frontend")
		assertContains(t, resp.TagValues, "api-gateway")
		assertContains(t, resp.TagValues, "user-service")
		assertContains(t, resp.TagValues, "order-service")
		assertContains(t, resp.TagValues, "payment-service")
	})

	t.Run("HttpMethod", func(t *testing.T) {
		var resp tagValuesResponse
		status := httpGetJSON(t, fmt.Sprintf("/api/search/tag/http.method/values?start=%s&end=%s", seedStart, seedEnd), &resp)
		assertStatus(t, status, http.StatusOK)

		assertContains(t, resp.TagValues, "GET")
		assertContains(t, resp.TagValues, "POST")
	})

	t.Run("Status", func(t *testing.T) {
		var resp tagValuesResponse
		status := httpGetJSON(t, fmt.Sprintf("/api/search/tag/status/values?start=%s&end=%s", seedStart, seedEnd), &resp)
		assertStatus(t, status, http.StatusOK)

		assertContains(t, resp.TagValues, "error")
		assertContains(t, resp.TagValues, "ok")
		assertContains(t, resp.TagValues, "unset")
	})

	t.Run("Kind", func(t *testing.T) {
		var resp tagValuesResponse
		status := httpGetJSON(t, fmt.Sprintf("/api/search/tag/kind/values?start=%s&end=%s", seedStart, seedEnd), &resp)
		assertStatus(t, status, http.StatusOK)

		assertContains(t, resp.TagValues, "server")
		assertContains(t, resp.TagValues, "client")
		assertContains(t, resp.TagValues, "internal")
	})

	t.Run("DbSystem", func(t *testing.T) {
		var resp tagValuesResponse
		status := httpGetJSON(t, fmt.Sprintf("/api/search/tag/db.system/values?start=%s&end=%s", seedStart, seedEnd), &resp)
		assertStatus(t, status, http.StatusOK)

		assertContains(t, resp.TagValues, "postgresql")
		assertContains(t, resp.TagValues, "redis")
	})

	t.Run("V2Typed", func(t *testing.T) {
		var resp tagValuesV2Response
		status := httpGetJSON(t, fmt.Sprintf("/api/v2/search/tag/http.method/values?start=%s&end=%s", seedStart, seedEnd), &resp)
		assertStatus(t, status, http.StatusOK)

		if len(resp.TagValues) < 1 {
			t.Fatal("expected at least 1 tag value")
		}
		for _, tv := range resp.TagValues {
			if tv.Type != "string" {
				t.Errorf("tag value type = %q, want %q", tv.Type, "string")
			}
		}
	})
}

// ==========================================================================
// Metrics Query Range Tests
// ==========================================================================

func TestMetricsQueryRange(t *testing.T) {
	t.Run("Rate", func(t *testing.T) {
		path := fmt.Sprintf("/api/metrics/query_range?q=%s&start=%s&end=%s&step=3600s",
			url.QueryEscape("{} | rate()"), seedStart, seedEnd)
		var resp queryRangeResponse
		status := httpGetJSON(t, path, &resp)
		assertStatus(t, status, http.StatusOK)

		if len(resp.Series) == 0 {
			t.Fatal("expected at least 1 series")
		}
		if len(resp.Series[0].Samples) == 0 {
			t.Fatal("expected at least 1 sample")
		}
	})

	t.Run("CountOverTime", func(t *testing.T) {
		path := fmt.Sprintf("/api/metrics/query_range?q=%s&start=%s&end=%s&step=3600s",
			url.QueryEscape("{} | count_over_time()"), seedStart, seedEnd)
		var resp queryRangeResponse
		status := httpGetJSON(t, path, &resp)
		assertStatus(t, status, http.StatusOK)

		if len(resp.Series) == 0 {
			t.Fatal("expected at least 1 series")
		}

		// Sum of all count values should be 26 (total spans)
		var totalCount float64
		for _, s := range resp.Series {
			for _, sample := range s.Samples {
				totalCount += sample.Value
			}
		}
		if totalCount != 26 {
			t.Errorf("total count = %v, want 26", totalCount)
		}
	})

	t.Run("RateByService", func(t *testing.T) {
		path := fmt.Sprintf("/api/metrics/query_range?q=%s&start=%s&end=%s&step=3600s",
			url.QueryEscape("{} | rate() by(resource.service.name)"), seedStart, seedEnd)
		var resp queryRangeResponse
		status := httpGetJSON(t, path, &resp)
		assertStatus(t, status, http.StatusOK)

		// Should have multiple series (one per service)
		if len(resp.Series) < 2 {
			t.Errorf("series count = %d, want >= 2", len(resp.Series))
		}
	})

	t.Run("AvgOverTime", func(t *testing.T) {
		path := fmt.Sprintf("/api/metrics/query_range?q=%s&start=%s&end=%s&step=3600s",
			url.QueryEscape("{} | avg_over_time(duration)"), seedStart, seedEnd)
		var resp queryRangeResponse
		status := httpGetJSON(t, path, &resp)
		assertStatus(t, status, http.StatusOK)

		if len(resp.Series) == 0 {
			t.Fatal("expected at least 1 series")
		}
		for _, s := range resp.Series {
			for _, sample := range s.Samples {
				if sample.Value <= 0 {
					t.Errorf("avg duration = %v, want > 0", sample.Value)
				}
			}
		}
	})

	t.Run("MissingQuery", func(t *testing.T) {
		path := fmt.Sprintf("/api/metrics/query_range?start=%s&end=%s", seedStart, seedEnd)
		var errResp errorResponse
		status := httpGetJSON(t, path, &errResp)
		assertStatus(t, status, http.StatusBadRequest)
	})

	t.Run("ErrorRate", func(t *testing.T) {
		path := fmt.Sprintf("/api/metrics/query_range?q=%s&start=%s&end=%s&step=3600s",
			url.QueryEscape("{status=error} | rate()"), seedStart, seedEnd)
		var resp queryRangeResponse
		status := httpGetJSON(t, path, &resp)
		assertStatus(t, status, http.StatusOK)

		if len(resp.Series) == 0 {
			t.Fatal("expected at least 1 series for error rate")
		}
	})
}

// ==========================================================================
// Metrics Query Instant Tests
// ==========================================================================

func TestMetricsQueryInstant(t *testing.T) {
	t.Run("Count", func(t *testing.T) {
		path := fmt.Sprintf("/api/metrics/query?q=%s&start=%s&end=%s",
			url.QueryEscape("{} | count_over_time()"), seedStart, seedEnd)
		var resp queryInstantResponse
		status := httpGetJSON(t, path, &resp)
		assertStatus(t, status, http.StatusOK)

		if len(resp.Series) == 0 {
			t.Fatal("expected at least 1 series")
		}
	})

	t.Run("MissingQuery", func(t *testing.T) {
		path := fmt.Sprintf("/api/metrics/query?start=%s&end=%s", seedStart, seedEnd)
		var errResp errorResponse
		status := httpGetJSON(t, path, &errResp)
		assertStatus(t, status, http.StatusBadRequest)
	})
}

// ==========================================================================
// Metrics Summary Tests
// ==========================================================================

func TestMetricsSummary(t *testing.T) {
	t.Run("All", func(t *testing.T) {
		path := fmt.Sprintf("/api/metrics/summary?q={}&start=%s&end=%s", seedStart, seedEnd)
		var resp metricsSummaryResponse
		status := httpGetJSON(t, path, &resp)
		assertStatus(t, status, http.StatusOK)

		if len(resp.Summaries) != 1 {
			t.Fatalf("summary count = %d, want 1", len(resp.Summaries))
		}
		s := resp.Summaries[0]
		if s.SpanCount != 26 {
			t.Errorf("spanCount = %d, want 26", s.SpanCount)
		}
		// Trace bbbb has 4 error spans (frontend, api-gateway, order-service, payment-service)
		if s.ErrorSpanCount < 1 {
			t.Errorf("errorSpanCount = %d, want >= 1", s.ErrorSpanCount)
		}
		if s.P99 <= 0 {
			t.Errorf("p99 = %v, want > 0", s.P99)
		}
		if s.P50 <= 0 {
			t.Errorf("p50 = %v, want > 0", s.P50)
		}
	})

	t.Run("ByService", func(t *testing.T) {
		path := fmt.Sprintf("/api/metrics/summary?q={}&groupBy=resource.service.name&start=%s&end=%s", seedStart, seedEnd)
		var resp metricsSummaryResponse
		status := httpGetJSON(t, path, &resp)
		assertStatus(t, status, http.StatusOK)

		if len(resp.Summaries) < 2 {
			t.Errorf("summary count = %d, want >= 2 (one per service)", len(resp.Summaries))
		}
		// Each summary should have labels
		for _, s := range resp.Summaries {
			if len(s.Series) == 0 {
				t.Error("summary missing series labels")
			}
		}
	})

	t.Run("WithLimit", func(t *testing.T) {
		path := fmt.Sprintf("/api/metrics/summary?q={}&groupBy=resource.service.name&limit=2&start=%s&end=%s", seedStart, seedEnd)
		var resp metricsSummaryResponse
		status := httpGetJSON(t, path, &resp)
		assertStatus(t, status, http.StatusOK)

		if len(resp.Summaries) > 2 {
			t.Errorf("summary count = %d, want <= 2", len(resp.Summaries))
		}
	})
}

// ==========================================================================
// Error / Edge Case Tests
// ==========================================================================

func TestErrors(t *testing.T) {
	t.Run("InvalidTraceQL", func(t *testing.T) {
		var errResp errorResponse
		status := httpGetJSON(t, searchPath(`{invalid!!!}`), &errResp)
		assertStatus(t, status, http.StatusBadRequest)
		if errResp.Error == "" {
			t.Error("expected error message")
		}
	})

	t.Run("MetricsNoQuery", func(t *testing.T) {
		var errResp errorResponse
		status := httpGetJSON(t, "/api/metrics/query_range", &errResp)
		assertStatus(t, status, http.StatusBadRequest)
	})

	t.Run("TraceByID_InvalidHex", func(t *testing.T) {
		var errResp errorResponse
		status := httpGetJSON(t, "/api/traces/not-a-valid-hex-id", &errResp)
		assertStatus(t, status, http.StatusBadRequest)
	})

	t.Run("MetricsQueryRange_NoAggregate", func(t *testing.T) {
		// Query without a metrics aggregate function
		path := fmt.Sprintf("/api/metrics/query_range?q=%s&start=%s&end=%s",
			url.QueryEscape("{}"), seedStart, seedEnd)
		var errResp errorResponse
		status := httpGetJSON(t, path, &errResp)
		assertStatus(t, status, http.StatusBadRequest)
	})
}

// ==========================================================================
// Materialized View Tests
// ==========================================================================

func TestMatViews(t *testing.T) {
	ctx := context.Background()

	// Get ClickHouse connection to execute mat view SQL
	dsn, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("getting connection string: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	// Execute matviews.sql via Docker exec into the ClickHouse container
	matviewSQL, err := os.ReadFile(filepath.Join("testdata", "matviews.sql"))
	if err != nil {
		t.Fatalf("reading matviews.sql: %v", err)
	}

	// Use container exec to run the SQL via clickhouse-client --multiquery
	exitCode, reader, err := container.Exec(ctx, []string{
		"clickhouse-client",
		"--user", "default",
		"--password", "opex_test",
		"--multiquery",
		"--query", string(matviewSQL),
	})
	if err != nil {
		t.Fatalf("exec matviews in container: %v", err)
	}
	if exitCode != 0 {
		out, _ := io.ReadAll(reader)
		t.Fatalf("matviews exec failed (code %d): %s", exitCode, string(out))
	}

	// Create a new ClickHouse client with mat views enabled
	mvCfg := config.DefaultConfig()
	mvCfg.ClickHouse.DSN = dsn
	mvCfg.ClickHouse.TracesTable = "otel.otel_traces"
	mvCfg.ClickHouse.UseMatViews = true
	mvCfg.ClickHouse.TraceMetadataTable = "otel.otel_trace_metadata"
	mvCfg.ClickHouse.SpanTagNamesTable = "otel.otel_span_tag_names"
	mvCfg.ClickHouse.ResourceTagNamesTable = "otel.otel_resource_tag_names"
	mvCfg.ClickHouse.ServiceNamesTable = "otel.otel_service_names"
	mvCfg.ClickHouse.DialTimeout = 10 * time.Second
	mvCfg.ClickHouse.ReadTimeout = 30 * time.Second

	mvClient, err := clickhouse.New(mvCfg.ClickHouse, logger)
	if err != nil {
		t.Fatalf("creating matview client: %v", err)
	}
	defer mvClient.Close()

	// Start a second server with mat views
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	mvBaseURL := fmt.Sprintf("http://%s", listener.Addr().String())

	mvSrv := server.New(mvCfg, mvClient, logger)
	mvHTTP := &http.Server{Handler: mvSrv.Handler()}
	go mvHTTP.Serve(listener)
	defer mvHTTP.Close()

	waitForServerTest(t, mvBaseURL+"/ready")

	t.Run("SearchTags", func(t *testing.T) {
		var resp tagsResponse
		status := httpGetJSONURL(t, fmt.Sprintf("%s/api/search/tags?scope=span&start=%s&end=%s", mvBaseURL, seedStart, seedEnd), &resp)
		assertStatus(t, status, http.StatusOK)

		if len(resp.TagNames) == 0 {
			t.Fatal("expected span tags from mat view")
		}
		assertContains(t, resp.TagNames, "http.method")
	})

	t.Run("SearchTagsV2", func(t *testing.T) {
		var resp tagsV2Response
		status := httpGetJSONURL(t, fmt.Sprintf("%s/api/v2/search/tags?start=%s&end=%s", mvBaseURL, seedStart, seedEnd), &resp)
		assertStatus(t, status, http.StatusOK)

		if len(resp.Scopes) < 3 {
			t.Fatalf("scope count = %d, want >= 3", len(resp.Scopes))
		}
	})

	t.Run("SearchTagValues_ServiceName", func(t *testing.T) {
		var resp tagValuesResponse
		status := httpGetJSONURL(t, fmt.Sprintf("%s/api/search/tag/service.name/values?start=%s&end=%s", mvBaseURL, seedStart, seedEnd), &resp)
		assertStatus(t, status, http.StatusOK)

		if len(resp.TagValues) < 5 {
			t.Errorf("service count = %d, want >= 5", len(resp.TagValues))
		}
		assertContains(t, resp.TagValues, "frontend")
		assertContains(t, resp.TagValues, "api-gateway")
	})

	t.Run("Search", func(t *testing.T) {
		params := url.Values{}
		params.Set("q", "{}")
		params.Set("start", seedStart)
		params.Set("end", seedEnd)
		var resp searchResponse
		status := httpGetJSONURL(t, fmt.Sprintf("%s/api/search?%s", mvBaseURL, params.Encode()), &resp)
		assertStatus(t, status, http.StatusOK)

		if len(resp.Traces) != 7 {
			t.Errorf("trace count = %d, want 7", len(resp.Traces))
		}
	})
}

// ---------------------------------------------------------------------------
// Mat view helpers
// ---------------------------------------------------------------------------

func waitForServerTest(t *testing.T, url string) {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	for i := 0; i < 30; i++ {
		resp, err := client.Get(url)
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("mat view server did not become ready")
}

func httpGetJSONURL(t *testing.T, fullURL string, result any) int {
	t.Helper()
	resp, err := http.Get(fullURL)
	if err != nil {
		t.Fatalf("GET %s: %v", fullURL, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading body: %v", err)
	}
	if err := json.Unmarshal(body, result); err != nil {
		t.Fatalf("decoding JSON from %s: %v\nbody: %s", fullURL, err, string(body))
	}
	return resp.StatusCode
}
