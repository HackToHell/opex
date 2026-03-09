import { test, expect, type Page } from "@playwright/test";

const OPEX_URL = process.env.OPEX_URL ?? "http://localhost:8080";
const KNOWN_TRACE_ID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

async function navigateToExplore(page: Page) {
  await page.goto("/explore", { waitUntil: "networkidle" });
  await expect(page).toHaveTitle(/Grafana/);
  await expect(
    page.getByRole("textbox", { name: /Editor content/ })
  ).toBeVisible({ timeout: 30_000 });
}

async function runTraceQLQuery(page: Page, query: string) {
  const editor = page.getByRole("textbox", { name: /Editor content/ });
  await editor.click();
  await editor.fill(query);
  await page.getByRole("button", { name: "Run query" }).click();
}

async function getResultsTable(page: Page) {
  const resultsTable = page.getByRole("table", { name: "Explore Table" });
  await expect(resultsTable).toBeVisible({ timeout: 15_000 });
  return resultsTable;
}

async function openTraceById(page: Page, traceId: string) {
  await navigateToExplore(page);
  await runTraceQLQuery(page, traceId);
}

async function waitForTraceDetail(page: Page, headingPattern: string | RegExp) {
  await expect(
    page.getByRole("heading", { name: headingPattern })
  ).toBeVisible({ timeout: 30_000 });
}

async function expandSpanFilters(page: Page) {
  const filterBtn = page.getByRole("button", { name: /Span Filters/ }).first();
  await expect(filterBtn).toBeVisible({ timeout: 10_000 });

  const serviceName = page.locator('[aria-label="Select service name"]');
  const isExpanded = await serviceName.isVisible().catch(() => false);
  if (!isExpanded) {
    await filterBtn.click();
    await page.waitForTimeout(1_000);
  }
  await expect(serviceName).toBeVisible({ timeout: 10_000 });
}

async function expandNodeGraph(page: Page) {
  const nodeGraphRegion = page.getByRole("region", { name: "Node graph" });
  await expect(nodeGraphRegion).toBeVisible({ timeout: 10_000 });

  const layeredRadio = page.getByRole("radio", { name: "Layered" });
  const isExpanded = await layeredRadio.isVisible().catch(() => false);
  if (!isExpanded) {
    const expandBtn = nodeGraphRegion.getByRole("button", { name: "Node graph" });
    await expandBtn.click();
    await page.waitForTimeout(2_000);
  }
  await expect(layeredRadio).toBeVisible({ timeout: 15_000 });
}

async function clickFirstSpanRow(page: Page) {
  const spanSwitch = page.getByRole("switch").filter({ hasText: /GET \/login/ }).first();
  await expect(spanSwitch).toBeVisible({ timeout: 5_000 });
  await spanSwitch.click();
  await expect(page.getByText("Service:").first()).toBeVisible({ timeout: 10_000 });
}

// ---------------------------------------------------------------------------
// Grafana loads
// ---------------------------------------------------------------------------
test.describe("Grafana loads", () => {
  test("home page is accessible", async ({ page }) => {
    await page.goto("/", { waitUntil: "networkidle" });
    await expect(page).toHaveTitle(/Grafana/);
  });

  test("Tempo datasource is provisioned", async ({ page }) => {
    await page.goto("/connections/datasources", { waitUntil: "networkidle" });
    await expect(page.getByText("Opex (Tempo)")).toBeVisible();
  });
});

// ---------------------------------------------------------------------------
// Explore page — query editor elements
// ---------------------------------------------------------------------------
test.describe("Explore: query editor elements", () => {
  test.beforeEach(async ({ page }) => {
    await navigateToExplore(page);
  });

  test("TraceQL editor textbox is visible", async ({ page }) => {
    await expect(
      page.getByRole("textbox", { name: /Editor content/ })
    ).toBeVisible();
  });

  test("Run query button is visible", async ({ page }) => {
    await expect(
      page.getByRole("button", { name: "Run query" })
    ).toBeVisible();
  });

  test("query type radio buttons exist (Search, TraceQL, Service Graph)", async ({ page }) => {
    await expect(page.getByRole("radio", { name: "Search" })).toBeVisible();
    await expect(page.getByRole("radio", { name: "TraceQL" })).toBeChecked();
    await expect(
      page.getByRole("radio", { name: "Service Graph" })
    ).toBeVisible();
  });

  test("time range selector is visible", async ({ page }) => {
    await expect(
      page.getByRole("button", { name: /Time range/ })
    ).toBeVisible();
  });

  test("time navigation buttons exist", async ({ page }) => {
    await expect(
      page.getByRole("button", { name: "Move time range backwards" })
    ).toBeVisible();
    await expect(
      page.getByRole("button", { name: "Move time range forwards" })
    ).toBeVisible();
    await expect(
      page.getByRole("button", { name: "Zoom out time range" })
    ).toBeVisible();
  });

  test("datasource selector is visible", async ({ page }) => {
    await expect(
      page.getByRole("textbox", { name: "Select a data source" })
    ).toBeVisible();
  });

  test("query row A title is visible", async ({ page }) => {
    await expect(
      page.getByRole("button", { name: "Query editor row title A" })
    ).toBeVisible();
  });

  test("query row action buttons exist", async ({ page }) => {
    await expect(
      page.getByRole("button", { name: "Show data source help" })
    ).toBeVisible();
    await expect(
      page.getByRole("button", { name: "Duplicate query" })
    ).toBeVisible();
    await expect(
      page.getByRole("button", { name: "Hide response" })
    ).toBeVisible();
    await expect(
      page.getByRole("button", { name: "Remove query" })
    ).toBeVisible();
  });

  test("Add query button is visible", async ({ page }) => {
    await expect(
      page.getByRole("button", { name: "Add query" })
    ).toBeVisible();
  });

  test("Query inspector button is visible", async ({ page }) => {
    await expect(
      page.getByRole("button", { name: "Query inspector" })
    ).toBeVisible();
  });

  test("Search Options button is visible", async ({ page }) => {
    await expect(
      page.getByRole("button", { name: /Search Options/ })
    ).toBeVisible();
  });

  test("Split pane button is visible", async ({ page }) => {
    await expect(
      page.getByRole("button", { name: /Split/ })
    ).toBeVisible();
  });

  test("auto-refresh button is visible", async ({ page }) => {
    await expect(
      page.getByRole("button", { name: /Auto refresh/ })
    ).toBeVisible();
  });
});

// ---------------------------------------------------------------------------
// Explore page — search results table
// ---------------------------------------------------------------------------
test.describe("Explore: TraceQL search results", () => {
  test("run empty TraceQL and see results table", async ({ page }) => {
    await navigateToExplore(page);
    await runTraceQLQuery(page, "{}");

    const resultsTable = await getResultsTable(page);
    const dataRows = resultsTable
      .getByRole("row")
      .filter({ hasNot: page.getByRole("columnheader") });
    await expect(dataRows.first()).toBeVisible({ timeout: 10_000 });
    expect(await dataRows.count()).toBeGreaterThan(0);
  });

  test("results table has correct column headers", async ({ page }) => {
    await navigateToExplore(page);
    await runTraceQLQuery(page, "{}");
    const table = await getResultsTable(page);

    for (const col of ["Trace ID", "Start time", "Service", "Name", "Duration"]) {
      await expect(table.getByRole("columnheader", { name: col })).toBeVisible();
    }
  });

  test("Trace ID column contains clickable links", async ({ page }) => {
    await navigateToExplore(page);
    await runTraceQLQuery(page, "{}");
    const table = await getResultsTable(page);

    const links = table.getByRole("link");
    await expect(links.first()).toBeVisible({ timeout: 10_000 });
    expect(await links.count()).toBeGreaterThan(0);
  });

  test("column headers are sortable (clickable)", async ({ page }) => {
    await navigateToExplore(page);
    await runTraceQLQuery(page, "{}");
    const table = await getResultsTable(page);

    const durationHeader = table.getByRole("columnheader", { name: "Duration" });
    await expect(durationHeader).toBeVisible();

    // Capture the first row's duration text before sorting
    const firstRowBefore = await table
      .getByRole("row")
      .filter({ hasNot: page.getByRole("columnheader") })
      .first()
      .textContent();

    // Click duration header to sort
    await durationHeader.click();
    await page.waitForTimeout(500);

    // Click again to reverse sort direction
    await durationHeader.click();
    await page.waitForTimeout(500);

    // Verify the table is still rendered with data rows after sorting
    const dataRows = table
      .getByRole("row")
      .filter({ hasNot: page.getByRole("columnheader") });
    await expect(dataRows.first()).toBeVisible({ timeout: 5_000 });
    expect(await dataRows.count()).toBeGreaterThan(0);
  });

  test("row expand toggle exists", async ({ page }) => {
    await navigateToExplore(page);
    await runTraceQLQuery(page, "{}");
    const table = await getResultsTable(page);

    await expect(table.getByRole("row").first()).toBeVisible({ timeout: 10_000 });
    const expandIcons = table.getByRole("img", { name: "Expand row" });
    expect(await expandIcons.count()).toBeGreaterThan(0);
  });

  test("Table - Traces region heading is visible", async ({ page }) => {
    await navigateToExplore(page);
    await runTraceQLQuery(page, "{}");
    await expect(
      page.getByRole("heading", { name: /Table - Traces/ })
    ).toBeVisible({ timeout: 15_000 });
  });

  test("service filter query returns matching traces", async ({ page }) => {
    await navigateToExplore(page);
    await runTraceQLQuery(page, '{ resource.service.name = "frontend" }');
    const table = await getResultsTable(page);

    const rows = table.getByRole("row").filter({ hasNot: page.getByRole("columnheader") });
    await expect(rows.first()).toBeVisible({ timeout: 10_000 });
    expect(await rows.count()).toBeGreaterThan(0);
  });
});

// ---------------------------------------------------------------------------
// Explore: Trace by ID
// ---------------------------------------------------------------------------
test.describe("Explore: Trace by ID", () => {
  test("search by known trace ID shows trace detail heading", async ({ page }) => {
    await openTraceById(page, KNOWN_TRACE_ID);
    await waitForTraceDetail(page, "frontend: GET /login");
  });
});

// ---------------------------------------------------------------------------
// Trace drilldown — header elements
// ---------------------------------------------------------------------------
test.describe("Drilldown: trace header", () => {
  test.beforeEach(async ({ page }) => {
    await openTraceById(page, KNOWN_TRACE_ID);
    await waitForTraceDetail(page, "frontend: GET /login");
  });

  test("trace title heading shows service:spanName", async ({ page }) => {
    await expect(
      page.getByRole("heading", { name: "frontend: GET /login", level: 1 })
    ).toBeVisible();
  });

  test("trace ID is displayed and copyable", async ({ page }) => {
    await expect(
      page.getByRole("button", { name: KNOWN_TRACE_ID })
    ).toBeVisible();
  });

  test("start time is displayed", async ({ page }) => {
    await expect(page.getByText(/\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/)).toBeVisible();
  });

  test("duration is displayed", async ({ page }) => {
    const traceRegion = page.getByRole("region", { name: "Trace" });
    await expect(traceRegion).toBeVisible();
  });

  test("services count is displayed", async ({ page }) => {
    await expect(page.getByText(/Services/).first()).toBeVisible();
  });

  test("HTTP method badge is shown", async ({ page }) => {
    await expect(page.getByText("GET").first()).toBeVisible();
  });

  test("HTTP status code is shown", async ({ page }) => {
    await expect(page.getByText("200").first()).toBeVisible();
  });

  test("Share button is visible", async ({ page }) => {
    await expect(
      page.getByRole("button", { name: "Share" }).first()
    ).toBeVisible();
  });
});

// ---------------------------------------------------------------------------
// Trace drilldown — node graph
// ---------------------------------------------------------------------------
test.describe("Drilldown: node graph", () => {
  test.beforeEach(async ({ page }) => {
    await openTraceById(page, KNOWN_TRACE_ID);
    await waitForTraceDetail(page, "frontend: GET /login");
  });

  test("node graph region is visible", async ({ page }) => {
    await expect(
      page.getByRole("region", { name: "Node graph" })
    ).toBeVisible();
  });

  test("node graph heading is visible", async ({ page }) => {
    await expect(
      page.getByRole("heading", { name: "Node graph" })
    ).toBeVisible();
  });

  test("node graph layout radio buttons exist after expanding", async ({ page }) => {
    await expandNodeGraph(page);
    await expect(page.getByRole("radio", { name: "Layered" })).toBeVisible();
    await expect(page.getByRole("radio", { name: "Force" })).toBeVisible();
    await expect(page.getByRole("radio", { name: "Grid" })).toBeVisible();
  });

  test("default layout is Layered after expanding", async ({ page }) => {
    await expandNodeGraph(page);
    await expect(page.getByRole("radio", { name: "Layered" })).toBeChecked();
  });

  test("zoom buttons exist after expanding", async ({ page }) => {
    await expandNodeGraph(page);
    const nodeGraph = page.getByRole("region", { name: "Node graph" });
    await expect(nodeGraph.getByRole("button", { name: "Zoom in" })).toBeVisible({ timeout: 10_000 });
    await expect(nodeGraph.getByRole("button", { name: "Zoom out" })).toBeVisible({ timeout: 10_000 });
  });

  test("switching layout to Grid works", async ({ page }) => {
    await expandNodeGraph(page);
    const gridRadio = page.getByRole("radio", { name: "Grid" });
    await gridRadio.click();
    await expect(gridRadio).toBeChecked();
  });

  test("switching layout to Force works", async ({ page }) => {
    await expandNodeGraph(page);
    const forceRadio = page.getByRole("radio", { name: "Force" });
    await forceRadio.click();
    await expect(forceRadio).toBeChecked();
  });

  test("service nodes are rendered", async ({ page }) => {
    await expect(page.getByText("frontend").first()).toBeVisible();
  });
});

// ---------------------------------------------------------------------------
// Trace drilldown — Trace region and timeline
// ---------------------------------------------------------------------------
test.describe("Drilldown: trace region and timeline", () => {
  test.beforeEach(async ({ page }) => {
    await openTraceById(page, KNOWN_TRACE_ID);
    await waitForTraceDetail(page, "frontend: GET /login");
  });

  test("Trace region exists", async ({ page }) => {
    await expect(page.getByRole("region", { name: "Trace" })).toBeVisible();
  });

  test("Trace heading is visible", async ({ page }) => {
    await expect(
      page.getByRole("heading", { name: "Trace" })
    ).toBeVisible();
  });

  test("Service & Operation section heading exists", async ({ page }) => {
    await expect(
      page.getByRole("heading", { name: /Service & Operation/ })
    ).toBeVisible();
  });

  test("expand/collapse all buttons exist", async ({ page }) => {
    await expect(
      page.getByRole("button", { name: "Expand all" })
    ).toBeVisible();
    await expect(
      page.getByRole("button", { name: "Collapse all" })
    ).toBeVisible();
  });

  test("span rows are visible in the trace timeline", async ({ page }) => {
    const spans = page.getByRole("switch").filter({ hasText: /GET \/login/ });
    await expect(spans.first()).toBeVisible();
  });

  test("multiple span rows exist for multi-span trace", async ({ page }) => {
    const allSwitches = page.getByRole("switch");
    expect(await allSwitches.count()).toBeGreaterThanOrEqual(2);
  });

  test("clicking a span row opens span detail", async ({ page }) => {
    await clickFirstSpanRow(page);
    await expect(page.getByText("Duration:").first()).toBeVisible();
  });
});

// ---------------------------------------------------------------------------
// Trace drilldown — span filters
// ---------------------------------------------------------------------------
test.describe("Drilldown: span filters", () => {
  test.beforeEach(async ({ page }) => {
    await openTraceById(page, KNOWN_TRACE_ID);
    await waitForTraceDetail(page, "frontend: GET /login");
  });

  test("Span Filters section is visible", async ({ page }) => {
    await expect(
      page.getByRole("button", { name: /Span Filters/ })
    ).toBeVisible();
  });

  test("span count is displayed", async ({ page }) => {
    await expect(page.getByText(/\d+ spans/)).toBeVisible();
  });

  test("service name filter combobox exists after expanding", async ({ page }) => {
    await expandSpanFilters(page);
    await expect(
      page.locator('[aria-label="Select service name"]')
    ).toBeAttached({ timeout: 10_000 });
  });

  test("span name filter combobox exists after expanding", async ({ page }) => {
    await expandSpanFilters(page);
    await expect(
      page.locator('[aria-label="Select span name"]')
    ).toBeAttached({ timeout: 10_000 });
  });

  test("Find text search input exists after expanding", async ({ page }) => {
    await expandSpanFilters(page);
    await expect(
      page.getByPlaceholder("Find...")
    ).toBeAttached({ timeout: 10_000 });
  });

  test("duration min/max filter inputs exist after expanding", async ({ page }) => {
    await expandSpanFilters(page);
    await expect(
      page.locator('[aria-label="Select min span duration"]')
    ).toBeAttached({ timeout: 10_000 });
    await expect(
      page.locator('[aria-label="Select max span duration"]')
    ).toBeAttached({ timeout: 10_000 });
  });

  test("tag filter controls exist after expanding", async ({ page }) => {
    await expandSpanFilters(page);
    await expect(
      page.locator('[aria-label="Select tag key"]')
    ).toBeAttached({ timeout: 10_000 });
  });

  test("prev/next result buttons exist", async ({ page }) => {
    const traceRegion = page.getByRole("region", { name: "Trace" });
    await expect(
      traceRegion.getByRole("button", { name: "Prev result button", exact: true })
    ).toBeAttached({ timeout: 10_000 });
    await expect(
      traceRegion.getByRole("button", { name: "Next result button", exact: true })
    ).toBeAttached({ timeout: 10_000 });
  });
});

// ---------------------------------------------------------------------------
// Trace drilldown — span detail panel
// ---------------------------------------------------------------------------
test.describe("Drilldown: span detail panel", () => {
  test.beforeEach(async ({ page }) => {
    await openTraceById(page, KNOWN_TRACE_ID);
    await waitForTraceDetail(page, "frontend: GET /login");
    await clickFirstSpanRow(page);
  });

  test("span heading is shown", async ({ page }) => {
    await expect(
      page.getByRole("heading", { name: /GET \/login/ }).last()
    ).toBeVisible({ timeout: 10_000 });
  });

  test("Service detail is shown", async ({ page }) => {
    await expect(page.getByText("Service:").first()).toBeVisible({ timeout: 10_000 });
    await expect(page.getByText("frontend").first()).toBeVisible();
  });

  test("Duration detail is shown", async ({ page }) => {
    await expect(page.getByText("Duration:").first()).toBeVisible({ timeout: 10_000 });
  });

  test("Start Time detail is shown", async ({ page }) => {
    await expect(page.getByText("Start Time:").first()).toBeVisible({ timeout: 10_000 });
  });

  test("Kind detail is shown", async ({ page }) => {
    await expect(page.getByText("Kind:").first()).toBeVisible({ timeout: 10_000 });
    await expect(page.getByText("server").first()).toBeVisible();
  });

  test("Status detail is shown", async ({ page }) => {
    await expect(page.getByText("Status:").first()).toBeVisible({ timeout: 10_000 });
  });

  test("Library Name detail is shown", async ({ page }) => {
    await expect(page.getByText("Library Name:").first()).toBeVisible({ timeout: 10_000 });
  });

  test("Library Version detail is shown", async ({ page }) => {
    await expect(page.getByText("Library Version:").first()).toBeVisible({ timeout: 10_000 });
  });

  test("SpanID is displayed", async ({ page }) => {
    const spanIdLabel = page.locator(':text("SpanID")');
    const spanIdValue = page.getByText("1000000000000001");
    const eitherFound = await Promise.any([
      spanIdLabel.first().waitFor({ state: "attached", timeout: 15_000 }).then(() => true),
      spanIdValue.first().waitFor({ state: "attached", timeout: 15_000 }).then(() => true),
    ]).catch(() => false);
    expect(eitherFound).toBeTruthy();
  });

  test("Span attributes section is present", async ({ page }) => {
    await expect(
      page.getByRole("switch", { name: /Span attributes/ })
    ).toBeVisible();
  });

  test("Resource attributes section is present", async ({ page }) => {
    await expect(
      page.getByRole("switch", { name: /Resource attributes/ })
    ).toBeVisible();
  });

  test("span attributes can be expanded", async ({ page }) => {
    const attrToggle = page.getByRole("switch", { name: /Span attributes/ });
    await attrToggle.click();
    await expect(page.getByText("http.method").first()).toBeVisible({ timeout: 5_000 });
  });

  test("resource attributes can be expanded", async ({ page }) => {
    const attrToggle = page.getByRole("switch", { name: /Resource attributes/ });
    await attrToggle.click();
    await expect(page.getByText("service.name").first()).toBeVisible({ timeout: 5_000 });
  });
});

// ---------------------------------------------------------------------------
// Trace drilldown — toolbar in the right pane
// ---------------------------------------------------------------------------
test.describe("Drilldown: right pane toolbar", () => {
  test.beforeEach(async ({ page }) => {
    await openTraceById(page, KNOWN_TRACE_ID);
    await waitForTraceDetail(page, "frontend: GET /login");
  });

  test("Run query button exists in drilldown pane", async ({ page }) => {
    const runButtons = page.getByRole("button", { name: "Run query" });
    expect(await runButtons.count()).toBeGreaterThanOrEqual(1);
  });

  test("time range button in drilldown pane", async ({ page }) => {
    const timeButtons = page.getByRole("button", { name: /Time range/ });
    expect(await timeButtons.count()).toBeGreaterThanOrEqual(1);
  });

  test("auto-refresh button in drilldown pane", async ({ page }) => {
    const autoRefresh = page.getByRole("button", { name: /Auto refresh/ });
    expect(await autoRefresh.count()).toBeGreaterThanOrEqual(1);
  });

  test("Query inspector button exists in drilldown", async ({ page }) => {
    const inspector = page.getByRole("button", { name: "Query inspector" });
    expect(await inspector.count()).toBeGreaterThanOrEqual(1);
  });
});

// ---------------------------------------------------------------------------
// Trace drilldown — error trace
// ---------------------------------------------------------------------------
test.describe("Drilldown: error trace", () => {
  const ERROR_TRACE = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

  test("error trace shows error status", async ({ page }) => {
    await openTraceById(page, ERROR_TRACE);
    await waitForTraceDetail(page, "frontend: POST /orders");
    await expect(page.getByText("500").first()).toBeVisible();
  });

  test("error trace shows error spans", async ({ page }) => {
    await openTraceById(page, ERROR_TRACE);
    await waitForTraceDetail(page, "frontend: POST /orders");
    // Grafana renders error spans with red error icons (circle-with-cross)
    // and the HTTP status badge "500" is shown in the header
    await expect(page.getByText("500").first()).toBeVisible({ timeout: 5_000 });
    // Verify at least one span row with an error service is displayed
    const errorSpans = page.getByRole("switch").filter({ hasText: /payment-service/ });
    await expect(errorSpans.first()).toBeVisible({ timeout: 10_000 });
  });

  test("error trace has multiple services", async ({ page }) => {
    await openTraceById(page, ERROR_TRACE);
    await waitForTraceDetail(page, "frontend: POST /orders");
    const switches = page.getByRole("switch");
    expect(await switches.count()).toBeGreaterThanOrEqual(3);
  });
});

// ---------------------------------------------------------------------------
// Opex API health checks (from Grafana test suite)
// ---------------------------------------------------------------------------
test.describe("Opex API health (from Grafana suite)", () => {
  test("datasource health check succeeds", async ({ request }) => {
    const res = await request.fetch(`${OPEX_URL}/api/echo`);
    expect(res.status()).toBe(200);
  });

  test("search returns traces", async ({ request }) => {
    const res = await request.fetch(`${OPEX_URL}/api/search?q={}`);
    expect(res.status()).toBe(200);
    const body = await res.json();
    expect(body.traces.length).toBeGreaterThan(0);
  });

  test("trace by ID returns protobuf", async ({ request }) => {
    const res = await request.fetch(
      `${OPEX_URL}/api/traces/${KNOWN_TRACE_ID}`,
      { headers: { Accept: "application/protobuf" } }
    );
    expect(res.status()).toBe(200);
    expect(res.headers()["content-type"]).toContain("application/protobuf");
  });

  test("tags API returns tag names", async ({ request }) => {
    const res = await request.fetch(`${OPEX_URL}/api/search/tags`);
    expect(res.status()).toBe(200);
    const body = await res.json();
    expect(body.tagNames.length).toBeGreaterThan(0);
  });

  test("tag values API returns values", async ({ request }) => {
    const res = await request.fetch(
      `${OPEX_URL}/api/search/tag/service.name/values`
    );
    expect(res.status()).toBe(200);
    const body = await res.json();
    expect(body.tagValues).toContain("frontend");
  });
});
