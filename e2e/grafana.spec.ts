import { test, expect, type Page } from "@playwright/test";

const OPEX_URL = process.env.OPEX_URL ?? "http://localhost:8080";

async function navigateToExplore(page: Page) {
  await page.goto("/explore", { waitUntil: "networkidle" });
  await expect(page).toHaveTitle(/Grafana/);
  // Wait for the Monaco editor textbox to appear (signals TraceQL editor loaded)
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

test.describe("Explore: TraceQL search", () => {
  test("run empty TraceQL and see results table", async ({ page }) => {
    await navigateToExplore(page);
    await runTraceQLQuery(page, "{}");

    // Grafana uses ARIA role="table" on a <div>, not a <table> element
    const resultsTable = page.getByRole("table", { name: "Explore Table" });
    await expect(resultsTable).toBeVisible({ timeout: 15_000 });

    // Verify we got data rows (skip header row)
    const dataRows = resultsTable
      .getByRole("row")
      .filter({ hasNot: page.getByRole("columnheader") });
    await expect(dataRows.first()).toBeVisible({ timeout: 10_000 });
    expect(await dataRows.count()).toBeGreaterThan(0);
  });

  test("click a trace to open detail panel", async ({ page }) => {
    await navigateToExplore(page);
    await runTraceQLQuery(page, "{}");

    const resultsTable = page.getByRole("table", { name: "Explore Table" });
    await expect(resultsTable).toBeVisible({ timeout: 15_000 });

    // Click the first trace ID link in the results
    const traceLink = resultsTable.getByRole("link").first();
    await expect(traceLink).toBeVisible({ timeout: 10_000 });
    await traceLink.click();

    // Trace detail panel renders a heading like "serviceName: spanName"
    await expect(
      page.getByRole("heading", { name: /:/ }).first()
    ).toBeVisible({ timeout: 30_000 });
  });
});

test.describe("Explore: Trace by ID", () => {
  test("search by known trace ID shows trace detail", async ({ page }) => {
    await navigateToExplore(page);
    await runTraceQLQuery(page, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

    // The known trace renders heading "frontend: GET /login"
    await expect(
      page.getByRole("heading", { name: "frontend: GET /login" })
    ).toBeVisible({ timeout: 30_000 });
  });
});

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
      `${OPEX_URL}/api/traces/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa`,
      { headers: { Accept: "application/protobuf" } }
    );
    expect(res.status()).toBe(200);
    expect(res.headers()["content-type"]).toContain("application/protobuf");
  });
});
