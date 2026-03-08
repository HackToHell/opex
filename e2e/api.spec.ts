import { test, expect } from "@playwright/test";

const KNOWN_TRACE_ID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

test.describe("Opex API", () => {
  test("GET /api/echo returns 200", async ({ request }) => {
    const res = await request.get("/api/echo");
    expect(res.status()).toBe(200);
    expect(await res.text()).toBe("echo");
  });

  test("GET /api/status/buildinfo returns version info", async ({
    request,
  }) => {
    const res = await request.get("/api/status/buildinfo");
    expect(res.status()).toBe(200);
    const body = await res.json();
    expect(body).toHaveProperty("version");
    expect(body).toHaveProperty("goVersion");
  });

  test("GET /ready returns 200", async ({ request }) => {
    const res = await request.get("/ready");
    expect(res.status()).toBe(200);
  });
});

test.describe("Search API", () => {
  test("GET /api/search with empty TraceQL returns traces", async ({
    request,
  }) => {
    const res = await request.get("/api/search", {
      params: { q: "{}" },
    });
    expect(res.status()).toBe(200);
    const body = await res.json();
    expect(body).toHaveProperty("traces");
    expect(body.traces.length).toBeGreaterThan(0);

    const trace = body.traces[0];
    expect(trace).toHaveProperty("traceID");
    expect(trace).toHaveProperty("rootServiceName");
    expect(trace).toHaveProperty("startTimeUnixNano");
    expect(trace).toHaveProperty("durationMs");
    expect(trace).toHaveProperty("spanSets");
  });

  test("GET /api/search with service filter returns matching traces", async ({
    request,
  }) => {
    const res = await request.get("/api/search", {
      params: { q: '{ resource.service.name = "frontend" }' },
    });
    expect(res.status()).toBe(200);
    const body = await res.json();
    expect(body.traces.length).toBeGreaterThan(0);
    for (const trace of body.traces) {
      expect(trace.rootServiceName).toBe("frontend");
    }
  });

  test("GET /api/search with limit param caps results", async ({
    request,
  }) => {
    const res = await request.get("/api/search", {
      params: { q: "{}", limit: "2" },
    });
    expect(res.status()).toBe(200);
    const body = await res.json();
    expect(body.traces.length).toBeLessThanOrEqual(2);
  });

  test("GET /api/search with invalid TraceQL returns 400", async ({
    request,
  }) => {
    const res = await request.get("/api/search", {
      params: { q: "not valid traceql ]]" },
    });
    expect(res.status()).toBe(400);
  });
});

test.describe("Trace by ID", () => {
  test("GET /api/traces/{traceID} returns JSON trace", async ({
    request,
  }) => {
    const res = await request.get(`/api/traces/${KNOWN_TRACE_ID}`);
    expect(res.status()).toBe(200);
    expect(res.headers()["content-type"]).toContain("application/json");
    const body = await res.json();
    expect(body).toHaveProperty("batches");
    expect(body.batches.length).toBeGreaterThan(0);

    const span = body.batches[0].scopeSpans[0].spans[0];
    expect(span.traceId).toBe(KNOWN_TRACE_ID);
    expect(span).toHaveProperty("name");
    expect(span).toHaveProperty("kind");
    expect(span).toHaveProperty("startTimeUnixNano");
  });

  test("GET /api/traces/{traceID} with Accept: protobuf returns protobuf", async ({
    request,
  }) => {
    const res = await request.get(`/api/traces/${KNOWN_TRACE_ID}`, {
      headers: { Accept: "application/protobuf" },
    });
    expect(res.status()).toBe(200);
    expect(res.headers()["content-type"]).toContain("application/protobuf");
    const buf = await res.body();
    expect(buf.length).toBeGreaterThan(0);
  });

  test("GET /api/v2/traces/{traceID} returns wrapped JSON", async ({
    request,
  }) => {
    const res = await request.get(`/api/v2/traces/${KNOWN_TRACE_ID}`);
    expect(res.status()).toBe(200);
    const body = await res.json();
    expect(body.status).toBe("complete");
    expect(body.trace).toBeTruthy();
    expect(body.trace.batches.length).toBeGreaterThan(0);
  });

  test("GET /api/v2/traces/{traceID} with Accept: protobuf returns protobuf", async ({
    request,
  }) => {
    const res = await request.get(`/api/v2/traces/${KNOWN_TRACE_ID}`, {
      headers: { Accept: "application/protobuf" },
    });
    expect(res.status()).toBe(200);
    expect(res.headers()["content-type"]).toContain("application/protobuf");
    const buf = await res.body();
    expect(buf.length).toBeGreaterThan(0);
  });

  test("GET /api/traces/nonexistent returns 404", async ({ request }) => {
    const res = await request.get(
      "/api/traces/00000000000000000000000000000000"
    );
    expect(res.status()).toBe(404);
  });

  test("GET /api/traces/invalid returns 400", async ({ request }) => {
    const res = await request.get("/api/traces/not-hex");
    expect(res.status()).toBe(400);
  });
});

test.describe("Tags API", () => {
  test("GET /api/search/tags returns tag names", async ({ request }) => {
    const res = await request.get("/api/search/tags");
    expect(res.status()).toBe(200);
    const body = await res.json();
    expect(body).toHaveProperty("tagNames");
    expect(body.tagNames.length).toBeGreaterThan(0);
  });

  test("GET /api/v2/search/tags returns scoped tags", async ({ request }) => {
    const res = await request.get("/api/v2/search/tags");
    expect(res.status()).toBe(200);
    const body = await res.json();
    expect(body).toHaveProperty("scopes");
    expect(body.scopes.length).toBeGreaterThan(0);

    const scope = body.scopes[0];
    expect(scope).toHaveProperty("name");
    expect(scope).toHaveProperty("tags");
  });

  test("GET /api/search/tag/service.name/values returns values", async ({
    request,
  }) => {
    const res = await request.get("/api/search/tag/service.name/values");
    expect(res.status()).toBe(200);
    const body = await res.json();
    expect(body).toHaveProperty("tagValues");
    expect(body.tagValues).toContain("frontend");
  });

  test("GET /api/v2/search/tag/service.name/values returns typed values", async ({
    request,
  }) => {
    const res = await request.get("/api/v2/search/tag/service.name/values");
    expect(res.status()).toBe(200);
    const body = await res.json();
    expect(body).toHaveProperty("tagValues");
    expect(body.tagValues.length).toBeGreaterThan(0);

    const val = body.tagValues[0];
    expect(val).toHaveProperty("type");
    expect(val).toHaveProperty("value");
  });
});
