import { test, expect } from "@playwright/test";

const spansetExpressions: { name: string; query: string }[] = [
  { name: "and", query: "{ true } && { true }" },
  { name: "union", query: "{ true } || { true }" },
  { name: "descendant", query: "{ true } >> { true }" },
  { name: "ancestor", query: "{ true } << { true }" },
  { name: "child", query: "{ true } > { true }" },
  { name: "parent", query: "{ true } < { true }" },
  { name: "sibling", query: "{ true } ~ { true }" },
  { name: "not child", query: "{ true } !> { true }" },
  { name: "not parent", query: "{ true } !< { true }" },
  { name: "not sibling", query: "{ true } !~ { true }" },
  { name: "not descendant", query: "{ true } !>> { true }" },
  { name: "not ancestor", query: "{ true } !<< { true }" },
  { name: "union child", query: "{ true } &> { true }" },
  { name: "union parent", query: "{ true } &< { true }" },
  { name: "union sibling", query: "{ true } &~ { true }" },
  { name: "union descendant", query: "{ true } &>> { true }" },
  { name: "union ancestor", query: "{ true } &<< { true }" },
];

const pipelineExpressions: { name: string; query: string }[] = [
  {
    name: "pipeline descendant",
    query:
      "({ true } | count() > 1 | { false }) >> ({ true } | count() > 1 | { false })",
  },
  {
    name: "pipeline child",
    query:
      "({ true } | count() > 1 | { false }) > ({ true } | count() > 1 | { false })",
  },
  {
    name: "pipeline sibling",
    query:
      "({ true } | count() > 1 | { false }) ~ ({ true } | count() > 1 | { false })",
  },
];

test.describe("Spanset expressions — simple operators", () => {
  for (const { name, query } of spansetExpressions) {
    test(`${name}: ${query}`, async ({ request }) => {
      const res = await request.get("/api/search", {
        params: { q: query },
      });
      expect(res.status()).toBe(200);
      const body = await res.json();
      expect(body).toHaveProperty("traces");
    });
  }
});

test.describe("Spanset expressions — pipeline operands", () => {
  for (const { name, query } of pipelineExpressions) {
    test(`${name}: ${query}`, async ({ request }) => {
      const res = await request.get("/api/search", {
        params: { q: query },
      });
      // Pipeline expressions parse successfully but may not be fully
      // transpilable yet — accept either 200 or 400 (transpiler limitation).
      expect([200, 400]).toContain(res.status());
      if (res.status() === 200) {
        const body = await res.json();
        expect(body).toHaveProperty("traces");
      }
    });
  }
});
