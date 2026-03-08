import { defineConfig } from "@playwright/test";

const OPEX_URL = process.env.OPEX_URL ?? "http://localhost:8080";
const GRAFANA_URL = process.env.GRAFANA_URL ?? "http://localhost:3000";

export default defineConfig({
  testDir: ".",
  timeout: 60_000,
  retries: 1,
  use: {
    baseURL: GRAFANA_URL,
    screenshot: "only-on-failure",
    trace: "retain-on-failure",
  },
  projects: [
    {
      name: "api",
      testMatch: ["api.spec.ts", "spanset-expressions.spec.ts"],
      use: { baseURL: OPEX_URL },
    },
    {
      name: "grafana",
      testMatch: "grafana.spec.ts",
      use: {
        baseURL: GRAFANA_URL,
        browserName: "chromium",
      },
    },
  ],
});
