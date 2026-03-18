# TraceQL Metrics Functions

Metrics functions compute time-series data from traces. They are used with the `traceql-metrics-range` and `traceql-metrics-instant` tools.

## rate()

Computes the per-second rate of matching spans.

```
{ resource.service.name = "api" } | rate()
```

With grouping:
```
{ resource.service.name = "api" } | rate() by (span.http.method)
```

**Use case:** "How many requests per second is the API service handling?"

## count_over_time()

Counts the total number of matching spans per time bucket.

```
{ resource.service.name = "api" && status = error } | count_over_time()
```

**Use case:** "How many errors occurred in each time period?"

## quantile_over_time(quantile)

Computes a percentile of a duration or numeric attribute over time.

```
{ resource.service.name = "api" } | quantile_over_time(duration, 0.99)
```

Common quantiles:
- `0.50` — Median (p50)
- `0.90` — 90th percentile (p90)
- `0.95` — 95th percentile (p95)
- `0.99` — 99th percentile (p99)

**Use case:** "What is the p99 latency for the API service over time?"

## min_over_time(), max_over_time(), avg_over_time(), sum_over_time()

Compute min/max/average/sum of a numeric attribute over time.

```
{ resource.service.name = "api" } | max_over_time(duration)
{ resource.service.name = "api" } | avg_over_time(duration)
```

## histogram_over_time()

Creates a duration histogram over time, bucketing spans by latency.

```
{ resource.service.name = "api" } | histogram_over_time(duration)
```

**Use case:** Heatmap visualizations showing latency distribution.

## Grouping with by()

All metrics functions support `by()` for grouping:

```
{ } | rate() by (resource.service.name)
{ } | quantile_over_time(duration, 0.99) by (resource.service.name, span.http.method)
```

## Instant vs Range Queries

- **Instant queries** (`traceql-metrics-instant`): Return a single value for the entire time range. Best for "what is the current value?" questions.
- **Range queries** (`traceql-metrics-range`): Return time-series data with one data point per time bucket. Best for "how has this changed over time?" questions.

**Prefer instant queries when a single value suffices.** Range queries can return large responses.

## Common Patterns

Error rate by service:
```
{ status = error } | rate() by (resource.service.name)
```

P99 latency for a specific endpoint:
```
{ resource.service.name = "api" && name = "GET /users" } | quantile_over_time(duration, 0.99)
```

Request rate comparison across services:
```
{ } | rate() by (resource.service.name)
```

Slowest operations:
```
{ resource.service.name = "api" } | max_over_time(duration) by (name)
```
