-- Materialized views for Opex query optimization.
-- These are optional and improve performance for trace metadata lookup
-- and tag discovery queries.
--
-- Run after init.sql to create the materialized views.

-- ============================================================================
-- 1. Trace Metadata Materialized View
-- ============================================================================
-- Pre-computes per-trace metadata (root service, root span name, duration,
-- span count) for fast search result enrichment.
--
-- Without this view, the search API must scan all spans for matched traces
-- to find the root span and compute trace duration. This view keeps that
-- data pre-aggregated.

CREATE TABLE IF NOT EXISTS otel.otel_trace_metadata
(
    `TraceId` String,
    `RootServiceName` AggregateFunction(argMin, LowCardinality(String), DateTime64(9)),
    `RootSpanName` AggregateFunction(argMin, LowCardinality(String), DateTime64(9)),
    `StartTime` SimpleAggregateFunction(min, DateTime64(9)),
    `MaxEndNano` SimpleAggregateFunction(max, Int64),
    `SpanCount` SimpleAggregateFunction(sum, UInt64),
    `ErrorCount` SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toDate(StartTime)
ORDER BY (TraceId)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS otel.otel_trace_metadata_mv
TO otel.otel_trace_metadata
AS SELECT
    TraceId,
    argMinState(ServiceName, if(ParentSpanId = '', Timestamp, toDateTime64('2099-01-01 00:00:00.000000000', 9))) AS RootServiceName,
    argMinState(SpanName, if(ParentSpanId = '', Timestamp, toDateTime64('2099-01-01 00:00:00.000000000', 9))) AS RootSpanName,
    min(Timestamp) AS StartTime,
    max(toInt64(toUnixTimestamp64Nano(Timestamp)) + toInt64(Duration)) AS MaxEndNano,
    toUInt64(count()) AS SpanCount,
    toUInt64(countIf(StatusCode = 'STATUS_CODE_ERROR')) AS ErrorCount
FROM otel.otel_traces
GROUP BY TraceId;


-- ============================================================================
-- 2. Tag Name Cache — Span Attributes
-- ============================================================================
-- Pre-computes the set of distinct span attribute keys so tag discovery
-- doesn't need to scan all spans with arrayJoin(mapKeys(...)).

CREATE TABLE IF NOT EXISTS otel.otel_span_tag_names
(
    `BucketStart` DateTime64(9),
    `TagName` LowCardinality(String),
    `Count` SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toDate(BucketStart)
ORDER BY (BucketStart, TagName)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS otel.otel_span_tag_names_mv
TO otel.otel_span_tag_names
AS SELECT
    toStartOfInterval(Timestamp, INTERVAL 5 MINUTE) AS BucketStart,
    arrayJoin(mapKeys(SpanAttributes)) AS TagName,
    toUInt64(count()) AS Count
FROM otel.otel_traces
GROUP BY BucketStart, TagName;


-- ============================================================================
-- 3. Tag Name Cache — Resource Attributes
-- ============================================================================
-- Pre-computes the set of distinct resource attribute keys.

CREATE TABLE IF NOT EXISTS otel.otel_resource_tag_names
(
    `BucketStart` DateTime64(9),
    `TagName` LowCardinality(String),
    `Count` SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toDate(BucketStart)
ORDER BY (BucketStart, TagName)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS otel.otel_resource_tag_names_mv
TO otel.otel_resource_tag_names
AS SELECT
    toStartOfInterval(Timestamp, INTERVAL 5 MINUTE) AS BucketStart,
    arrayJoin(mapKeys(ResourceAttributes)) AS TagName,
    toUInt64(count()) AS Count
FROM otel.otel_traces
GROUP BY BucketStart, TagName;


-- ============================================================================
-- 4. Service Name List
-- ============================================================================
-- Pre-computes distinct service names for fast service.name tag value lookup.

CREATE TABLE IF NOT EXISTS otel.otel_service_names
(
    `BucketStart` DateTime64(9),
    `ServiceName` LowCardinality(String),
    `SpanCount` SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toDate(BucketStart)
ORDER BY (BucketStart, ServiceName)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS otel.otel_service_names_mv
TO otel.otel_service_names
AS SELECT
    toStartOfInterval(Timestamp, INTERVAL 5 MINUTE) AS BucketStart,
    ServiceName,
    toUInt64(count()) AS SpanCount
FROM otel.otel_traces
GROUP BY BucketStart, ServiceName;


-- ============================================================================
-- Usage Notes
-- ============================================================================
-- 
-- Materialized views populate automatically for new data inserted after
-- creation. To backfill existing data:
--
--   INSERT INTO otel.otel_trace_metadata
--   SELECT
--       TraceId,
--       argMinState(ServiceName, if(ParentSpanId = '', Timestamp, toDateTime64('2099-01-01 00:00:00.000000000', 9))),
--       argMinState(SpanName, if(ParentSpanId = '', Timestamp, toDateTime64('2099-01-01 00:00:00.000000000', 9))),
--       min(Timestamp),
--       max(toInt64(toUnixTimestamp64Nano(Timestamp)) + toInt64(Duration)),
--       toUInt64(count()),
--       toUInt64(countIf(StatusCode = 'STATUS_CODE_ERROR'))
--   FROM otel.otel_traces
--   GROUP BY TraceId;
--
--   INSERT INTO otel.otel_span_tag_names
--   SELECT toStartOfInterval(Timestamp, INTERVAL 5 MINUTE), arrayJoin(mapKeys(SpanAttributes)), toUInt64(count())
--   FROM otel.otel_traces GROUP BY 1, 2;
--
--   INSERT INTO otel.otel_resource_tag_names
--   SELECT toStartOfInterval(Timestamp, INTERVAL 5 MINUTE), arrayJoin(mapKeys(ResourceAttributes)), toUInt64(count())
--   FROM otel.otel_traces GROUP BY 1, 2;
--
--   INSERT INTO otel.otel_service_names
--   SELECT toStartOfInterval(Timestamp, INTERVAL 5 MINUTE), ServiceName, toUInt64(count())
--   FROM otel.otel_traces GROUP BY 1, ServiceName;
