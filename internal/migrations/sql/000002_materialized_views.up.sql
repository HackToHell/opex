CREATE TABLE IF NOT EXISTS {{ .TraceMetadataTable }}
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

CREATE MATERIALIZED VIEW IF NOT EXISTS {{ .TraceMetadataView }}
TO {{ .TraceMetadataTable }}
AS SELECT
    TraceId,
    argMinState(ServiceName, if(ParentSpanId = '', Timestamp, toDateTime64('2099-01-01 00:00:00.000000000', 9))) AS RootServiceName,
    argMinState(SpanName, if(ParentSpanId = '', Timestamp, toDateTime64('2099-01-01 00:00:00.000000000', 9))) AS RootSpanName,
    min(Timestamp) AS StartTime,
    max(toInt64(toUnixTimestamp64Nano(Timestamp)) + toInt64(Duration)) AS MaxEndNano,
    toUInt64(count()) AS SpanCount,
    toUInt64(countIf(StatusCode = 'STATUS_CODE_ERROR')) AS ErrorCount
FROM {{ .TracesTable }}
GROUP BY TraceId;

INSERT INTO {{ .TraceMetadataTable }}
SELECT
    TraceId,
    argMinState(ServiceName, if(ParentSpanId = '', Timestamp, toDateTime64('2099-01-01 00:00:00.000000000', 9))) AS RootServiceName,
    argMinState(SpanName, if(ParentSpanId = '', Timestamp, toDateTime64('2099-01-01 00:00:00.000000000', 9))) AS RootSpanName,
    min(Timestamp) AS StartTime,
    max(toInt64(toUnixTimestamp64Nano(Timestamp)) + toInt64(Duration)) AS MaxEndNano,
    toUInt64(count()) AS SpanCount,
    toUInt64(countIf(StatusCode = 'STATUS_CODE_ERROR')) AS ErrorCount
FROM {{ .TracesTable }}
WHERE NOT EXISTS (SELECT 1 FROM {{ .TraceMetadataTable }} LIMIT 1)
GROUP BY TraceId;

CREATE TABLE IF NOT EXISTS {{ .SpanTagNamesTable }}
(
    `BucketStart` DateTime64(9),
    `TagName` LowCardinality(String),
    `Count` SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toDate(BucketStart)
ORDER BY (BucketStart, TagName)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS {{ .SpanTagNamesView }}
TO {{ .SpanTagNamesTable }}
AS SELECT
    toStartOfInterval(Timestamp, INTERVAL 5 MINUTE) AS BucketStart,
    arrayJoin(mapKeys(SpanAttributes)) AS TagName,
    toUInt64(count()) AS Count
FROM {{ .TracesTable }}
GROUP BY BucketStart, TagName;

INSERT INTO {{ .SpanTagNamesTable }}
SELECT
    toStartOfInterval(Timestamp, INTERVAL 5 MINUTE) AS BucketStart,
    arrayJoin(mapKeys(SpanAttributes)) AS TagName,
    toUInt64(count()) AS Count
FROM {{ .TracesTable }}
WHERE NOT EXISTS (SELECT 1 FROM {{ .SpanTagNamesTable }} LIMIT 1)
GROUP BY BucketStart, TagName;

CREATE TABLE IF NOT EXISTS {{ .ResourceTagNamesTable }}
(
    `BucketStart` DateTime64(9),
    `TagName` LowCardinality(String),
    `Count` SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toDate(BucketStart)
ORDER BY (BucketStart, TagName)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS {{ .ResourceTagNamesView }}
TO {{ .ResourceTagNamesTable }}
AS SELECT
    toStartOfInterval(Timestamp, INTERVAL 5 MINUTE) AS BucketStart,
    arrayJoin(mapKeys(ResourceAttributes)) AS TagName,
    toUInt64(count()) AS Count
FROM {{ .TracesTable }}
GROUP BY BucketStart, TagName;

INSERT INTO {{ .ResourceTagNamesTable }}
SELECT
    toStartOfInterval(Timestamp, INTERVAL 5 MINUTE) AS BucketStart,
    arrayJoin(mapKeys(ResourceAttributes)) AS TagName,
    toUInt64(count()) AS Count
FROM {{ .TracesTable }}
WHERE NOT EXISTS (SELECT 1 FROM {{ .ResourceTagNamesTable }} LIMIT 1)
GROUP BY BucketStart, TagName;

CREATE TABLE IF NOT EXISTS {{ .ServiceNamesTable }}
(
    `BucketStart` DateTime64(9),
    `ServiceName` LowCardinality(String),
    `SpanCount` SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toDate(BucketStart)
ORDER BY (BucketStart, ServiceName)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS {{ .ServiceNamesView }}
TO {{ .ServiceNamesTable }}
AS SELECT
    toStartOfInterval(Timestamp, INTERVAL 5 MINUTE) AS BucketStart,
    ServiceName,
    toUInt64(count()) AS SpanCount
FROM {{ .TracesTable }}
GROUP BY BucketStart, ServiceName;

INSERT INTO {{ .ServiceNamesTable }}
SELECT
    toStartOfInterval(Timestamp, INTERVAL 5 MINUTE) AS BucketStart,
    ServiceName,
    toUInt64(count()) AS SpanCount
FROM {{ .TracesTable }}
WHERE NOT EXISTS (SELECT 1 FROM {{ .ServiceNamesTable }} LIMIT 1)
GROUP BY BucketStart, ServiceName;
