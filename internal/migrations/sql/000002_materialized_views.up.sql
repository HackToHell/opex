CREATE TABLE IF NOT EXISTS {{ .TraceMetadataTable }}
(
    `TraceId` String,
    `RootServiceName` AggregateFunction(argMin, LowCardinality(String), DateTime64(9)),
    `RootSpanName` AggregateFunction(argMin, LowCardinality(String), DateTime64(9)),
    `StartTime` SimpleAggregateFunction(min, DateTime64(9)),
    `EndTime` SimpleAggregateFunction(max, DateTime64(9)),
    `Duration` AggregateFunction(max, UInt64),
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
    max(Timestamp) AS EndTime,
    maxState(Duration) AS Duration,
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
    max(Timestamp) AS EndTime,
    maxState(Duration) AS Duration,
    toUInt64(count()) AS SpanCount,
    toUInt64(countIf(StatusCode = 'STATUS_CODE_ERROR')) AS ErrorCount
FROM {{ .TracesTable }}
WHERE NOT EXISTS (SELECT 1 FROM {{ .TraceMetadataTable }} LIMIT 1)
GROUP BY TraceId;

CREATE TABLE IF NOT EXISTS {{ .SpanTagNamesTable }}
(
    `TagName` LowCardinality(String),
    `Count` SimpleAggregateFunction(sum, UInt64),
    `LastSeen` SimpleAggregateFunction(max, DateTime64(9))
)
ENGINE = AggregatingMergeTree()
ORDER BY (TagName)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS {{ .SpanTagNamesView }}
TO {{ .SpanTagNamesTable }}
AS SELECT
    arrayJoin(mapKeys(SpanAttributes)) AS TagName,
    toUInt64(count()) AS Count,
    max(Timestamp) AS LastSeen
FROM {{ .TracesTable }}
GROUP BY TagName;

INSERT INTO {{ .SpanTagNamesTable }}
SELECT
    arrayJoin(mapKeys(SpanAttributes)) AS TagName,
    toUInt64(count()) AS Count,
    max(Timestamp) AS LastSeen
FROM {{ .TracesTable }}
WHERE NOT EXISTS (SELECT 1 FROM {{ .SpanTagNamesTable }} LIMIT 1)
GROUP BY TagName;

CREATE TABLE IF NOT EXISTS {{ .ResourceTagNamesTable }}
(
    `TagName` LowCardinality(String),
    `Count` SimpleAggregateFunction(sum, UInt64),
    `LastSeen` SimpleAggregateFunction(max, DateTime64(9))
)
ENGINE = AggregatingMergeTree()
ORDER BY (TagName)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS {{ .ResourceTagNamesView }}
TO {{ .ResourceTagNamesTable }}
AS SELECT
    arrayJoin(mapKeys(ResourceAttributes)) AS TagName,
    toUInt64(count()) AS Count,
    max(Timestamp) AS LastSeen
FROM {{ .TracesTable }}
GROUP BY TagName;

INSERT INTO {{ .ResourceTagNamesTable }}
SELECT
    arrayJoin(mapKeys(ResourceAttributes)) AS TagName,
    toUInt64(count()) AS Count,
    max(Timestamp) AS LastSeen
FROM {{ .TracesTable }}
WHERE NOT EXISTS (SELECT 1 FROM {{ .ResourceTagNamesTable }} LIMIT 1)
GROUP BY TagName;

CREATE TABLE IF NOT EXISTS {{ .ServiceNamesTable }}
(
    `ServiceName` LowCardinality(String),
    `SpanCount` SimpleAggregateFunction(sum, UInt64),
    `LastSeen` SimpleAggregateFunction(max, DateTime64(9))
)
ENGINE = AggregatingMergeTree()
ORDER BY (ServiceName)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS {{ .ServiceNamesView }}
TO {{ .ServiceNamesTable }}
AS SELECT
    ServiceName,
    toUInt64(count()) AS SpanCount,
    max(Timestamp) AS LastSeen
FROM {{ .TracesTable }}
GROUP BY ServiceName;

INSERT INTO {{ .ServiceNamesTable }}
SELECT
    ServiceName,
    toUInt64(count()) AS SpanCount,
    max(Timestamp) AS LastSeen
FROM {{ .TracesTable }}
WHERE NOT EXISTS (SELECT 1 FROM {{ .ServiceNamesTable }} LIMIT 1)
GROUP BY ServiceName;
