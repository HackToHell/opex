DROP VIEW IF EXISTS {{ .ServiceNamesView }};
DROP TABLE IF EXISTS {{ .ServiceNamesTable }};
DROP VIEW IF EXISTS {{ .ResourceTagNamesView }};
DROP TABLE IF EXISTS {{ .ResourceTagNamesTable }};
DROP VIEW IF EXISTS {{ .SpanTagNamesView }};
DROP TABLE IF EXISTS {{ .SpanTagNamesTable }};
DROP VIEW IF EXISTS {{ .TraceMetadataView }};
DROP TABLE IF EXISTS {{ .TraceMetadataTable }};
