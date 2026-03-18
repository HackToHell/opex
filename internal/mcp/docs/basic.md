# TraceQL Basic Syntax

## Intrinsics

Intrinsics are built-in span attributes that don't require a scope prefix:

| Intrinsic | Type | Description | Example |
|-----------|------|-------------|---------|
| `duration` | duration | Span duration | `{ duration > 500ms }` |
| `name` | string | Span name | `{ name = "GET /api/users" }` |
| `status` | enum | Span status | `{ status = error }` |
| `kind` | enum | Span kind | `{ kind = server }` |
| `rootServiceName` | string | Root span's service | `{ rootServiceName = "api-gateway" }` |
| `rootName` | string | Root span's name | `{ rootName = "GET /checkout" }` |
| `traceDuration` | duration | Total trace duration | `{ traceDuration > 2s }` |

## Attribute Scopes

- `span.<name>` — Span attributes: `{ span.http.method = "GET" }`
- `resource.<name>` — Resource attributes: `{ resource.service.name = "api" }`
- `.<name>` — Unscoped (searches both): `{ .http.status_code = 200 }`

## Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `=` | Equals | `{ span.http.method = "GET" }` |
| `!=` | Not equals | `{ status != ok }` |
| `>` | Greater than | `{ duration > 1s }` |
| `>=` | Greater or equal | `{ .http.status_code >= 400 }` |
| `<` | Less than | `{ duration < 100ms }` |
| `<=` | Less or equal | `{ .http.status_code <= 299 }` |
| `=~` | Regex match | `{ name =~ "GET.*users" }` |
| `!~` | Regex not match | `{ name !~ "health.*" }` |
| `&&` | Logical AND | `{ .http.method = "GET" && status = error }` |
| `\|\|` | Logical OR | `{ status = error \|\| duration > 5s }` |

## Spanset Operations

- `&&` between spansets: traces containing BOTH span patterns (INTERSECT)
- `\|\|` between spansets: traces containing EITHER span pattern (UNION)
- `\|` pipeline: filter results of previous stage

## Duration Literals

`1ns`, `1us`, `1ms`, `1s`, `1m`, `1h`

## Status Values

`ok`, `error`, `unset`

## Kind Values

`server`, `client`, `producer`, `consumer`, `internal`

## Common Query Patterns

Find all error spans:
```
{ status = error }
```

Find slow spans in a specific service:
```
{ resource.service.name = "api" && duration > 1s }
```

Find traces with errors in multiple services:
```
{ resource.service.name = "frontend" && status = error } && { resource.service.name = "backend" && status = error }
```

Find traces by HTTP method and status:
```
{ span.http.method = "POST" && .http.status_code >= 500 }
```

Find spans matching a name pattern:
```
{ name =~ "GET /api/v[0-9]+/.*" }
```

## Important Notes

- Map values (span and resource attributes) are always strings in ClickHouse
- Numeric comparisons on map values use automatic type coercion (toInt64OrZero, toFloat64OrZero)
- `resource.service.name` maps to a dedicated `ServiceName` column for efficient filtering
- Unscoped attributes (`.name`) search both span and resource attributes
