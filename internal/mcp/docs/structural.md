# TraceQL Structural Operators

Structural operators match spans based on their position in the trace tree (parent-child, ancestor-descendant, sibling relationships).

## Child (>)

Matches a parent span with a direct child span:

```
{ resource.service.name = "api-gateway" } > { resource.service.name = "auth-service" }
```

This finds traces where `api-gateway` has a direct child span in `auth-service`.

## Parent (<)

Matches a child span with a direct parent span:

```
{ resource.service.name = "auth-service" } < { resource.service.name = "api-gateway" }
```

## Descendant (>>)

Matches an ancestor span with any descendant span (not just direct children):

```
{ resource.service.name = "api-gateway" } >> { status = error }
```

This finds traces where any descendant of an `api-gateway` span has an error.

## Ancestor (<<)

Matches a descendant span with any ancestor span:

```
{ status = error } << { resource.service.name = "api-gateway" }
```

## Sibling (~)

Matches spans that share the same parent:

```
{ span.http.method = "GET" } ~ { span.http.method = "POST" }
```

## Negated Operators

Negated structural operators match when the relationship does NOT exist:

| Operator | Description | Example |
|----------|-------------|---------|
| `!>` | Not child | `{ resource.service.name = "api" } !> { status = error }` |
| `!<` | Not parent | `{ status = error } !< { resource.service.name = "api" }` |
| `!>>` | Not descendant | `{ resource.service.name = "api" } !>> { status = error }` |
| `!<<` | Not ancestor | `{ status = error } !<< { resource.service.name = "api" }` |
| `!~` | Not sibling | `{ span.http.method = "GET" } !~ { span.http.method = "POST" }` |

## Union Operators

Union structural operators match spans from either side of the relationship:

| Operator | Description |
|----------|-------------|
| `&>` | Child union |
| `&<` | Parent union |
| `&>>` | Descendant union |
| `&<<` | Ancestor union |
| `&~` | Sibling union |

## Common Patterns

Find slow database calls under API requests:
```
{ resource.service.name = "api" && name =~ "GET.*" } >> { name =~ "db\\.query.*" && duration > 500ms }
```

Find error propagation across services:
```
{ resource.service.name = "frontend" && status = error } >> { resource.service.name = "backend" && status = error }
```

Find traces where a service does NOT call another service:
```
{ resource.service.name = "api-gateway" } !> { resource.service.name = "auth-service" }
```
