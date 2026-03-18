# TraceQL Aggregate Functions

## Pipeline Aggregates

Aggregate functions operate on spansets produced by filters and return scalar values.

### count()

Count the number of matching spans in each spanset.

```
{ resource.service.name = "api" } | count() > 5
```

### sum(attribute)

Sum a numeric attribute across spans in each spanset.

```
{ resource.service.name = "api" } | sum(duration) > 10s
```

### avg(attribute)

Average a numeric attribute across spans in each spanset.

```
{ resource.service.name = "api" } | avg(duration) > 500ms
```

### min(attribute)

Minimum value of an attribute across spans in each spanset.

```
{ resource.service.name = "api" } | min(duration) < 10ms
```

### max(attribute)

Maximum value of an attribute across spans in each spanset.

```
{ resource.service.name = "api" } | max(duration) > 5s
```

## by() Grouping

The `by()` clause groups results by one or more attributes:

```
{ } | count() by (resource.service.name)
{ } | avg(duration) by (resource.service.name, span.http.method)
```

## coalesce()

`coalesce()` merges all spans from each trace into a single spanset:

```
{ resource.service.name = "frontend" } | coalesce() | count() > 10
```

## select()

`select()` picks specific attributes to include in the result:

```
{ status = error } | select(resource.service.name, name, duration)
```

## Pipeline Chaining

Multiple pipeline stages can be chained:

```
{ resource.service.name = "api" } | count() > 3 | avg(duration) > 1s
```

## Important Notes

- Aggregates operate on spansets, not individual spans
- The `by()` clause creates separate groups, each evaluated independently
- `count()` takes no arguments; `sum()`, `avg()`, `min()`, `max()` require a numeric attribute
- Duration attributes (e.g., `duration`) can be compared with duration literals (`500ms`, `1s`)
