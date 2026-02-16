# AGENTS.md — Opex Coding Agent Guidelines

## Project Overview

Opex is a TraceQL-to-ClickHouse SQL transpiler with Tempo-compatible APIs.
Module: `github.com/hacktohell/opex`, Go 1.25.5. All application code lives under `internal/`.

## Build / Test / Lint Commands

```bash
make build                       # Compiles bin/opex with ldflags
go build ./...                   # Quick compile check

make test                        # go test -v -race ./...
go test ./internal/...           # All unit tests (no race, faster)
go test ./internal/transpiler/...                            # Single package
go test ./internal/transpiler/... -run TestTranspileAllKinds # Single test
go test ./internal/transpiler/... -coverprofile=cover.out    # Coverage

go vet ./...                     # Static analysis (run before committing)
go fmt ./...                     # Format
make run-dev                     # Run locally (needs ClickHouse)
```

**Go proxy workaround** (required in this environment):
```bash
GOPROXY=https://proxy.golang.org,direct GONOSUMCHECK='*' GONOSUMDB='*' go get <package>
```

## Code Style

### Imports

Two groups separated by a blank line: stdlib first, then everything else (third-party and internal together).

```go
import (
    "fmt"
    "net/http"

    "github.com/gorilla/mux"
    "github.com/hacktohell/opex/internal/clickhouse"
)
```

### Naming

- **Types**: PascalCase exported (`TranspileResult`, `SpanRow`). Handler groups: `XxxHandlers`.
- **Constructors**: `New` + type — `New(cfg, logger)`, `NewSearchHandlers(ch, cfg, logger)`.
- **Enum constants**: PascalCase with type prefix — `OpEqual`, `TypeInt`, `StatusError`.
- **Unexported**: camelCase — `transpilePipeline`, `parseTimeRange`, `mustTimeFilter`.
- **Local vars**: Short, clear — `cfg`, `ch`, `sql`, `opts`, `tf`, `lhs`, `rhs`.
- **Struct fields**: PascalCase exported, camelCase unexported (`conn`, `cfg`, `logger`).

### Error Handling

Wrap with `fmt.Errorf` using `%w`. Lowercase context, no trailing period:

```go
return nil, fmt.Errorf("parsing ClickHouse DSN: %w", err)
return nil, fmt.Errorf("pipeline stage %d: %w", i, err)
```

In HTTP handlers: log the real error, return a generic message to the client:

```go
h.logger.Error("search query failed", "sql", result.SQL, "error", err)
response.WriteError(w, http.StatusInternalServerError, "query execution failed")
```

For write-to-ResponseWriter errors: `_, _ = w.Write(...)`.

### HTTP Handlers

Each functional area is a handler struct with constructor. Pattern:

1. Parse and validate input → `response.WriteError(w, 400, "message")` on failure
2. Execute business logic (parse TraceQL, transpile, query ClickHouse)
3. Handle errors → log internal details, return generic error
4. Return success → `response.WriteJSON(w, http.StatusOK, &result)`

Always initialize empty slices before JSON encoding (never return `null` arrays):

```go
if traces == nil {
    traces = []response.TraceSearchMetadata{}
}
```

### Logging

Uses `log/slog` from stdlib. Injected via constructor as `*slog.Logger`:

```go
c.logger.Debug("executing query", "query_type", queryType, "sql", truncateSQL(sql, 500))
c.logger.Error("query failed", "error", err, "duration_ms", duration.Milliseconds())
```

Levels: `Debug` for query details, `Info` for lifecycle, `Warn` for degraded states, `Error` for failures.

### Doc Comments

All exported symbols start with `// Name ...`. Packages start with `// Package name ...`.
Large files use `// ---------------------------------------------------------------------------` section dividers.

### Tests

- **No assertion libraries** — use stdlib `t.Errorf` / `t.Fatalf` only.
- **Table-driven tests** with `t.Run`:

```go
tests := []struct {
    name, input, expected string
}{
    {"case name", "input", "expected"},
}
for _, tc := range tests {
    t.Run(tc.name, func(t *testing.T) {
        got := functionUnderTest(tc.input)
        if got != tc.expected {
            t.Errorf("func() = %q, want %q", got, tc.expected)
        }
    })
}
```

- **Test helpers** call `t.Helper()`:

```go
func assertContains(t *testing.T, sql, substr string) {
    t.Helper()
    if !strings.Contains(sql, substr) {
        t.Errorf("expected SQL to contain %q\ngot: %s", substr, sql)
    }
}
```

- **Naming**: `TestFunctionName` or `TestFunctionName_Variant`.

## Project Layout

```
cmd/opex/main.go              Entry point
internal/
  api/                         HTTP handlers (handler.go, search.go, tags.go, trace.go, metrics.go)
  clickhouse/                  DB client (client.go, trace.go)
  config/                      YAML config loading (config.go)
  metrics/                     Prometheus instrumentation (metrics.go)
  response/                    Response types + marshaling (types.go, marshal.go, trace.go)
  server/                      HTTP server + routing (server.go)
  traceql/                     TraceQL parser (ast.go, lexer.go, parser.go)
  transpiler/                  AST-to-SQL (transpiler.go, field.go)
deploy/                        Docker Compose, ClickHouse DDL, Grafana config
docs/                          Phase reference documents
```

## Key Patterns

- **Config**: `DefaultConfig()` returns defaults; `LoadFromFile()` merges YAML on top.
- **Metrics**: Prometheus `promauto` with `opex_` namespace prefix.
- **Router**: `gorilla/mux` with `.Methods(http.MethodGet)`.
- **ClickHouse**: Map values are always strings. Numeric comparisons need `toInt64OrZero()` / `toFloat64OrZero()`. `resource.service.name` maps to the `ServiceName` column.
- **TraceQL enums**: `status`/`kind` are context-dependent — identifiers on LHS (intrinsics), enum values on RHS.
- **Phase plan**: Read `full_implementation_plan.md` and `docs/phaseN_reference.md` before making changes.
