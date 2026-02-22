# ADR 0017: Wired Query Middleware

## Status
Accepted

## Context
Query execution needed extensibility for logging, cost checking, caching, and
authentication without modifying the core Execute() path. Adding if-statements
for each concern would make the query path unmaintainable.

## Decision
We integrated a QueryMiddleware chain into the core Execute() method in query.go.
When middlewares are registered, Execute() routes through them before reaching
ExecuteContext(). The middleware chain uses a next-function pattern:

```go
type QueryMiddlewareFunc = func(q *Query, next func(*Query) (*Result, error)) (*Result, error)
```

The final step in the chain calls ExecuteContext() directly (not Execute(),
to avoid infinite recursion).

Middleware examples:
- Query logging (record all queries)
- Cost estimation (reject expensive queries)
- Result caching (return cached results)
- Authentication (check query permissions)

## Consequences
- **Positive**: Clean separation of concerns; composable; zero cost when no middleware registered
- **Negative**: Middleware ordering matters; debugging the chain can be complex
- **Mitigation**: Middlewares have priority numbers; MiddlewareCount() check prevents overhead when empty;
  query profiler can trace through middleware execution
