# Migration Guide

This document tracks breaking and notable changes between Chronicle versions.
Use it when upgrading to ensure a smooth transition.

## Migrating to v0.1.0

### New Context-Aware API (Recommended)

Chronicle now provides context-aware variants of all core write and query methods:

| Old Method | New Method | Notes |
|------------|-----------|-------|
| `db.Write(point)` | `db.WriteContext(ctx, point)` | Checks context before processing |
| `db.WriteBatch(points)` | `db.WriteBatchContext(ctx, points)` | Checks context before validation, cardinality, and flush |
| `db.Execute(query)` | `db.ExecuteContext(ctx, query)` | Checks context during partition iteration |

The old methods still work — they call the context variants with `context.Background()`.
No action required, but production code should migrate to the context variants for
proper timeout and cancellation support.

### `interface{}` → `any` Migration

All exported types now use `any` instead of `interface{}`. If you have code that
references Chronicle types using `interface{}`, update to `any`:

```go
// Before
var metadata interface{} = db.SomeMethod()

// After
var metadata any = db.SomeMethod()
```

### CORS Configuration Change

The CORS middleware in `QueryConsole` and `GrafanaBackend` no longer uses a wildcard
(`*`) origin. You must now explicitly configure allowed origins:

```go
// Before (implicit wildcard)
cfg := chronicle.DefaultQueryConsoleConfig()
cfg.EnableCORS = true

// After (explicit origins required)
cfg := chronicle.DefaultQueryConsoleConfig()
cfg.EnableCORS = true
cfg.AllowedOrigins = []string{"http://localhost:3000", "https://grafana.example.com"}
```

If no `AllowedOrigins` are configured, CORS headers are not set (same-origin only).

### FeatureManager Lazy Initialization

Non-core features in `FeatureManager` are now lazily initialized via `sync.Once`.
This is a transparent change — the first call to a feature getter (e.g.,
`fm.CQLEngine()`) initializes the feature. No code changes needed unless you
were relying on side effects during `NewFeatureManager()`.

### Deprecated APIs

The following APIs are deprecated and will be removed in a future major version:

| Deprecated | Replacement |
|------------|-------------|
| `MaterializedViewEngine` (V1) | `MaterializedViewV2Engine` |
| `StreamDSL` (V1) | `StreamDSLV2Engine` |
| `AdaptiveCompressionEngine` (V1) | `BanditCompressor` (V3) |
| `AdaptiveCompressionV2` (V2) | `BanditCompressor` (V3) |

### X-Forwarded-For Trust Change

`getClientIP()` now only trusts `X-Forwarded-For` and `X-Real-IP` headers from
loopback addresses (`127.0.0.1`, `::1`). If your setup uses a non-loopback reverse
proxy, the rate limiter will see the proxy's IP instead of the client's IP.

---

## Version Compatibility Matrix

| Chronicle | Go Version | Status |
|-----------|-----------|--------|
| v0.1.0 | 1.24+ | Current |

## Getting Help

- [GitHub Discussions](https://github.com/chronicle-db/chronicle/discussions) for questions
- [FAQ](FAQ.md) for common issues
- [CHANGELOG](../CHANGELOG.md) for full release notes
