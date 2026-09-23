# Chronicle Error Reference

This guide documents Chronicle's error types, typical symptoms, HTTP mappings, and troubleshooting steps.
It complements [TROUBLESHOOTING.md](./TROUBLESHOOTING.md) (build/setup) and the [API Error Responses](./API.md#error-responses) section.

Source of truth for sentinel errors and typed wrappers: `errors.go` in the root package.

## HTTP error envelope

HTTP APIs return JSON errors:

```json
{
  "error": "error message description"
}
```

| Status | When it usually appears |
|--------|-------------------------|
| `400 Bad Request` | Malformed query/body, invalid parameters, schema validation failure |
| `404 Not Found` | Unknown metric, tenant, or resource id |
| `408` / client cancel | Query canceled via context (client disconnect / timeout) |
| `500 Internal Server Error` | Unexpected server/storage failure (message may be generic for safety) |

Exact status codes can vary by endpoint; always read the `error` string and match it with `errors.Is` when using the Go library API.

## Sentinel errors (library)

Use `errors.Is(err, chronicle.Err…)` to branch on these:

| Sentinel | Message | Typical cause | What to try |
|----------|---------|---------------|-------------|
| `ErrClosed` | `database is closed` | Call after `Close()`, or using a DB that failed to open | Open a new DB; ensure lifecycle ownership is clear |
| `ErrQueryTimeout` | `query timeout` | Query exceeded configured timeout | Narrow time range, add tag filters, raise timeout carefully, avoid unbounded scans |
| `ErrMemoryBudgetExceeded` | `query memory budget exceeded` | Result/working set larger than memory budget | Reduce cardinality, downsample, tighten `WHERE`, raise budget only if host allows |
| `ErrInvalidQuery` | `invalid query` | Empty metric, bad AST, unsupported clause | Validate CQL/PromQL syntax; check parser error position |
| `ErrQueryCanceled` | `query canceled` | Context canceled/deadline exceeded | Retry with longer deadline; check client disconnects |
| `ErrSchemaValidation` | `schema validation failed` | Point fields/tags violate registered schema | Align write payload with schema; inspect schema registry |
| `ErrCardinalityLimit` | `cardinality limit exceeded` | Too many unique series (tag explosion) | Drop high-cardinality tags, aggregate earlier, raise limit only after review |
| `ErrStorageCorruption` | `storage corruption detected` | On-disk structure failed integrity checks | Stop writers; restore from backup/PITR; run storage checks |
| `ErrWALSync` | `WAL sync failed` | WAL flush/fsync failed (disk full, I/O error) | Free disk; check filesystem; inspect WAL path permissions |
| `ErrStorageRead` | `storage read failed` | Backend read error | Verify path/S3 credentials; check permissions and network |
| `ErrStorageWrite` | `storage write failed` | Backend write error | Same as read; confirm disk quota and backend health |
| `ErrUnsupportedOperation` | `operation not supported` | Feature/backend cannot perform the op | Use a supported backend or alternative API |
| `ErrFeatureDisabled` | `feature is disabled` | Compile/config flag turned the feature off | Enable the feature in config/build; use another path |
| `ErrInvalidMetricName` | `invalid metric name` | Metric name fails naming rules | Fix name (see naming docs); avoid empty/illegal characters |
| `ErrStreamingDisabled` | `streaming not enabled` | Streaming API used while disabled | Enable streaming in config before calling stream endpoints |
| `ErrCircuitOpen` | `circuit breaker is open` | Retry circuit open after repeated failures | Wait for cooldown; fix underlying dependency; backoff |

## Typed errors

### `QueryError`

Carries `Type` (`Timeout` / `Memory` / `Invalid` / `Canceled` / `Unknown`), optional `Query`, and `Cause`.
`QueryError.Is` maps types onto `ErrQueryTimeout`, `ErrMemoryBudgetExceeded`, `ErrInvalidQuery`, and `ErrQueryCanceled`.

**Symptoms:** HTTP `400` for invalid queries; timeouts often surface as canceled/timeout messages; memory errors mention budget.

**Fix pattern:**

1. Log `Type` + `Message` + unwrapped `Cause`.
2. For `Invalid`: fix syntax (see CQL examples in `API.md`).
3. For `Timeout`/`Memory`: shrink scan window and series set.
4. For `Canceled`: inspect caller context deadlines.

### `StorageError`

Carries `Type` (`Read` / `Write` / `Corruption` / `Sync` / `Unknown`), optional `Path`, and `Cause`.
Maps onto `ErrStorageRead`, `ErrStorageWrite`, `ErrStorageCorruption`, and `ErrWALSync`.

**Symptoms:** Writes/queries fail with path in the message (`… [path]: cause`).

**Fix pattern:**

1. Confirm the storage path exists and is writable.
2. For corruption: isolate the file, restore from backup, do not keep writing blindly.
3. For sync failures: check disk space (`df -h`) and mount options.

### `WALSyncError`

Aggregates `FlushErr` and/or `SyncErr`. Matches `ErrWALSync`. Both underlying errors are exposed via `Unwrap() []error`.

### `WriteError`

Wraps a failed write with the metric name: `write failed for metric "…": …`.
Unwrap to inspect schema/cardinality/storage causes.

### `ConfigError`

`invalid config <field>: <message>` — raised during configuration validation before the DB becomes ready.

**Fix:** Correct the named field (timeouts, paths, limits, feature flags) and restart/reopen.

## CQL / query parse errors

CQL parser failures typically return `ErrInvalidQuery` (or a wrapped form) with a position hint, for example:

```json
{
  "error": "unexpected token at position 12"
}
```

**Checklist:**

1. Confirm the endpoint expects CQL vs PromQL.
2. Balance quotes and parentheses; check `WINDOW` / `GAP_FILL` / `ASOF JOIN` clause order.
3. Retry with a minimal `SELECT` against a known metric.
4. Compare against examples in `docs/API.md` (CQL section).

## Operational playbooks

### Disk full / WAL sync failures

1. `df -h` on the data and WAL volumes.
2. Pause high-rate writers if possible.
3. Free space or expand volume; confirm fsync succeeds.
4. Restart only after WAL path is healthy.

### Cardinality explosion

1. Identify tags with unbounded values (user id, request id, raw URL).
2. Drop or bucket those tags at ingest.
3. Revisit `ErrCardinalityLimit` threshold only after the series shape is sane.

### Query timeout / memory budget

1. Add time bounds and selective tag filters.
2. Prefer aggregations/downsampling over raw wide scans.
3. Raise timeout/budget only when the host has headroom.

### Database already closed

1. Ensure a single owner calls `Close()`.
2. Do not share a closed instance across goroutines after shutdown.
3. Open a new instance for post-shutdown work.

## Go library matching examples

```go
import (
    "errors"
    "github.com/josedab/chronicle"
)

err := db.Query(ctx, q)
switch {
case errors.Is(err, chronicle.ErrClosed):
    // reopen or abort cleanly
case errors.Is(err, chronicle.ErrInvalidQuery):
    // fix caller query
case errors.Is(err, chronicle.ErrQueryTimeout), errors.Is(err, chronicle.ErrMemoryBudgetExceeded):
    // shrink the query
case errors.Is(err, chronicle.ErrCardinalityLimit):
    // reduce tag cardinality
default:
    // log and surface
}
```

```go
var qerr *chronicle.QueryError
if errors.As(err, &qerr) {
    // inspect qerr.Type / qerr.Message / qerr.Cause
}
```

## Related docs

- [TROUBLESHOOTING.md](./TROUBLESHOOTING.md) — build, tooling, `make doctor`
- [API.md](./API.md) — HTTP endpoints and status codes
- [CONFIGURATION.md](./CONFIGURATION.md) — timeouts, budgets, feature flags
