# Frequently Asked Questions

Common gotchas and debugging tips for Chronicle developers.

## Queries return empty results after Write()

**Problem:** You call `db.Write()` followed by `db.Execute()`, but get zero points back.

**Cause:** `Write()` adds data to an in-memory buffer. The buffer is only flushed to storage on partition rotation or explicit flush. `Execute()` queries persisted storage, not the buffer.

**Fix:** Call `db.Flush()` before querying:

```go
db.Write(point)
db.Flush()             // persist buffered writes
result, _ := db.Execute(&query)
```

In production, partitions rotate automatically (based on `PartitionDuration`), so this primarily affects tests and short-lived scripts.

## Queries miss data near time range boundaries

**Problem:** You query with `Start: t0` but data written at `t0` is not returned.

**Cause:** `BTree.Range(start, end)` only returns partitions whose start time is `>= start`. If your data lives in a partition that started *before* your query start time, it may be excluded.

**Fix:** Widen your query range slightly, or use `Start: 0` for unbounded queries:

```go
result, _ := db.Execute(&chronicle.Query{
    Metric: "cpu",
    Start:  t0 - int64(time.Hour),  // include the prior partition
    End:    t1,
})
```

## Test ID collisions in NotebookEngine

**Problem:** Tests that create multiple notebooks in rapid succession get duplicate IDs.

**Cause:** `NotebookEngine.CreateNotebook` generates IDs using `time.Now().UnixNano()`. In fast loops, multiple calls resolve to the same nanosecond.

**Fix:** Provide explicit IDs in tests:

```go
nb := &chronicle.Notebook{ID: "test-nb-1", Name: "Test"}
engine.CreateNotebook(nb)
```

## DefaultConfig does not set retention

**Problem:** Data accumulates without bounds when using `DefaultConfig()`.

**Cause:** `DefaultConfig()` sets storage parameters but leaves `RetentionDuration` at zero (unlimited).

**Fix:** Set retention explicitly:

```go
cfg := chronicle.DefaultConfig("data.db")
cfg.Retention.RetentionDuration = 7 * 24 * time.Hour
```

Or use `ConfigBuilder`:

```go
cfg, _ := chronicle.NewConfigBuilder("data.db").
    WithRetention(7 * 24 * time.Hour).
    Build()
```

## Tests are slow — how do I iterate faster?

Use the tiered test targets:

| Command | Scope | Time |
|---------|-------|------|
| `make test-fast` | `./internal/...` only | ~15s |
| `make check` | `go vet` + internal tests | ~15s |
| `make quickcheck` | `go vet` + all short tests | ~30s |
| `make test-short` | All tests, skip slow ones | ~25s |
| `make test` | Full test suite with race | ~60s |

For a single test: `go test -run TestMyFeature -count=1 -v .`

## How do I know if an API is stable?

Check [`api_stability.go`](../api_stability.go) for the full classification. The README marks features with stability tiers:

- ✅ **Stable** — safe for production, covered by semver
- ⚠️ **Beta** — may change between minor versions
- 🧪 **Experimental** — may change or be removed without notice

Core types (`DB`, `Point`, `Query`, `Config`, `Open()`, `Write()`, `Execute()`) are Stable.

## What timestamp unit does Chronicle use?

`Point.Timestamp`, `Query.Start`, and `Query.End` use Unix nanoseconds. Passing
Unix seconds or milliseconds makes points appear far outside the expected query
range.

Use `time.Time.UnixNano()` consistently:

```go
now := time.Now()
point.Timestamp = now.UnixNano()

query := &chronicle.Query{
    Metric: "cpu",
    Start:  now.Add(-time.Hour).UnixNano(),
    End:    now.UnixNano(),
}
```

`Query.Start` is inclusive, `Query.End` is exclusive, and a zero value leaves
that side of the range unbounded.

## Why is the HTTP API not listening on port 8086?

The HTTP server is disabled by default. Enable it before calling `Open`:

```go
cfg := chronicle.DefaultConfig("data.db")
cfg.HTTP.HTTPEnabled = true
cfg.HTTP.HTTPPort = 8086

db, err := chronicle.Open(cfg.Path, cfg)
```

If the port is already in use, choose another value between 0 and 65535. See
the [HTTP API configuration](CONFIGURATION.md#http-api) for the available
endpoints and related options.

## How do I change the query timeout?

Queries run through `Execute` have a 30-second timeout by default. Set
`Query.QueryTimeout` before opening the database:

```go
cfg := chronicle.DefaultConfig("data.db")
cfg.Query.QueryTimeout = time.Minute
```

Use `ExecuteContext` when a caller needs its own cancellation or deadline. A
timeout can be detected with `errors.Is(err, chronicle.ErrQueryTimeout)`; a
caller-canceled query matches `chronicle.ErrQueryCanceled`.

## Why do operations fail with "database is closed"?

`Write`, `WriteBatch`, `Execute`, `Flush`, `DeleteMetric`, and `Compact` reject
work after `Close` and return `chronicle.ErrClosed`. Keep the database alive for
as long as its workers need it, and call `Close` only during shutdown:

```go
db, err := chronicle.Open("data.db", chronicle.DefaultConfig("data.db"))
if err != nil {
    return err
}
defer db.Close()
```

Use `errors.Is(err, chronicle.ErrClosed)` instead of comparing error strings.

## Why has expired data not disappeared yet?

Retention cleanup runs in the background; it is not guaranteed to happen at
the instant a point crosses the retention boundary. The default compaction
interval is 30 minutes.

Set both the retention duration and an appropriate compaction interval:

```go
cfg.Retention.RetentionDuration = 7 * 24 * time.Hour
cfg.Retention.CompactionInterval = 10 * time.Minute
```

The retention duration must be at least as long as the partition duration, and
the compaction interval should not exceed the partition duration. `Validate`
reports invalid combinations before the database is opened.

## How should I choose a partition duration?

Use shorter partitions for high write rates and longer partitions for sparse
data. As a starting point:

- 15-30 minutes for more than 1,000 points per second
- 1 hour for 10-1,000 points per second (the default)
- 6-24 hours for fewer than 10 points per second

Smaller partitions reduce the amount of data touched by narrow queries but
create more partition metadata. Always keep `Retention.RetentionDuration`
greater than or equal to `Storage.PartitionDuration`.

## How can I catch Chronicle errors reliably?

Use `errors.Is` with Chronicle's exported sentinel errors rather than matching
message text. Typed errors retain the underlying category through wrapping:

```go
result, err := db.Execute(query)
switch {
case errors.Is(err, chronicle.ErrQueryTimeout):
    // Narrow the range or increase Query.QueryTimeout.
case errors.Is(err, chronicle.ErrMemoryBudgetExceeded):
    // Reduce the query scope or increase the configured budget.
case errors.Is(err, chronicle.ErrInvalidQuery):
    // Correct the metric, range, filters, or aggregation.
}
_ = result
```

Storage, WAL, schema, cardinality, disabled-feature, and closed-database errors
also have exported sentinel values in `errors.go`.

## Why does configuration validation return several errors at once?

`Config.Validate` uses `errors.Join` so it can report every invalid field in one
pass. Fix all listed problems before opening the database:

```go
cfg := chronicle.DefaultConfig("data.db")
cfg.Storage.BufferSize = -1
cfg.HTTP.HTTPPort = 99999

if err := cfg.Validate(); err != nil {
    log.Printf("invalid Chronicle configuration: %v", err)
}
```

Validation also checks cross-field rules, such as retention versus partition
duration and the encryption key required when encryption is enabled.

## Why do I see warnings about deprecated configuration fields?

Older releases exposed flat fields such as `MaxMemory`, `QueryTimeout`, and
`HTTPEnabled`. They still work, but Chronicle normalizes them into grouped
configuration and emits a warning.

Prefer the grouped fields in new code:

```go
cfg.Storage.MaxMemory = 128 * 1024 * 1024
cfg.Query.QueryTimeout = 45 * time.Second
cfg.HTTP.HTTPEnabled = true
```

The [legacy-field table](CONFIGURATION.md#legacy-fields) lists every old field
and its replacement.

## What durability does the write-ahead log provide?

Chronicle records writes in a WAL and replays it when the database is reopened
after an unclean shutdown. By default, the WAL syncs once per second, rotates at
128 MB, and retains three old WAL files.

Lowering `WAL.SyncInterval` reduces the amount of recent data exposed to a
process or machine failure, at the cost of more frequent disk synchronization:

```go
cfg.WAL.SyncInterval = 100 * time.Millisecond
cfg.WAL.WALMaxSize = 64 * 1024 * 1024
cfg.WAL.WALRetain = 5
```

Call `Close` during normal shutdown so buffered data and WAL state are flushed
cleanly.

## Build or setup problems?

See the **[Troubleshooting Guide](TROUBLESHOOTING.md)** for solutions to CGO errors, Apple Silicon issues, golangci-lint mismatches, and more.
