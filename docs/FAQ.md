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
