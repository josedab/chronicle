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

## What timestamp unit should I use?

Use nanosecond Unix timestamps for `chronicle.Point.Timestamp`, query `Start`
and `End` values, and most Chronicle JSON responses. In Go, the easiest source
is `time.Now().UnixNano()`.

Prometheus-compatible endpoints follow Prometheus conventions and accept Unix
timestamps in seconds for parameters such as `time`, `start`, and `end`.

## How do I enable the HTTP API?

The embedded Go API works without HTTP. To expose HTTP endpoints, enable HTTP in
the config:

```go
cfg, _ := chronicle.NewConfigBuilder("metrics.db").
    WithHTTP(8086).
    Build()
```

Or set the structured fields directly:

```go
cfg := chronicle.DefaultConfig("metrics.db")
cfg.HTTP.HTTPEnabled = true
cfg.HTTP.HTTPPort = 8086
```

## Which write formats does Chronicle accept?

Use the embedded Go API for in-process writes:

```go
db.Write(chronicle.Point{Metric: "cpu", Value: 0.72, Timestamp: time.Now().UnixNano()})
```

When HTTP is enabled, Chronicle also accepts:

| Endpoint | Format |
|----------|--------|
| `/write` | InfluxDB line protocol |
| `/v1/metrics` | OpenTelemetry OTLP JSON |
| `/prometheus/write` | Prometheus remote write protobuf |

## Why does Prometheus remote write fail?

Check three things first:

1. HTTP is enabled.
2. Prometheus remote write is enabled in the HTTP config.
3. The request uses `POST /prometheus/write` with
   `Content-Type: application/x-protobuf` and `Content-Encoding: snappy`.

For regular PromQL reads, use `/api/v1/query` or `/api/v1/query_range` instead.

## When should I use CQL instead of the Query struct?

Use the Go `Query` struct for simple embedded reads, especially when your
application already builds filters programmatically.

Use CQL when you want a SQL-like interface, time-series extensions, query
validation, or explain plans through the HTTP API. CQL is a better fit for tools
that accept user-entered queries.

## How should I choose a partition duration?

Partition duration controls how much time each storage partition covers.

| Workload | Suggested partition duration |
|----------|------------------------------|
| High-frequency data, over 1000 points/sec | 15-30 minutes |
| Medium-frequency data, 10-1000 points/sec | 1 hour |
| Low-frequency data, under 10 points/sec | 6-24 hours |

Shorter partitions can make high-frequency retention and compaction more
manageable. Longer partitions reduce metadata overhead for sparse metrics.

## How do I keep disk usage bounded?

Set explicit retention and storage limits instead of relying on unlimited
defaults:

```go
cfg := chronicle.DefaultConfig("metrics.db")
cfg.Retention.RetentionDuration = 30 * 24 * time.Hour
cfg.Storage.MaxStorageBytes = 10 * 1024 * 1024 * 1024 // 10GB
```

For long-lived deployments, add downsample rules so older high-resolution data
is compacted into coarser windows.

## How do I trade write latency for durability?

Tune the WAL sync interval. A lower interval syncs to disk more often and
improves durability, but can increase write latency:

```go
cfg := chronicle.DefaultConfig("metrics.db")
cfg.WAL.SyncInterval = 100 * time.Millisecond
```

The default is a practical starting point for most workloads.

## How do I list labels for Grafana or Prometheus-style tooling?

Use the Prometheus-compatible label endpoint:

```bash
curl http://localhost:8086/api/v1/prom/labels
```

Use `/api/v1/query` and `/api/v1/query_range` for PromQL-compatible instant and
range queries.

## How can I inspect the HTTP API interactively?

When the HTTP API is enabled, Chronicle exposes:

| Endpoint | Purpose |
|----------|---------|
| `/openapi.json` | OpenAPI 3.0 specification |
| `/swagger` | Interactive Swagger UI |

These are useful for generating client SDKs, importing the API into Postman, or
checking request and response shapes while integrating.

## Is Chronicle suitable for edge devices?

Yes, Chronicle is designed for constrained and edge environments. Start with a
small memory budget, explicit retention, and partitions sized for your ingest
rate:

```go
cfg, _ := chronicle.NewConfigBuilder("edge.db").
    WithMaxMemory(32 * 1024 * 1024).
    WithRetention(7 * 24 * time.Hour).
    Build()
```

For very small devices, prefer shorter retention windows and downsampling over
unbounded raw data.

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

## Build or setup problems?

See the **[Troubleshooting Guide](TROUBLESHOOTING.md)** for solutions to CGO errors, Apple Silicon issues, golangci-lint mismatches, and more.
