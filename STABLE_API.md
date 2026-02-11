# Stable API Reference

Chronicle exports 1,400+ types, but **you only need ~20 for 90% of use cases**.
This document lists the stable, production-safe API surface.

## Quick Start (5 symbols)

```go
import "github.com/chronicle-db/chronicle"

db, err := chronicle.Open("data.db", chronicle.DefaultConfig("data.db"))
defer db.Close()

db.Write(chronicle.Point{...})
result, err := db.Execute(&chronicle.Query{...})
```

## Core Types

| Type | Purpose | Import |
|------|---------|--------|
| `DB` | Database handle. All operations go through this. | `chronicle.DB` |
| `Point` | A single data point: metric, value, timestamp, tags | `chronicle.Point` |
| `Query` | Query spec: metric, time range, tags, aggregation | `chronicle.Query` |
| `Result` | Query output containing matched `Points` | `chronicle.Result` |
| `Config` | Database configuration | `chronicle.Config` |
| `Aggregation` | Aggregation parameters (function + window) | `chronicle.Aggregation` |

## Core Functions

| Function | Signature | Description |
|----------|-----------|-------------|
| `Open` | `Open(path string, cfg Config) (*DB, error)` | Create or open a database |
| `DefaultConfig` | `DefaultConfig(path string) Config` | Config with sensible defaults |
| `DB.Close` | `Close() error` | Close the database |
| `DB.Write` | `Write(Point) error` | Write a single point |
| `DB.WriteBatch` | `WriteBatch([]Point) error` | Write multiple points |
| `DB.Execute` | `Execute(*Query) (*Result, error)` | Run a query |
| `DB.Flush` | `Flush() error` | Flush write buffer to storage |
| `DB.Metrics` | `Metrics() []string` | List all metric names |

## Configuration Types

| Type | Purpose |
|------|---------|
| `StorageConfig` | Memory limits, partition duration, buffer size |
| `WALConfig` | Sync interval, max size, retention |
| `RetentionConfig` | Data retention, compaction workers/interval |
| `QueryConfig` | Query timeout |
| `HTTPConfig` | HTTP server enable/port |
| `AuthConfig` | API key authentication |
| `EncryptionConfig` | Encryption at rest |

## Query Helpers

| Symbol | Purpose |
|--------|---------|
| `AggFunc` | Enum: `AggCount`, `AggSum`, `AggMean`, `AggMin`, `AggMax`, `AggStddev`, `AggRate` |
| `TagFilter` | Filter by tag with `TagOpEq`, `TagOpNotEq`, `TagOpIn`, `TagOpRegex`, `TagOpNotRegex` |
| `ConfigBuilder` | Fluent API: `NewConfigBuilder(path).WithMaxMemory(...).Build()` |

## Context-Aware Variants

Every write/query method has a context-aware variant:

| Method | Context Variant |
|--------|----------------|
| `Write(Point)` | `WriteContext(ctx, Point)` |
| `WriteBatch([]Point)` | `WriteBatchContext(ctx, []Point)` |
| `Execute(*Query)` | `ExecuteContext(ctx, *Query)` |

## What NOT to Depend On

Everything not listed above is **Beta** or **Experimental**:

- Feature manager accessors (`db.Features().CQLEngine()`, etc.)
- HTTP route handlers
- Internal types (anything in `internal/`)
- Types marked `// Deprecated:` in their GoDoc
- Types listed as Experimental in `api_stability.go`

See [`FEATURE_MATURITY.md`](FEATURE_MATURITY.md) for the full maturity assessment.
See [`api_stability.go`](api_stability.go) for the complete stability classification.
