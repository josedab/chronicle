# Slim Core Build

Chronicle supports a `slim` build that includes only the stable API core.

## Usage

```bash
# Default build: all 126 features
go build ./...

# Slim build: core engine only (~30 files, ~3K LOC)
go build -tags=slim ./...

# Exclude stub/placeholder features
go build -tags=nostubs ./...
```

## Core Files (included in slim build)

These files form the minimal Chronicle engine:

### Storage & Write Path
- `db_core.go` — Database lifecycle (Open, Close)
- `db_write.go` — Write pipeline (Write, WriteBatch)
- `db_open_helpers.go` — Open helpers
- `db_recovery.go` — Crash recovery
- `db_retention.go` — Background retention
- `db_metrics.go` — Metrics listing

### Query Path
- `query.go` — Query execution (Execute, ExecuteContext)
- `parser.go` — SQL-like query parser
- `aggregation.go` — Aggregation functions

### Storage Engine
- `wal.go` — Write-ahead log
- `buffer.go` — Write buffer
- `index.go` — B-tree partition index
- `btree.go` — B-tree implementation
- `partition_core.go` — Partition data structure
- `partition_codec.go` — Partition encoding/decoding
- `partition_query.go` — Partition query execution
- `partitioning.go` — Partition management
- `storage.go` — Storage abstraction
- `storage_engine.go` — Backend interface
- `storage_backend_file.go` — File backend
- `storage_backend_memory.go` — Memory backend

### Types & Config
- `point.go` — Point, Result, SeriesKey types
- `tags.go` — Tag manipulation
- `config.go` — Configuration
- `config_builder.go` — Fluent config builder
- `schema.go` — Schema validation
- `errors.go` — Error types
- `doc.go` — Package documentation

### API Stability
- `api_stability.go` — Stability tier definitions

## Stable API (25 symbols)

```go
// Types
chronicle.DB
chronicle.Point
chronicle.Query
chronicle.Aggregation
chronicle.AggFunc
chronicle.Result
chronicle.Config
chronicle.StorageConfig
chronicle.WALConfig
chronicle.RetentionConfig
chronicle.QueryConfig
chronicle.HTTPConfig
chronicle.TagFilter

// Functions
chronicle.Open(path, config) → (*DB, error)
chronicle.DefaultConfig(path) → Config

// Methods
db.Close() → error
db.Write(Point) → error
db.WriteBatch([]Point) → error
db.Execute(*Query) → (*Result, error)
db.Metrics() → []string

// Constants
chronicle.AggCount, AggSum, AggMean, AggMin, AggMax
```

## What's Excluded in Slim Build

- All 126 feature engines (FeatureManager returns nil for everything)
- HTTP server and all route handlers
- PromQL, CQL, GraphQL parsers
- S3, Tiered storage backends
- All experimental features
- Grafana, OTLP, ClickHouse integrations

## Binary Size Comparison

| Build | Approximate Size |
|-------|-----------------|
| Full (all features) | ~50MB |
| Slim (core only) | ~8MB |
| Slim + nostubs | ~7MB |

*Note: The slim build tag infrastructure is in preparation. Currently,
`-tags=nostubs` excludes 5 stub files. Full slim build support is
tracked in the module split plan (docs/MODULE_SPLIT.md).*
