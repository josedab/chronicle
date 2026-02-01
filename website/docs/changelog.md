---
sidebar_position: 104
---

# Changelog

All notable changes to Chronicle are documented here. This project follows [Semantic Versioning](https://semver.org/).

:::tip Stay Updated
Watch the [GitHub repository](https://github.com/chronicle-db/chronicle) for release notifications.
:::

## Unreleased

### Added

#### Core Features
- Single-file storage with append-only partitions
- Gorilla float compression for efficient storage (10-15x compression)
- Delta timestamp encoding
- Dictionary tag compression
- SQL-like query parser (limited subset)
- Time-based and size-based retention policies
- Downsampling background workers
- WAL-based crash recovery with rotation

#### API & Integration
- HTTP API with Influx line protocol support
- Prometheus remote write ingestion
- PromQL subset support with `/api/v1/query` and `/api/v1/query_range` endpoints
- GraphQL API with interactive playground at `/graphql/playground`
- OpenTelemetry receiver for OTLP JSON ingestion
- WebSocket streaming API for real-time subscriptions

#### Storage & Backends
- Pluggable storage backends: File, Memory, S3, Tiered
- WebAssembly compilation for browser/edge runtime
- Encryption at rest with AES-256-GCM

#### Analytics
- Time-series forecasting with Holt-Winters
- Anomaly detection
- Recording rules engine for pre-computed queries
- Native histograms with exponential bucketing
- Continuous queries (materialized views)

#### Operations
- Schema registry for metric validation
- Alerting engine with webhook notifications
- Multi-tenancy with namespace isolation
- Cardinality management with limits and alerts
- Delta/incremental backups
- Query federation across instances
- Admin UI dashboard

#### ML & Advanced
- Vector embeddings with k-NN search
- Query assistant (natural language to SQL/PromQL)
- Continuous profiling with metric correlation
- Data export to CSV, JSON, and Parquet

### Security
- HTTP body size limits (10MB max)
- Request validation and timeout handling
- No `unsafe` package usage in core code
- Path traversal protection

---

## Roadmap

### v0.5.0 (Planned)
- [ ] Raft-based clustering for high availability
- [ ] Native ARM64 optimizations
- [ ] Improved query planner
- [ ] SQL JOIN support for metadata tables

### v1.0.0 (Planned)
- [ ] Stable API guarantee
- [ ] Production hardening
- [ ] Comprehensive security audit
- [ ] Performance benchmarks certification

---

## Versioning Policy

Chronicle follows [Semantic Versioning](https://semver.org/):

- **MAJOR** (1.x.x): Incompatible API changes
- **MINOR** (x.1.x): New features, backward compatible
- **PATCH** (x.x.1): Bug fixes, backward compatible

### Pre-1.0 Notice

Until v1.0.0, minor version bumps may include breaking changes. We recommend pinning to specific versions in production.

```go
// go.mod
require github.com/chronicle-db/chronicle v0.4.0
```

---

## Upgrade Guides

### Upgrading from v0.3.x to v0.4.x

No breaking changes. New features are opt-in.

```go
// New: Query assistant (optional)
assistant := chronicle.NewQueryAssistant(db, chronicle.QueryAssistantConfig{
    Model: "gpt-4",
})

// New: Vector embeddings (optional)
db.WriteVector(chronicle.VectorPoint{
    ID:     "doc-1",
    Vector: []float32{0.1, 0.2, 0.3},
})
```

### Upgrading from v0.2.x to v0.3.x

**Config changes:**
```go
// Old
config := chronicle.Config{
    Compression: true,
}

// New: Compression is now always enabled, option removed
config := chronicle.Config{
    // Compression removed - always on
}
```

### Database Migration

Chronicle automatically handles database file migrations. On first open after upgrade, older files are migrated to the new format.

```go
// Migration happens automatically
db, err := chronicle.Open("data.db", config)
if err != nil {
    // Check for migration errors
    if errors.Is(err, chronicle.ErrMigrationFailed) {
        // Restore from backup
    }
}
```

:::warning Backup Before Upgrade
Always backup your database before upgrading Chronicle versions.
:::

---

## Release Schedule

- **Patch releases**: As needed for bug fixes
- **Minor releases**: Every 4-6 weeks
- **Major releases**: When significant breaking changes are required

---

## Contributing

Found a bug or have a feature request? 

- [Open an issue](https://github.com/chronicle-db/chronicle/issues/new)
- [View all issues](https://github.com/chronicle-db/chronicle/issues)
- [Contributing guide](/docs/contributing)
