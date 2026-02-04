# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial open source release
- Single-file storage with append-only partitions
- Gorilla float compression for efficient storage
- Delta timestamp encoding
- Dictionary tag compression
- SQL-like query parser (limited subset)
- Time-based and size-based retention policies
- Downsampling background workers
- WAL-based crash recovery with rotation
- Optional HTTP API with Influx line protocol support
- Optional Prometheus remote write ingestion
- Continuous queries (materialized views)
- Outbound replication to central endpoint
- Comprehensive documentation and examples
- CI/CD pipeline with GitHub Actions

#### Phase 2 Next-Gen Features (v0.5.0)
- **Chronicle Query Language (CQL)**: Purpose-built query language with WINDOW, GAP_FILL, ALIGN, ASOF JOIN
- **Streaming ETL Pipelines**: Declarative fluent-API stream processing with backpressure and checkpointing
- **Adaptive Tiered Storage**: Cost-aware automatic data migration across Hot/Warm/Cold/Archive tiers
- **Distributed Consensus Hardening**: Log compaction, snapshot transfer, joint consensus, leader transfer
- **Hybrid Vector+Temporal Index**: HNSW approximate nearest-neighbor search combined with temporal partitioning
- **Production Observability Suite**: Self-monitoring metrics, health checks, and HTTP status endpoints
- **Incremental Materialized Views**: Delta-apply materialized query results with shadow verification
- **OpenAPI Specification & SDK Generation**: Auto-generated OpenAPI 3.0 spec with Python/TypeScript SDK templates
- **Chaos Engineering Framework**: Fault injection, network/disk simulation, declarative test scenarios
- **Offline-First CRDT Sync**: Vector clocks, bloom filters, G-counters, LWW-registers, OR-sets for edge sync

#### Next-Gen Features (v0.2.0)
- **Pluggable Storage Backends**: FileBackend, MemoryBackend, S3Backend (placeholder), TieredBackend
- **Grafana Data Source Plugin**: TypeScript scaffold for visualization integration
- **PromQL Subset Support**: Prometheus-compatible query language with `/api/v1/query` and `/api/v1/query_range` endpoints
- **WebAssembly Compilation**: Browser/edge runtime support via `wasm/` package
- **Encryption at Rest**: AES-256-GCM encryption with PBKDF2 key derivation
- **Schema Registry**: Metric validation with required tags, allowed values, and value ranges
- **Alerting Engine**: Threshold-based alerts with webhook notifications
- **OpenTelemetry Receiver**: OTLP JSON ingestion via `/v1/metrics` endpoint
- **Multi-Tenancy**: Namespace isolation with tag-based tenant separation
- **Streaming API**: WebSocket real-time subscriptions via `/stream` endpoint

#### Analytics & Advanced Features (v0.3.0)
- **Time-Series Forecasting**: Holt-Winters triple exponential smoothing, anomaly detection via `/api/v1/forecast`
- **Recording Rules Engine**: Pre-compute expensive queries on schedules
- **Native Histograms**: Prometheus-compatible exponential bucketing histograms via `/api/v1/histogram`
- **Exemplar Support**: Link metrics to distributed traces
- **Cardinality Management**: Track and limit series cardinality with alerts
- **Delta/Incremental Backup**: Full and incremental backups with compression and retention
- **GraphQL API**: Flexible querying via `/graphql` with playground at `/graphql/playground`
- **Admin UI Dashboard**: Web-based administration at `/admin` with query explorer
- **Query Federation**: Query across multiple Chronicle instances

#### ML & Export Features (v0.4.0)
- **Data Export**: Export to CSV, JSON lines, and Parquet-like columnar format with compression
- **Vector Embeddings**: Store and search ML embeddings with k-NN (cosine, euclidean, dot product)
- **Query Assistant**: Natural language to SQL/PromQL translation with caching
- **Continuous Profiling**: Store pprof profiles and correlate with metric spikes

### Security
- HTTP body size limits (10MB max)
- Request validation and timeout handling
- No unsafe package usage
- AES-256-GCM encryption at rest (opt-in)

## [0.1.0] - TBD

Initial public release.

---

## Release Notes Format

For each release, document:

### Added
New features and capabilities.

### Changed
Changes in existing functionality.

### Deprecated
Features that will be removed in future versions.

### Removed
Features that have been removed.

### Fixed
Bug fixes.

### Security
Security-related changes and vulnerability fixes.
