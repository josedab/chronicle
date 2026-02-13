# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-02-22

### Added
- **Write Pipeline Integration**: PointValidator, WriteHooks, and AuditLog wired into core Write() path
- **Query Middleware Pipeline**: Composable middleware chain wired into core Execute() path
- **Health Check System**: /health, /health/ready, /health/live endpoints for K8s probes
- **19 features promoted to Beta**: including TagIndex, WritePipeline, PointValidator, AuditLog, ResultCache, WALSnapshot, IncrementalBackup, and more
- **Feature Flags**: Runtime enable/disable of experimental features with 3 policies
- **Self-Instrumentation**: 12 OTel-compatible self-monitoring metrics
- **API Deprecation Lifecycle**: Structured deprecation with migration guides and removal timelines
- **Data Masking**: Field-level masking rules (redact/hash/truncate) at query time
- **Tenant Isolation**: Per-tenant memory budgets, query quotas, and storage limits
- **Migration Tool**: Import from InfluxDB line protocol and CSV formats
- **Chaos Recovery Testing**: 5 built-in chaos scenarios with automated recovery validation
- **WAL Snapshots**: Snapshot-based WAL compaction for faster recovery
- **Prophet-style Forecasting v2**: Seasonal decomposition with changepoint detection
- **Wire Protocol Server**: TCP-based binary protocol for non-Go clients
- **Embedded Prometheus Scraper**: Built-in /metrics endpoint scraping
- **Query Plan Visualization**: Execution plan trees with DOT/text output
- **Data Rehydration Pipeline**: Auto-fetch cold data from S3 with LRU cache
- **Comprehensive Benchmark Suite**: 8 end-to-end benchmark functions with real measurements
- **Property-Based Testing**: 6 QuickCheck-style tests for core correctness invariants
- **Fuzzing Harness**: 4 fuzz targets for parser, InfluxDB protocol, point validation, WAL
- **Contributor Welcome Bot**: Automated welcome for first-time contributors
- **5 new Architecture Decision Records**: FeatureManager, Write Pipeline, API Tiers, Query Languages, Query Middleware
- **9 new documentation pages**: benchmarks, demo, launch post, compliance, edge deployment, hardware benchmarks, bounty program, community setup, OpenAPI

### Changed
- Duplicate HTTP route registrations fixed (was causing panic on TestHTTPWriteJSON)
- Grafana backend routes namespaced to /api/v1/grafana/*
- ClickHouse routes namespaced to /api/v1/clickhouse/*
- Health check engine routes moved to /api/v1/health/* (admin routes handle /health)
- BENCHMARKS.md updated with real measured numbers
- FEATURE_MATURITY.md updated with 19 promoted beta features
- APIVersion bumped from "0.1.0" to "0.2.0"
- Multi-arch Dockerfile (amd64/arm64/armv7)
- Security audit CI workflow added
- Build tags on 5 stub files (ebpf, zk_query, tinyml, deno_runtime, jupyter_kernel)
- Dead binary removed from examples/core-only/

### Fixed
- Duplicate HTTP route panic: /api/v1/cluster/stats, /api/v1/audit/*, /api/v1/plugins, /health
- QueryMiddleware infinite recursion (now calls ExecuteContext directly)
- HealthCheck.IsLive() returns false before Start() (HTTP handlers now call Start)
- .gitignore missing entries for edge-sync, core-only, iot-gateway binaries

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

## [0.1.0] - 2026-02-20

### Added
- Initial public release with core embedded time-series database
- Single-file storage with append-only partitions and Gorilla compression
- SQL-like query parser, PromQL subset, and CQL query language
- Pluggable storage backends (file, memory, S3, tiered)
- WAL-based crash recovery
- Context-aware API: WriteContext, WriteBatchContext, ExecuteContext
- HTTP API with Influx line protocol and Prometheus remote write
- OpenTelemetry OTLP receiver
- Grafana data source plugin scaffold
- Encryption at rest (AES-256-GCM)
- Schema registry, alerting engine, multi-tenancy
- Time-series forecasting and anomaly detection
- Comprehensive documentation site and 6 example projects

### Changed
- Migrated all `interface{}` to `any` for modern Go idioms
- FeatureManager now uses lazy initialization (sync.Once) for non-core features
- Write buffer Drain() uses slice swap instead of copy for better performance
- Query path pre-allocates result slices and skips redundant sort
- WAL batches writes instead of flushing per-write
- CORS middleware now requires explicit allowed origins (no more wildcard)

### Deprecated
- materialized_views.go V1 (use MaterializedViewV2Engine)
- stream_dsl.go V1 (use StreamDSLV2Engine)
- adaptive_compression.go V1 (use BanditCompressor V3)
- adaptive_compression_v2.go V2 (use BanditCompressor V3)

### Security
- Fixed CORS wildcard origin in query console and Grafana backend
- Added SLSA provenance attestation for releases
- Added SBOM (SPDX) generation for supply chain transparency
- Strengthened golangci-lint with gosec, errorlint, and additional linters
- Added benchmark regression CI for performance monitoring

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
