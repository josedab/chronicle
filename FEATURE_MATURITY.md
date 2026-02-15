# Feature Maturity

Chronicle is pre-1.0. This document provides an honest assessment of every major feature's production readiness.

## Maturity Levels

| Level | Meaning |
|-------|---------|
| **✅ Production** | Fully implemented, tested, safe for production use |
| **⚠️ Beta** | Mostly functional, may change between minor versions |
| **🧪 Alpha** | Early implementation, API unstable, not for production |
| **📋 Stub** | Type definitions or skeleton only — not functional |

## Core Database

| Feature | Maturity | Notes |
|---------|----------|-------|
| Single-file storage | ✅ Production | Append-only partitions, Gorilla compression |
| WAL crash recovery | ✅ Production | Background sync, rotation, configurable retention |
| B-tree index | ✅ Production | O(log N) partition lookup |
| Write buffer | ✅ Production | Batching with configurable flush |
| Retention policies | ✅ Production | Time-based and size-based |
| Downsampling | ✅ Production | Background workers |
| Configuration & validation | ✅ Production | Cross-field validation, legacy field normalization |

## Query Engine

| Feature | Maturity | Notes |
|---------|----------|-------|
| SQL-like parser | ✅ Production | Aggregations, tag filters, time ranges |
| PromQL subset | ⚠️ Beta | Binary ops, regex matchers, 30+ functions. Missing: full range function execution, subqueries |
| CQL (Chronicle Query Language) | ⚠️ Beta | WINDOW, GAP_FILL, ALIGN, ASOF JOIN |
| GraphQL API | ⚠️ Beta | Interactive playground included |
| Query cost estimation | ✅ Production | Partition count and series estimation |

## Storage Backends

| Feature | Maturity | Notes |
|---------|----------|-------|
| FileBackend | ✅ Production | Default local file storage |
| MemoryBackend | ✅ Production | In-memory for testing |
| S3Backend | ⚠️ Beta | Multipart upload, LRU+TTL cache, retry logic. Not tested with real S3. |
| TieredBackend | ⚠️ Beta | Hot/warm/cold/archive migration |

## Integrations

| Feature | Maturity | Notes |
|---------|----------|-------|
| HTTP API | ✅ Production | Influx line protocol, body limits, rate limiting |
| Prometheus remote write | ✅ Production | Drop-in `/api/v1/prom/write` |
| OpenTelemetry OTLP | ⚠️ Beta | JSON-based receiver. Not protobuf/gRPC. |
| Grafana datasource plugin | ✅ Production | Backend with streaming, annotations, templates |
| WebSocket streaming | ✅ Production | Real-time subscriptions with ping/pong |
| CDC (Change Data Capture) | ✅ Production | Filtered change streams |
| C/FFI Bindings | ⚠️ Beta | Open/close, write, query, metrics management. `delete_metric` and `compact` not yet implemented. |
| Edge Mesh | ⚠️ Beta | Gossip protocol, consistent hash ring, scatter-gather queries, peer health checks |

## Enterprise Features

| Feature | Maturity | Notes |
|---------|----------|-------|
| Encryption at rest | ✅ Production | AES-256-GCM, PBKDF2 key derivation |
| Schema registry | ✅ Production | Metric validation, strict mode |
| Multi-tenancy | ⚠️ Beta | Namespace isolation, tag-based separation |
| Alerting engine | ✅ Production | Threshold alerts, webhook delivery |
| Anomaly detection | ✅ Production | Z-score, IQR, streaming pipeline, webhook alerts |
| Multi-region replication | ⚠️ Beta | Vector clocks, conflict resolution, snapshot sync |

## Operational

| Feature | Maturity | Notes |
|---------|----------|-------|
| CLI tool (13 commands) | ✅ Production | query, import, export, inspect, watch, completion |
| Helm chart | ✅ Production | Deployment, service, configmap, health probes |
| Dockerfile | ✅ Production | Multi-stage, distroless base, multi-arch (amd64/arm64/armv7) |
| Feature Registry | ✅ Production | Plugin-style feature management |

## Recently Promoted to Beta (v0.9.0)

| Feature | Maturity | Notes |
|---------|----------|-------|
| Point Validation Pipeline | ⚠️ Beta | NaN/Inf/bounds/cardinality checks wired into Write() |
| Write Pipeline Hooks | ⚠️ Beta | Pre/post write hooks with reject/modify semantics |
| Query Middleware | ⚠️ Beta | Composable middleware chain in Execute() |
| Health Checks | ⚠️ Beta | /health/ready, /health/live for K8s probes |
| Audit Logging | ⚠️ Beta | All operations logged with timestamps |
| Result Cache | ⚠️ Beta | LRU cache with TTL and write invalidation |
| Rate Controller | ⚠️ Beta | Token bucket per-metric and global |
| Tag Inverted Index | ⚠️ Beta | Fast tag lookups via posting lists |
| Query Cost Estimator | ⚠️ Beta | Pre-execution cost estimation |
| Anomaly Detection v2 | ⚠️ Beta | STL decomposition with adaptive thresholds |
| Schema Evolution | ⚠️ Beta | Auto-detect tag changes, versioned schemas |
| Query Profiler | ⚠️ Beta | Per-query stage timing and recommendations |
| Distributed Query | ⚠️ Beta | Scatter-gather with sort-preserving merge |
| Continuous Aggregation | ⚠️ Beta | Incremental materialized aggregations |
| Data Quality Monitor | ⚠️ Beta | Gap/duplicate/outlier/skew detection |
| WAL Snapshots | ⚠️ Beta | Snapshot-based WAL compaction |
| Incremental Backup | ⚠️ Beta | Changed-partition-only backups |
| Prometheus Drop-in | ⚠️ Beta | Full /api/v1/query, /api/v1/query_range |
| Embedded Cluster | ⚠️ Beta | Gossip-based node discovery |

## Non-Functional / Stubs

> These features exist as type definitions or skeleton implementations.
> They are **not functional** and should not be relied upon.

| Feature | Status | What Exists | What's Missing |
|---------|--------|-------------|----------------|
| K8s Operator | 📋 Stub | CRD types, reconciler skeleton | Real K8s API client, actual resource management |
| Terraform Provider | 📋 Stub | Resource type definitions | Terraform SDK integration, CRUD operations |
| PostgreSQL Wire Protocol | 📋 Stub | Bridge types, internal/pgwire package | Wire protocol handshake, query execution |
| Apache Arrow Flight | 📋 Stub | Custom binary protocol types | Not compatible with real Arrow Flight clients |
| WASM Runtime | 🧪 Alpha | JS bridge, TypeScript types | Untested browser execution, large binary size |
| Deno Runtime | 📋 Stub | Type definitions | No runtime integration |
| Jupyter Kernel | 📋 Stub | Message protocol types | No actual Jupyter communication |
| TinyML | 📋 Stub | Model types | No real ML inference |
| eBPF | 📋 Stub | Probe types | No kernel integration |
| Zero-Knowledge Proofs | 📋 Stub | Circuit types | No actual cryptographic proofs |
| Blockchain Audit | 📋 Stub | Chain types | No actual blockchain |

## How to Read api_stability.go

The `api_stability.go` file classifies every exported symbol into Stable, Beta, or Experimental tiers.
Stable symbols are covered by semver. Beta may change between minor versions. Experimental may change or be removed at any time.
