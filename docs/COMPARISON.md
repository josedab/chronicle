# Chronicle vs Other Time-Series Databases

This document compares Chronicle with other popular time-series databases to help you evaluate which solution fits your use case.

## Feature Comparison Matrix

| Feature | Chronicle | InfluxDB | Prometheus | VictoriaMetrics | TimescaleDB | QuestDB |
|---|---|---|---|---|---|---|
| **Deployment Model** | | | | | | |
| Embedded / in-process | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Edge / constrained devices | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Browser (WASM) | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Standalone server | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Kubernetes operator | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Storage** | | | | | | |
| Single-file format | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Pluggable backends (file, memory, S3) | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Gorilla float compression | ✅ | ❌ | ✅ | ✅ | ❌ | ❌ |
| Delta timestamp encoding | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ |
| Dictionary tag compression | ✅ | ✅ | ❌ | ❌ | ❌ | ✅ |
| Tiered storage (hot/warm/cold) | ✅ | ✅ | ❌ | ❌ | ✅ | ❌ |
| Encryption at rest | ✅ | ✅ (Enterprise) | ❌ | ❌ | ✅ (via PG) | ✅ (Enterprise) |
| **Query** | | | | | | |
| SQL-like query language | ✅ | ✅ (InfluxQL/Flux) | ❌ | ❌ | ✅ (SQL) | ✅ (SQL) |
| PromQL support | ✅ (subset) | ❌ | ✅ (native) | ✅ (native) | ❌ | ❌ |
| Streaming SQL | ✅ | ❌ | ❌ | ❌ | ✅ | ❌ |
| Query federation | ✅ | ❌ | ✅ (Thanos) | ✅ | ❌ | ❌ |
| Time-travel queries | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Ingestion** | | | | | | |
| HTTP write API | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Prometheus remote write | ✅ | ❌ | ✅ | ✅ | ❌ | ❌ |
| OpenTelemetry (OTLP) | ✅ | ✅ | ✅ (receiver) | ✅ | ❌ | ❌ |
| Line protocol | ✅ | ✅ | ❌ | ✅ | ❌ | ✅ |
| gRPC ingestion | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Analytics** | | | | | | |
| Built-in forecasting | ✅ | ❌ | ❌ | ❌ | ✅ (extensions) | ❌ |
| Anomaly detection | ✅ | ❌ | ❌ | ✅ | ❌ | ❌ |
| Native histograms | ✅ | ❌ | ✅ | ✅ | ❌ | ❌ |
| Exemplars | ✅ | ❌ | ✅ | ✅ | ❌ | ❌ |
| Downsampling | ✅ | ✅ | ✅ (recording rules) | ✅ | ✅ | ❌ |
| Recording rules | ✅ | ✅ (tasks) | ✅ | ✅ | ❌ | ❌ |
| **Integrations** | | | | | | |
| Grafana plugin | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| GraphQL API | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Admin UI | ✅ | ✅ | ✅ (basic) | ✅ | ❌ | ✅ |
| WASM plugin runtime | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Operations** | | | | | | |
| Multi-tenancy | ✅ | ✅ | ✅ (Cortex/Mimir) | ✅ | ✅ | ❌ |
| Schema registry | ✅ | ❌ | ❌ | ❌ | ✅ (via PG) | ❌ |
| Alerting engine | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ |
| Backup / PITR | ✅ | ✅ | ✅ (snapshots) | ✅ | ✅ | ✅ |
| **Language** | Go | Go/Rust | Go | Go | C/SQL | Java/C++ |

## When to Choose Chronicle

**Choose Chronicle when you need:**

- **Embedded time-series storage** — Chronicle runs in-process as a Go library with zero external dependencies, similar to how SQLite works for relational data.
- **Edge and IoT deployments** — Single-file storage, configurable memory limits, and minimal resource footprint make it ideal for constrained environments.
- **Browser-based analytics** — WASM compilation lets you run a full time-series database in the browser for offline-capable dashboards and local analytics.
- **Gradual scaling** — Start embedded, then grow to standalone server or clustered deployment without changing your data model or query language.
- **Multi-signal observability** — Metrics, traces, and logs in a single database with correlation queries across signal types.

**Consider alternatives when:**

- **You need a battle-tested production cluster** — InfluxDB, VictoriaMetrics, and TimescaleDB have years of production use at scale.
- **Your team already uses Prometheus** — If you only need metrics with PromQL, Prometheus or VictoriaMetrics may be simpler to operate.
- **You need full SQL compatibility** — TimescaleDB (built on PostgreSQL) or QuestDB offer richer SQL support with JOINs and the full PostgreSQL ecosystem.
- **Write throughput is your primary concern** — QuestDB and VictoriaMetrics are optimized for very high write throughput in server deployments.

## Architecture Differences

### Chronicle: Embedded-First
Chronicle is designed as a library that can be imported into any Go application. The database runs in the same process, eliminating network overhead for reads and writes. This is fundamentally different from client-server databases and enables use cases like:
- Sensor data collection on edge gateways
- In-browser time-series analysis
- Metrics collection in CLI tools and desktop applications
- Testing without external infrastructure

### InfluxDB / VictoriaMetrics: Server-First
These databases are designed as standalone services. They excel at centralized metric collection and querying but require network access and operational overhead.

### TimescaleDB: Extension-Based
TimescaleDB extends PostgreSQL with time-series optimizations. It benefits from the PostgreSQL ecosystem but inherits its resource requirements and operational complexity.

### Prometheus: Pull-Based
Prometheus uses a unique pull-based model where it scrapes metrics from targets. Chronicle supports Prometheus remote write for integration but also offers push-based ingestion, making it more flexible for edge deployments where pull-based scraping isn't practical.
