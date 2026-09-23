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

## Decision scenarios

Use these scenarios to choose between Chronicle, InfluxDB, VictoriaMetrics, and Prometheus.
They complement the feature matrix above with operational trade-offs.

### Scenario A — Library inside your Go service

**Pick Chronicle** when metrics must live in-process (CLI, agent, edge gateway, test harness) with no sidecar DB.
You import the package, open a DB handle, and write/query without standing up a network service.

**Pick InfluxDB / VictoriaMetrics** when many independent services must share one remote store and you already run that store as platform infrastructure.

**Pick Prometheus** when the service only needs to *expose* scrapable metrics and long-term storage is handled elsewhere (or not needed).

### Scenario B — Edge / IoT with flaky network

**Pick Chronicle** for single-file (or pluggable) local retention, optional WASM/browser analytics, and push when the link returns.
Pull-only scrapes are a poor fit when devices are behind NAT or sleep often.

**Pick Prometheus** only if you can run a collector that reaches the device on a schedule and accept scrape gaps.

**Pick VictoriaMetrics / InfluxDB** at the aggregation tier (regional gateway), not necessarily on every constrained device.

### Scenario C — Prometheus-native metrics shop

**Pick Prometheus + VictoriaMetrics (or Prometheus remote write to Chronicle)** when PromQL and the exporter ecosystem are already the org standard.
Chronicle’s PromQL support is a **subset** — great for embedded/bridge cases, not a drop-in replacement for every PromQL feature.

**Stay on Prometheus/VictoriaMetrics** for large shared multi-tenant metric platforms with mature operators and battle-tested cardinality controls at cluster scale.

### Scenario D — SQL-first analytics team

Chronicle offers a SQL-like / CQL path and GraphQL, but it is not full PostgreSQL.
**Prefer TimescaleDB / QuestDB** (see matrix) when you need rich SQL joins, window functions across relational tables, or BI tools that assume PostgreSQL.

**Prefer Chronicle** when the primary need is embedded time-series with optional PromQL, not general-purpose SQL warehousing.

### Scenario E — Migrating off Prometheus remote storage pain

1. Keep Prometheus for scrape/rules if it works.
2. Evaluate Chronicle when you want **embedded** long-term storage beside an app, or a lighter edge store that still accepts remote write.
3. Evaluate VictoriaMetrics/InfluxDB when you want a **central** remote-write sink with horizontal scale.

### Scenario F — Total cost / ops headcount

| Constraint | Lean toward |
|------------|-------------|
| No DB operators; ship inside the binary | Chronicle |
| Existing Prom org; maximize scrapers/exporters | Prometheus (± VictoriaMetrics) |
| Need managed cloud TSDB and dashboards ASAP | InfluxDB Cloud / vendor offerings |
| Extreme ingest on beefy servers | VictoriaMetrics / QuestDB (validate with your benchmark) |

## Query language cheat sheet

| Need | Chronicle | InfluxDB | VictoriaMetrics | Prometheus |
|------|-----------|----------|-----------------|------------|
| PromQL | Subset | No (InfluxQL/Flux/SQL) | Native | Native |
| SQL-like | CQL + related APIs | InfluxQL / SQL (version-dependent) | MetricsQL (PromQL+) | PromQL only |
| In-process API | First-class Go API | Client → server | Client → server | Client → server |

Practical rule: if your team’s muscle memory is PromQL dashboards, start with Prometheus/VictoriaMetrics; if your product *is* the database embedded in a Go binary, start with Chronicle.

## Migration notes (high level)

### From Prometheus

- Exporters can keep working; send remote write to a Chronicle server deployment, or keep Prometheus and use Chronicle only where embedded storage helps.
- Revisit recording rules and cardinality — Chronicle enforces budget/cardinality errors you should handle explicitly (see [ERRORS.md](./ERRORS.md)).

### From InfluxDB

- Map line protocol / HTTP write concepts to Chronicle’s write APIs.
- Replace Flux-specific tasks with Chronicle continuous query / recording-rule style features where available, or keep a thin ETL.

### From VictoriaMetrics

- Remote-write pipelines are the usual bridge.
- Expect to re-validate MetricsQL-only functions against Chronicle’s PromQL subset before cutting over dashboards.

## What this comparison is not

- Not a benchmark claim — run [BENCHMARKS.md](./BENCHMARKS.md) / your own workload.
- Not a guarantee of feature parity with every vendor “Enterprise” checkbox.
- Matrix checkmarks reflect Chronicle’s documented capabilities; always verify against the version you deploy.

## Related docs

- [FEATURES.md](./FEATURES.md) — capability overview
- [ARCHITECTURE.md](./ARCHITECTURE.md) — embedded-first design
- [GETTING_STARTED.md](./GETTING_STARTED.md) — first install
- [ERRORS.md](./ERRORS.md) — error types when evaluating limits/timeouts/cardinality
