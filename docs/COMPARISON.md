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

## Focused Comparisons

### Chronicle vs InfluxDB

Choose Chronicle over InfluxDB when the database should live inside a Go
application, edge gateway, CLI, or browser build. Chronicle keeps deployment
simple: no separate server process, no sidecar, and no network hop for local
reads and writes. It is a better fit when the application owns its telemetry
and needs a compact local store with optional HTTP compatibility.

Choose InfluxDB when you want a mature standalone time-series platform with
managed cloud options, established operational tooling, and broad ecosystem
support for InfluxQL, Flux, dashboards, and task scheduling. Chronicle supports
Influx line protocol ingestion, but it is not a full InfluxDB replacement for
teams that depend on Flux tasks, cloud management features, or mature cluster
operations.

| Requirement | Prefer Chronicle | Prefer InfluxDB |
|---|---|---|
| Embedded storage | Yes, in-process Go library | No, server-first |
| Edge devices | Strong fit for local storage | Possible, but heavier |
| Influx line protocol | Supported for ingestion | Native |
| Flux compatibility | Not a goal | Native |
| Managed hosted service | Not the focus | Strong option |

### Chronicle vs VictoriaMetrics

Choose Chronicle over VictoriaMetrics when telemetry should be collected and
queried close to the application that produces it. Chronicle is designed for
embedded and edge-first scenarios where local persistence, small operational
surface area, and direct library calls matter more than centralized ingestion
throughput.

Choose VictoriaMetrics when you need a high-throughput Prometheus-compatible
server, long retention, multi-node scaling, and mature operational features for
large metric fleets. Chronicle can accept Prometheus remote write and provides a
PromQL subset, but VictoriaMetrics remains the better choice for central metrics
platforms that rely on full MetricsQL, high availability, and large-scale
scrape or remote-write fan-in.

| Requirement | Prefer Chronicle | Prefer VictoriaMetrics |
|---|---|---|
| In-process database | Yes | No |
| Central metrics backend | Possible, but not primary | Strong fit |
| Prometheus remote write | Supported | Native and highly optimized |
| MetricsQL compatibility | No | Native |
| Very high write throughput | Good for embedded workloads | Strong fit |

### Chronicle vs Prometheus

Choose Chronicle over Prometheus when metrics need to be pushed into an
embedded store, retained inside the application, or queried without running a
separate scraper and TSDB. Chronicle is especially useful for IoT, desktop,
offline, test, and single-binary deployments where service discovery and pull
scraping add unnecessary complexity.

Choose Prometheus when you need its pull-based discovery model, mature alerting
rules, Alertmanager integration, exporter ecosystem, and operational conventions
for Kubernetes or service-oriented platforms. Chronicle integrates with
Prometheus-style workflows through remote write and a PromQL subset, but it is
not intended to replace every Prometheus server feature in monitoring stacks
that already rely on full PromQL and scrape management.

| Requirement | Prefer Chronicle | Prefer Prometheus |
|---|---|---|
| Push-based local writes | Yes | Remote write receiver or gateway required |
| Pull-based scraping | Optional/integration oriented | Native model |
| Alertmanager workflow | Not the primary focus | Native ecosystem |
| Single-binary app storage | Strong fit | No |
| Full PromQL behavior | Subset support | Native |

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
