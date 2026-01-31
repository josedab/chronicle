---
sidebar_position: 102
---

# Comparison

How Chronicle compares to other time-series databases.

## Quick Comparison

| Feature | Chronicle | InfluxDB | Prometheus | TimescaleDB | VictoriaMetrics |
|---------|-----------|----------|------------|-------------|-----------------|
| **Deployment** | Embedded | Server | Server | Server | Server |
| **Dependencies** | None | Server process | Server process | PostgreSQL | Server process |
| **Language** | Go | Go | Go | C/SQL | Go |
| **Storage** | Single file | Custom | Custom | PostgreSQL | Custom |
| **PromQL** | ✅ | ❌ | ✅ | ❌ | ✅ |
| **SQL** | ✅ | InfluxQL/Flux | ❌ | ✅ | MetricsQL |
| **WASM** | ✅ | ❌ | ❌ | ❌ | ❌ |
| **GraphQL** | ✅ | ❌ | ❌ | ❌ | ❌ |

## When to Use Chronicle

### Choose Chronicle If You Need

- **Embedded database** - No external dependencies, single binary deployment
- **Go integration** - First-class Go API, no serialization overhead
- **Edge/IoT** - Small footprint, runs on constrained devices
- **Browser/WASM** - Client-side time-series analysis
- **Simplicity** - No infrastructure to manage

### Choose Something Else If You Need

- **Massive scale** - Petabytes of data, thousands of nodes
- **High availability** - Built-in clustering and replication
- **Multi-language** - Native clients for many languages
- **Managed service** - Cloud-hosted, fully managed

## Detailed Comparison

### vs InfluxDB

| Aspect | Chronicle | InfluxDB |
|--------|-----------|----------|
| **Deployment** | Embedded library | Separate server |
| **API** | Go native | HTTP/gRPC |
| **Query Language** | SQL + PromQL | InfluxQL/Flux |
| **Compression** | Gorilla + Snappy | Gorilla + Snappy |
| **Clustering** | Federation | Enterprise only |
| **Learning curve** | Low | Medium |

**When Chronicle wins:**
- Embedded use cases
- Go applications
- No network overhead needed

**When InfluxDB wins:**
- Multi-language support
- Enterprise features
- Flux for complex transformations

### vs Prometheus

| Aspect | Chronicle | Prometheus |
|--------|-----------|------------|
| **Architecture** | Embedded | Pull-based server |
| **Storage** | Single file | TSDB blocks |
| **Long-term storage** | Built-in | Requires remote storage |
| **PromQL** | ✅ Supported | ✅ Native |
| **Alerting** | ✅ Built-in | Via Alertmanager |
| **Service discovery** | ❌ | ✅ |

**When Chronicle wins:**
- Embedded applications
- Push-based metrics
- Simpler deployment

**When Prometheus wins:**
- Kubernetes monitoring
- Service discovery
- Mature ecosystem

### vs TimescaleDB

| Aspect | Chronicle | TimescaleDB |
|--------|-----------|-------------|
| **Foundation** | Custom engine | PostgreSQL |
| **SQL** | Subset | Full PostgreSQL |
| **Deployment** | Embedded | Requires PostgreSQL |
| **Joins** | ❌ | ✅ |
| **Extensions** | Go plugins | PostgreSQL extensions |

**When Chronicle wins:**
- No external dependencies
- Embedded use cases
- Go-native performance

**When TimescaleDB wins:**
- Complex SQL queries
- Joins with relational data
- PostgreSQL ecosystem

### vs SQLite + Time-Series Extensions

| Aspect | Chronicle | SQLite |
|--------|-----------|--------|
| **Focus** | Time-series native | General purpose |
| **Compression** | Optimized for TSDB | Generic |
| **Queries** | PromQL + SQL | SQL only |
| **Performance** | Optimized for append | Balanced |

**When Chronicle wins:**
- Time-series specific features
- Better compression
- PromQL support

**When SQLite wins:**
- General purpose database
- Wider language support
- Mature tooling

## Performance Comparison

### Write Performance

Synthetic benchmark: 1M points, single series, local SSD

| Database | Points/sec | Notes |
|----------|-----------|-------|
| Chronicle | ~500K | In-process |
| InfluxDB | ~300K | HTTP overhead |
| Prometheus | N/A | Pull-based |
| TimescaleDB | ~200K | PostgreSQL overhead |

*Results vary significantly based on configuration and hardware.*

### Query Performance

Benchmark: 1 hour aggregate over 1 million points

| Database | Latency | Notes |
|----------|---------|-------|
| Chronicle | ~10ms | In-memory index |
| InfluxDB | ~50ms | Network + query |
| TimescaleDB | ~100ms | PostgreSQL planning |

### Storage Efficiency

1 million points, random values, 10 series

| Database | Size | Compression |
|----------|------|-------------|
| Chronicle | ~12 MB | ~13:1 |
| InfluxDB | ~15 MB | ~10:1 |
| TimescaleDB | ~20 MB | ~8:1 |

## Feature Matrix

### Storage Features

| Feature | Chronicle | InfluxDB | Prometheus | TimescaleDB |
|---------|-----------|----------|------------|-------------|
| Compression | ✅ | ✅ | ✅ | ✅ |
| WAL | ✅ | ✅ | ✅ | ✅ |
| Encryption at rest | ✅ | Enterprise | ❌ | ✅ |
| S3 backend | ✅ | ❌ | Via adapter | ❌ |
| Partitioning | ✅ | ✅ | ✅ | ✅ |

### Query Features

| Feature | Chronicle | InfluxDB | Prometheus | TimescaleDB |
|---------|-----------|----------|------------|-------------|
| SQL | ✅ Subset | InfluxQL | ❌ | ✅ Full |
| PromQL | ✅ | ❌ | ✅ | ❌ |
| GraphQL | ✅ | ❌ | ❌ | ❌ |
| Aggregations | ✅ | ✅ | ✅ | ✅ |
| Downsampling | ✅ | ✅ | Recording rules | ✅ |

### Operational Features

| Feature | Chronicle | InfluxDB | Prometheus | TimescaleDB |
|---------|-----------|----------|------------|-------------|
| Backup | ✅ | ✅ | ✅ | ✅ |
| Replication | ✅ | Enterprise | Federation | ✅ |
| Multi-tenancy | ✅ | ✅ | ❌ | ✅ |
| Alerting | ✅ | ✅ | Via Alertmanager | Via extensions |

## Migration Guides

See the [Prometheus Integration](/docs/guides/prometheus-integration) guide for migrating from Prometheus.
