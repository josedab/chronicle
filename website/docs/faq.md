---
sidebar_position: 100
---

# FAQ

Frequently asked questions about Chronicle.

## General

### What is Chronicle?

Chronicle is an embedded time-series database written in Go. Unlike server-based databases like InfluxDB or Prometheus, Chronicle runs as a library within your application with zero external dependencies.

### Is Chronicle production-ready?

Chronicle is pre-1.0 but has:
- Comprehensive test suite with 67%+ coverage
- Race condition testing
- Fuzzing for the query parser
- Real-world usage in internal projects

It's suitable for production use in non-critical workloads. For mission-critical systems, evaluate thoroughly and consider the maturity of alternatives.

### What's the license?

Apache 2.0 - free for commercial and personal use, with no copyleft requirements.

### Who maintains Chronicle?

Chronicle is open source and community-maintained. Contributions are welcome!

## Technical

### How much data can Chronicle handle?

Chronicle is designed for:
- **Series**: Up to millions of unique series
- **Points**: Billions of data points
- **Storage**: Limited by disk space
- **Query speed**: Sub-second for most queries

Performance depends heavily on hardware, configuration, and access patterns.

### What compression ratio can I expect?

For typical metrics data:
- **Regular intervals**: 10-15x compression
- **Irregular intervals**: 5-10x compression
- **High-variance values**: 3-5x compression

Chronicle uses Gorilla encoding (delta-of-delta for timestamps, XOR for values) plus Snappy block compression.

### Does Chronicle support clustering?

Chronicle doesn't include built-in distributed clustering. For multi-node deployments:
- Use **federation** to query across instances
- Use **replication** for redundancy
- Consider sharding by tenant or metric

Full Raft-based clustering is on the roadmap but requires significant development effort.

### What's the maximum query timeout?

Default is 30 seconds, configurable via `QueryTimeout` in config. For long-running queries, increase the timeout or use streaming results.

### Can I use Chronicle with other languages?

Chronicle is Go-native, but you can access it from other languages via:
- **HTTP API** - Any language with HTTP client
- **GraphQL** - Standard GraphQL clients
- **WASM** - JavaScript/browser environments
- **CGO** - C bindings (not officially supported)

## Deployment

### How do I backup Chronicle?

Several options:
1. **Snapshot** - Consistent point-in-time backup while running
2. **Delta backup** - Incremental backups for large databases
3. **Export** - CSV/JSON export for migration
4. **File copy** - Copy database file while stopped

See [Backup & Restore](/docs/guides/backup-restore) for details.

### Can I run Chronicle in a container?

Yes! Chronicle works well in containers:

```dockerfile
FROM golang:1.21-alpine
COPY myapp /app/myapp
VOLUME /data
CMD ["/app/myapp", "-db", "/data/chronicle.db"]
```

Ensure the data volume is persistent.

### What about Kubernetes?

Chronicle runs in Kubernetes as a sidecar or standalone pod. For persistent storage:
- Use PersistentVolumeClaims
- Consider StatefulSets for stable storage
- Chronicle doesn't require a Kubernetes Operator

### How do I monitor Chronicle itself?

Chronicle exposes internal metrics via:
- `/metrics` endpoint (Prometheus format)
- `db.CardinalityStats()` for series counts
- Admin UI at `/admin`

## Performance

### Why are writes slow?

Common causes:
1. **Sync interval too low** - Increase `SyncInterval`
2. **Buffer too small** - Increase `BufferSize`
3. **Disk I/O** - Use SSD/NVMe
4. **Single writes** - Use `WriteBatch` instead

### Why are queries slow?

Common causes:
1. **No time bounds** - Always specify Start/End
2. **High cardinality** - Too many unique series
3. **Full scans** - Use tag filters
4. **Large results** - Use aggregation or LIMIT

### How do I reduce memory usage?

1. Decrease `MaxMemory` in config
2. Reduce `BufferSize`
3. Use shorter `PartitionDuration`
4. Enable `RetentionDuration` to delete old data

## Data

### Can I import data from Prometheus/InfluxDB?

Yes:
- **Prometheus**: Use remote write or export/import
- **InfluxDB**: Export to line protocol, import to Chronicle

See [Prometheus Integration](/docs/guides/prometheus-integration) for details.

### How do I delete specific data?

Chronicle doesn't support deleting individual points. Options:
1. Wait for retention to expire data
2. Export desired data, recreate database, import
3. Use tag-based filtering in queries to exclude data

### What happens if the database file is corrupted?

Chronicle uses WAL for crash recovery:
1. On startup, uncommitted WAL entries are replayed
2. If the main file is corrupted, restore from backup
3. WAL corruption may lose recent uncommitted data

Always maintain backups for important data.

## Features

### Does Chronicle support SQL JOINs?

No. Chronicle supports a SQL-like query language for time-series data, but not relational operations like JOINs. For relational queries, export data to a SQL database.

### Can I use Chronicle for logs?

Chronicle is optimized for numeric time-series data, not logs. For logs, consider:
- Loki (pairs well with Prometheus/Grafana)
- Elasticsearch
- Chronicle's vector embeddings for log similarity search

### Does Chronicle support histograms?

Yes! Chronicle has native histogram support:

```go
db.WriteHistogram(chronicle.HistogramPoint{
    Metric:  "request_duration",
    Buckets: []chronicle.HistogramBucket{...},
    Sum:     45.2,
    Count:   500,
})
```

### Is there a GUI?

Chronicle includes a built-in admin UI at `/admin` when HTTP is enabled. For dashboarding, use Grafana with Chronicle's Prometheus-compatible API.

## Troubleshooting

### "file is locked" error

Only one process can open a Chronicle database at a time. Ensure no other process has the file open.

### "WAL replay failed" error

The WAL file may be corrupted. Options:
1. Delete `.wal` files (loses uncommitted data)
2. Restore from backup

### High CPU during compaction

Compaction is CPU-intensive. Options:
1. Reduce `CompactionWorkers`
2. Increase `CompactionInterval`
3. Schedule compaction during low-traffic periods
