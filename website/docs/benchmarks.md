---
sidebar_position: 103
---

# Benchmarks

Performance benchmarks for Chronicle on various workloads.

:::info
Benchmarks were run on a MacBook Pro M1 with 16GB RAM and NVMe SSD. Your results may vary based on hardware and configuration.
:::

## Write Performance

### Single Point Writes

```
BenchmarkWrite-8                   500000    2340 ns/op    256 B/op    4 allocs/op
BenchmarkWriteWithTags-8           300000    3890 ns/op    512 B/op    8 allocs/op
```

| Metric | Value |
|--------|-------|
| Throughput | ~425,000 points/sec |
| Latency (p50) | 2.3 µs |
| Latency (p99) | 15 µs |

### Batch Writes

```
BenchmarkWriteBatch100-8           100000   12400 ns/op   2560 B/op   12 allocs/op
BenchmarkWriteBatch1000-8           10000  115000 ns/op  25600 B/op   15 allocs/op
BenchmarkWriteBatch10000-8           1000 1120000 ns/op 256000 B/op   18 allocs/op
```

| Batch Size | Throughput | Per-Point Latency |
|------------|------------|-------------------|
| 100 | 8M points/sec | 124 ns |
| 1,000 | 8.7M points/sec | 115 ns |
| 10,000 | 8.9M points/sec | 112 ns |

**Takeaway**: Use batch writes for best performance. Batching 1000+ points achieves ~9 million points/second.

## Query Performance

### Simple Queries

```
BenchmarkQueryLastHour-8            50000   28400 ns/op   4096 B/op   32 allocs/op
BenchmarkQueryWithTags-8            30000   42100 ns/op   8192 B/op   48 allocs/op
```

| Query Type | Latency | Points Scanned |
|------------|---------|----------------|
| Last hour (1 series) | 28 µs | 3,600 |
| With tag filter | 42 µs | 3,600 |
| Last 24h (1 series) | 350 µs | 86,400 |

### Aggregation Queries

```
BenchmarkAggMean1Hour-8             20000   65400 ns/op   8192 B/op   64 allocs/op
BenchmarkAggMean24Hour-8             2000  780000 ns/op  32768 B/op  128 allocs/op
BenchmarkAggWithGroupBy-8            1000 1250000 ns/op  65536 B/op  256 allocs/op
```

| Aggregation | Time Range | Series | Latency |
|-------------|------------|--------|---------|
| Mean | 1 hour | 1 | 65 µs |
| Mean | 24 hours | 1 | 780 µs |
| Mean + GroupBy | 24 hours | 10 | 1.25 ms |
| Sum | 1 hour | 100 | 2.1 ms |

### PromQL Queries

```
BenchmarkPromQLSimple-8             10000  125000 ns/op  16384 B/op  128 allocs/op
BenchmarkPromQLRate-8                5000  245000 ns/op  32768 B/op  256 allocs/op
BenchmarkPromQLAggregation-8         3000  380000 ns/op  49152 B/op  384 allocs/op
```

| PromQL Query | Latency |
|--------------|---------|
| `metric_name{label="value"}` | 125 µs |
| `rate(metric[5m])` | 245 µs |
| `sum by (label) (rate(metric[5m]))` | 380 µs |

## Compression

### Compression Ratios

| Data Type | Raw Size | Compressed | Ratio |
|-----------|----------|------------|-------|
| Regular metrics (1s) | 16 B/point | 1.1 B/point | 14.5:1 |
| Regular metrics (1m) | 16 B/point | 1.3 B/point | 12.3:1 |
| Sparse/irregular | 16 B/point | 2.1 B/point | 7.6:1 |
| High variance | 16 B/point | 3.2 B/point | 5.0:1 |

### Storage Efficiency

1 million points, 10 series, 1-minute intervals:

| Database | Storage Size | Compression |
|----------|--------------|-------------|
| Chronicle | 1.2 MB | 13.3:1 |
| InfluxDB | 1.5 MB | 10.7:1 |
| Raw JSON | 45 MB | 1:1 |

## Memory Usage

### Baseline Memory

| Configuration | Memory |
|---------------|--------|
| Empty database | 8 MB |
| 1K series, 1M points | 45 MB |
| 10K series, 10M points | 180 MB |
| 100K series, 100M points | 850 MB |

### Query Memory

| Query Type | Memory Overhead |
|------------|-----------------|
| Simple query (1K points) | 64 KB |
| Aggregation (100K points) | 512 KB |
| Large result set (1M points) | 48 MB |

## Disk I/O

### Write Patterns

```
BenchmarkWriteIO-8    Sequential writes: 450 MB/s
BenchmarkFlushIO-8    Sync to disk: 12 ms (with WAL)
```

### Read Patterns

| Operation | I/O Pattern | Throughput |
|-----------|-------------|------------|
| Partition scan | Sequential | 1.2 GB/s |
| Index lookup | Random | 50K IOPS |
| Cold query | Mixed | 200 MB/s |

## Scalability

### Series Cardinality

| Series Count | Write Latency | Query Latency | Memory |
|--------------|---------------|---------------|--------|
| 1K | 2.3 µs | 28 µs | 45 MB |
| 10K | 2.8 µs | 35 µs | 180 MB |
| 100K | 4.2 µs | 85 µs | 850 MB |
| 1M | 12 µs | 450 µs | 4.2 GB |

### Data Volume

| Points | Write Time | Query Time (24h agg) | Storage |
|--------|------------|---------------------|---------|
| 1M | 1.2s | 45 ms | 1.2 MB |
| 10M | 11s | 420 ms | 12 MB |
| 100M | 1.8 min | 4.2s | 120 MB |
| 1B | 18 min | 42s | 1.2 GB |

## Comparison

### vs InfluxDB

| Metric | Chronicle | InfluxDB |
|--------|-----------|----------|
| Write (single) | 425K/s | 280K/s |
| Write (batch) | 9M/s | 1.2M/s |
| Query (simple) | 28 µs | 1.2 ms |
| Query (agg) | 780 µs | 8 ms |
| Compression | 13:1 | 10:1 |

*Note: InfluxDB numbers include HTTP overhead. Chronicle is embedded.*

### vs Prometheus

| Metric | Chronicle | Prometheus |
|--------|-----------|------------|
| Ingestion | 9M/s (push) | 100K/s (scrape) |
| Query (PromQL) | 125 µs | 50 µs |
| Storage efficiency | 13:1 | 12:1 |

*Note: Different models—Chronicle is push-based, Prometheus is pull-based.*

## Running Benchmarks

Run the benchmarks yourself:

```bash
cd /path/to/chronicle
go test -bench=. -benchmem ./...
```

Specific benchmarks:

```bash
# Write benchmarks
go test -bench=BenchmarkWrite -benchmem

# Query benchmarks
go test -bench=BenchmarkQuery -benchmem

# Full benchmark suite with CPU profiling
go test -bench=. -benchmem -cpuprofile=cpu.prof
go tool pprof cpu.prof
```

## Configuration for Best Performance

### High Write Throughput

```go
config := chronicle.Config{
    BufferSize:   100_000,           // Large buffer
    SyncInterval: 5 * time.Second,   // Less frequent sync
    MaxMemory:    512 * 1024 * 1024, // More memory for buffers
}
```

### Low Query Latency

```go
config := chronicle.Config{
    MaxMemory:         1024 * 1024 * 1024, // Large cache
    PartitionDuration: 6 * time.Hour,       // Fewer partitions
    CompactionWorkers: 4,                   // Faster compaction
}
```

### Memory Constrained

```go
config := chronicle.Config{
    MaxMemory:         32 * 1024 * 1024, // 32MB
    BufferSize:        1000,              // Small buffer
    PartitionDuration: 15 * time.Minute,  // Small partitions
}
```

## Methodology

All benchmarks:
- Run on dedicated hardware (no other workloads)
- Averaged over 10 runs
- Include warmup period
- Report median values
- Memory measured after GC

Test data:
- Realistic metric patterns (not random)
- Mix of regular and irregular intervals
- Varying tag cardinality
