# ADR-0002: Time-Based Partition Architecture

## Status

Accepted

## Context

Time-series data has a natural temporal ordering, and queries almost always include time range predicates. We needed a data organization strategy that would:

1. **Optimize time-range queries**: The most common query pattern is "give me data from time A to time B"
2. **Enable efficient retention**: Old data must be deletable without expensive garbage collection
3. **Support incremental backups**: Changed data should be identifiable for backup purposes
4. **Balance memory usage**: Not all data should be loaded into memory at once

We considered several partitioning strategies:

- **No partitioning** (single series store): Simple but poor query performance and no incremental operations
- **Metric-based partitioning**: Good for metric-specific queries but poor for cross-metric time ranges
- **Hash-based partitioning**: Good distribution but destroys temporal locality
- **Time-based partitioning**: Aligns with query patterns and enables partition-level operations

## Decision

Chronicle organizes data into **fixed-duration time partitions** with the following characteristics:

- **Default partition duration**: 1 hour (configurable via `PartitionDuration`)
- **Partition identification**: `partition_id = timestamp / partition_duration`
- **Index structure**: B-tree mapping timestamps to partition pointers for O(log n) lookup
- **Lazy loading**: Partition data loaded from disk on first access, not at startup

Each partition contains:
- Start and end timestamps defining its time range
- Map of series keys to series data
- Columnar-encoded timestamps and values per series
- Shared dictionary for string compression within the partition

```go
type Partition struct {
    ID        int64
    StartTime int64
    EndTime   int64
    Series    map[string]*SeriesData
}
```

## Consequences

### Positive

- **Fast time-range queries**: B-tree index identifies relevant partitions in O(log n); irrelevant partitions are never touched
- **Efficient retention**: Deleting old data is O(1) partition deletion, not row-by-row garbage collection
- **Memory efficiency**: Only active partitions need to be in memory; historical partitions loaded on demand
- **Natural backup boundaries**: Each partition is a self-contained unit for incremental backup
- **Compression benefits**: Data within a time window often has similar patterns, improving compression ratios

### Negative

- **Cross-partition queries**: Queries spanning many partitions require multiple partition loads and result merging
- **Small partition overhead**: Very short partition durations increase metadata overhead and B-tree depth
- **Partition boundary effects**: Points exactly at partition boundaries require consistent assignment rules
- **Uneven partition sizes**: Bursty workloads may create partitions of varying sizes

### Trade-off: Partition Duration

The default 1-hour partition duration balances:

| Shorter (e.g., 10 min) | Longer (e.g., 24 hours) |
|------------------------|-------------------------|
| Finer retention granularity | Coarser retention granularity |
| More partitions to manage | Fewer partitions |
| Smaller memory per partition | Larger memory per partition |
| More B-tree nodes | Shallower B-tree |

Users can tune `PartitionDuration` based on their retention requirements and query patterns.

### Enabled

- Retention policies expressed as duration ("keep 7 days") map directly to partition count
- Downsampling can operate partition-by-partition
- Parallel query execution across partitions (future optimization)
- Time-based sharding for distributed deployments

### Prevented

- Efficient "latest N points per series" queries without time bounds (must scan recent partitions)
- Sub-partition granularity retention (cannot keep some series longer than others within a partition)
