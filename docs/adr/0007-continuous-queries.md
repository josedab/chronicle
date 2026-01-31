# ADR-0007: Continuous Queries for Materialized Aggregations

## Status

Accepted

## Context

Time-series monitoring workloads exhibit predictable query patterns:

1. **Dashboard queries**: Same aggregations executed repeatedly (e.g., every 30 seconds)
2. **Downsampled views**: Coarse-grained aggregates for long time ranges
3. **Alert evaluations**: Repeated queries for threshold checking

Computing these aggregations from raw data on every request has costs:

- **CPU overhead**: Aggregating millions of points repeatedly
- **Query latency**: Users wait for computation on each dashboard refresh
- **Resource contention**: Query load competes with ingestion

Traditional solutions include:
- **Pre-aggregation at write time**: Inflexible, requires knowing queries upfront
- **Query caching**: Limited benefit for time-advancing windows
- **Materialized views**: Compute once, query many times

## Decision

Chronicle implements **continuous queries** that create materialized aggregations:

### Definition

A continuous query defines:
- Source metric and filters
- Aggregation function and grouping
- Target metric name for materialized results
- Refresh interval

```go
type ContinuousQuery struct {
    Name           string
    Query          string        // Source query
    TargetMetric   string        // Where to store results
    Interval       time.Duration // Refresh interval
    Window         time.Duration // Aggregation window
    RetentionDays  int           // How long to keep results
}
```

### Example

```go
cq := ContinuousQuery{
    Name:         "hourly_cpu_avg",
    Query:        "SELECT mean(value) FROM cpu_usage GROUP BY host",
    TargetMetric: "cpu_usage:hourly_avg",
    Interval:     5 * time.Minute,
    Window:       1 * time.Hour,
}
```

### Execution

1. **Background goroutine** runs each continuous query at its interval
2. **Incremental execution**: Only processes data since last execution
3. **Results stored** as regular metrics (queryable like any other metric)
4. **Query rewriting**: Optimizer can substitute materialized metrics when beneficial

### Query Rewriting

When a query matches a continuous query's pattern, the optimizer rewrites it:

```
Original:  SELECT mean(value) FROM cpu_usage WHERE time > now()-24h GROUP BY host
Rewritten: SELECT mean(value) FROM cpu_usage:hourly_avg WHERE time > now()-24h GROUP BY host
```

The rewrite is transparent to the user.

## Consequences

### Positive

- **Faster dashboard queries**: Pre-computed results return in milliseconds
- **Reduced CPU load**: Aggregations computed once, not per-query
- **Consistent results**: All dashboard viewers see same pre-computed values
- **Downsampling integration**: Continuous queries can feed downsampled storage tiers

### Negative

- **Storage overhead**: Materialized results consume additional storage
- **Staleness**: Results may be up to one interval behind real-time
- **Configuration complexity**: Users must define continuous queries
- **Refresh overhead**: Background computation consumes resources

### Design Trade-offs

**Refresh Interval**:
- Shorter: More current results, higher CPU overhead
- Longer: More stale results, lower overhead
- Default: 5 minutes (balances freshness and cost)

**Query Rewriting**:
- Automatic rewriting reduces user burden but may surprise users
- Rewriting is conservative: only applies when result is provably equivalent

**Storage Strategy**:
- Materialized metrics stored identically to regular metrics
- Same retention, compression, and query capabilities apply
- Enables querying materialized data with full query language

### Comparison with Alternatives

| Approach | Freshness | Storage | Query Latency | Configuration |
|----------|-----------|---------|---------------|---------------|
| No optimization | Real-time | None | High | None |
| Query cache | Per-cache-TTL | Memory only | Medium | None |
| Continuous queries | Per-interval | Persistent | Low | Required |
| Write-time rollup | Real-time | Persistent | Low | Schema change |

### Enabled

- Sub-second dashboard load times for pre-defined views
- Cost-effective long-term storage via aggressive downsampling
- Alert evaluation on pre-aggregated data
- Separation of ingestion and query workloads

### Prevented

- Real-time accuracy for materialized queries (inherent staleness)
- Ad-hoc query acceleration (only pre-defined patterns benefit)
- Zero-configuration performance optimization
