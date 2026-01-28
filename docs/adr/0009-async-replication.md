# ADR-0009: Fire-and-Forget Async Replication Model

## Status

Accepted

## Context

Edge deployments often need to replicate data to a central location:

1. **Central visibility**: Operations teams need aggregated view across edge sites
2. **Backup/DR**: Edge devices may fail; central copy provides durability
3. **Analysis**: Advanced analytics run better on centralized, powerful infrastructure

Replication strategies involve fundamental trade-offs:

| Strategy | Consistency | Latency | Availability |
|----------|-------------|---------|--------------|
| Synchronous | Strong | High | Low (blocks on remote) |
| Semi-sync | Bounded staleness | Medium | Medium |
| Asynchronous | Eventual | Low | High |

Edge environments have specific constraints:
- **Unreliable networks**: Intermittent connectivity is normal
- **Latency sensitivity**: Local writes must not block on remote
- **Bandwidth limits**: Cannot replicate all data in real-time

## Decision

Chronicle implements **fire-and-forget asynchronous replication**:

### Architecture

```
┌─────────────┐     ┌─────────────────┐     ┌─────────────┐
│ Write API   │────▶│ Replication     │────▶│ Remote      │
│             │     │ Queue (channel) │     │ Endpoint    │
└─────────────┘     └─────────────────┘     └─────────────┘
      │                     │
      ▼                     ▼
┌─────────────┐     Async batched HTTP POST
│ Local Store │     (gzip compressed)
└─────────────┘
```

### Key Characteristics

1. **Non-blocking writes**: Local writes succeed immediately regardless of replication status
2. **Buffered queue**: Channel-based buffer absorbs bursts (configurable size)
3. **Batched transmission**: Points batched by size and time (default 2 seconds)
4. **Compression**: Gzip compression reduces bandwidth
5. **Metric filtering**: Whitelist controls which metrics replicate

### Configuration

```go
type ReplicationConfig struct {
    Enabled       bool
    Endpoint      string        // Remote Chronicle URL
    BatchSize     int           // Max points per batch (default: 1000)
    BatchBytes    int           // Max bytes per batch (default: 1MB)
    FlushInterval time.Duration // Max time before flush (default: 2s)
    BufferSize    int           // Queue capacity (default: 100,000)
    Metrics       []string      // Whitelist (empty = all)
    Compress      bool          // Enable gzip (default: true)
}
```

### Failure Handling

- **Queue full**: New points dropped with warning log (local write still succeeds)
- **Remote unavailable**: Batch retried with exponential backoff
- **Partial failure**: Failed batch logged, continues with next batch

## Consequences

### Positive

- **Local write performance**: Replication adds no latency to writes
- **Resilience**: Edge continues operating during network outages
- **Bandwidth efficiency**: Batching and compression reduce network usage
- **Selective replication**: Filter to replicate only important metrics
- **Simple implementation**: No distributed consensus protocol

### Negative

- **Potential data loss**: Queue overflow or extended outages lose data
- **Eventual consistency**: Central view lags edge by seconds to minutes
- **No delivery guarantees**: No acknowledgment-based reliability
- **Limited visibility**: Difficult to know replication lag or loss rate

### Data Loss Scenarios

| Scenario | Data Lost | Mitigation |
|----------|-----------|------------|
| Queue overflow | Newest points dropped | Increase buffer, filter metrics |
| Edge crash | Queued points lost | Reduce batch interval |
| Network partition | Points during partition | Increase queue size |
| Remote crash | In-flight batch | Retry with backoff |

### Comparison with Alternatives

**Synchronous replication**:
- Pro: No data loss
- Con: Local writes block, unavailable during outage
- Verdict: Unacceptable for edge deployments

**WAL shipping**:
- Pro: Durable queue survives crashes
- Con: Complex, requires compatible WAL format
- Verdict: Good for future enhancement

**Kafka/message queue**:
- Pro: Durable, scalable, multi-consumer
- Con: Additional infrastructure dependency
- Verdict: Out of scope for embedded database

### Operational Guidance

**Sizing the buffer**:
```
buffer_size = (points_per_second) × (max_outage_seconds)
Example: 1000 pts/s × 300s = 300,000 point buffer
```

**Monitoring replication health**:
- Queue depth metric (high = falling behind)
- Replication lag metric (time since last success)
- Drop rate metric (points lost to overflow)

### Enabled

- Edge deployments with intermittent connectivity
- High-throughput ingestion without remote bottleneck
- Selective replication for bandwidth optimization
- Simple operational model (no distributed state)

### Prevented

- Strong consistency between edge and central
- Guaranteed delivery (at-least-once, exactly-once semantics)
- Bi-directional replication (this is one-way, edge → central)
- Automatic failover (no consensus for leader election)
