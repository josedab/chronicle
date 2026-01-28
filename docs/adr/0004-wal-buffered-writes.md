# ADR-0004: Write-Ahead Log with Buffered Writes

## Status

Accepted

## Context

Chronicle must balance several competing concerns for write operations:

1. **Durability**: Data should survive crashes without loss
2. **Throughput**: High point ingestion rates are essential for monitoring workloads
3. **Resource efficiency**: Edge devices have limited I/O bandwidth and storage wear concerns
4. **Latency**: Writes should complete quickly for real-time monitoring

Traditional approaches to durability include:

- **Synchronous writes**: Every write goes to disk immediately (durable but slow)
- **Periodic checkpoints**: Batch writes to disk at intervals (fast but potential data loss)
- **Write-ahead logging**: Log writes sequentially, apply to main storage asynchronously (balanced)

Edge devices add additional constraints:
- Flash storage has limited write cycles
- I/O bandwidth may be shared with application workloads
- Power loss is more common than in datacenter environments

## Decision

Chronicle uses a **write-ahead log (WAL) combined with an in-memory write buffer**:

### Write Buffer

- **Capacity**: 10,000 points default (configurable via `BufferSize`)
- **Behavior**: Accumulates points in memory until capacity reached or flush triggered
- **Flush triggers**: Buffer full, explicit flush call, or periodic timer

```go
type WriteBuffer struct {
    points   []Point
    capacity int
    mu       sync.Mutex
}
```

### Write-Ahead Log

- **Format**: Append-only binary log with framed entries
- **Sync interval**: 1 second default (configurable via `WALSyncInterval`)
- **Rotation**: New file at 128MB (configurable via `WALMaxSize`)
- **Retention**: Keep 3 most recent files (configurable via `WALMaxFiles`)

### Write Path

1. Point received via API
2. Point appended to WAL (async, batched sync)
3. Point added to write buffer
4. When buffer full: compress and write partition to main storage
5. After successful partition write: WAL entries can be reclaimed

### Recovery Path

1. On startup, scan WAL files
2. Replay entries not yet in main storage
3. Rebuild write buffer state
4. Resume normal operation

## Consequences

### Positive

- **High throughput**: Buffering amortizes compression and I/O costs across many points
- **Controlled durability**: WAL sync interval allows tuning durability vs performance
- **Reduced storage wear**: Fewer, larger writes instead of many small writes
- **Crash recovery**: WAL replay recovers uncommitted data after crash
- **Predictable memory**: Fixed buffer size bounds memory usage

### Negative

- **Potential data loss window**: Points between WAL syncs may be lost on crash (default: up to 1 second)
- **Memory overhead**: Write buffer consumes RAM proportional to buffer size
- **Recovery time**: Large WAL files increase startup time
- **Complexity**: Two-phase write path is more complex than direct writes

### Configuration Trade-offs

| Setting | Lower Value | Higher Value |
|---------|-------------|--------------|
| `BufferSize` | More frequent flushes, lower memory | Better compression, higher memory |
| `WALSyncInterval` | Better durability, more I/O | Higher throughput, larger loss window |
| `WALMaxSize` | Faster rotation, more files | Fewer files, longer recovery |

### Default Configuration Rationale

- **BufferSize: 10,000**: Balances memory (~1MB for typical points) with compression efficiency
- **WALSyncInterval: 1s**: Acceptable loss window for most monitoring use cases
- **WALMaxSize: 128MB**: Large enough to batch effectively, small enough for reasonable recovery

### Enabled

- Sustained write throughput of 100K+ points/second on modest hardware
- Tunable durability for different deployment scenarios
- Background compaction without blocking writes

### Prevented

- Zero-data-loss guarantee without costly synchronous writes
- Immediate query visibility (points visible after buffer flush, not immediately)
- Multi-process write access (single WAL writer assumption)
