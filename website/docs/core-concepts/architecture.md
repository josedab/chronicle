---
sidebar_position: 1
---

# Architecture

Chronicle is designed as an embedded time-series database optimized for Go applications. This page explains the key architectural components and how they work together.

## High-Level Overview

```mermaid
flowchart TB
    subgraph Client["Client Layer"]
        GO[Go API]
        HTTP[HTTP API]
        PROM[PromQL]
        GQL[GraphQL]
    end
    
    subgraph Core["Core Engine"]
        BUF[Write Buffer]
        WAL[Write-Ahead Log]
        IDX[Index]
        PART[Partitions]
    end
    
    subgraph Storage["Storage Layer"]
        FILE[Local File]
        S3[S3 Backend]
        CUSTOM[Custom Backend]
    end
    
    GO --> BUF
    HTTP --> BUF
    PROM --> BUF
    GQL --> BUF
    
    BUF --> WAL
    WAL --> IDX
    IDX --> PART
    
    PART --> FILE
    PART --> S3
    PART --> CUSTOM
```

## Core Components

### Write Buffer

The write buffer batches incoming points before persisting them:

- **Purpose**: Reduces disk I/O by batching small writes
- **Default size**: 10,000 points
- **Behavior**: Automatically flushes when full or on `db.Flush()`
- **Thread-safe**: Multiple goroutines can write concurrently

```go
// Points are buffered automatically
db.Write(point)  // Adds to buffer

// Explicit flush when needed
db.Flush()  // Writes buffer to disk
```

### Write-Ahead Log (WAL)

The WAL ensures durability and crash recovery:

- **Purpose**: Guarantees no data loss on crash
- **Sync interval**: Configurable (default: 1 second)
- **Rotation**: Rotates at configurable size (default: 128MB)
- **Recovery**: Replays uncommitted data on startup

```mermaid
sequenceDiagram
    participant App
    participant Buffer
    participant WAL
    participant Storage
    
    App->>Buffer: Write(point)
    Buffer->>Buffer: Accumulate
    Buffer->>WAL: Sync points
    WAL->>Storage: Persist to disk
    Note over WAL,Storage: Crash-safe
```

### Index

The index provides fast series lookup:

- **Series registry**: Maps metric+tags → series ID
- **Partition index**: Maps time ranges → partitions
- **Tag index**: Enables efficient tag filtering
- **Memory-resident**: Loaded on startup, persisted on close

### Partitions

Data is organized into time-based partitions:

- **Time-based**: Each partition covers a time window (default: 1 hour)
- **Benefits**: Enables efficient retention, parallel queries, compaction
- **Lazy loading**: Partitions load data on demand
- **Compression**: Gorilla + Snappy encoding

```mermaid
flowchart LR
    subgraph "Time Axis"
        P1["Partition 1<br/>00:00-01:00"]
        P2["Partition 2<br/>01:00-02:00"]
        P3["Partition 3<br/>02:00-03:00"]
        P4["Partition 4<br/>03:00-04:00"]
    end
    
    P1 --> P2 --> P3 --> P4
    
    style P1 fill:#e0e0e0
    style P2 fill:#e0e0e0
    style P4 fill:#90EE90
```

## Data Flow

### Write Path

1. **Receive**: Point arrives via Go API or HTTP
2. **Validate**: Schema validation (if configured)
3. **Track**: Cardinality tracking
4. **Buffer**: Add to write buffer
5. **WAL**: Write to WAL (sync interval)
6. **Flush**: Write to partition when buffer full
7. **Index**: Update series and tag indexes

### Query Path

1. **Parse**: Parse SQL/PromQL query
2. **Plan**: Determine partitions and series
3. **Filter**: Apply tag filters using index
4. **Scan**: Read matching points from partitions
5. **Aggregate**: Apply aggregation functions
6. **Return**: Return results to caller

```mermaid
flowchart LR
    Q[Query] --> P[Parse]
    P --> PL[Plan]
    PL --> F[Filter]
    F --> S[Scan]
    S --> A[Aggregate]
    A --> R[Results]
```

## Storage Engine

### Compression

Chronicle uses a two-stage compression approach:

1. **Gorilla encoding**: Delta-of-delta for timestamps, XOR for values
2. **Snappy compression**: Fast block compression

Typical compression ratios: **10-15x** for metrics data.

### File Format

```
┌─────────────────────────────┐
│         File Header         │
├─────────────────────────────┤
│        Partition 1          │
│  ┌───────────────────────┐  │
│  │ Series 1 (compressed) │  │
│  │ Series 2 (compressed) │  │
│  │ ...                   │  │
│  └───────────────────────┘  │
├─────────────────────────────┤
│        Partition 2          │
│  ┌───────────────────────┐  │
│  │ Series 1 (compressed) │  │
│  │ ...                   │  │
│  └───────────────────────┘  │
├─────────────────────────────┤
│           Index             │
├─────────────────────────────┤
│         File Footer         │
└─────────────────────────────┘
```

## Background Processes

Chronicle runs several background goroutines:

| Process | Interval | Purpose |
|---------|----------|---------|
| WAL Sync | 1s (configurable) | Durability |
| Retention | 5 min | Delete expired data |
| Downsampling | 5 min | Create rollups |
| Compaction | 30 min (configurable) | Reclaim space |

## Memory Management

Chronicle carefully manages memory:

- **MaxMemory**: Total budget for buffers and caches
- **Query limits**: Per-query memory cap
- **Buffer sizing**: Automatic based on MaxMemory
- **Partition caching**: LRU eviction

```go
db, _ := chronicle.Open("data.db", chronicle.Config{
    MaxMemory: 256 * 1024 * 1024,  // 256MB total
    BufferSize: 50_000,             // 50k point buffer
})
```

## Concurrency Model

Chronicle is fully thread-safe:

- **Writes**: Lock-free buffer with mutex on flush
- **Reads**: RWMutex allows concurrent queries
- **Background**: Separate goroutines for maintenance
- **No global locks**: Partition-level locking

```mermaid
flowchart TB
    subgraph Writers["Writer Goroutines"]
        W1[Writer 1]
        W2[Writer 2]
        W3[Writer 3]
    end
    
    subgraph Buffer["Write Buffer"]
        BUF[Lock-free Queue]
    end
    
    subgraph Readers["Reader Goroutines"]
        R1[Reader 1]
        R2[Reader 2]
    end
    
    W1 --> BUF
    W2 --> BUF
    W3 --> BUF
    
    R1 -.->|RLock| IDX[Index]
    R2 -.->|RLock| IDX
```

## Extension Points

Chronicle is designed for extensibility:

### Storage Backends

Implement `StorageBackend` for custom storage:

```go
type StorageBackend interface {
    Read(ctx context.Context, key string) (io.ReadCloser, error)
    Write(ctx context.Context, key string, data io.Reader) error
    Delete(ctx context.Context, key string) error
    List(ctx context.Context, prefix string) ([]string, error)
}
```

### Query Languages

- Native Go API
- SQL-like parser
- PromQL executor
- GraphQL schema

## What's Next?

- [Data Model](/docs/core-concepts/data-model) - Metrics, tags, and series
- [Storage](/docs/core-concepts/storage) - Storage engine details
- [Storage Backends](/docs/advanced/storage-backends) - S3 and custom backends
