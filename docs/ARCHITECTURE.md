# Chronicle Architecture

## 5-Minute Overview

New to Chronicle? Start here.

**What it is:** An embedded time-series database for Go — think SQLite for metrics.

**Core data flow:**

```
Write Path:  Point → Buffer (in-memory) → WAL (durability) → Partition (on flush)
Query Path:  Query → Index lookup → Partition scan → Aggregation → Result
Background:  Retention GC · Downsampling · Compaction · Replication
```

**Key concepts:**

| Concept | What it does | Key file(s) |
|---------|-------------|-------------|
| **DB** | Database handle — entry point for all operations | `db_core.go`, `db_write.go` |
| **Point** | A single time-series data point (metric + timestamp + value + tags) | `point.go` |
| **Buffer** | In-memory write buffer; flushes to partitions | `buffer.go` |
| **Partition** | Time-bounded data segment with columnar compression | `partition_core.go`, `partitioning.go` |
| **WAL** | Write-ahead log for crash recovery | `wal.go` |
| **BTree** | Partition index for time-range lookups | `btree.go` |
| **Storage Backend** | Pluggable persistence (file, memory, S3, tiered) | `storage_backend_*.go` |
| **Query Engine** | SQL-like + PromQL query execution | `query.go`, `promql.go` |
| **Config** | Database configuration with sensible defaults | `config.go`, `config_builder.go` |

**Bridge pattern:** Files named `*_bridge.go` (e.g., `admin_ui_bridge.go`, `raft_bridge.go`)
re-export types from `internal/` packages into the public `chronicle` package. They provide
the public API surface while keeping implementation details private. See any bridge file's
header comment for details.

**API stability:** Types are classified as Stable, Beta (🔬), or Experimental (🧪).
Check `api_stability.go` or look for markers in godoc comments. Only Stable APIs
are covered by semver guarantees.

**Quick commands:**

```bash
make check        # Pre-commit: vet + fast tests
make test-short   # All tests in short mode
make test-ci      # CI equivalent: vet + race + short tests
```

For the full architecture deep dive, continue reading below.

---

## Overview

Chronicle is an embedded time-series database for Go, designed for constrained and edge environments. It provides:

- **Single-file storage** with append-only writes
- **SQL-like query language** for familiar data access
- **PromQL subset support** for Prometheus compatibility
- **Columnar compression** using Gorilla and delta-of-delta algorithms
- **Pluggable storage backends** (file, memory, S3, tiered)
- **Optional HTTP API** with Prometheus remote write and OTLP support

## High-Level Architecture

```mermaid
flowchart TB
    subgraph API["API Layer"]
        HTTP["/write, /query<br>HTTP API"]
        PromQL["/api/v1/query<br>PromQL API"]
        OTLP["/v1/metrics<br>OTLP API"]
        WS["/stream<br>WebSocket"]
        GraphQL["/graphql<br>GraphQL API"]
    end

    subgraph Core["Core Engine"]
        Buffer["Write Buffer<br>+ Schema Validation"]
        QE["Query Engine<br>SQL + PromQL"]
        Alert["Alert Manager<br>+ Webhooks"]
        Stream["Stream Hub<br>+ Subscribers"]
    end

    subgraph Partitions["Partition Manager"]
        P0["Partition 0<br>[t0, t1)"]
        P1["Partition 1<br>[t1, t2)"]
        P2["Partition 2<br>[t2, t3)"]
        PN["Partition N<br>[tn, now)"]
    end

    subgraph Storage["Storage Backend"]
        File["FileBackend<br>(local disk)"]
        Mem["MemoryBackend<br>(WASM/test)"]
        S3["S3Backend<br>(AWS S3)"]
        Tiered["TieredBackend<br>(hot+cold)"]
    end

    subgraph Background["Background Services"]
        WAL["WAL<br>Recovery"]
        Retention["Retention<br>Cleanup"]
        DS["Downsample<br>Workers"]
        Compact["Compaction"]
        Repl["Replication"]
    end

    API --> Core
    Core --> Partitions
    Partitions --> Storage
    Background -.-> Core
    Background -.-> Partitions
```

### ASCII Diagram (for terminals)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Chronicle DB                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌───────────────┐  ┌───────────────┐  ┌───────────────┐  ┌──────────────┐ │
│  │   HTTP API    │  │  PromQL API   │  │  OTLP API     │  │  WebSocket   │ │
│  │ /write,/query │  │ /api/v1/query │  │ /v1/metrics   │  │  /stream     │ │
│  └───────┬───────┘  └───────┬───────┘  └───────┬───────┘  └──────┬───────┘ │
│          │                  │                  │                  │         │
│  ┌───────▼──────────────────▼──────────────────▼──────────────────▼───────┐ │
│  │                         Core Engine                                     │ │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌────────────────┐ │ │
│  │  │ Write Buffer│  │Query Engine │  │Alert Manager│  │ Stream Hub     │ │ │
│  │  │  + Schema   │  │ SQL+PromQL  │  │ + Webhooks  │  │ + Subscribers  │ │ │
│  │  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └────────────────┘ │ │
│  └─────────┼────────────────┼────────────────┼───────────────────────────┘  │
│            │                │                │                              │
│  ┌─────────▼────────────────▼────────────────▼─────────────────────────────┐│
│  │                       Partition Manager                                  ││
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌───────────┐ ││
│  │  │Partition0│  │Partition1│  │Partition2│  │    ...   │  │PartitionN │ ││
│  │  │ [t0,t1)  │  │ [t1,t2)  │  │ [t2,t3)  │  │          │  │ [tn,now)  │ ││
│  │  └────┬─────┘  └────┬─────┘  └────┬─────┘  └──────────┘  └─────┬─────┘ ││
│  └───────┼─────────────┼─────────────┼────────────────────────────┼───────┘│
│          └─────────────┴─────────────┴────────────────────────────┘        │
│                                      │                                      │
│  ┌───────────────────────────────────▼─────────────────────────────────────┐│
│  │                       Storage Backend                                    ││
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌─────────────┐ ││
│  │  │ FileBackend  │  │MemoryBackend │  │  S3Backend   │  │TieredBackend│ ││
│  │  │ (local disk) │  │ (WASM/test)  │  │ (AWS S3)     │  │ (hot+cold)  │ ││
│  │  └──────────────┘  └──────────────┘  └──────────────┘  └─────────────┘ ││
│  └──────────────────────────────────────────────────────────────────────────┘│
│                                                                              │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                    Background Services                                 │  │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌─────────┐ │  │
│  │  │   WAL    │  │Retention │  │Downsample│  │Compaction│  │Replicat.│ │  │
│  │  │ Recovery │  │ Cleanup  │  │ Workers  │  │          │  │         │ │  │
│  │  └──────────┘  └──────────┘  └──────────┘  └──────────┘  └─────────┘ │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Storage Model

### Partitions

Data is organized into time-bounded partitions (default: 1 hour). Each partition contains:

- **Header**: Metadata (time range, series count)
- **Series Index**: B-tree for O(log n) series lookup
- **Column Blocks**: Compressed timestamp and value columns

```mermaid
classDiagram
    class Partition {
        +uint64 ID
        +int64 StartTime
        +int64 EndTime
        +int64 MinTime
        +int64 MaxTime
        +uint64 PointCount
        +map~string,SeriesData~ Series
        +Append(points)
        +Query(ctx, filters)
    }

    class SeriesData {
        +Series Series
        +[]int64 Timestamps
        +[]float64 Values
        +int64 MinTime
        +int64 MaxTime
    }

    class Series {
        +uint64 ID
        +string Metric
        +map~string,string~ Tags
    }

    Partition "1" --> "*" SeriesData : contains
    SeriesData --> Series : metadata
```

### Partition Internal Layout

```
┌──────────────────────────────────────────┐
│ Partition Header                          │
├──────────────────────────────────────────┤
│ Series: "cpu{host=a}"                    │
│   ├─ Timestamps: [t1, t2, t3, ...]       │  ◄─ Delta-of-delta compressed
│   └─ Values: [0.5, 0.7, 0.6, ...]        │  ◄─ Gorilla XOR compressed
├──────────────────────────────────────────┤
│ Series: "cpu{host=b}"                    │
│   ├─ Timestamps: [t1, t2, t3, ...]       │
│   └─ Values: [0.8, 0.9, 0.85, ...]       │
└──────────────────────────────────────────┘
```

### File Format

```mermaid
block-beta
    columns 1
    block:header
        columns 3
        A["Magic: CHRDB1 (6B)"] B["Version (2B)"] C["Reserved (8B)"]
    end
    block:partitions
        columns 1
        D["Partition Block 1: type(1B) | len(4B) | checksum(4B) | compressed data"]
        E["Partition Block 2: type(1B) | len(4B) | checksum(4B) | compressed data"]
        F["... more partitions ..."]
    end
    block:index
        columns 1
        G["Index Block: partition metadata + series index"]
    end
    block:footer
        columns 3
        H["Footer Magic: CHRFTR1"] I["Index Offset (8B)"] J["Checksum (4B)"]
    end
```

### Compression Algorithms

| Column Type | Algorithm | Compression Ratio | Description |
|-------------|-----------|-------------------|-------------|
| Timestamps | Delta-of-delta | ~10:1 for regular intervals | Encodes difference of differences |
| Float values | Gorilla XOR | ~12:1 (Facebook paper) | XOR with previous, bit-pack leading/trailing zeros |
| String tags | Dictionary encoding | Depends on cardinality | Map strings to integers |

```mermaid
flowchart LR
    subgraph Gorilla["Gorilla XOR Compression"]
        V1["Value 1: 22.5"] --> XOR1["XOR with prev"]
        V2["Value 2: 22.6"] --> XOR1
        XOR1 --> Pack["Bit-pack<br>meaningful bits"]
        Pack --> Out["~2 bits/value"]
    end

    subgraph Delta["Delta-of-Delta"]
        T1["t1: 1000"] --> D1["Δ1 = 10"]
        T2["t2: 1010"] --> D1
        T3["t3: 1020"] --> D2["Δ2 = 10"]
        D1 --> DD["ΔΔ = 0"]
        D2 --> DD
        DD --> Enc["1 bit for zero"]
    end
```

## Query Engine

### SQL-Like Syntax

```sql
SELECT mean(value), max(value)
FROM cpu
WHERE host = 'server-01'
  AND time >= '2024-01-01' AND time < '2024-01-02'
GROUP BY time(5m)
```

### Query Execution Pipeline

```mermaid
flowchart TB
    subgraph Parse["1. Parse Phase"]
        SQL["SQL String"] --> Parser["QueryParser"]
        Parser --> AST["Query AST"]
    end

    subgraph Plan["2. Plan Phase"]
        AST --> Planner["Query Planner"]
        Planner --> TimeRange["Identify Time Range"]
        TimeRange --> Partitions["Find Partitions<br>(B-tree lookup)"]
        Partitions --> Filters["Push Down Filters"]
    end

    subgraph Execute["3. Execute Phase"]
        Filters --> ParScan["Parallel Partition Scan"]
        ParScan --> P1["Partition 1<br>decompress + filter"]
        ParScan --> P2["Partition 2<br>decompress + filter"]
        ParScan --> PN["Partition N<br>decompress + filter"]
        P1 --> Agg["Streaming Aggregation"]
        P2 --> Agg
        PN --> Agg
    end

    subgraph Return["4. Return Phase"]
        Agg --> Limit["Apply Limit"]
        Limit --> Result["Query Result"]
    end
```

### Query Data Flow

```mermaid
sequenceDiagram
    participant Client
    participant DB
    participant Index
    participant Partition
    participant DataStore

    Client->>DB: Execute(Query)
    DB->>Index: FindPartitions(start, end)
    Index-->>DB: [Partition refs]
    
    loop Each Partition
        DB->>Partition: ensureLoaded()
        alt Not in memory
            Partition->>DataStore: ReadPartition(id)
            DataStore-->>Partition: compressed data
            Partition->>Partition: decompress()
        end
        DB->>Partition: query(filters, aggregation)
        Partition-->>DB: partial results
    end
    
    DB->>DB: merge results
    DB-->>Client: Result
```

## Concurrency Model

- **Single writer**: All writes go through a buffered channel
- **Multiple readers**: Queries run concurrently
- **Background tasks**: Flush, downsampling, retention run in separate goroutines
- **Graceful shutdown**: Context propagation for clean termination

```mermaid
flowchart TB
    subgraph Writers["Write Path (Single Writer)"]
        W1["Write Request 1"]
        W2["Write Request 2"]
        W3["Write Request N"]
        W1 --> Buffer["Write Buffer<br>(mutex protected)"]
        W2 --> Buffer
        W3 --> Buffer
        Buffer --> Flush["Flush Goroutine"]
    end

    subgraph Readers["Query Path (Multiple Readers)"]
        Q1["Query 1"] --> QE["Query Engine"]
        Q2["Query 2"] --> QE
        Q3["Query N"] --> QE
        QE --> RWLock["Index RWLock<br>(read lock)"]
    end

    subgraph BG["Background Goroutines"]
        Retention["Retention Worker<br>(5 min interval)"]
        Downsample["Downsample Worker"]
        Compact["Compaction Worker"]
        WALSync["WAL Sync<br>(1 sec interval)"]
    end

    Flush --> Partitions[(Partitions)]
    RWLock --> Partitions
    BG -.-> Partitions
```

## Package Structure

```mermaid
graph TB
    subgraph Public["Public API (chronicle package)"]
        DB["db.go<br>Main entry point"]
        Point["point.go<br>Data point struct"]
        Query["query.go<br>Query execution"]
        Config["config.go<br>Configuration"]
    end

    subgraph Core["Core Components"]
        Index["index.go<br>Partition/series index"]
        Partition["partition.go<br>Time-bounded data"]
        Buffer["buffer.go<br>Write buffer"]
        WAL["wal.go<br>Crash recovery"]
    end

    subgraph Storage["Storage Layer"]
        StorageGo["storage.go<br>File format"]
        Backend["storage_backend.go<br>Pluggable backends"]
        DataStore["data_store.go<br>Partition I/O"]
    end

    subgraph Compression["Compression"]
        Gorilla["gorilla.go<br>Float compression"]
        Delta["delta.go<br>Timestamp compression"]
        Dict["dictionary.go<br>String dedup"]
    end

    subgraph Internal["internal/"]
        Bits["bits/<br>Bit-level I/O"]
        Encoding["encoding/<br>Codec implementations"]
        QueryPkg["query/<br>Parser & aggregation"]
    end

    DB --> Index
    DB --> Buffer
    DB --> WAL
    Index --> Partition
    Partition --> StorageGo
    StorageGo --> Backend
    Partition --> Gorilla
    Partition --> Delta
    Partition --> Dict
    Gorilla --> Internal
    Delta --> Internal
```

### Directory Layout

```
chronicle/
├── internal/
│   ├── bits/       # Bit-level I/O for compression
│   ├── encoding/   # Gorilla, delta, dictionary codecs
│   └── query/      # SQL parser and aggregation
├── examples/
│   └── simple/     # Usage examples
└── *.go            # Public API (DB, Point, Query, etc.)
```

## Key Design Decisions

### 1. Single-File Storage
**Decision**: Store all data in one file instead of directory structure.
**Rationale**: Simplifies deployment, backup, and atomic operations for edge devices.
**Trade-off**: Limited to ~2GB practical size on 32-bit systems.

### 2. Append-Only Writes
**Decision**: Never modify existing data, only append.
**Rationale**: Enables simple crash recovery, reduces disk wear on SSDs/flash.
**Trade-off**: Requires compaction for space reclamation.

### 3. In-Process Only
**Decision**: No client-server mode, embedding only.
**Rationale**: Minimal overhead for edge/IoT applications.
**Trade-off**: Single-process access only (use replication for multi-process).

### 4. Pluggable Storage Backends
**Decision**: Abstract storage behind an interface.
**Rationale**: Enables S3 for cloud, memory for WASM, tiered for cost optimization.
**Trade-off**: Slight performance overhead from interface dispatch.

### 5. Multi-Tenancy via Tags
**Decision**: Implement tenancy using a reserved `__tenant__` tag rather than separate databases.
**Rationale**: Single codebase, shared infrastructure, simpler operations.
**Trade-off**: Tenant isolation is logical, not physical.

---

## Write Path Deep Dive

The write path is optimized for high-throughput ingestion while maintaining durability.

```mermaid
flowchart TB
    subgraph Ingest["1. Ingestion"]
        Write["DB.Write(Point)"]
        Batch["DB.WriteBatch([]Point)"]
        HTTP["HTTP /write"]
        OTLP["OTLP /v1/metrics"]
    end

    subgraph Validate["2. Validation"]
        Schema["Schema Validation<br>(if StrictSchema)"]
        Cardinality["Cardinality Check<br>(if enabled)"]
        Tenant["Tenant Isolation<br>(if multi-tenant)"]
    end

    subgraph Buffer["3. Buffering"]
        WB["WriteBuffer.Add()"]
        Check{"Buffer Full?"}
        Wait["Accumulate"]
        Trigger["Trigger Flush"]
    end

    subgraph Persist["4. Persistence"]
        WAL["WAL.Write()<br>(crash recovery)"]
        Group["Group by Partition<br>(time bucket)"]
        Append["Partition.Append()"]
        Compress["Compress & Write<br>to DataStore"]
    end

    subgraph Notify["5. Post-Write"]
        Stream["StreamHub.Publish()"]
        Repl["Replication Queue"]
        Alert["Alert Evaluation"]
    end

    Ingest --> Validate
    Validate --> Buffer
    WB --> Check
    Check -->|No| Wait
    Check -->|Yes| Trigger
    Wait -.-> Check
    Trigger --> Persist
    Persist --> Notify
```

### Write Path Sequence

```mermaid
sequenceDiagram
    participant Client
    participant DB
    participant Schema as Schema Registry
    participant Buffer as WriteBuffer
    participant WAL
    participant Index
    participant Partition
    participant Store as DataStore

    Client->>DB: Write(Point)
    
    opt StrictSchema enabled
        DB->>Schema: Validate(Point)
        Schema-->>DB: OK / Error
    end
    
    DB->>Buffer: Add(Point)
    Buffer-->>DB: count
    
    alt Buffer Full
        DB->>DB: flush()
        DB->>WAL: Write(points)
        DB->>DB: groupByPartition(points)
        
        loop Each Partition
            DB->>Index: GetOrCreatePartition(id, start, end)
            Index-->>DB: Partition
            DB->>Partition: Append(points)
            DB->>Store: WritePartition(id, compressed)
        end
        
        DB->>WAL: Reset()
    end
    
    DB-->>Client: nil / error
```

---

## Feature Architecture

### Storage Backends

```mermaid
classDiagram
    class StorageBackend {
        <<interface>>
        +Read(key string) []byte, error
        +Write(key string, data []byte) error
        +Delete(key string) error
        +List(prefix string) []string, error
        +Exists(key string) bool
        +Close() error
    }

    class FileBackend {
        -basePath string
        +safePath(key) string
    }

    class MemoryBackend {
        -data map[string][]byte
        -mu sync.RWMutex
    }

    class S3Backend {
        -client *s3.Client
        -bucket string
        -cache *LRUCache
        +Retry with backoff
    }

    class TieredBackend {
        -hot StorageBackend
        -cold StorageBackend
        -hotThreshold time.Duration
        +Auto-promote from cold
    }

    StorageBackend <|.. FileBackend
    StorageBackend <|.. MemoryBackend
    StorageBackend <|.. S3Backend
    StorageBackend <|.. TieredBackend
    TieredBackend o-- StorageBackend : hot
    TieredBackend o-- StorageBackend : cold
```

```
┌─────────────────────────────────────────────────────────────────┐
│                    StorageBackend Interface                      │
│  Read(key) | Write(key, data) | Delete(key) | List(prefix)     │
└─────────────────────────────────────────────────────────────────┘
        │                │                │                │
        ▼                ▼                ▼                ▼
┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│ FileBackend │  │MemoryBackend│  │  S3Backend  │  │TieredBackend│
│             │  │             │  │             │  │  hot + cold │
│ os.ReadFile │  │ map[string] │  │ AWS SDK v2  │  │ auto-promote│
│ os.WriteFile│  │   []byte    │  │ GetObject   │  │ from cold   │
└─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘
```

### Streaming Architecture

```mermaid
flowchart TB
    subgraph Hub["Stream Hub"]
        Dispatch["Fan-out Dispatcher"]
    end

    subgraph Subs["Subscriptions"]
        S1["Subscription 1<br>metric=cpu<br>filter=host:a"]
        S2["Subscription 2<br>metric=mem<br>filter=none"]
        SN["Subscription N<br>metric=*"]
    end

    subgraph Clients["WebSocket Clients"]
        C1["Client 1"]
        C2["Client 2"]
        CN["Client N"]
    end

    Write["New Point Written"] --> Dispatch
    Dispatch --> S1
    Dispatch --> S2
    Dispatch --> SN
    S1 --> C1
    S2 --> C2
    SN --> CN
```

```
                    ┌──────────────────────────┐
                    │       Stream Hub         │
                    │  (fan-out dispatcher)    │
                    └────────────┬─────────────┘
                                 │
        ┌────────────────────────┼────────────────────────┐
        │                        │                        │
        ▼                        ▼                        ▼
┌───────────────┐    ┌───────────────┐    ┌───────────────┐
│ Subscription 1│    │ Subscription 2│    │ Subscription N│
│ metric=cpu    │    │ metric=mem    │    │ metric=*      │
│ filter=host:a │    │ filter=none   │    │ filter=none   │
└───────┬───────┘    └───────┬───────┘    └───────┬───────┘
        │                    │                    │
        ▼                    ▼                    ▼
   WebSocket             WebSocket            WebSocket
    Client 1              Client 2             Client N
```

### Alerting Flow

```mermaid
stateDiagram-v2
    [*] --> OK: Initial State
    OK --> Pending: Condition TRUE
    Pending --> OK: Condition FALSE
    Pending --> Firing: Duration Exceeded
    Firing --> OK: Condition FALSE
    Firing --> Firing: Condition TRUE
    
    note right of Firing
        Send webhook notification
        on state change
    end note
```

```mermaid
flowchart LR
    Rule["Alert Rule<br>threshold"] --> Eval["Evaluate<br>(periodic)"]
    Eval --> Check["Check<br>Condition"]
    Check -->|TRUE| Duration["For<br>Duration"]
    Check -->|FALSE| OK["OK State"]
    Duration --> Fire["Fire<br>Alert"]
    Fire --> Webhook["Webhook<br>Notify"]
```

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ Alert Rule  │────►│  Evaluate   │────►│   Check     │
│ threshold   │     │  (periodic) │     │  Condition  │
└─────────────┘     └─────────────┘     └──────┬──────┘
                                               │
                           ┌───────────────────┴───────────────────┐
                           │                                       │
                           ▼                                       ▼
                    ┌─────────────┐                         ┌─────────────┐
                    │  Condition  │                         │  Condition  │
                    │    TRUE     │                         │    FALSE    │
                    └──────┬──────┘                         └─────────────┘
                           │
                           ▼
                    ┌─────────────┐
                    │  For        │ (duration check)
                    │  Duration   │
                    └──────┬──────┘
                           │
                           ▼
                    ┌─────────────┐
                    │   Fire      │
                    │   Alert     │
                    └──────┬──────┘
                           │
                           ▼
                    ┌─────────────┐
                    │  Webhook    │
                    │  Notify     │
                    └─────────────┘
```

### Schema Validation

```mermaid
flowchart TB
    Point["Incoming Point"] --> Registry["Schema Registry"]
    Registry --> Lookup{"Schema<br>Exists?"}
    Lookup -->|No| Write["Write Point"]
    Lookup -->|Yes| Validate["Validate"]
    
    Validate --> Tags["Check Tags<br>- Required present?<br>- Values allowed?<br>- Pattern match?"]
    Tags --> Values["Check Values<br>- Type correct?<br>- In range?"]
    Values --> Result{"Valid?"}
    Result -->|Yes| Write
    Result -->|No| Error["Return Error"]
```

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Point     │────►│   Schema    │────►│  Validate   │
│  (incoming) │     │  Registry   │     │  - Tags     │
└─────────────┘     └─────────────┘     │  - Values   │
                                        └──────┬──────┘
                                               │
                           ┌───────────────────┴───────────────────┐
                           │                                       │
                           ▼                                       ▼
                    ┌─────────────┐                         ┌─────────────┐
                    │    PASS     │                         │    FAIL     │
                    │   (write)   │                         │   (error)   │
                    └─────────────┘                         └─────────────┘
```

---

## Analytics Features

### Time-Series Forecasting

Chronicle includes built-in forecasting with multiple algorithms:

```mermaid
flowchart LR
    subgraph Input
        History["Historical Data"]
    end

    subgraph Train["Model Training"]
        History --> Select["Select Algorithm"]
        Select --> HW["Holt-Winters<br>(seasonal)"]
        Select --> DE["Double Exponential<br>(trend)"]
        Select --> SE["Simple Exponential<br>(level)"]
        Select --> MA["Moving Average"]
    end

    subgraph Output
        HW --> Forecast["Forecast<br>Future Values"]
        DE --> Forecast
        SE --> Forecast
        MA --> Forecast
        Forecast --> Anomaly["Anomaly Detection<br>(3σ threshold)"]
    end
```

```
┌─────────────────────────────────────────────────────────────────┐
│                    Forecasting Engine                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐      │
│  │   History    │───►│   Train      │───►│  Forecast    │      │
│  │   Data       │    │   Model      │    │  Future      │      │
│  └──────────────┘    └──────────────┘    └──────────────┘      │
│                                                │                 │
│                                                ▼                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    Algorithms                            │   │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐        │   │
│  │  │Holt-Winters│  │  Double    │  │  Simple    │        │   │
│  │  │(seasonal)  │  │Exponential │  │Exponential │        │   │
│  │  └────────────┘  └────────────┘  └────────────┘        │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                Anomaly Detection                         │   │
│  │  Prediction + Confidence Bounds ──► Outlier Score        │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### Recording Rules

Pre-compute expensive queries on a schedule:

```mermaid
flowchart LR
    subgraph Rules["Recording Rules Engine"]
        Timer["Periodic Timer<br>(1 min default)"]
        Timer --> Eval["Evaluate Query"]
        Eval --> Target["Write to Target Metric"]
    end

    subgraph Queries
        Q1["rule: http_latency_p99<br>query: percentile(http_latency, 99)"]
        Q2["rule: cpu_avg_5m<br>query: avg(cpu) over 5m"]
    end

    Queries --> Timer
```

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Rule      │────►│  Evaluate   │────►│   Write     │
│ (periodic)  │     │  Query      │     │  Results    │
└─────────────┘     └─────────────┘     └──────┬──────┘
                                               │
                                               ▼
                                        ┌─────────────┐
                                        │  Target     │
                                        │  Metric     │
                                        └─────────────┘
```

### Native Histograms

Prometheus-compatible exponential bucketing:

```mermaid
flowchart TB
    subgraph Histogram["Native Histogram Structure"]
        Schema["Schema: 3 (bucket width)"]
        Zero["Zero Bucket: count=2"]
        Pos["Positive Buckets:<br>[0,1): 5 | [1,2): 12 | [2,4): 8"]
        Neg["Negative Buckets: (mirrored)"]
    end

    subgraph Ops["Operations"]
        Observe["Observe(value)<br>→ Increment bucket"]
        Quantile["Quantile(q)<br>→ Estimate percentile"]
        Merge["Merge(other)<br>→ Combine histograms"]
        Encode["Encode/Decode<br>→ Delta encoding"]
    end

    Histogram --> Ops
```

```
┌─────────────────────────────────────────────────────────────────┐
│                    Native Histogram                              │
├─────────────────────────────────────────────────────────────────┤
│  Schema: 3 (exponential bucket width)                           │
│                                                                  │
│  Positive Buckets:  [0, 1)   [1, 2)   [2, 4)   [4, 8)   ...   │
│  Counts:               5        12       8        3              │
│                                                                  │
│  Zero Bucket:   count = 2                                        │
│  Negative Buckets: (mirrored)                                    │
│                                                                  │
│  Operations:                                                     │
│    Observe(value)     ─► Increment bucket                        │
│    Quantile(q)        ─► Estimate percentile                     │
│    Merge(other)       ─► Combine histograms                      │
│    Encode()/Decode()  ─► Efficient serialization                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Distributed Features

### Query Federation

Query across multiple Chronicle instances:

```mermaid
flowchart TB
    Client["Client Query"]
    
    subgraph Coordinator["Federation Coordinator"]
        Plan["Query Plan"]
        Dispatch["Dispatch to Instances"]
        Merge["Merge Results<br>(Union/Priority)"]
    end

    subgraph Instances["Chronicle Instances"]
        Local["Local Instance"]
        East["Remote (DC-East)"]
        West["Remote (DC-West)"]
    end

    Client --> Plan
    Plan --> Dispatch
    Dispatch --> Local
    Dispatch --> East
    Dispatch --> West
    Local --> Merge
    East --> Merge
    West --> Merge
    Merge --> Client
```

```
                         ┌─────────────────────┐
                         │      Client         │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │    Federation       │
                         │    Coordinator      │
                         └──────────┬──────────┘
                                    │
        ┌───────────────────────────┼───────────────────────────┐
        │                           │                           │
        ▼                           ▼                           ▼
┌───────────────┐         ┌───────────────┐         ┌───────────────┐
│    Local      │         │   Remote 1    │         │   Remote N    │
│   Chronicle   │         │  (DC-East)    │         │  (DC-West)    │
└───────┬───────┘         └───────┬───────┘         └───────┬───────┘
        │                         │                         │
        └─────────────────────────┼─────────────────────────┘
                                  │
                                  ▼
                         ┌─────────────────────┐
                         │   Merge Results     │
                         │   (Union/Priority)  │
                         └─────────────────────┘
```

### Backup & Recovery

Full and incremental backup flow:

```mermaid
flowchart LR
    subgraph Source["Chronicle DB"]
        DB[(Database)]
        WAL["WAL Files"]
    end

    subgraph Manager["Backup Manager"]
        Full["Full Backup<br>DB + All Partitions"]
        Incr["Incremental Backup<br>WAL Since Last"]
        Manifest["Backup Manifest<br>Track History"]
    end

    subgraph Dest["Destination"]
        Local["Local Filesystem"]
        S3["S3 / GCS"]
    end

    DB --> Full
    WAL --> Incr
    Full --> Manifest
    Incr --> Manifest
    Manifest --> Local
    Manifest --> S3
```

```
┌─────────────────────────────────────────────────────────────────┐
│                    Backup Manager                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Full Backup:                                                    │
│    DB State + All Partitions ──► Compressed Archive              │
│                                                                  │
│  Incremental Backup:                                             │
│    WAL Since Last Backup ──► Delta Archive                       │
│                                                                  │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐      │
│  │  Chronicle   │───►│   Backup     │───►│  Destination │      │
│  │     DB       │    │   Manager    │    │  (Local/S3)  │      │
│  └──────────────┘    └──────────────┘    └──────────────┘      │
│                                                                  │
│  Retention: Keep last N backups, auto-cleanup                   │
└─────────────────────────────────────────────────────────────────┘
```

### Replication

Outbound replication for disaster recovery:

```mermaid
sequenceDiagram
    participant Writer
    participant DB
    participant Queue as Replication Queue
    participant Remote as Remote Target

    Writer->>DB: Write(points)
    DB->>DB: Persist locally
    DB->>Queue: Enqueue(points)
    
    loop Batch Processing
        Queue->>Queue: Accumulate batch<br>(1000 points / 512KB)
        Queue->>Remote: POST /write (batch)
        alt Success
            Remote-->>Queue: 200 OK
            Queue->>Queue: Dequeue batch
        else Failure
            Remote-->>Queue: Error
            Queue->>Queue: Exponential backoff
            Note over Queue: Circuit breaker<br>after N failures
        end
    end
```

---

## API Architecture

### GraphQL Layer

```mermaid
flowchart TB
    subgraph Endpoints["GraphQL Endpoints"]
        Post["/graphql (POST)"]
        Playground["/graphql/playground"]
        WS["WebSocket Subscriptions"]
    end

    subgraph Schema["GraphQL Schema"]
        Query["Query<br>{ metrics, points, stats }"]
        Mutation["Mutation<br>{ write, delete }"]
        Subscription["Subscription<br>{ live }"]
    end

    subgraph Resolvers
        QE["Query Engine"]
        Writer["Data Writer"]
        StreamHub["Stream Hub"]
    end

    Post --> Schema
    WS --> Subscription
    Query --> QE
    Mutation --> Writer
    Subscription --> StreamHub
```

```
┌─────────────────────────────────────────────────────────────────┐
│                    GraphQL Server                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Schema:                                                         │
│    Query { metrics, points, stats }                             │
│    Mutation { write, delete }                                   │
│    Subscription { live }                                        │
│                                                                  │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐      │
│  │   /graphql   │    │  /graphql/   │    │ WebSocket    │      │
│  │   (POST)     │    │  playground  │    │ Subscriptions│      │
│  └──────────────┘    └──────────────┘    └──────────────┘      │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Admin UI

```
┌─────────────────────────────────────────────────────────────────┐
│                    Admin Dashboard                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │   Stats      │  │   Metrics    │  │   Query      │          │
│  │   Panel      │  │   Explorer   │  │   Editor     │          │
│  │ - Uptime     │  │ - List all   │  │ - SQL/PromQL │          │
│  │ - Memory     │  │ - Details    │  │ - Results    │          │
│  │ - Goroutines │  │ - Filter     │  │ - Export     │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
│                                                                  │
│  API Endpoints:                                                  │
│    /admin           ─► Dashboard HTML                           │
│    /admin/api/stats ─► JSON stats                               │
│    /admin/api/health─► Health check                             │
└─────────────────────────────────────────────────────────────────┘
```

---

## Observability Features

### Cardinality Management

Track and limit high-cardinality series:

```mermaid
flowchart TB
    Write["Incoming Write"] --> Extract["Extract Series Key<br>(metric + tags)"]
    Extract --> MetricCheck{"Per-Metric<br>Limit OK?"}
    MetricCheck -->|No| Alert1["Generate Alert"]
    MetricCheck -->|Yes| GlobalCheck{"Global<br>Limit OK?"}
    GlobalCheck -->|No| Alert2["Generate Alert"]
    GlobalCheck -->|Yes| HardCheck{"Hard<br>Limit?"}
    HardCheck -->|Exceeded| Reject["Reject Write"]
    HardCheck -->|OK| Accept["Accept Write"]
    Alert1 --> HardCheck
    Alert2 --> HardCheck
```

```
┌─────────────────────────────────────────────────────────────────┐
│                 Cardinality Tracker                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Per-Metric Limits:                                              │
│    cpu        ─► 10,000 max series                              │
│    http_req   ─► 50,000 max series                              │
│                                                                  │
│  Global Limit: 100,000 total series                             │
│                                                                  │
│  On Write:                                                       │
│    1. Extract series key (metric + tags)                        │
│    2. Check per-metric limit                                    │
│    3. Check global limit                                        │
│    4. Generate alert if threshold exceeded                      │
│    5. Reject write if hard limit exceeded                       │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Exemplars

Link metrics to distributed traces:

```mermaid
flowchart LR
    subgraph Metric
        M["http_latency<br>0.125s"]
    end

    subgraph Exemplar
        E["trace_id: abc123<br>span_id: def456"]
    end

    subgraph Tracing["Tracing Backend"]
        Jaeger["Jaeger / Zipkin / etc"]
    end

    M <-->|"linked"| E
    E -->|"lookup"| Jaeger
```

```
┌──────────────┐              ┌──────────────┐
│    Metric    │──────────────│   Exemplar   │
│ http_latency │  linked to   │  trace_id    │
│   0.125s     │              │  span_id     │
└──────────────┘              └──────────────┘
         │                            │
         └────────────────────────────┘
                      │
                      ▼
              ┌──────────────┐
              │   Tracing    │
              │   Backend    │
              │ (Jaeger/etc) │
              └──────────────┘
```

---

## Recovery & Crash Safety

Chronicle uses a Write-Ahead Log (WAL) to ensure durability:

```mermaid
sequenceDiagram
    participant Client
    participant DB
    participant WAL
    participant Partitions
    participant Disk

    Note over DB: Normal Operation
    Client->>DB: Write(points)
    DB->>WAL: Append(points)
    WAL->>Disk: fsync (periodic)
    DB->>Partitions: Buffer in memory
    
    Note over DB: Flush Triggered
    DB->>Partitions: Persist to disk
    Partitions->>Disk: Write partition block
    DB->>WAL: Reset()

    Note over DB: Crash Recovery (on startup)
    DB->>Disk: Load index from footer
    DB->>WAL: ReadAll()
    WAL-->>DB: Unsynced points
    DB->>Partitions: Replay points
    DB->>WAL: Reset()
```

---

## Performance Characteristics

### Complexity Analysis

| Operation | Time Complexity | Space Complexity | Notes |
|-----------|-----------------|------------------|-------|
| Write (single point) | O(1) amortized | O(1) | Buffered, WAL append |
| Write (batch N points) | O(N) | O(N) | Single WAL write |
| Query (time range) | O(log P + S×D) | O(R) | P=partitions, S=series, D=datapoints, R=results |
| Partition lookup | O(log P) | O(1) | B-tree index |
| Series filter | O(S) | O(M) | M=matching series |
| Aggregation | O(D) | O(B) | B=buckets |
| Compression (Gorilla) | O(N) | O(N) | Single pass |

### Memory Budget

```mermaid
pie title Memory Distribution (64MB default)
    "Write Buffer" : 10
    "Partition Cache" : 40
    "Index Structures" : 20
    "Query Working Memory" : 20
    "WAL Buffer" : 10
```

### Typical Performance

| Metric | Value | Conditions |
|--------|-------|------------|
| Write throughput | ~76K batches/sec | 10K points/batch |
| Query latency (p99) | <10ms | 10K points, aggregation |
| Compression ratio | 10-15x | Regular intervals, similar values |
| Cold start | <100ms | 1GB database |

See [BENCHMARKS.md](./BENCHMARKS.md) for detailed measurements.

---

## Security Architecture

```mermaid
flowchart TB
    subgraph Input["Input Validation"]
        BodyLimit["Request Body Limit<br>(10MB default)"]
        Timeout["Query Timeout<br>(30s default)"]
        PathCheck["Path Traversal<br>Protection"]
    end

    subgraph Encryption["Encryption at Rest"]
        KDF["PBKDF2 Key Derivation<br>(100K iterations)"]
        AES["AES-256-GCM<br>Authenticated Encryption"]
        Salt["32-byte Random Salt"]
        Nonce["12-byte Random Nonce"]
    end

    subgraph Isolation["Tenant Isolation"]
        TagBased["__tenant__ Tag<br>Logical Isolation"]
        Filter["Query-time Filtering"]
    end

    Input --> Core["Core Engine"]
    Encryption --> Storage["Storage Layer"]
    Isolation --> Core
```

**Security Features:**
- No `unsafe` package usage in core code
- Request body limits prevent memory exhaustion
- Query timeouts prevent resource starvation
- Path traversal protection in storage backends
- Optional AES-256-GCM encryption at rest

---

## Error Handling

Chronicle uses typed errors for precise error handling:

```mermaid
classDiagram
    class ChronicleError {
        <<interface>>
        +Error() string
        +Unwrap() error
    }

    class QueryError {
        +Type QueryErrorType
        +Message string
        +Query string
    }

    class StorageError {
        +Op string
        +Path string
        +Err error
    }

    class ValidationError {
        +Field string
        +Value any
        +Constraint string
    }

    ChronicleError <|-- QueryError
    ChronicleError <|-- StorageError
    ChronicleError <|-- ValidationError
```

**Error Types:**
- `ErrDatabaseClosed` - Operations on closed DB
- `ErrQueryTimeout` - Query exceeded timeout
- `ErrInvalidQuery` - Malformed query syntax
- `ErrSchemaViolation` - Schema validation failed
- `ErrCardinalityLimit` - Too many unique series
- `ErrStorageFull` - Max storage exceeded

---

## Limitations & Trade-offs

| Limitation | Reason | Workaround |
|------------|--------|------------|
| Single-process only | Embedded design, no IPC | Use replication for multi-process |
| ~2GB practical limit (32-bit) | File offset limitations | Use 64-bit systems or partitioning |
| No transactions | Append-only simplicity | Use WAL for consistency |
| Logical tenant isolation | Single-file design | Use separate databases if physical isolation required |
| No secondary indexes | Time-series optimized | Use tag filtering + time range |

---

## See Also

- [README](../README.md) - Quick start and API overview
- [FEATURES](./FEATURES.md) - Detailed feature documentation
- [CONFIGURATION](./CONFIGURATION.md) - All configuration options
- [API](./API.md) - HTTP API reference
- [CONTRIBUTING](../CONTRIBUTING.md) - Development setup
- [pkg.go.dev](https://pkg.go.dev/github.com/chronicle-db/chronicle) - API documentation
