---
sidebar_position: 2
---

# Architecture Decisions

This page documents the key architectural decisions made in Chronicle. Each decision includes the context, the decision itself, and its consequences.

Understanding these decisions helps you:
- Grasp why Chronicle works the way it does
- Make informed decisions about whether Chronicle fits your use case
- Contribute to the project with knowledge of design constraints

---

## ADR-0001: Single-File Columnar Storage

### Status
Accepted

### Context

Chronicle is designed for edge and resource-constrained environments where:
- Complex multi-file database layouts increase deployment and backup complexity
- File systems may not provide strong consistency for directory operations
- Users need to easily copy, backup, or move database files

### Decision

Chronicle uses a **single-file storage model**:

```
chronicle.db     - Main database file containing all partitions
chronicle.wal    - Write-ahead log for crash recovery (separate)
```

### Consequences

**Positive:**
- Simple deployment: copy one file to backup
- Atomic snapshots capture consistent state
- Works on any filesystem without special requirements

**Negative:**
- Single-file model requires coordination for concurrent writes
- No multi-process write access to the same database

---

## ADR-0002: Time-Based Partitions

### Status
Accepted

### Context

Time-series data has natural temporal locality. Queries typically request recent data or specific time ranges. We needed an organization that optimizes for these access patterns.

### Decision

Data is organized into **time-based partitions** (default: 1 hour per partition):

```
├── Partition [00:00 - 01:00]
│   └── Series data (compressed)
├── Partition [01:00 - 02:00]
│   └── Series data (compressed)
└── Partition [02:00 - 03:00]
    └── Series data (compressed)
```

### Consequences

**Positive:**
- Efficient time-range queries (only scan relevant partitions)
- Simple retention: delete old partitions entirely
- Natural boundaries for compaction and backup

**Negative:**
- Out-of-order writes to old partitions require partition reopening
- Very short partitions increase metadata overhead

---

## ADR-0003: Adaptive Per-Column Compression

### Status
Accepted

### Context

Time-series data has distinct characteristics by data type:
- **Timestamps**: Monotonically increasing, often regular intervals
- **Float values**: May change slowly or have patterns
- **String tags**: Repeated categorical values

### Decision

Chronicle implements **adaptive per-column compression**:

| Algorithm | Target | Technique |
|-----------|--------|-----------|
| Gorilla | float64 values | XOR-based encoding |
| Delta-of-Delta | timestamps | Variable-bit encoding |
| Dictionary | strings | Integer indices |
| RLE | repeated values | Run-length encoding |

The encoder automatically selects the best algorithm per column.

### Consequences

**Positive:**
- 10-15x compression typical for metrics data
- No user configuration required
- Graceful degradation for incompressible data

**Negative:**
- Random access within partitions requires sequential decompression
- Multiple algorithms to maintain and test

---

## ADR-0004: WAL with Buffered Writes

### Status
Accepted

### Context

Edge devices may lose power unexpectedly. We need crash recovery without sacrificing write performance.

### Decision

Chronicle uses a **write-ahead log (WAL)** with buffered writes:

1. Points are buffered in memory
2. Periodically synced to WAL (configurable interval)
3. WAL replayed on startup to recover uncommitted data

### Consequences

**Positive:**
- Crash recovery with no data loss (within sync interval)
- High write throughput via batching
- Size-based WAL rotation prevents unbounded growth

**Negative:**
- Data written but not synced is lost on crash
- Additional disk space for WAL files

---

## ADR-0005: Pluggable Storage Backends

### Status
Accepted

### Context

While the single-file model works well for edge deployments, some use cases need:
- Cloud object storage (S3) for durability
- Tiered storage (hot/cold data separation)
- Testing with in-memory storage

### Decision

Chronicle provides a **storage backend abstraction**:

```go
type StorageBackend interface {
    Read(ctx context.Context, key string) (io.ReadCloser, error)
    Write(ctx context.Context, key string, data io.Reader) error
    Delete(ctx context.Context, key string) error
    List(ctx context.Context, prefix string) ([]string, error)
}
```

Built-in backends: File, Memory, S3, Tiered.

### Consequences

**Positive:**
- Same Chronicle API for local and cloud storage
- Easy to add new backends
- Testing without disk I/O

**Negative:**
- Abstraction adds some overhead
- Backend-specific features may not be exposed

---

## ADR-0006: Dual Query Language Support

### Status
Accepted

### Context

Users come from different backgrounds:
- Prometheus users expect PromQL
- Traditional database users expect SQL-like syntax
- We wanted Chronicle to be approachable for both groups

### Decision

Chronicle supports both **SQL-like queries** and **PromQL**:

```go
// SQL-like
db.Execute(&chronicle.Query{
    Metric: "temperature",
    Aggregation: &chronicle.Aggregation{Function: chronicle.AggMean, Window: time.Hour},
})

// PromQL
executor := chronicle.NewPromQLExecutor(db)
executor.Query(`avg(temperature[1h])`, time.Now())
```

### Consequences

**Positive:**
- Prometheus users can migrate easily
- SQL familiarity for general developers
- HTTP API supports both via different endpoints

**Negative:**
- Two query parsers to maintain
- Feature parity is challenging (some PromQL features lack SQL equivalents)

---

## ADR-0008: Tag-Based Multi-Tenancy

### Status
Accepted

### Context

SaaS applications need to isolate data between customers while sharing infrastructure.

### Decision

Chronicle uses **tag-based tenant isolation**:

```go
tenants := chronicle.NewTenantManager(config)

// Each tenant gets isolated namespace via tags
db, _ := tenants.GetTenant("customer-123")
// Writes automatically include tenant tag
// Queries automatically filter by tenant
```

### Consequences

**Positive:**
- Single database can serve multiple tenants
- Simple data model (tags are already first-class)
- Per-tenant retention and quotas possible

**Negative:**
- Requires careful query construction to prevent cross-tenant access
- High tenant count increases cardinality

---

## ADR-0011: Encryption at Rest

### Status
Accepted

### Context

Edge devices may be physically accessible to unauthorized parties. Sensitive metrics need protection.

### Decision

Chronicle offers **optional AES-256-GCM encryption at rest**:

```go
db, _ := chronicle.Open("secure.db", chronicle.Config{
    Encryption: &chronicle.EncryptionConfig{
        Enabled:     true,
        KeyPassword: os.Getenv("DB_KEY"),
    },
})
```

Key derivation uses PBKDF2 with random salt.

### Consequences

**Positive:**
- Protects data on stolen/decommissioned devices
- Industry-standard encryption (AES-256-GCM)
- Transparent to application code after config

**Negative:**
- ~10-15% performance overhead
- Key management is user's responsibility
- Cannot query encrypted data without key

---

## ADR-0012: Embedded with Optional HTTP

### Status
Accepted

### Context

Chronicle's primary value is as an embedded library. But some use cases benefit from HTTP access for:
- Remote queries
- Integration with existing tools (Grafana, curl)
- Language-agnostic access

### Decision

HTTP server is **optional and configurable**:

```go
// Embedded only (default)
db, _ := chronicle.Open("data.db", config)

// With HTTP API
db, _ := chronicle.Open("data.db", chronicle.Config{
    HTTPEnabled: true,
    HTTPPort:    8086,
})
```

### Consequences

**Positive:**
- Zero overhead when HTTP not needed
- Full embedded API available without HTTP
- HTTP enables tool integration when desired

**Negative:**
- Two API surfaces to maintain (Go + HTTP)
- Some features are Go-only (e.g., custom backends)

---

## Further Reading

- [Architecture Overview](/docs/core-concepts/architecture) — System design documentation
- [GitHub: Architecture Decision Records](https://github.com/chronicle-db/chronicle/tree/main/docs/adr) — Full ADR files with detailed context
