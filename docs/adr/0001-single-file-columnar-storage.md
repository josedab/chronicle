# ADR-0001: Single-File Columnar Storage Model

## Status

Accepted

## Context

Chronicle is designed as an embedded time-series database for edge and resource-constrained environments. These environments have specific characteristics that influenced our storage architecture decision:

1. **Deployment simplicity**: Edge devices often have limited operational tooling. Complex multi-file database layouts with directories, segments, and manifest files increase deployment and backup complexity.

2. **Atomic operations**: Distributed file systems and network-attached storage on edge devices may not provide strong consistency guarantees for directory operations.

3. **Resource constraints**: Edge devices typically have limited disk I/O bandwidth and benefit from sequential access patterns.

4. **Portability**: Users need to easily copy, backup, or move database files between systems without specialized tooling.

We evaluated several storage approaches:

- **LSM-tree with segment files** (like LevelDB/RocksDB): Excellent write throughput but complex compaction, many files
- **Directory of partition files** (like InfluxDB): Simpler than LSM but still requires directory management
- **Single file with embedded structure** (like SQLite): Simple deployment, atomic operations, portable

## Decision

Chronicle uses a **single-file storage model** with the following structure:

```
chronicle.db     - Main database file containing all partitions
chronicle.wal    - Write-ahead log for crash recovery (separate file)
```

The main database file contains:
- File header with magic bytes, version, and metadata
- Partition blocks with checksums (CRC32)
- Each partition contains columnar-encoded series data
- Shared string dictionary for tag compression

The WAL is kept separate to allow:
- Append-only writes without modifying the main file
- Size-based rotation (default 128MB per file)
- Independent retention (default 3 files)

## Consequences

### Positive

- **Simple deployment**: Copy one file to deploy or backup the database
- **Atomic snapshots**: File-level snapshots capture consistent state
- **Reduced file handles**: Single open file descriptor for reads
- **Portable**: Works on any filesystem without special requirements
- **Predictable I/O**: Sequential reads within partitions

### Negative

- **Concurrent write limitations**: Single-file model requires coordination for writes; we use a write buffer and periodic flushes rather than concurrent writers
- **Compaction complexity**: In-place compaction requires careful block management; we mitigate this with partition-based organization where old partitions are simply truncated
- **File size limits**: Very large deployments may hit filesystem limits (mitigated by retention policies and the storage backend abstraction for S3/tiered storage)
- **No parallel partition writes**: Unlike directory-per-partition models, we cannot write to multiple partitions simultaneously from different processes

### Enabled

- Simple backup/restore via file copy
- Easy integration with container orchestration (single volume mount)
- Straightforward disaster recovery procedures
- Storage backend abstraction (the single-file model maps cleanly to object storage like S3)

### Prevented

- Multi-process write access to the same database
- Unlimited horizontal scaling on a single node (addressed via federation instead)
