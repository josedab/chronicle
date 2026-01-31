# Architecture Decision Records

This directory contains Architecture Decision Records (ADRs) for Chronicle.

ADRs document significant architectural decisions made during the development of Chronicle, providing context for why the system is built the way it is.

## Index

| ADR | Title | Status |
|-----|-------|--------|
| [ADR-0001](0001-single-file-columnar-storage.md) | Single-File Columnar Storage Model | Accepted |
| [ADR-0002](0002-time-based-partitions.md) | Time-Based Partition Architecture | Accepted |
| [ADR-0003](0003-adaptive-column-compression.md) | Adaptive Per-Column Compression Selection | Accepted |
| [ADR-0004](0004-wal-buffered-writes.md) | Write-Ahead Log with Buffered Writes | Accepted |
| [ADR-0005](0005-pluggable-storage-backends.md) | Pluggable Storage Backend Abstraction | Accepted |
| [ADR-0006](0006-dual-query-languages.md) | Dual Query Language Support | Accepted |
| [ADR-0007](0007-continuous-queries.md) | Continuous Queries for Materialized Aggregations | Accepted |
| [ADR-0008](0008-tag-based-multitenancy.md) | Tag-Based Multi-Tenancy Isolation | Accepted |
| [ADR-0009](0009-async-replication.md) | Fire-and-Forget Async Replication Model | Accepted |
| [ADR-0010](0010-protocol-agnostic-ingestion.md) | Protocol-Agnostic Ingestion Layer | Accepted |

## ADR Format

Each ADR follows this structure:

- **Title**: Short descriptive name
- **Status**: Accepted, Superseded, or Deprecated
- **Context**: What prompted this decision?
- **Decision**: What was decided?
- **Consequences**: Tradeoffs, implications, what this enabled/prevented

## Further Reading

- [ARCHITECTURE.md](../ARCHITECTURE.md) - System architecture overview
- [CONFIGURATION.md](../CONFIGURATION.md) - Configuration options
- [Michael Nygard's ADR article](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions)
