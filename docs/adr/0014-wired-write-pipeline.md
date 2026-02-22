# ADR 0014: Wired Write Pipeline

## Status
Accepted

## Context
Write validation, enrichment, and auditing were originally optional sidecars.
Users had to manually call validators before writing. This led to inconsistent
data quality and made it easy to bypass validation.

## Decision
We integrated PointValidator, WriteHooks, Schema validation, Cardinality tracking,
and AuditLog directly into the core `WriteContext()` and `WriteBatchContext()` methods
in db_write.go. The pipeline runs on every write automatically.

Pipeline order:
1. PointValidator — reject NaN, Inf, empty metric, invalid tags
2. WriteHooks (pre) — user-defined modification/rejection
3. Schema validation — check against registered schemas
4. Cardinality tracking — enforce series limits
5. Buffer → WAL → Flush — actual persistence
6. WriteHooks (post) — fire-and-forget notifications
7. AuditLog — record the operation

## Consequences
- **Positive**: Every write is validated; users cannot bypass checks; consistent audit trail
- **Negative**: ~5μs overhead per write from validation pipeline; features are nil-checked on every write
- **Mitigation**: Pipeline stages short-circuit if feature is nil (zero-cost when disabled)
