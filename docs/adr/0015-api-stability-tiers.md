# ADR 0015: API Stability Tiers

## Status
Accepted

## Context
Chronicle exports 1,895 types from a single package. Without stability classification,
every type is implicitly part of the public API and subject to semver. This is unsustainable
for a pre-1.0 project with 126 features.

## Decision
We classify every exported symbol into three tiers, tracked in api_stability.go:

- **Stable (25 symbols)**: Covered by semver. Breaking changes only in major versions.
  Core types: DB, Point, Query, Result, Config, Open, DefaultConfig, Write, Execute, etc.

- **Beta (28 symbols)**: May change between minor versions with migration guidance.
  Mature features: HealthCheck, AuditLog, PointValidator, WritePipeline, ResultCache, etc.

- **Experimental (59 symbols)**: May change or be removed without notice.
  All other feature engines.

Users are documented to only depend on Stable + Beta symbols for production use.

## Consequences
- **Positive**: Clear expectations; enables rapid iteration on experimental features
- **Negative**: Users must check stability before depending on a type; graduation process needed
- **Mitigation**: BetaAPI() and ExperimentalAPI() functions return all symbols with their tier;
  feature flags can disable experimental features at runtime
