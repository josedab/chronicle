# ADR 0013: Feature Manager Pattern

## Status
Accepted

## Context
Chronicle grew from a simple embedded TSDB to a platform with 126+ optional features.
Each feature needs: configuration, lazy initialization, HTTP routes, API stability classification,
and a DB accessor method. Without a pattern, this would create massive coupling and startup overhead.

## Decision
We use a FeatureManager with sync.Once lazy initialization for all optional features.

Each feature is wired through exactly 4 files:
1. `feature_manager.go` — struct field + sync.Once + accessor method
2. `db_features.go` — DB convenience accessor that nil-checks features
3. `http_routes_nextgen.go` — route registration via registerFeatureRoutes
4. `api_stability.go` — stability tier classification

## Consequences
- **Positive**: Zero startup cost for unused features; clean accessor pattern; centralized wiring
- **Negative**: feature_manager.go grows linearly (currently 1,526 lines); all features share one sync.Once namespace
- **Mitigation**: Planned module split (docs/MODULE_SPLIT.md) will distribute features across submodules
