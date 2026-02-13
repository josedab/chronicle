# Go Module Split Plan

## Overview

Chronicle will be split into two Go modules to provide clear stability guarantees:

- `github.com/chronicle-db/chronicle` — Core stable API only
- `github.com/chronicle-db/chronicle/x` — Experimental and beta features

## Module Boundary

### Core Module (`chronicle`)

**Stable types** (25 symbols, semver-covered):

```
DB, Point, Query, Aggregation, AggFunc, Result, Config,
StorageConfig, WALConfig, RetentionConfig, QueryConfig, HTTPConfig,
TagFilter, Open, DefaultConfig, DB.Close, DB.Write, DB.WriteBatch,
DB.Execute, DB.Metrics, AggCount, AggSum, AggMean, AggMin, AggMax
```

**Core files** (will remain in root module):
- `db_core.go`, `db_write.go`, `db_recovery.go`, `db_retention.go`
- `query.go`, `parser.go`, `point.go`, `tags.go`
- `config.go`, `config_builder.go`
- `wal.go`, `buffer.go`, `index.go`, `btree.go`
- `partition_core.go`, `partition_codec.go`, `partition_query.go`
- `storage.go`, `storage_engine.go`, `storage_backend_*.go`
- `schema.go`, `encryption.go`
- `errors.go`, `doc.go`

### Experimental Module (`chronicle/x`)

**All other features** (28 beta + 56 experimental symbols):
- `x/analytics/` — forecasting, anomaly detection, correlation
- `x/cloud/` — S3, tiered storage, sync fabric, rehydration
- `x/cluster/` — gossip, raft, distributed query
- `x/devtools/` — LSP, notebooks, studio, playground
- `x/integrations/` — Grafana, OTLP, ClickHouse, Prometheus
- `x/ml/` — auto ML, embeddings, RAG, foundation models
- `x/ops/` — K8s, Terraform, GitOps, chaos, fleet
- `x/security/` — masking, compliance, blockchain audit

## Migration Path

### Phase 1: Documentation (v0.9.0)
- Document which symbols are in each module
- Mark experimental imports with `// x:` comment prefix
- Add `go vet` check for experimental usage in core tests

### Phase 2: Interface Extraction (v0.10.0)
- Define interfaces in core for feature access
- Experimental module implements interfaces
- Core depends on interfaces, not concrete types

### Phase 3: Module Creation (v1.0.0)
- Create `x/` directory with `go.mod`
- Move experimental types to `x/` packages
- Core module gets clean `go.sum` without experimental deps
- Provide `chronicle/compat` package for gradual migration

## Import Changes

**Before (current):**
```go
import "github.com/chronicle-db/chronicle"

db, _ := chronicle.Open(path, cfg)
db.ForecastV2()  // experimental — accessed from same module
```

**After (v1.0.0):**
```go
import (
    "github.com/chronicle-db/chronicle"
    "github.com/chronicle-db/chronicle/x/analytics"
)

db, _ := chronicle.Open(path, cfg)
fv2 := analytics.NewForecastV2(db)  // explicit experimental import
```

## Benefits

1. **Clear stability boundary** — `go get chronicle` has zero experimental code
2. **Smaller core binary** — core module compiles ~50% faster
3. **Independent versioning** — experimental can break without semver violation
4. **Dependency isolation** — experimental deps don't pollute core `go.sum`

## Risks

1. **Breaking existing users** → 2-version transition period with `compat` package
2. **Maintenance overhead** → Automated CI checks for cross-module compatibility
3. **Interface bloat** → Limit to 5-10 core interfaces, not one per feature
