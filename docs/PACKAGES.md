# Package Organization & Restructuring Plan

Chronicle's root package currently contains ~386 Go files (~182k LoC). This document
maps the logical module boundaries and tracks the incremental restructuring plan.

## Current Logical Groups

### Core (must remain in root — public API)
| Files | Purpose |
|-------|---------|
| `db_core.go`, `db_write.go`, `db_features.go`, `db_recovery.go`, `db_retention.go`, `db_schema.go`, `db_open_helpers.go`, `db_metrics.go` | Database lifecycle and operations |
| `point.go`, `query.go`, `config.go`, `config_builder.go`, `errors.go` | Core types |
| `api_stability.go`, `api_aliases.go`, `doc.go` | API surface documentation |
| `buffer.go`, `wal.go`, `index.go`, `btree.go`, `storage.go` | Storage engine |
| `partition_core.go`, `partition_query.go`, `partition_codec.go`, `partitioning.go` | Partition management |

### Internal Packages (already extracted)
| Package | Purpose |
|---------|---------|
| `internal/raft/` | Raft consensus implementation |
| `internal/adminui/` | Admin UI dashboard components |
| `internal/cql/` | CQL query engine and parser |
| `internal/encoding/` | Compression codecs (Gorilla, Delta, Dictionary) |
| `internal/cluster/` | Cluster coordination |
| `internal/anomaly/` | Anomaly detection |
| `internal/cep/` | Complex event processing |
| `internal/edgemesh/` | Edge mesh networking |
| `internal/continuousquery/` | Continuous query engine |

### Bridge Files (thin wrappers connecting internal → root)
`raft_bridge.go`, `admin_ui_bridge.go`, `cluster_bridge.go`, `anomaly_bridge.go`,
`continuous_queries_bridge.go`, `cql_bridge.go`, `pgwire_bridge.go`,
`streaming_etl_bridge.go`, `parquet_bridge.go`

### Candidates for Future Extraction

**Phase 1 — Storage Backends (low risk)**
Move to `chronicle/storage/`:
- `storage_backend_file.go`
- `storage_backend_memory.go`
- `storage_backend_s3.go`
- `storage_backend_tiered.go`
- `storage_backend_interface.go` (defines `StorageBackend` interface)

**Phase 2 — HTTP Layer (medium risk)**
Move to `chronicle/http/` or `internal/http/`:
- `http_core.go`, `http_helpers.go`, `http_server.go`
- `http_routes_*.go` (5 files)

**Phase 3 — Experimental Features (medium risk)**
Move to `chronicle/x/` with DB interface:
- `federated_learning.go` → `chronicle/x/federatedml/`
- `blockchain_audit.go` → requires DB interface extraction first
- `tinyml.go` → requires DB interface extraction first

**Phase 4 — Feature Engines (high risk, requires DB interface)**
Requires defining a `chronicle.Engine` interface:
```go
type Engine interface {
    Start() error
    Stop() error
}
```
Then 44+ engine types can implement it uniformly.

## Guidelines for New Code

1. **New features** should be added as `internal/` packages with bridge files
2. **New engines** should implement the `Engine` interface when it exists
3. **New configs** should be added to `FeatureManagerConfig` if feature-specific
4. **Test helpers** go in `internal/testutil/`

## Current Progress (v0.1.0)

### Completed
- ✅ `FeatureRegistry` created (`feature_registry.go`) with `Feature`, `StartableFeature`, `HTTPFeature` interfaces
- ✅ 5 core features bridged to registry: CQL, Observability, AnomalyPipeline, QueryPlanner, Dashboard
- ✅ `safeInit()` wrapper prevents nil-pointer panics in FeatureManager
- ✅ 19 internal packages extracted
- ✅ `STABLE_API.md` documents the ~20 core types users should depend on
- ✅ `FEATURE_MATURITY.md` honestly labels stubs

### Next Steps (v0.2.0)
1. Migrate all remaining FeatureManager features to FeatureRegistry
2. Extract query parsers (PromQL, CQL, SQL) to `internal/query/` sub-packages
3. Extract HTTP routes to `internal/http/` package
4. Reduce root package to <50 exported types via type aliases
5. Add CI check: fail if new exported types added to root without approval
