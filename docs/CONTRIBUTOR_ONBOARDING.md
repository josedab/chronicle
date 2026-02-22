# Contributor Onboarding Guide

Welcome to Chronicle! This guide will help you make your first contribution in under 30 minutes.

## Quick Start (5 minutes)

```bash
# Clone and build
git clone https://github.com/chronicle-db/chronicle.git
cd chronicle
make setup           # Install development tools
make check           # Run quick validation (~15s)

# Verify everything works
go test -short ./... # Full test suite (~30s)
```

## Architecture Overview (5 minutes)

```
┌─────────────────────────────────────────────────────────────┐
│                        API Layer                            │
│  Write() → PointValidator → WriteHooks → Schema → Buffer   │
│  Execute() → QueryMiddleware → Partitions → Aggregate       │
│  HTTP → /write, /query, /health, /admin, /api/v1/*          │
├─────────────────────────────────────────────────────────────┤
│                     Core Engine                              │
│  WriteBuffer → WAL → Flush → Partition → DataStore          │
│  Index (B-tree) → Partition Lookup → Query Execution         │
├─────────────────────────────────────────────────────────────┤
│                   Storage Backends                           │
│  FileBackend │ MemoryBackend │ S3Backend │ TieredBackend    │
├─────────────────────────────────────────────────────────────┤
│                  Feature Manager (121 features)              │
│  Lazy-initialized via sync.Once │ Registered in 4 files     │
│  feature_manager.go │ db_features.go │ http_routes_nextgen  │
│  api_stability.go                                            │
└─────────────────────────────────────────────────────────────┘
```

### Key Files to Know

| File | Purpose | Lines |
|------|---------|-------|
| `db_core.go` | Database lifecycle: Open, Close, background workers | ~200 |
| `db_write.go` | Write path: validation → hooks → schema → buffer → WAL | ~180 |
| `query.go` | Query path: middleware → partitions → aggregate → limit | ~150 |
| `config.go` | Configuration: all settings with validation | ~400 |
| `feature_manager.go` | Feature registry: 121 lazy-init feature accessors | ~1500 |
| `point.go` | Core types: Point, Result, Series, SeriesKey | ~120 |

### How to Add a New Feature

1. **Create** `my_feature.go` with Config, DefaultConfig, Engine, New, Start/Stop, RegisterHTTPHandlers
2. **Create** `my_feature_test.go` using `setupTestDB(t)`
3. **Wire** into `feature_manager.go` (struct field + sync.Once + accessor method)
4. **Wire** into `db_features.go` (DB accessor method)
5. **Wire** into `http_routes_nextgen.go` (add to registerFeatureRoutes)
6. **Wire** into `api_stability.go` (add to ExperimentalAPI)
7. **Test**: `go test -run TestMyFeature -v`

## Mentored Areas

These areas actively welcome contributions:

### 🟢 Easy (Good First Issues)
- Add more example queries to the playground (`website/src/components/Playground/datasets.ts`)
- Improve documentation in `docs/` directory
- Add test cases for edge conditions in existing features
- Fix typos and improve code comments

### 🟡 Medium
- Implement new aggregation functions (percentile, mode, variance)
- Add tag filter operators (NOT IN, regex patterns)
- Improve error messages with context
- Add Prometheus metric exposition for internal DB stats

### 🔴 Advanced
- Optimize the B-tree index for high-cardinality tag lookups
- Implement true partition pruning based on tag indexes
- Add native protobuf encoding for the wire protocol
- Implement distributed query coordination over real network

## Development Workflow

```bash
# Before committing
make check              # Quick validation (15s)

# Before pushing
make quickcheck         # Full validation with all tests

# For performance work
make bench              # Run benchmarks
make benchmark          # Run + save for comparison

# For coverage
make cover              # Print coverage summary
make cover-report       # HTML coverage report
```

## Code Conventions

- **Naming**: `DefaultXConfig()` for defaults, `NewX(db, config)` for constructors, `WithY()` for builders
- **Concurrency**: Always use `sync.RWMutex`, never bare maps in feature engines
- **Tests**: Use `setupTestDB(t)` + `defer db.Close()`, subtests with `t.Run()`
- **Errors**: Return `fmt.Errorf("context: %w", err)`, never panic
- **Comments**: Only where logic is non-obvious. No boilerplate comments.

## Getting Help

- **Issues**: Use GitHub Issues with the `question` label
- **Discussions**: GitHub Discussions for design proposals
- **Security**: Email chronicle-security@googlegroups.com (48h SLA)

## Recognition

All contributors are recognized in:
- Git commit history (Co-authored-by trailer)
- CHANGELOG.md release notes
- GitHub contributor graphs
