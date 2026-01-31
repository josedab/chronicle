# ADR-0012: Embedded Database with Optional HTTP Server

## Status

Accepted

## Context

Chronicle needed to serve multiple deployment models:

1. **Embedded library**: Applications that want direct database access without network overhead (IoT devices, edge applications, CLI tools)
2. **Standalone server**: Traditional database server accessed over HTTP by multiple clients
3. **Sidecar pattern**: Kubernetes pods running Chronicle alongside application containers

Each model has different requirements:

| Deployment | Network | Latency | Isolation | Resource Control |
|------------|---------|---------|-----------|------------------|
| Embedded | None | Microseconds | Process | Shared |
| Standalone | Required | Milliseconds | Network | Dedicated |
| Sidecar | Localhost | Microseconds | Container | Limited |

We evaluated several architectural approaches:

- **Server-only** (like InfluxDB, TimescaleDB): Requires network stack, harder to embed
- **Library-only** (like SQLite): No remote access, requires wrapper for server mode
- **Dual-mode** (like BadgerDB/Dgraph): Single codebase, optional server layer

## Decision

Chronicle implements an **embedded-first architecture with an optional HTTP server layer**:

### Core Library (Always Available)

```go
// Direct embedded usage - no HTTP required
db, err := chronicle.Open("/path/to/data.db", chronicle.Config{
    MaxMemory: 64 * 1024 * 1024,
})

// Write directly
db.Write(chronicle.Point{
    Metric: "temperature",
    Tags:   map[string]string{"sensor": "living_room"},
    Value:  22.5,
})

// Query directly
points, err := db.Query(chronicle.Query{
    Metric: "temperature",
    Start:  time.Now().Add(-1 * time.Hour).UnixNano(),
    End:    time.Now().UnixNano(),
})
```

### Optional HTTP Server

```go
// Enable HTTP server via configuration
db, err := chronicle.Open("/path/to/data.db", chronicle.Config{
    HTTPEnabled: true,
    HTTPPort:    8086,
    // Optional features enabled on demand
    PrometheusRemoteWriteEnabled: true,
    GraphQLEnabled:               true,
})
```

### Feature Manager Pattern

Advanced features are encapsulated in a `FeatureManager` to keep the core minimal:

```go
type FeatureManager struct {
    exemplarStore      *ExemplarStore      // Prometheus exemplars
    histogramStore     *HistogramStore     // Native histograms
    cardinalityTracker *CardinalityTracker // Series cardinality limits
    alertManager       *AlertManager       // Alerting engine
    schemaRegistry     *SchemaRegistry     // Schema validation
}

// Features initialized only when configured
func (db *DB) initFeatures() {
    if db.config.Exemplars.Enabled {
        db.features.exemplarStore = NewExemplarStore(db.config.Exemplars)
    }
    // ... other features
}
```

### Dependency Management

The core `DB` type has minimal dependencies:
- Standard library only for embedded mode
- HTTP server imports `net/http` (standard library)
- Protocol-specific features import their dependencies only when enabled

```
chronicle (core)
├── No external dependencies for basic usage
├── +net/http (when HTTPEnabled)
├── +gorilla/websocket (when streaming enabled)
├── +prometheus/prometheus (when Prometheus enabled)
└── +aws-sdk-go-v2 (when S3 backend used)
```

## Consequences

### Positive

- **Minimal footprint**: Embedded usage has no network dependencies
- **Single codebase**: Same code runs embedded or as server
- **Incremental adoption**: Start embedded, add HTTP later without migration
- **Resource efficiency**: No HTTP overhead when not needed
- **Testing simplicity**: Unit tests use embedded mode without network setup
- **WASM compatibility**: Core can compile to WebAssembly (no network)

### Negative

- **API surface complexity**: Two ways to access (direct + HTTP)
- **Feature discoverability**: Optional features less obvious
- **Documentation burden**: Must document both embedded and server usage
- **Initialization complexity**: Feature manager adds startup logic

### Usage Patterns

**Pattern 1: Pure Embedded (IoT/Edge)**
```go
db, _ := chronicle.Open("metrics.db", chronicle.DefaultConfig())
defer db.Close()
// Direct API calls only
```

**Pattern 2: Standalone Server**
```go
db, _ := chronicle.Open("metrics.db", chronicle.Config{
    HTTPEnabled: true,
    HTTPPort:    8086,
    Auth:        &chronicle.AuthConfig{Enabled: true, APIKeys: []string{"..."}},
})
// Block forever, serve HTTP
select {}
```

**Pattern 3: Hybrid (Application + Sidecar Export)**
```go
db, _ := chronicle.Open("metrics.db", chronicle.Config{
    HTTPEnabled: true,
    HTTPPort:    8086,
})
// Application writes directly (low latency)
db.Write(point)
// Prometheus scrapes HTTP endpoint
```

**Pattern 4: Library with Custom Server**
```go
db, _ := chronicle.Open("metrics.db", chronicle.DefaultConfig())
// Application wraps DB in custom HTTP/gRPC server
myServer := NewCustomServer(db)
myServer.ListenAndServe()
```

### Binary Size Impact

| Configuration | Approximate Binary Size |
|--------------|------------------------|
| Core only | ~8 MB |
| + HTTP server | ~10 MB |
| + Prometheus support | ~15 MB |
| + All features | ~25 MB |

(Sizes are approximate and depend on Go version and build flags)

### Enabled

- Embedding in resource-constrained environments
- WebAssembly compilation for browser-based analytics
- Custom protocol wrappers (gRPC, etc.)
- Zero-network-dependency testing
- Gradual feature adoption

### Prevented

- Forced network dependency
- Mandatory authentication setup for local use
- Heavy startup cost when features not needed
- Tight coupling between storage and transport layers
