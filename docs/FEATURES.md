# Chronicle Features Guide

This document provides detailed information about Chronicle's features and how to use them.

## Table of Contents

- [Core Features](#core-features)
  - [Write Operations](#write-operations)
  - [Query Operations](#query-operations)
  - [Retention Policies](#retention-policies)
  - [Downsampling](#downsampling)
- [Storage Backends](#storage-backends)
  - [File Backend](#file-backend)
  - [Memory Backend](#memory-backend)
  - [S3 Backend](#s3-backend)
  - [Tiered Backend](#tiered-backend)
- [Query Languages](#query-languages)
  - [SQL-Like Queries](#sql-like-queries)
  - [PromQL Support](#promql-support)
- [Enterprise Features](#enterprise-features)
  - [Schema Registry](#schema-registry)
  - [Multi-Tenancy](#multi-tenancy)
  - [Encryption at Rest](#encryption-at-rest)
  - [Alerting Engine](#alerting-engine)
  - [Streaming API](#streaming-api)
- [Integrations](#integrations)
  - [HTTP API](#http-api)
  - [Prometheus Remote Write](#prometheus-remote-write)
  - [OpenTelemetry OTLP](#opentelemetry-otlp)
  - [Grafana Plugin](#grafana-plugin)
  - [WebAssembly](#webassembly)

---

## Core Features

### Write Operations

#### Single Point Write

```go
err := db.Write(chronicle.Point{
    Metric:    "cpu_usage",
    Tags:      map[string]string{"host": "server-01", "region": "us-west"},
    Value:     45.7,
    Timestamp: time.Now().UnixNano(),
})
```

#### Batch Write

For higher throughput, use batch writes:

```go
points := []chronicle.Point{
    {Metric: "cpu", Tags: map[string]string{"host": "a"}, Value: 45.0},
    {Metric: "cpu", Tags: map[string]string{"host": "b"}, Value: 52.0},
    {Metric: "mem", Tags: map[string]string{"host": "a"}, Value: 78.5},
}
err := db.WriteBatch(points)
```

**Performance tip**: Batch writes are ~10x faster than individual writes.

### Query Operations

#### Basic Query

```go
result, err := db.Execute(&chronicle.Query{
    Metric: "cpu_usage",
    Tags:   map[string]string{"host": "server-01"},
    Start:  time.Now().Add(-1 * time.Hour).UnixNano(),
    End:    time.Now().UnixNano(),
})

for _, point := range result.Points {
    fmt.Printf("%v: %.2f\n", point.Timestamp, point.Value)
}
```

#### Aggregation Query

```go
result, err := db.Execute(&chronicle.Query{
    Metric: "cpu_usage",
    Aggregation: &chronicle.Aggregation{
        Function: chronicle.AggMean,
        Window:   5 * time.Minute,
    },
    GroupBy: []string{"host"},
})
```

**Supported aggregation functions:**

| Function | Description |
|----------|-------------|
| `AggCount` | Count of points |
| `AggSum` | Sum of values |
| `AggMean` | Average value |
| `AggMin` | Minimum value |
| `AggMax` | Maximum value |
| `AggFirst` | First value in window |
| `AggLast` | Last value in window |
| `AggStddev` | Standard deviation |
| `AggPercentile` | Percentile calculation |
| `AggRate` | Rate of change |

### Retention Policies

Configure automatic data cleanup:

```go
cfg := chronicle.Config{
    RetentionDuration: 7 * 24 * time.Hour,  // Keep 7 days
    MaxStorageBytes:   1 * 1024 * 1024 * 1024,  // Max 1GB
}
```

Data older than `RetentionDuration` is automatically deleted during compaction.

### Downsampling

Reduce storage by pre-aggregating historical data:

```go
cfg := chronicle.Config{
    Downsample: []chronicle.DownsampleConfig{
        {
            After:    24 * time.Hour,
            Window:   5 * time.Minute,
            Function: chronicle.AggMean,
        },
        {
            After:    7 * 24 * time.Hour,
            Window:   1 * time.Hour,
            Function: chronicle.AggMean,
        },
    },
}
```

---

## Storage Backends

### File Backend

Default backend using local filesystem:

```go
backend, err := chronicle.NewFileBackend("/var/lib/chronicle/data")
```

### Memory Backend

In-memory storage for testing and WASM:

```go
backend := chronicle.NewMemoryBackend()
```

**Note**: Data is lost when the process exits.

### S3 Backend

Store partitions in Amazon S3 or compatible services:

```go
backend, err := chronicle.NewS3Backend(chronicle.S3BackendConfig{
    Bucket:          "my-chronicle-bucket",
    Region:          "us-west-2",
    Prefix:          "chronicle/",
    AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
    SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
    CacheSize:       100,  // Cache 100 partitions locally
})
```

**For S3-compatible services (MinIO, etc.):**

```go
backend, err := chronicle.NewS3Backend(chronicle.S3BackendConfig{
    Bucket:       "chronicle",
    Region:       "us-east-1",
    Endpoint:     "http://localhost:9000",
    UsePathStyle: true,
})
```

### Tiered Backend

Automatically tier data between hot and cold storage:

```go
hot, _ := chronicle.NewFileBackend("/fast-ssd/chronicle")
cold, _ := chronicle.NewS3Backend(s3Config)

backend := chronicle.NewTieredBackend(hot, cold, 24*time.Hour)
// Data older than 24h moves to cold storage
```

---

## Query Languages

### SQL-Like Queries

Chronicle supports a SQL-like query syntax:

```go
parser := &chronicle.QueryParser{}
query, err := parser.Parse(`
    SELECT mean(value) 
    FROM cpu_usage 
    WHERE host = 'server-01' AND region = 'us-west'
    GROUP BY time(5m), host
    LIMIT 100
`)
result, err := db.Execute(query)
```

**Supported syntax:**

```sql
SELECT <aggregation>(<field>)
FROM <metric>
WHERE <tag> = '<value>'
  AND <tag> != '<value>'
  AND <tag> IN ('<v1>', '<v2>')
  AND time > now() - <duration>
  AND time < now()
GROUP BY time(<window>), <tag1>, <tag2>
LIMIT <n>
```

### PromQL Support

Chronicle supports a subset of PromQL for Prometheus compatibility:

```go
parser := &chronicle.PromQLParser{}
query, err := parser.Parse(`sum(rate(http_requests_total{status="200"}[5m])) by (method)`)
chronicleQuery := query.ToChronicleQuery(start, end)
result, err := db.Execute(chronicleQuery)
```

**Supported PromQL features:**

| Feature | Example | Supported |
|---------|---------|-----------|
| Instant selector | `http_requests_total` | ✅ |
| Label matching | `{method="GET"}` | ✅ |
| Not-equal matching | `{status!="500"}` | ✅ |
| Regex matching | `{path=~"/api/.*"}` | ⚠️ Parsed, not filtered |
| Range selector | `[5m]` | ✅ |
| Aggregations | `sum`, `avg`, `min`, `max`, `count` | ✅ |
| `by` clause | `sum by (method)` | ✅ |
| `without` clause | `sum without (instance)` | ✅ |
| `rate()` | `rate(counter[5m])` | ✅ |

---

## Enterprise Features

### Schema Registry

Define and enforce schemas for metrics:

```go
registry := chronicle.NewSchemaRegistry(true)  // strict mode

err := registry.Register(chronicle.MetricSchema{
    Name: "temperature",
    Tags: []chronicle.TagSchema{
        {Name: "sensor_id", Required: true},
        {Name: "location", AllowedVals: []string{"indoor", "outdoor"}},
    },
    Fields: []chronicle.FieldSchema{
        {Name: "value", MinValue: ptr(-50.0), MaxValue: ptr(100.0)},
    },
})

// Points are validated before writing
db.RegisterSchema(schema)
```

### Multi-Tenancy

Isolate data by tenant:

```go
tm := chronicle.NewTenantManager(db, chronicle.TenantConfig{
    Enabled: true,
})

// Get tenant-scoped database
tenant, err := tm.GetTenant("customer-123")

// Writes are automatically tagged with __tenant__
tenant.Write(point)

// Queries are automatically filtered
result, err := tenant.Query(query)
```

### Encryption at Rest

Enable AES-256-GCM encryption:

```go
cfg := chronicle.Config{
    Encryption: &chronicle.EncryptionConfig{
        Enabled:     true,
        KeyPassword: os.Getenv("CHRONICLE_KEY"),
    },
}
```

The key is derived using PBKDF2 with a random salt stored in the database header.

### Alerting Engine

Create threshold-based alerts:

```go
am := chronicle.NewAlertManager(db)

am.AddRule(chronicle.AlertRule{
    Name:        "high_cpu",
    Metric:      "cpu_usage",
    Condition:   chronicle.ConditionAbove,
    Threshold:   90.0,
    ForDuration: 5 * time.Minute,
    Labels:      map[string]string{"severity": "critical"},
    Webhooks:    []string{"https://alerts.example.com/webhook"},
})

am.Start(context.Background())
```

### Streaming API

Subscribe to real-time data:

```go
hub := chronicle.NewStreamHub(db, chronicle.DefaultStreamConfig())

// Subscribe to a metric
sub := hub.Subscribe("cpu_usage", map[string]string{"host": "server-01"})

go func() {
    for point := range sub.C {
        fmt.Printf("Real-time: %v = %.2f\n", point.Metric, point.Value)
    }
}()

// Points written are automatically published
db.Write(point)  // sub.C receives this
```

---

## Integrations

### HTTP API

Enable the built-in HTTP server:

```go
cfg := chronicle.Config{
    HTTPEnabled: true,
    HTTPPort:    8086,
}
```

See [API.md](./API.md) for endpoint documentation.

### Prometheus Remote Write

Chronicle accepts Prometheus remote write format:

```yaml
# prometheus.yml
remote_write:
  - url: http://localhost:8086/api/v1/prom/write
```

### OpenTelemetry OTLP

Send metrics via OTLP JSON:

```bash
curl -X POST http://localhost:8086/v1/metrics \
  -H "Content-Type: application/json" \
  -d @metrics.json
```

### Grafana Plugin

A Grafana data source plugin is included:

```bash
cd grafana-plugin
npm install
npm run build
# Copy dist/ to your Grafana plugins directory
```

### WebAssembly

Chronicle compiles to WebAssembly for browser use:

```bash
GOOS=js GOARCH=wasm go build -o chronicle.wasm ./wasm
```

```html
<script src="wasm_exec.js"></script>
<script>
const go = new Go();
WebAssembly.instantiateStreaming(fetch("chronicle.wasm"), go.importObject)
    .then(result => go.run(result.instance));

// Use the global chronicle object
await chronicle.open("demo.db");
await chronicle.write("cpu", 45.5, {host: "browser"});
const results = await chronicle.query("SELECT mean(value) FROM cpu");
</script>
```

---

## Analytics Features

### Time-Series Forecasting

Chronicle includes built-in forecasting with multiple algorithms:

```go
// Create forecaster with Holt-Winters (triple exponential smoothing)
config := chronicle.ForecastConfig{
    Method:          chronicle.ForecastMethodHoltWinters,
    SeasonalPeriods: 12,  // Monthly seasonality
    Alpha:           0.5, // Level smoothing
    Beta:            0.1, // Trend smoothing
    Gamma:           0.1, // Seasonal smoothing
    AnomalyThreshold: 3.0, // Standard deviations
}
forecaster := chronicle.NewForecaster(config)

// Prepare historical data
data := chronicle.TimeSeriesData{
    Timestamps: timestamps,
    Values:     values,
}

// Generate forecast
result, err := forecaster.Forecast(data, 10) // Predict 10 periods
for _, p := range result.Predictions {
    fmt.Printf("T=%d: %.2f [%.2f, %.2f]\n", 
        p.Timestamp, p.Value, p.LowerBound, p.UpperBound)
}

// Detect anomalies
anomalies, _ := forecaster.DetectAnomalies(data)
for _, a := range anomalies {
    fmt.Printf("Anomaly at %d: actual=%.2f, expected=%.2f (score=%.2f)\n",
        a.Timestamp, a.ActualValue, a.ExpectedValue, a.Score)
}
```

Available methods:
- `ForecastMethodHoltWinters` - Triple exponential with seasonality
- `ForecastMethodDoubleExponential` - Holt's method with trend
- `ForecastMethodSimpleExponential` - Basic smoothing
- `ForecastMethodMovingAverage` - Simple moving average

### Recording Rules

Pre-compute expensive queries on a schedule:

```go
engine := chronicle.NewRecordingRulesEngine(db)

// Add a rule to pre-compute hourly averages
engine.AddRule(chronicle.RecordingRule{
    Name:         "cpu_hourly_avg",
    Query:        "SELECT mean(value) FROM cpu GROUP BY time(1h), host",
    TargetMetric: "cpu:hourly:avg",
    Interval:     time.Minute,
    Labels:       map[string]string{"source": "recording_rule"},
    Enabled:      true,
})

// Start the evaluation loop
ctx := context.Background()
engine.Start(ctx)

// View stats
for _, stat := range engine.Stats() {
    fmt.Printf("Rule %s: %d evals, %d errors\n", 
        stat.Name, stat.EvalCount, stat.ErrorCount)
}
```

---

## Native Histograms

Prometheus-compatible native histograms with exponential bucketing:

```go
// Create histogram
h := chronicle.NewHistogram(3) // Schema 3 for fine granularity

// Record observations
h.Observe(0.5)
h.Observe(1.2)
h.Observe(3.7)

// Query quantiles
p50 := h.Quantile(0.5)
p99 := h.Quantile(0.99)

// Store in database
store := chronicle.NewHistogramStore(db)
store.Write(chronicle.HistogramPoint{
    Metric:    "request_duration",
    Tags:      map[string]string{"endpoint": "/api"},
    Histogram: h,
    Timestamp: time.Now().UnixNano(),
})
```

---

## Exemplars

Link metrics to distributed traces:

```go
store := chronicle.NewExemplarStore(db, chronicle.DefaultExemplarConfig())

// Write metric with exemplar
store.Write(chronicle.ExemplarPoint{
    Metric:    "http_requests_total",
    Tags:      map[string]string{"method": "GET"},
    Value:     1,
    Timestamp: time.Now().UnixNano(),
    Exemplar: &chronicle.Exemplar{
        Labels:    map[string]string{"trace_id": "abc123", "span_id": "def456"},
        Value:     0.25,
        Timestamp: time.Now().UnixNano(),
    },
})

// Query exemplars by trace ID
exemplars, _ := store.QueryByTraceID("abc123")
```

---

## Cardinality Management

Track and limit series cardinality:

```go
tracker := chronicle.NewCardinalityTracker(chronicle.CardinalityConfig{
    MaxSeriesPerMetric:   10000,
    MaxTotalSeries:       100000,
    AlertThresholdPct:    90,
})

// Track points
tracker.TrackPoint(point)

// Get stats
stats := tracker.Stats()
fmt.Printf("Total series: %d, Alerts: %d\n", 
    stats.TotalSeries, stats.AlertCount)

// Check alerts
for _, alert := range tracker.GetAlerts() {
    fmt.Printf("ALERT: %s - %s\n", alert.Metric, alert.Message)
}
```

---

## Backup & Recovery

Full and incremental backups:

```go
bm, _ := chronicle.NewBackupManager(db, chronicle.BackupConfig{
    DestinationPath:    "/backups",
    Compression:        true,
    RetentionCount:     10,
    IncrementalEnabled: true,
})

// Full backup
result, _ := bm.FullBackup()
fmt.Printf("Full backup: %s (%d bytes)\n", result.Record.ID, result.Record.Size)

// Incremental backup (only changes since last backup)
result, _ = bm.IncrementalBackup()

// List backups
for _, b := range bm.ListBackups() {
    fmt.Printf("%s: %s (%d bytes)\n", b.Type, b.Timestamp, b.Size)
}

// Restore latest
bm.RestoreLatest()
```

---

## GraphQL API

Flexible querying via GraphQL:

```go
server := chronicle.NewGraphQLServer(db)
http.Handle("/graphql", server.Handler())
http.Handle("/graphql/playground", server.ServePlayground())
```

Example queries:

```graphql
# List metrics
{ metrics }

# Query data
{
  points(metric: "cpu", start: "1h", end: "now") {
    timestamp
    value
    tags { key value }
  }
}

# Get stats
{ stats { metrics uptime version } }
```

---

## Admin UI

Web-based administration dashboard:

```go
admin := chronicle.NewAdminUI(db, chronicle.AdminConfig{
    Prefix: "/admin",
})
http.Handle("/admin", admin)
http.Handle("/admin/", admin)
```

Features:
- System stats and memory usage
- Metric list and exploration
- Interactive query explorer
- Configuration view
- Health status

---

## Query Federation

Query across multiple Chronicle instances:

```go
fed := chronicle.NewFederation(db, chronicle.FederationConfig{
    Timeout:              30 * time.Second,
    MaxConcurrentQueries: 10,
    MergeStrategy:        chronicle.MergeStrategyUnion,
})

// Add remote instances
fed.AddRemote("dc-east", "http://chronicle-east:8086", 1)
fed.AddRemote("dc-west", "http://chronicle-west:8086", 2)

// Federated query
result, _ := fed.Query(ctx, &chronicle.Query{
    Metric: "cpu",
    Start:  time.Now().Add(-1*time.Hour).UnixNano(),
    End:    time.Now().UnixNano(),
})

fmt.Printf("Results from %d sources: %d points\n", 
    len(result.Sources), len(result.Points))
```

---

## Data Export

Export time-series data to analytics-friendly formats:

```go
// Export to CSV
result, err := db.ExportToCSV("/backups/data.csv", nil, 0, 0)
fmt.Printf("Exported %d points (%d bytes)\n", result.PointsExported, result.BytesWritten)

// Export to JSON lines
result, _ = db.ExportToJSON("/backups/data.jsonl", []string{"cpu", "memory"}, start, end)

// Export to Parquet-like format
result, _ = db.ExportToParquet("/backups/data.parquet", nil, 0, 0)

// Advanced configuration
config := chronicle.DefaultExportConfig()
config.Format = chronicle.ExportFormatCSV
config.Compression = true
config.TimestampFormat = time.RFC3339
config.OutputPath = "/backups/export.csv.gz"

exporter := chronicle.NewExporter(db, config)
result, _ = exporter.Export()
```

Supported formats:
- **CSV** - Comma-separated values with headers
- **JSON** - JSON lines (one object per line)
- **Parquet** - Columnar format for analytics tools

---

## Vector Embeddings

Store and search ML embeddings alongside time-series:

```go
// Create vector store
vs := chronicle.NewVectorStore(db, chronicle.DefaultVectorConfig())

// Store embeddings
vs.Write(chronicle.VectorPoint{
    Metric:    "document_embeddings",
    Tags:      map[string]string{"doc_id": "123"},
    Vector:    []float32{0.1, 0.2, 0.3, ...}, // 768-dim BERT embedding
    Timestamp: time.Now().UnixNano(),
    Metadata:  map[string]string{"title": "Example Document"},
})

// Semantic search - find k nearest neighbors
results, _ := vs.Search(
    queryVector,     // Your query embedding
    10,              // Top 10 results
    "document_embeddings",
    nil,             // Optional tag filter
)

for _, r := range results {
    fmt.Printf("ID: %s, Score: %.3f\n", r.ID, r.Score)
}

// Configure distance metric
config := chronicle.DefaultVectorConfig()
config.DistanceMetric = chronicle.DistanceCosine     // Default
config.DistanceMetric = chronicle.DistanceEuclidean  // L2 distance
config.DistanceMetric = chronicle.DistanceDotProduct // For normalized vectors
config.NormalizeVectors = true                       // Auto-normalize on insert
```

---

## Query Assistant

Natural language to SQL/PromQL translation:

```go
qa := chronicle.NewQueryAssistant(db, chronicle.DefaultAssistantConfig())

// Translate natural language to query
result, _ := qa.Translate(ctx, "average CPU usage in the last hour")
// result.Query = "SELECT mean(value) FROM cpu WHERE time > now() - 1h GROUP BY time(5m)"
// result.QueryType = "sql"
// result.Confidence = 0.9

// Get query explanation
explanation := qa.Explain("SELECT max(value) FROM memory")
// "This query finds the maximum value of the memory metric."

// Get suggested queries based on available data
suggestions := qa.Suggest()
for _, s := range suggestions {
    fmt.Printf("%s: %s\n", s.Description, s.Query)
}

// With LLM backend (OpenAI/Anthropic)
config := chronicle.DefaultAssistantConfig()
config.Provider = "openai"
config.APIKey = os.Getenv("OPENAI_API_KEY")
config.Model = "gpt-4"
qa = chronicle.NewQueryAssistant(db, config)
```

Supported natural language patterns:
- "show all metrics"
- "average of cpu in last 1 hour"
- "max memory over the last 24 hours"
- "count requests where value > 1000"
- "rate of http_requests"
- "top 10 cpu by host"

---

## Continuous Profiling

Store and correlate pprof profiles with metrics:

```go
ps := chronicle.NewProfileStore(db, chronicle.DefaultProfileConfig())

// Store a CPU profile
ps.Write(chronicle.Profile{
    Type:        chronicle.ProfileTypeCPU,
    Labels:      map[string]string{"service": "api", "version": "v1"},
    Data:        pprofData, // Raw pprof bytes
    Duration:    30 * time.Second,
    SampleCount: 1000,
})

// Query profiles by time range
profiles, _ := ps.Query(chronicle.ProfileTypeCPU, nil, start, end)

// Correlate profiles with metric spikes
// "Show me profiles from when CPU was above 90%"
correlations, _ := ps.QueryByMetricCondition(
    "cpu_usage",           // Metric to check
    90.0,                  // Threshold
    chronicle.ProfileTypeCPU,
    start, end,
)

for _, c := range correlations {
    fmt.Printf("CPU at %.1f%% - Profile: %s\n", c.MetricPoint.Value, c.Profile)
}

// Get and decompress a specific profile
profile, _ := ps.GetProfile("profile-id-123")
data, _ := profile.Decompress()
```

Profile types:
- `ProfileTypeCPU` - CPU time
- `ProfileTypeHeap` - Memory allocations
- `ProfileTypeGoroutine` - Goroutine stacks
- `ProfileTypeMutex` - Mutex contention
- `ProfileTypeBlock` - Blocking operations
- `ProfileTypeAllocs` - Allocation counts

---

## See Also

- [README](../README.md) - Quick start
- [ARCHITECTURE](./ARCHITECTURE.md) - System design
- [CONFIGURATION](./CONFIGURATION.md) - All options
- [API](./API.md) - HTTP endpoints

---

## Next-Gen Features (v0.5.0)

### Chronicle Query Language (CQL)

A purpose-built query language unifying SQL, PromQL, and time-series operations.

```go
engine := chronicle.NewCQLEngine(db, chronicle.DefaultCQLConfig())

// Execute a CQL query
result, err := engine.Execute(ctx, `
    SELECT avg(value) FROM cpu_usage 
    WHERE host = 'server1' 
    WINDOW 5m 
    GAP_FILL linear
`)

// Validate syntax
err = engine.Validate("SELECT value FROM temperature LAST 1h")

// Explain query plan
plan, err := engine.Explain("SELECT sum(value) FROM requests GROUP BY host WINDOW 1m")
```

**CQL-specific features:**
- `WINDOW` clause for time-based windowed aggregation
- `GAP_FILL` with linear interpolation, previous value, or zero fill
- `ALIGN` for calendar-aware bucketing
- `ASOF JOIN` for temporal joins with tolerance
- `LAST` shorthand for recent time ranges
- `rate()`, `delta()`, `increase()` functions

**HTTP API:**
- POST `/api/v1/cql` — Execute CQL query
- POST `/api/v1/cql/validate` — Validate CQL syntax
- POST `/api/v1/cql/explain` — Show query plan

### Streaming ETL Pipelines

Declarative stream processing with fluent API for transforming time-series data in-flight.

```go
pipeline := chronicle.NewETLPipeline(chronicle.DefaultETLPipelineConfig())
pipeline.
    From(chronicle.NewETLChannelSource(inputCh)).
    Filter(func(p *chronicle.Point) bool { return p.Value > 0 }).
    Transform(func(p *chronicle.Point) (*chronicle.Point, error) {
        p.Tags["processed"] = "true"
        return p, nil
    }).
    Aggregate(chronicle.ETLAggregateConfig{
        Window:   time.Minute,
        Function: chronicle.AggMean,
        GroupBy:  []string{"host"},
    }).
    To(chronicle.NewETLDatabaseSink(db))

pipeline.Start()
defer pipeline.Stop()
```

**Features:**
- Fluent pipeline builder API
- Built-in sources: database polling, Go channels
- Built-in sinks: database writer, channels, multi-sink fan-out
- Windowed aggregation with configurable emit modes
- Backpressure strategies: drop, block, or sample
- JSON-based checkpointing for crash recovery
- Pipeline registry for managing multiple pipelines

### Adaptive Tiered Storage

Intelligent data lifecycle management that auto-migrates partitions between storage tiers based on access patterns.

```go
config := chronicle.DefaultAdaptiveTieredConfig()
backend, err := chronicle.NewAdaptiveTieredBackend(config)

stats := backend.Stats() // partition distribution across tiers
```

**Storage tiers:** Hot → Warm → Cold → Archive

**Features:**
- Per-partition access pattern tracking with recency-weighted scoring
- Background migration engine with configurable cooldown periods
- Cost optimizer with monthly budget tracking and recommendations
- Cost projection for capacity planning

### Distributed Consensus Hardening

Production-grade extensions to the Raft consensus implementation.

```go
// Snapshot management
snapMgr := chronicle.NewSnapshotManager(chronicle.DefaultSnapshotManagerConfig())
snapshot, err := snapMgr.CreateSnapshot(lastIndex, lastTerm, stateData)
snapMgr.PruneSnapshots() // keep only N most recent

// Log compaction
compactor := chronicle.NewLogCompactor(raftLog, snapMgr, chronicle.DefaultLogCompactorConfig())
compactor.Start()

// Two-phase membership changes
jc := chronicle.NewJointConsensus(chronicle.DefaultJointConsensusConfig())
transition, err := jc.ProposeChange(change)
err = jc.CommitTransition(transition.ID)

// Correctness verification
verifier := chronicle.NewConsensusVerifier()
err = verifier.VerifyLinearizability()
err = verifier.VerifyElectionSafety()
```

### Hybrid Vector + Time-Series Index

Composite index combining temporal B-tree partitioning with HNSW vector search.

```go
index := chronicle.NewTemporalPartitionedIndex(chronicle.DefaultHybridIndexConfig())

// Insert points with vector embeddings
index.Insert(&chronicle.HybridPoint{
    ID: "p1", Vector: embedding, Timestamp: ts,
    Metric: "anomaly_score", Value: 0.95,
})

// Search: "find similar anomaly patterns in last 24h"
results, err := index.Search(&chronicle.HybridSearchQuery{
    Vector: queryEmbedding, K: 10,
    StartTime: now - 86400*1e9, EndTime: now,
})

// Built-in pattern library
lib := chronicle.NewPatternLibrary()
matches := lib.FindSimilarPatterns(timeSeries, 5)
```

### Production Observability Suite

Self-monitoring: internal metrics, health checks, and HTTP endpoints.

```go
suite := chronicle.NewObservabilitySuite(chronicle.DefaultObservabilitySuiteConfig())
suite.Start()

// Record custom metrics
suite.Metrics().IncrCounter("writes_total", 1)
suite.Metrics().RecordDuration("query_latency", elapsed)

// Register health checks
suite.Health().RegisterCheck("wal", chronicle.WALHealthCheck(db))
suite.Health().RegisterCheck("storage", chronicle.StorageHealthCheck(db))

// HTTP endpoints (auto-registered)
// GET /internal/metrics — metrics snapshot
// GET /internal/health — health status
// GET /internal/status — combined overview
```

### Incremental Materialized Views

Materialized query results that update incrementally as new data arrives.

```go
engine := chronicle.NewMaterializedViewEngine(db, chronicle.DefaultMaterializedViewConfig())
engine.Start()

engine.CreateView(&chronicle.MaterializedViewDefinition{
    Name:         "cpu_avg_5m",
    SourceMetric: "cpu_usage",
    Aggregation:  chronicle.AggMean,
    Window:       5 * time.Minute,
    GroupBy:      []string{"host"},
    RefreshMode:  chronicle.RefreshEager,
})

// Query from materialized data (100x faster than full scan)
result, err := engine.Query("cpu_avg_5m", startTime, endTime)

// Incremental updates on every write
engine.OnWrite(&point)
```

### OpenAPI Specification & SDK Generation

Auto-generated OpenAPI 3.0 spec and multi-language SDK templates.

```go
gen := chronicle.NewOpenAPIGenerator(chronicle.DefaultOpenAPIGeneratorConfig())
spec := gen.Generate()
jsonBytes, _ := gen.ToJSON()

// SDK generation
sdkGen := chronicle.NewSDKGenerator(spec, chronicle.DefaultSDKGeneratorConfig(chronicle.SDKPython))
output, _ := sdkGen.GenerateClient()
// output.Files contains generated Python client code

// HTTP endpoints
// GET /openapi.json — OpenAPI spec
// GET /swagger — Swagger UI redirect
```

### Chaos Engineering Framework

Built-in fault injection and testing toolkit.

```go
injector := chronicle.NewFaultInjector(chronicle.DefaultChaosConfig())

// Inject faults
fault, _ := injector.InjectFault(&chronicle.FaultConfig{
    Type:     chronicle.FaultNetworkDelay,
    Duration: 5 * time.Second,
    Target:   "node-2",
})

// Network simulation
netSim := chronicle.NewNetworkFaultSimulator(injector)
netSim.PartitionNodes("node-1", "node-2")

// Declarative test scenarios
scenario := chronicle.NetworkPartitionScenario("node-1", "node-2", 10*time.Second)
result, err := scenario.Execute(ctx, injector)

// Built-in scenarios
chronicle.SplitBrainScenario(nodes)
chronicle.RollingRestartScenario(nodes, interval)
chronicle.DiskFailureScenario(path, duration)
```

### Offline-First CRDT Sync Protocol

CRDT-based conflict resolution for edge nodes with unreliable connectivity.

```go
mgr := chronicle.NewOfflineSyncManager(chronicle.OfflineSyncConfig{
    NodeID:           "edge-001",
    ConflictStrategy: chronicle.CRDTLastWriterWins,
})
mgr.Start()

// Track local writes
mgr.RecordWrite(&point)

// Prepare delta for peer sync
delta, _ := mgr.PrepareSyncDelta("cloud-node")

// Receive and apply remote delta
result, _ := mgr.ReceiveSyncDelta(remoteDelta)
// result.Conflicts contains any resolved conflicts
```

**CRDT primitives:**
- `GCounter` — grow-only distributed counter
- `LWWRegister` — last-writer-wins register
- `ORSet` — observed-remove set
- `VectorClock` — causal ordering with happens-before
- `BloomFilter` — space-efficient change detection

**Conflict strategies:** Last-Writer-Wins, Max-Value-Wins, Merge-All
