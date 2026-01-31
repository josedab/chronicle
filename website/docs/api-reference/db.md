---
sidebar_position: 3
---

# DB API

Complete reference for Chronicle database operations.

## Opening a Database

### Open

```go
func Open(path string, cfg Config) (*DB, error)
```

Opens or creates a Chronicle database.

```go
db, err := chronicle.Open("data.db", chronicle.DefaultConfig("data.db"))
if err != nil {
    log.Fatal(err)
}
defer db.Close()
```

**Errors:**
- Path is required
- Disk full
- Permission denied
- WAL corruption (recoverable)

## Write Operations

### Write

```go
func (db *DB) Write(p Point) error
```

Writes a single point. Points are buffered before being flushed to disk.

```go
err := db.Write(chronicle.Point{
    Metric:    "cpu_usage",
    Tags:      map[string]string{"host": "server-01"},
    Value:     45.2,
    Timestamp: time.Now().UnixNano(),
})
```

**Behavior:**
- Auto-sets timestamp to now if zero
- Auto-creates empty tags map if nil
- Validates against schema if configured
- Tracks cardinality

**Errors:**
- Schema validation failed
- Cardinality limit exceeded

### WriteBatch

```go
func (db *DB) WriteBatch(points []Point) error
```

Writes multiple points efficiently. Recommended for high-throughput ingestion.

```go
points := []chronicle.Point{
    {Metric: "cpu_usage", Tags: map[string]string{"host": "server-01"}, Value: 45.2},
    {Metric: "cpu_usage", Tags: map[string]string{"host": "server-02"}, Value: 32.1},
}
err := db.WriteBatch(points)
```

**Performance:** 10-100x faster than individual `Write()` calls.

### Flush

```go
func (db *DB) Flush() error
```

Forces buffered points to storage immediately.

```go
err := db.Flush()
```

**When to use:**
- Before reading recently written data
- Before graceful shutdown
- After critical writes

### WriteHistogram

```go
func (db *DB) WriteHistogram(p HistogramPoint) error
```

Writes a histogram point.

```go
err := db.WriteHistogram(chronicle.HistogramPoint{
    Metric:    "request_duration",
    Tags:      map[string]string{"endpoint": "/api"},
    Timestamp: time.Now().UnixNano(),
    Buckets: []chronicle.HistogramBucket{
        {UpperBound: 0.1, Count: 100},
        {UpperBound: 0.5, Count: 450},
        {UpperBound: 1.0, Count: 500},
    },
    Sum:   45.2,
    Count: 500,
})
```

### WriteExemplar

```go
func (db *DB) WriteExemplar(p ExemplarPoint) error
```

Writes a point with trace correlation.

```go
err := db.WriteExemplar(chronicle.ExemplarPoint{
    Point: chronicle.Point{
        Metric: "request_latency",
        Value:  0.523,
    },
    TraceID: "abc123",
    SpanID:  "def456",
    Labels:  map[string]string{"user": "alice"},
})
```

## Query Operations

### Execute

```go
func (db *DB) Execute(q *Query) (*Result, error)
```

Executes a query and returns results.

```go
result, err := db.Execute(&chronicle.Query{
    Metric: "cpu_usage",
    Tags:   map[string]string{"host": "server-01"},
    Start:  time.Now().Add(-time.Hour).UnixNano(),
    End:    time.Now().UnixNano(),
})

for _, point := range result.Points {
    fmt.Printf("%v: %.2f\n", time.Unix(0, point.Timestamp), point.Value)
}
```

**With aggregation:**

```go
result, err := db.Execute(&chronicle.Query{
    Metric: "cpu_usage",
    Start:  time.Now().Add(-24 * time.Hour).UnixNano(),
    End:    time.Now().UnixNano(),
    Aggregation: &chronicle.Aggregation{
        Function: chronicle.AggMean,
        Window:   time.Hour,
    },
    GroupBy: []string{"host"},
})
```

**Errors:**
- Query timeout
- Memory budget exceeded
- Invalid query

### Metrics

```go
func (db *DB) Metrics() []string
```

Returns all registered metric names.

```go
metrics := db.Metrics()
for _, name := range metrics {
    fmt.Println(name)
}
```

## Schema Operations

### RegisterSchema

```go
func (db *DB) RegisterSchema(schema MetricSchema) error
```

Registers a metric schema at runtime.

```go
err := db.RegisterSchema(chronicle.MetricSchema{
    Name:         "cpu_usage",
    Description:  "CPU utilization",
    Type:         chronicle.SchemaTypeGauge,
    Unit:         "percent",
    RequiredTags: []string{"host"},
})
```

### UnregisterSchema

```go
func (db *DB) UnregisterSchema(name string)
```

Removes a metric schema.

```go
db.UnregisterSchema("cpu_usage")
```

### GetSchema

```go
func (db *DB) GetSchema(name string) *MetricSchema
```

Returns the schema for a metric, or nil if not found.

```go
schema := db.GetSchema("cpu_usage")
if schema != nil {
    fmt.Printf("Required tags: %v\n", schema.RequiredTags)
}
```

### ListSchemas

```go
func (db *DB) ListSchemas() []MetricSchema
```

Returns all registered schemas.

```go
schemas := db.ListSchemas()
for _, s := range schemas {
    fmt.Printf("%s: %s\n", s.Name, s.Description)
}
```

## Feature Stores

### ExemplarStore

```go
func (db *DB) ExemplarStore() *ExemplarStore
```

Returns the exemplar store for direct access.

```go
store := db.ExemplarStore()
exemplars := store.Query("request_latency", start, end)
```

### HistogramStore

```go
func (db *DB) HistogramStore() *HistogramStore
```

Returns the histogram store for direct access.

```go
store := db.HistogramStore()
histograms := store.Query("request_duration", start, end)
```

### CardinalityTracker

```go
func (db *DB) CardinalityTracker() *CardinalityTracker
```

Returns the cardinality tracker.

```go
tracker := db.CardinalityTracker()
// Configure alerts, check limits
```

### CardinalityStats

```go
func (db *DB) CardinalityStats() CardinalityStats
```

Returns current cardinality statistics.

```go
stats := db.CardinalityStats()
fmt.Printf("Total series: %d\n", stats.TotalSeries)
fmt.Printf("Active series: %d\n", stats.ActiveSeries)
```

## Lifecycle

### Close

```go
func (db *DB) Close() error
```

Flushes data and closes the database.

```go
err := db.Close()
```

**Behavior:**
- Flushes write buffer
- Persists index
- Stops background workers
- Closes HTTP server (if enabled)
- Safe to call multiple times

**Always defer Close:**

```go
db, err := chronicle.Open("data.db", config)
if err != nil {
    log.Fatal(err)
}
defer db.Close()
```

## Thread Safety

All `DB` methods are thread-safe:

```go
// Safe: concurrent writes from multiple goroutines
var wg sync.WaitGroup
for i := 0; i < 10; i++ {
    wg.Add(1)
    go func(id int) {
        defer wg.Done()
        for j := 0; j < 1000; j++ {
            db.Write(chronicle.Point{
                Metric: "counter",
                Tags:   map[string]string{"worker": fmt.Sprintf("%d", id)},
                Value:  float64(j),
            })
        }
    }(i)
}
wg.Wait()
```
