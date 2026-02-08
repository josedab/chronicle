# Core API Reference

This page covers the **10 functions and types you need for 90% of use cases**. For advanced features, see [FEATURES.md](FEATURES.md). For the full API surface and stability classification, see [`api_stability.go`](../api_stability.go).

## Open a Database

```go
// With sensible defaults
db, err := chronicle.Open("data.db", chronicle.DefaultConfig("data.db"))

// With explicit config
db, err := chronicle.Open("data.db", chronicle.Config{
    Storage: chronicle.StorageConfig{
        MaxMemory:         64 * 1024 * 1024,
        PartitionDuration: time.Hour,
    },
    Retention: chronicle.RetentionConfig{
        RetentionDuration: 7 * 24 * time.Hour,
    },
})

// With ConfigBuilder (fluent API)
cfg, err := chronicle.NewConfigBuilder("data.db").
    WithMaxMemory(64 * 1024 * 1024).
    WithRetention(7 * 24 * time.Hour).
    Build()
db, err := chronicle.Open("data.db", cfg)
```

## Close

```go
defer db.Close()
```

## Write

```go
// Single point
err := db.Write(chronicle.Point{
    Metric:    "temperature",
    Value:     22.5,
    Timestamp: time.Now().UnixNano(),
    Tags:      map[string]string{"room": "lab"},
})

// Batch (~10x faster for bulk writes)
err := db.WriteBatch([]chronicle.Point{
    {Metric: "temperature", Value: 22.5, Timestamp: time.Now().UnixNano(), Tags: map[string]string{"room": "lab"}},
    {Metric: "temperature", Value: 19.8, Timestamp: time.Now().UnixNano(), Tags: map[string]string{"room": "office"}},
})
```

## Flush

Force buffered writes to storage. Required before queries can see recently written data.

```go
db.Flush()
```

## Query

```go
// All points for a metric
result, err := db.Execute(&chronicle.Query{Metric: "temperature"})

// With time range and tag filter
result, err := db.Execute(&chronicle.Query{
    Metric: "temperature",
    Tags:   map[string]string{"room": "lab"},
    Start:  time.Now().Add(-time.Hour).UnixNano(),
    End:    time.Now().UnixNano(),
})

// With aggregation
result, err := db.Execute(&chronicle.Query{
    Metric: "temperature",
    Aggregation: &chronicle.Aggregation{
        Function: chronicle.AggMean,
        Window:   5 * time.Minute,
    },
    GroupBy: []string{"room"},
})
```

## List Metrics

```go
names := db.Metrics() // []string{"temperature", "humidity", ...}
```

## Types at a Glance

| Type | Fields | Description |
|------|--------|-------------|
| `Point` | `Metric`, `Value` (float64), `Timestamp` (int64 nanoseconds), `Tags` (map) | A single data point |
| `Query` | `Metric`, `Start`, `End`, `Tags`, `Aggregation`, `GroupBy`, `Limit` | Query specification |
| `Aggregation` | `Function` (AggFunc), `Window` (time.Duration) | Aggregation parameters |
| `Result` | `Points` ([]Point) | Query output |
| `Config` | `Storage`, `Retention`, `Query`, `HTTP`, `Encryption` | Database configuration |

## Aggregation Functions

| Constant | Description |
|----------|-------------|
| `AggCount` | Number of points |
| `AggSum` | Sum of values |
| `AggMean` | Average value |
| `AggMin` | Minimum value |
| `AggMax` | Maximum value |
| `AggFirst` | First value in window |
| `AggLast` | Last value in window |
| `AggStddev` | Standard deviation |

## Minimal Working Example

```go
package main

import (
    "fmt"
    "log"
    "time"

    "github.com/chronicle-db/chronicle"
)

func main() {
    db, err := chronicle.Open("demo.db", chronicle.DefaultConfig("demo.db"))
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    _ = db.Write(chronicle.Point{
        Metric:    "cpu",
        Value:     42.5,
        Timestamp: time.Now().UnixNano(),
    })
    db.Flush()

    result, err := db.Execute(&chronicle.Query{Metric: "cpu"})
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Points: %d\n", len(result.Points))
}
```

## What's Next?

| Goal | Guide |
|------|-------|
| Step-by-step tutorial | [Getting Started](GETTING_STARTED.md) |
| HTTP API, PromQL, Grafana | [Features Guide](FEATURES.md) |
| All configuration options | [Configuration Reference](CONFIGURATION.md) |
| Architecture and internals | [Architecture Overview](ARCHITECTURE.md) |
| API stability tiers | [`api_stability.go`](../api_stability.go) |
