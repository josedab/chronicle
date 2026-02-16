# Getting Started with Chronicle

This guide walks you through Chronicle's core features in 10 minutes. By the end, you'll have a working time-series database that writes, queries, and serves data over HTTP.

## Prerequisites

- **Go 1.24 or later** (see [go.dev/dl](https://go.dev/dl/))

## Install

```bash
go get github.com/chronicle-db/chronicle
```

## Step 1: Open a Database

Chronicle stores data in a single file, like SQLite.

```go
package main

import (
    "fmt"
    "log"
    "time"

    "github.com/chronicle-db/chronicle"
)

func main() {
    db, err := chronicle.Open("sensors.db", chronicle.DefaultConfig("sensors.db"))
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    fmt.Println("Database opened successfully")
}
```

Run it:

```bash
go run main.go
```

This creates `sensors.db` in the current directory.

## Step 2: Write Data

A **Point** has a metric name, a float64 value, a nanosecond timestamp, and optional tags:

```go
now := time.Now().UnixNano()

// Write a single point
err = db.Write(chronicle.Point{
    Metric:    "temperature",
    Value:     22.5,
    Timestamp: now,
    Tags:      map[string]string{"room": "lab", "sensor": "dht22"},
})
if err != nil {
    log.Fatal(err)
}

// Write a batch of points
points := []chronicle.Point{
    {Metric: "temperature", Value: 23.1, Timestamp: now + int64(time.Second), Tags: map[string]string{"room": "lab"}},
    {Metric: "humidity", Value: 45.0, Timestamp: now, Tags: map[string]string{"room": "lab"}},
    {Metric: "temperature", Value: 19.8, Timestamp: now, Tags: map[string]string{"room": "office"}},
}
err = db.WriteBatch(points)
```

## Step 3: Query Data

Use `Execute` with a `Query` struct to read data back:

```go
result, err := db.Execute(&chronicle.Query{
    Metric: "temperature",
})
if err != nil {
    log.Fatal(err)
}

for _, p := range result.Points {
    fmt.Printf("  %s = %.1f at %v\n", p.Metric, p.Value, time.Unix(0, p.Timestamp).Format(time.RFC3339))
}
```

### Filter by Tags

```go
result, err := db.Execute(&chronicle.Query{
    Metric: "temperature",
    Tags:   map[string]string{"room": "lab"},
})
```

### Filter by Time Range

```go
oneHourAgo := time.Now().Add(-time.Hour).UnixNano()
result, err := db.Execute(&chronicle.Query{
    Metric: "temperature",
    Start:  oneHourAgo,
    End:    time.Now().UnixNano(),
})
```

## Step 4: Aggregate Data

Chronicle supports built-in aggregation functions:

```go
result, err := db.Execute(&chronicle.Query{
    Metric: "temperature",
    Aggregation: &chronicle.Aggregation{
        Function: chronicle.AggMean,
        Window:   5 * time.Minute,
    },
    GroupBy: []string{"room"},
})
```

Available aggregation functions:

| Function | Description |
|----------|-------------|
| `AggCount` | Number of points |
| `AggSum` | Sum of values |
| `AggMean` | Average value |
| `AggMin` | Minimum value |
| `AggMax` | Maximum value |
| `AggFirst` | First value in window |
| `AggLast` | Last value in window |
| `AggStddev` | Standard deviation |

## Step 5: Configure for Production

Use the `ConfigBuilder` for type-safe configuration:

```go
cfg, err := chronicle.NewConfigBuilder("metrics.db").
    WithMaxMemory(256 * 1024 * 1024).      // 256MB memory budget
    WithPartitionDuration(time.Hour).        // 1-hour partitions
    WithBufferSize(10_000).                  // Buffer 10K points before flush
    WithRetention(7 * 24 * time.Hour).       // Keep 7 days of data
    Build()
if err != nil {
    log.Fatal(err)
}

db, err := chronicle.Open("metrics.db", cfg)
```

## Step 6: Enable the HTTP API

Add HTTP to serve data remotely:

```go
cfg, err := chronicle.NewConfigBuilder("metrics.db").
    WithHTTP(8086).
    WithRetention(7 * 24 * time.Hour).
    Build()
```

Then query via curl:

```bash
# Write a point
curl -X POST http://localhost:8086/write \
  -d '{"metric":"cpu","value":72.5,"tags":{"host":"web01"}}'

# Query data
curl 'http://localhost:8086/query?metric=cpu'

# Health check
curl http://localhost:8086/health
```

## Step 7: Prometheus Compatibility

Chronicle supports PromQL queries and Prometheus remote write:

```go
cfg, err := chronicle.NewConfigBuilder("metrics.db").
    WithHTTP(8086).
    WithPrometheusRemoteWrite(true).
    Build()
```

Query with PromQL:

```bash
curl 'http://localhost:8086/api/v1/query?query=temperature{room="lab"}'
curl 'http://localhost:8086/api/v1/query_range?query=avg(temperature)&start=2024-01-01T00:00:00Z&end=2024-01-02T00:00:00Z&step=5m'
```

Add Chronicle as a Prometheus data source in Grafana using `http://localhost:8086` as the URL.

## Complete Example

Here's a full working program:

```go
package main

import (
    "fmt"
    "log"
    "math/rand"
    "time"

    "github.com/chronicle-db/chronicle"
)

func main() {
    // Open database
    db, err := chronicle.Open("demo.db", chronicle.DefaultConfig("demo.db"))
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Write sample data
    rooms := []string{"lab", "office", "server-room"}
    for i := 0; i < 100; i++ {
        room := rooms[rand.Intn(len(rooms))]
        _ = db.Write(chronicle.Point{
            Metric:    "temperature",
            Value:     18.0 + rand.Float64()*10.0,
            Timestamp: time.Now().Add(-time.Duration(i) * time.Minute).UnixNano(),
            Tags:      map[string]string{"room": room},
        })
    }
    db.Flush()

    // Query: average temperature per room
    result, err := db.Execute(&chronicle.Query{
        Metric: "temperature",
        Aggregation: &chronicle.Aggregation{
            Function: chronicle.AggMean,
            Window:   time.Hour,
        },
        GroupBy: []string{"room"},
    })
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("Query returned %d aggregated points\n", len(result.Points))
    for _, p := range result.Points {
        fmt.Printf("  room=%s  avg=%.1f°C\n", p.Tags["room"], p.Value)
    }
}
```

## What's Next?

- **[Configuration Reference](CONFIGURATION.md)** — All config options explained
- **[API Reference](API.md)** — HTTP API endpoints
- **[Architecture](ARCHITECTURE.md)** — How Chronicle works internally
- **[Features](FEATURES.md)** — Advanced features: replication, streaming, ML inference
- **[Plugin Development](PLUGIN_DEVELOPMENT.md)** — Extend Chronicle with plugins
- **[Examples](../examples/)** — More complete examples

## Choosing Chronicle

Chronicle is best for:

- **Embedded use** — Single-file database, no external dependencies
- **Edge/IoT** — Low memory footprint, works on constrained devices
- **Go applications** — Native Go library, no CGo required
- **Prometheus compatibility** — Drop-in PromQL support, remote write
- **Rapid prototyping** — Zero configuration to get started

## What Features Should I Use?

Chronicle has many features at different maturity levels. Here's a guide:

### ✅ Production-Ready (Stable API)

Use these with confidence — they're covered by semver guarantees.

- **Core DB**: `Open`, `Write`, `WriteBatch`, `Execute`, `Close`
- **Configuration**: `Config`, `DefaultConfig`, `ConfigBuilder`
- **Queries**: `Query`, `Aggregation`, `TagFilter`, `Result`
- **Retention**: Time-based and size-based data expiration
- **WAL**: Write-ahead log for crash recovery
- **Compression**: Gorilla float encoding, delta timestamps, dictionary tags

### ⚠️ Beta (May Change Between Minor Versions)

These work well but their API may evolve:

- **HTTP API**: Write, query, and admin endpoints
- **PromQL**: Prometheus-compatible query language
- **Grafana Plugin**: Visualization data source
- **Replication**: Outbound replication to remote instances
- **Streaming**: Real-time WebSocket subscriptions
- **Schema Registry**: Metric validation
- **Alerting**: Threshold and anomaly-based alerts

### 🧪 Experimental (May Change Without Notice)

Interesting capabilities, but use at your own risk:

- Jupyter kernel, WASM runtime, Deno runtime
- TinyML inference, AutoML
- Zero-knowledge query verification
- Federated learning, natural language queries
- eBPF collectors, Cloudflare Workers runtime
