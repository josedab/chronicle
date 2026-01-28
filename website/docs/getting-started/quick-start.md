---
sidebar_position: 2
---

# Quick Start

Get productive with Chronicle in 5 minutes. You'll create a database, write metrics, query data, and enable the HTTP API.

## Step 1: Create a Database

```go
package main

import (
    "fmt"
    "time"

    "github.com/chronicle-db/chronicle"
)

func main() {
    db, err := chronicle.Open("metrics.db", chronicle.Config{
        Path:              "metrics.db",
        MaxMemory:         64 * 1024 * 1024,  // 64MB buffer
        PartitionDuration: time.Hour,          // 1 partition per hour
        RetentionDuration: 7 * 24 * time.Hour, // Keep 7 days
        BufferSize:        10_000,             // Flush every 10k points
    })
    if err != nil {
        panic(err)
    }
    defer db.Close()
    
    // Continue with examples below...
}
```

## Step 2: Write Data

Write individual points:

```go
err := db.Write(chronicle.Point{
    Metric:    "cpu_usage",
    Tags:      map[string]string{"host": "server-01", "region": "us-west"},
    Value:     45.7,
    Timestamp: time.Now().UnixNano(),
})
```

Write batches for better performance:

```go
points := []chronicle.Point{
    {Metric: "cpu_usage", Tags: map[string]string{"host": "server-01"}, Value: 45.7},
    {Metric: "cpu_usage", Tags: map[string]string{"host": "server-02"}, Value: 32.1},
    {Metric: "memory_used", Tags: map[string]string{"host": "server-01"}, Value: 8589934592},
}
err := db.WriteBatch(points)
```

:::tip
Use `WriteBatch` when ingesting multiple points—it's significantly faster than individual writes.
:::

## Step 3: Query Data

Raw query:

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

With aggregation:

```go
result, err := db.Execute(&chronicle.Query{
    Metric: "cpu_usage",
    Start:  time.Now().Add(-24 * time.Hour).UnixNano(),
    End:    time.Now().UnixNano(),
    Aggregation: &chronicle.Aggregation{
        Function: chronicle.AggMean,
        Window:   time.Hour,  // Hourly averages
    },
})
```

## Step 4: Enable HTTP API

Enable the built-in HTTP server for remote access:

```go
db, err := chronicle.Open("metrics.db", chronicle.Config{
    Path:        "metrics.db",
    HTTPEnabled: true,
    HTTPPort:    8086,
})
```

Now you can write data via HTTP:

```bash
# Write using InfluxDB line protocol
curl -X POST http://localhost:8086/write \
  -d 'cpu_usage,host=server-01,region=us-west value=45.7'

# Write using JSON
curl -X POST http://localhost:8086/write \
  -H "Content-Type: application/json" \
  -d '{"points":[{"metric":"cpu_usage","tags":{"host":"server-01"},"value":45.7}]}'
```

Query via HTTP:

```bash
curl -X POST http://localhost:8086/query \
  -H "Content-Type: application/json" \
  -d '{"metric":"cpu_usage","start":0,"end":9999999999999999999}'
```

## Step 5: Use PromQL (Optional)

Chronicle supports PromQL for Prometheus compatibility:

```go
executor := chronicle.NewPromQLExecutor(db)
result, err := executor.Query(`rate(cpu_usage{host="server-01"}[5m])`, time.Now())
```

Via HTTP (Prometheus-compatible):

```bash
curl "http://localhost:8086/api/v1/query?query=cpu_usage{host='server-01'}"
```

## Complete Example

```go
package main

import (
    "fmt"
    "math/rand"
    "time"

    "github.com/chronicle-db/chronicle"
)

func main() {
    // Create database with HTTP API
    db, err := chronicle.Open("demo.db", chronicle.Config{
        Path:              "demo.db",
        PartitionDuration: time.Hour,
        RetentionDuration: 24 * time.Hour,
        HTTPEnabled:       true,
        HTTPPort:          8086,
    })
    if err != nil {
        panic(err)
    }
    defer db.Close()

    // Generate sample data
    hosts := []string{"web-1", "web-2", "db-1"}
    for i := 0; i < 1000; i++ {
        for _, host := range hosts {
            db.Write(chronicle.Point{
                Metric:    "cpu_usage",
                Tags:      map[string]string{"host": host},
                Value:     rand.Float64() * 100,
                Timestamp: time.Now().Add(-time.Duration(i) * time.Minute).UnixNano(),
            })
        }
    }
    db.Flush()

    // Query: hourly average per host
    result, _ := db.Execute(&chronicle.Query{
        Metric:  "cpu_usage",
        Start:   time.Now().Add(-24 * time.Hour).UnixNano(),
        End:     time.Now().UnixNano(),
        GroupBy: []string{"host"},
        Aggregation: &chronicle.Aggregation{
            Function: chronicle.AggMean,
            Window:   time.Hour,
        },
    })

    for _, p := range result.Points {
        fmt.Printf("[%s] %s: %.2f%%\n", 
            p.Tags["host"], 
            time.Unix(0, p.Timestamp).Format("15:04"), 
            p.Value)
    }

    fmt.Println("\nHTTP API running at http://localhost:8086")
    fmt.Println("Press Ctrl+C to exit")
    select {} // Block forever
}
```

## What's Next?

- [First Queries](/docs/getting-started/first-queries) - Learn the query language
- [Data Model](/docs/core-concepts/data-model) - Understand metrics, tags, and series
- [HTTP API Guide](/docs/guides/http-api) - Complete HTTP endpoint reference
