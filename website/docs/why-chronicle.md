---
sidebar_position: 0
slug: /why-chronicle
---

# Why Chronicle?

Chronicle solves a specific problem: **you need time-series storage but can't (or don't want to) run a separate database server.**

## The Problem

Traditional time-series databases like InfluxDB, Prometheus, or TimescaleDB are powerful, but they come with operational overhead:

- **Server management** — You need to deploy, configure, and maintain a separate service
- **Network latency** — Every read/write crosses the network
- **Resource requirements** — Minimum memory/CPU requirements that may exceed your edge device
- **Deployment complexity** — Docker, Kubernetes, or manual installation
- **Dependency on availability** — If the database server is down, your app is affected

For many use cases—IoT devices, embedded systems, desktop applications, single-tenant SaaS, or development/testing—this overhead isn't justified.

## The Chronicle Approach

Chronicle takes a different path: **embed the database directly in your application.**

```go
// That's it. No server to start, no connection string, no network.
db, _ := chronicle.Open("metrics.db", chronicle.DefaultConfig("metrics.db"))
defer db.Close()
```

Your data is stored in a single file. Your application *is* the database server. Queries execute in-process with zero network overhead.

## When Chronicle Shines

### ✅ Edge Computing & IoT

You're deploying to devices with limited resources—Raspberry Pi, industrial gateways, or embedded systems. You need to store sensor data locally, analyze trends, and maybe sync to the cloud occasionally.

```go
// Runs happily on a Raspberry Pi with 16MB memory budget
db, _ := chronicle.Open("/data/sensors.db", chronicle.Config{
    Path:              "/data/sensors.db",
    MaxMemory:         16 * 1024 * 1024,  // 16MB
    RetentionDuration: 24 * time.Hour,     // Auto-cleanup
})
```

### ✅ Desktop & Mobile Applications

You're building a desktop app that needs to track metrics, display charts, or analyze time-series data. You don't want users to install a database server.

```go
// Embedded in your Electron/Tauri/native app
dbPath := filepath.Join(userDataDir, "app-metrics.db")
db, _ := chronicle.Open(dbPath, chronicle.DefaultConfig(dbPath))
```

### ✅ Browser-Based Analytics

You need client-side time-series analysis. Chronicle compiles to WebAssembly.

```javascript
// Yes, this runs in the browser
import { Chronicle } from '@chronicle-db/wasm';
const db = await Chronicle.open('metrics');
```

### ✅ Prometheus Replacement for Simple Deployments

You want Prometheus-style queries and alerting but don't need the full Prometheus stack.

```go
// PromQL support, recording rules, alerting — no Prometheus server
executor := chronicle.NewPromQLExecutor(db)
result, _ := executor.Query(`rate(http_requests_total[5m])`, time.Now())
```

### ✅ Development & Testing

You're developing an application that will use InfluxDB/Prometheus in production, but you want fast local iteration without spinning up containers.

```go
// In tests: in-memory database, no cleanup needed
db, _ := chronicle.Open("", chronicle.Config{InMemory: true})
```

### ✅ Single-Binary Deployments

You want to ship a single binary with zero runtime dependencies.

```bash
# Your entire application, including time-series storage
$ ls -la myapp
-rwxr-xr-x 1 user user 12M Feb 1 10:00 myapp
```

## When to Use Something Else

Chronicle isn't the right choice for every scenario:

### ❌ Massive Scale

If you need to store petabytes of data across hundreds of nodes with automatic sharding and replication, use a distributed system like VictoriaMetrics, Thanos, or InfluxDB Enterprise.

### ❌ High Availability Requirements

Chronicle is single-instance. If you need automatic failover and zero downtime, you need a clustered solution.

### ❌ Multi-Language Polyglot Architecture

Chronicle is Go-native. If your stack is Python/Java/Node and you need native client libraries, a server-based database with multiple client libraries is more appropriate.

### ❌ Managed Cloud Service

If you want zero operational overhead and are willing to pay for it, use a managed service like InfluxDB Cloud, Amazon Timestream, or Google Cloud Monitoring.

## Migration Stories

### From Prometheus

A monitoring agent that previously scraped metrics and forwarded to Prometheus now stores and queries locally:

**Before:**
```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'myapp'
    static_configs:
      - targets: ['localhost:9090']
```

**After:**
```go
// No external Prometheus, same PromQL queries
db, _ := chronicle.Open("metrics.db", config)
executor := chronicle.NewPromQLExecutor(db)

// Same PromQL you know
result, _ := executor.Query(`sum(rate(requests_total[5m])) by (status)`, time.Now())
```

### From InfluxDB

An IoT gateway that previously sent data to InfluxDB now stores locally with the same line protocol:

**Before:**
```bash
curl -X POST 'http://influxdb:8086/write?db=sensors' \
  -d 'temperature,room=kitchen value=22.5'
```

**After:**
```go
// Same line protocol, embedded database
db, _ := chronicle.Open("sensors.db", chronicle.Config{
    HTTPEnabled: true,
    HTTPPort:    8086,
})
// curl command unchanged
```

### From SQLite + Custom Code

A desktop app that was manually managing time-series in SQLite with custom aggregation code:

**Before:**
```go
// Custom time-series on SQLite
db.Exec(`CREATE TABLE metrics (timestamp INTEGER, name TEXT, value REAL)`)
db.Exec(`INSERT INTO metrics VALUES (?, ?, ?)`, ts, name, value)

// Manual aggregation queries
rows, _ := db.Query(`
    SELECT strftime('%H', datetime(timestamp, 'unixepoch')), AVG(value)
    FROM metrics WHERE name = ? GROUP BY 1
`, name)
```

**After:**
```go
// Purpose-built time-series API
db.Write(chronicle.Point{Metric: name, Value: value, Timestamp: ts})

result, _ := db.Execute(&chronicle.Query{
    Metric: name,
    Aggregation: &chronicle.Aggregation{
        Function: chronicle.AggMean,
        Window:   time.Hour,
    },
})
```

## Feature Comparison

| Feature | Chronicle | InfluxDB | Prometheus | SQLite |
|---------|-----------|----------|------------|--------|
| Embedded | ✅ | ❌ | ❌ | ✅ |
| Time-series native | ✅ | ✅ | ✅ | ❌ |
| PromQL | ✅ | ❌ | ✅ | ❌ |
| SQL-like queries | ✅ | InfluxQL | ❌ | ✅ |
| Compression | 10-15x | 10x | 12x | 1x |
| WASM support | ✅ | ❌ | ❌ | ✅ |
| Zero dependencies | ✅ | ❌ | ❌ | ✅ |
| Forecasting | ✅ | ❌ | ❌ | ❌ |
| Alerting | ✅ | ✅ | ✅ | ❌ |

## Try It

```bash
go get github.com/chronicle-db/chronicle
```

```go
package main

import (
    "fmt"
    "time"
    "github.com/chronicle-db/chronicle"
)

func main() {
    db, _ := chronicle.Open("demo.db", chronicle.DefaultConfig("demo.db"))
    defer db.Close()

    db.Write(chronicle.Point{
        Metric:    "hello",
        Value:     42,
        Timestamp: time.Now().UnixNano(),
    })

    result, _ := db.Execute(&chronicle.Query{Metric: "hello"})
    fmt.Printf("Stored and retrieved: %.0f\n", result.Points[0].Value)
}
```

**Ready to dive deeper?** Check out the [Quick Start Guide](/docs/getting-started/quick-start).
