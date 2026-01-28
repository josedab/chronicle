---
sidebar_position: 1
slug: /
---

# Introduction

Chronicle is an **embedded time-series database** for Go, designed for constrained and edge environments.

## Why Chronicle?

| Problem | Chronicle Solution |
|---------|-------------------|
| Need time-series storage but can't run Prometheus/InfluxDB | Single-file embedded database |
| Deploying to edge/IoT with limited resources | Zero dependencies, 10MB binary |
| Want Prometheus compatibility without the infrastructure | PromQL support, remote write ingestion |
| Need analytics without external tools | Built-in forecasting, alerting, anomaly detection |

## Key Features

- **🗄️ Single-file storage** — No servers, no configuration files
- **📊 Prometheus compatible** — PromQL queries, remote write, native histograms
- **⚡ High compression** — Gorilla + delta encoding (10x compression)
- **🔮 Built-in analytics** — Forecasting, anomaly detection, recording rules
- **🌐 Edge-first** — Runs on WASM, Raspberry Pi, serverless functions
- **🔒 Enterprise ready** — Encryption, multi-tenancy, schema validation

## Quick Example

```go
package main

import (
    "log"
    "time"
    "github.com/chronicle-db/chronicle"
)

func main() {
    db, _ := chronicle.Open("metrics.db", chronicle.DefaultConfig("metrics.db"))
    defer db.Close()

    // Write data
    db.Write(chronicle.Point{
        Metric:    "temperature",
        Tags:      map[string]string{"sensor": "living_room"},
        Value:     22.5,
        Timestamp: time.Now().UnixNano(),
    })

    // Query data
    result, _ := db.Execute(&chronicle.Query{
        Metric: "temperature",
        Start:  time.Now().Add(-24 * time.Hour).UnixNano(),
        End:    time.Now().UnixNano(),
    })

    log.Printf("Found %d data points", len(result.Points))
}
```

## What's Next?

<div className="row">
  <div className="col col--6">
    <a href="/docs/getting-started/installation" className="card padding--lg">
      <h3>🚀 Installation</h3>
      <p>Get Chronicle installed in 30 seconds</p>
    </a>
  </div>
  <div className="col col--6">
    <a href="/docs/getting-started/quick-start" className="card padding--lg">
      <h3>⚡ Quick Start</h3>
      <p>Build your first time-series application</p>
    </a>
  </div>
</div>
