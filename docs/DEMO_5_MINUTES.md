# Chronicle in 5 Minutes

A complete walkthrough from zero to working time-series database.

## Step 1: Install (30 seconds)

```bash
mkdir chronicle-demo && cd chronicle-demo
go mod init demo
go get github.com/chronicle-db/chronicle
```

## Step 2: Write Data (1 minute)

Create `main.go`:

```go
package main

import (
    "fmt"
    "log"
    "math"
    "time"

    "github.com/chronicle-db/chronicle"
)

func main() {
    // Open database — single file, no server needed
    db, err := chronicle.Open("demo.db", chronicle.DefaultConfig("demo.db"))
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Write 1000 sensor readings
    fmt.Println("📝 Writing 1000 data points...")
    now := time.Now()
    for i := 0; i < 1000; i++ {
        db.Write(chronicle.Point{
            Metric:    "temperature",
            Value:     20 + 5*math.Sin(float64(i)/50) + float64(i%3)*0.5,
            Tags:      map[string]string{"sensor": fmt.Sprintf("s%d", i%3), "room": "lab"},
            Timestamp: now.Add(time.Duration(i) * 10 * time.Second).UnixNano(),
        })
    }
    db.Flush()
    fmt.Println("✅ Written! Database file:", "demo.db")

    // Query it back
    fmt.Println("\n🔍 Querying last 100 points for sensor s0...")
    result, _ := db.Execute(&chronicle.Query{
        Metric: "temperature",
        Tags:   map[string]string{"sensor": "s0"},
        Limit:  5,
    })
    for _, p := range result.Points {
        t := time.Unix(0, p.Timestamp)
        fmt.Printf("   %s  %.1f°C  %v\n", t.Format("15:04:05"), p.Value, p.Tags)
    }

    // Show metrics
    fmt.Println("\n📊 Known metrics:", db.Metrics())
    fmt.Println("\n🎉 Done! Your data is in demo.db (try: ls -la demo.db)")
}
```

## Step 3: Run It (30 seconds)

```bash
go run main.go
```

Output:
```
📝 Writing 1000 data points...
✅ Written! Database file: demo.db

🔍 Querying last 100 points for sensor s0...
   14:30:00  20.0°C  map[room:lab sensor:s0]
   14:30:30  20.3°C  map[room:lab sensor:s0]
   14:31:00  20.6°C  map[room:lab sensor:s0]
   14:31:30  20.9°C  map[room:lab sensor:s0]
   14:32:00  21.1°C  map[room:lab sensor:s0]

📊 Known metrics: [temperature]

🎉 Done! Your data is in demo.db (try: ls -la demo.db)
```

## Step 4: Enable HTTP API (1 minute)

Add HTTP server to `main.go`:

```go
cfg := chronicle.DefaultConfig("demo.db")
cfg.HTTP.HTTPEnabled = true
cfg.HTTP.HTTPPort = 8080
```

Then query via HTTP:
```bash
# Health check
curl http://localhost:8080/health

# Query via API
curl -X POST http://localhost:8080/api/v1/query \
  -d '{"metric":"temperature","limit":5}'

# Write via Influx line protocol
curl -X POST http://localhost:8080/write \
  -d 'temperature,sensor=s4,room=lab value=22.5'

# Prometheus-compatible remote write
# (configure Prometheus to remote_write to http://localhost:8080/api/v1/prom/write)
```

## Step 5: Explore (1 minute)

```bash
# List all metrics
curl http://localhost:8080/metrics

# Health readiness (for Kubernetes)
curl http://localhost:8080/health/ready

# Admin dashboard
open http://localhost:8080/admin
```

## What Just Happened?

In 5 minutes you:
1. ✅ Installed Chronicle as a Go library (no separate server!)
2. ✅ Wrote 1000 time-series points to a single file
3. ✅ Queried with tag filtering and limits
4. ✅ Enabled HTTP API with Prometheus/Influx compatibility
5. ✅ Got Kubernetes-ready health checks

The entire database is in one file: `demo.db`. Back it up with `cp demo.db backup.db`.

## Next Steps

- [Getting Started Guide](GETTING_STARTED.md) — deeper walkthrough
- [Configuration Guide](CONFIGURATION.md) — tuning for your workload
- [Edge Deployment](EDGE_DEPLOYMENT.md) — deploy on Raspberry Pi / Jetson
- [Examples](../examples/) — IoT gateway, analytics dashboard, Prometheus integration

## Video Recording Script

*For recording the demo video:*

1. Open terminal with large font (24pt)
2. Show empty directory: `ls`
3. Run `go mod init demo && go get github.com/chronicle-db/chronicle`
4. Paste `main.go` (have it ready in clipboard)
5. Run `go run main.go` — point out the output
6. Show file size: `ls -lh demo.db`
7. Add HTTP config, restart, show `curl` commands
8. End with: "That's Chronicle — an embedded TSDB in 5 minutes"

*Total recording time: 4-5 minutes with narration*
