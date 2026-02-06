---
sidebar_position: 9
---

# Recipes

Copy-paste solutions for common Chronicle use cases. Each recipe is self-contained and production-ready.

## Data Ingestion

### High-Throughput Batch Writer

Optimal configuration for ingesting millions of points per second:

```go
package main

import (
    "log"
    "sync"
    "time"

    "github.com/chronicle-db/chronicle"
)

func main() {
    db, err := chronicle.Open("high-throughput.db", chronicle.Config{
        Path:              "high-throughput.db",
        MaxMemory:         512 * 1024 * 1024,  // 512MB for large buffers
        BufferSize:        100_000,            // Large buffer
        PartitionDuration: time.Hour,
        SyncInterval:      5 * time.Second,    // Less frequent sync
    })
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Use multiple goroutines with batching
    var wg sync.WaitGroup
    batchSize := 10000
    workers := 4

    for w := 0; w < workers; w++ {
        wg.Add(1)
        go func(workerID int) {
            defer wg.Done()
            batch := make([]chronicle.Point, 0, batchSize)
            
            for i := 0; i < 100000; i++ {
                batch = append(batch, chronicle.Point{
                    Metric:    "events",
                    Tags:      map[string]string{"worker": string(rune('0' + workerID))},
                    Value:     float64(i),
                    Timestamp: time.Now().UnixNano(),
                })
                
                if len(batch) >= batchSize {
                    db.WriteBatch(batch)
                    batch = batch[:0]
                }
            }
            
            if len(batch) > 0 {
                db.WriteBatch(batch)
            }
        }(w)
    }

    wg.Wait()
    db.Flush()
    log.Println("Ingestion complete")
}
```

### Prometheus Remote Write Receiver

Accept metrics from Prometheus instances:

```go
package main

import (
    "log"
    "time"

    "github.com/chronicle-db/chronicle"
)

func main() {
    db, err := chronicle.Open("prom-receiver.db", chronicle.Config{
        Path:              "prom-receiver.db",
        HTTPEnabled:       true,
        HTTPPort:          9090,
        PartitionDuration: time.Hour,
        RetentionDuration: 7 * 24 * time.Hour,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    log.Println("Prometheus remote write endpoint: http://localhost:9090/api/v1/prom/write")
    log.Println("Add to prometheus.yml:")
    log.Println("  remote_write:")
    log.Println("    - url: http://localhost:9090/api/v1/prom/write")
    
    select {}
}
```

### InfluxDB Line Protocol Ingestion

Parse and ingest InfluxDB line protocol:

```go
// Ingest via HTTP
// curl -X POST http://localhost:8086/write \
//   -d 'cpu,host=server01,region=us-west usage=64.5,idle=35.5 1609459200000000000'

package main

import (
    "log"
    "time"

    "github.com/chronicle-db/chronicle"
)

func main() {
    db, err := chronicle.Open("influx-compat.db", chronicle.Config{
        Path:        "influx-compat.db",
        HTTPEnabled: true,
        HTTPPort:    8086,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    log.Println("InfluxDB-compatible endpoint: http://localhost:8086/write")
    log.Println("Example:")
    log.Println(`  curl -X POST http://localhost:8086/write -d 'temperature,room=kitchen value=22.5'`)
    
    select {}
}
```

## Querying

### Moving Average Calculation

Calculate rolling averages for trend analysis:

```go
func movingAverage(db *chronicle.DB, metric string, window, step time.Duration) ([]chronicle.Point, error) {
    now := time.Now()
    result, err := db.Execute(&chronicle.Query{
        Metric: metric,
        Start:  now.Add(-24 * time.Hour).UnixNano(),
        End:    now.UnixNano(),
        Aggregation: &chronicle.Aggregation{
            Function: chronicle.AggMean,
            Window:   window,
        },
    })
    return result.Points, err
}

// Usage
points, _ := movingAverage(db, "cpu_usage", 5*time.Minute, time.Minute)
for _, p := range points {
    fmt.Printf("%v: %.2f (5min avg)\n", time.Unix(0, p.Timestamp), p.Value)
}
```

### Percentile Calculations

Get p50, p95, p99 latencies:

```go
func getPercentiles(db *chronicle.DB, metric string, duration time.Duration) map[string]float64 {
    now := time.Now()
    percentiles := map[string]float64{}
    
    for name, fn := range map[string]chronicle.AggFunc{
        "p50": chronicle.AggP50,
        "p95": chronicle.AggP95,
        "p99": chronicle.AggP99,
    } {
        result, _ := db.Execute(&chronicle.Query{
            Metric: metric,
            Start:  now.Add(-duration).UnixNano(),
            End:    now.UnixNano(),
            Aggregation: &chronicle.Aggregation{
                Function: fn,
            },
        })
        if len(result.Points) > 0 {
            percentiles[name] = result.Points[0].Value
        }
    }
    return percentiles
}

// Usage
p := getPercentiles(db, "request_latency", time.Hour)
fmt.Printf("p50=%.2fms p95=%.2fms p99=%.2fms\n", p["p50"], p["p95"], p["p99"])
```

### Top-N Query

Find the top consumers by a metric:

```go
func topN(db *chronicle.DB, metric, groupBy string, n int, duration time.Duration) []chronicle.Point {
    now := time.Now()
    result, _ := db.Execute(&chronicle.Query{
        Metric:  metric,
        Start:   now.Add(-duration).UnixNano(),
        End:     now.UnixNano(),
        GroupBy: []string{groupBy},
        Aggregation: &chronicle.Aggregation{
            Function: chronicle.AggSum,
        },
        Limit: n,
        OrderBy: &chronicle.OrderBy{
            Field: "value",
            Desc:  true,
        },
    })
    return result.Points
}

// Usage: Top 5 hosts by CPU usage
top := topN(db, "cpu_usage", "host", 5, time.Hour)
for i, p := range top {
    fmt.Printf("%d. %s: %.2f%%\n", i+1, p.Tags["host"], p.Value)
}
```

### Compare Time Periods

Compare metrics between two time periods:

```go
func comparePeriods(db *chronicle.DB, metric string, current, previous time.Duration) (float64, float64, float64) {
    now := time.Now()
    
    // Current period
    curr, _ := db.Execute(&chronicle.Query{
        Metric: metric,
        Start:  now.Add(-current).UnixNano(),
        End:    now.UnixNano(),
        Aggregation: &chronicle.Aggregation{Function: chronicle.AggMean},
    })
    
    // Previous period
    prev, _ := db.Execute(&chronicle.Query{
        Metric: metric,
        Start:  now.Add(-current - previous).UnixNano(),
        End:    now.Add(-current).UnixNano(),
        Aggregation: &chronicle.Aggregation{Function: chronicle.AggMean},
    })
    
    currVal := curr.Points[0].Value
    prevVal := prev.Points[0].Value
    change := ((currVal - prevVal) / prevVal) * 100
    
    return currVal, prevVal, change
}

// Usage: Compare last hour to previous hour
curr, prev, change := comparePeriods(db, "requests_per_second", time.Hour, time.Hour)
fmt.Printf("Current: %.2f, Previous: %.2f, Change: %+.1f%%\n", curr, prev, change)
```

## Alerting

### Threshold Alert with Hysteresis

Prevent alert flapping with hysteresis:

```go
engine := chronicle.NewAlertEngine(db, chronicle.AlertConfig{
    EvaluationInterval: time.Minute,
    NotificationURL:    "https://hooks.slack.com/services/xxx",
})

// Alert fires at 90%, resolves at 80%
engine.AddRule(chronicle.AlertRule{
    Name:       "HighCPUWithHysteresis",
    Expression: "avg(cpu_usage) > 90",  // Fire threshold
    Duration:   5 * time.Minute,
    Labels: map[string]string{
        "severity":          "warning",
        "resolve_threshold": "80",  // Custom annotation for resolution
    },
    Annotations: map[string]string{
        "summary": "CPU usage above 90% for 5 minutes",
    },
})
```

### Composite Alert (Multiple Conditions)

Alert only when multiple conditions are met:

```go
// Alert when CPU is high AND memory is high
engine.AddRule(chronicle.AlertRule{
    Name:       "ResourceExhaustion",
    Expression: "avg(cpu_usage) > 85 and avg(memory_usage) > 90",
    Duration:   3 * time.Minute,
    Labels:     map[string]string{"severity": "critical"},
    Annotations: map[string]string{
        "summary":     "System resources critically low",
        "description": "Both CPU (>85%) and memory (>90%) are exhausted",
        "runbook":     "https://wiki.example.com/runbooks/resource-exhaustion",
    },
})
```

### Rate of Change Alert

Detect sudden spikes or drops:

```go
// Alert on sudden traffic spike (>50% increase in 5 minutes)
engine.AddRule(chronicle.AlertRule{
    Name:       "TrafficSpike",
    Expression: "rate(requests_total[5m]) > 1.5 * rate(requests_total[1h] offset 5m)",
    Duration:   2 * time.Minute,
    Labels:     map[string]string{"severity": "warning"},
    Annotations: map[string]string{
        "summary": "Traffic increased by >50% in the last 5 minutes",
    },
})

// Alert on traffic drop (potential outage)
engine.AddRule(chronicle.AlertRule{
    Name:       "TrafficDrop",
    Expression: "rate(requests_total[5m]) < 0.5 * rate(requests_total[1h] offset 5m)",
    Duration:   2 * time.Minute,
    Labels:     map[string]string{"severity": "critical"},
    Annotations: map[string]string{
        "summary": "Traffic dropped by >50% - possible outage",
    },
})
```

## Data Management

### Automatic Data Retention

Configure time and size-based retention:

```go
db, _ := chronicle.Open("managed.db", chronicle.Config{
    Path:              "managed.db",
    RetentionDuration: 30 * 24 * time.Hour,    // Keep 30 days
    MaxStorageBytes:   10 * 1024 * 1024 * 1024, // Max 10GB
    DownsampleConfig: &chronicle.DownsampleConfig{
        Rules: []chronicle.DownsampleRule{
            {Age: 7 * 24 * time.Hour, Resolution: 5 * time.Minute},   // After 7 days: 5min
            {Age: 30 * 24 * time.Hour, Resolution: time.Hour},         // After 30 days: 1hr
        },
    },
})
```

### Backup and Restore

Create consistent backups while running:

```go
// Create a snapshot backup
func backupDatabase(db *chronicle.DB, backupPath string) error {
    backup := chronicle.NewBackup(db)
    return backup.Snapshot(context.Background(), chronicle.BackupOptions{
        Path:        backupPath,
        Compress:    true,
        Incremental: false,
    })
}

// Restore from backup
func restoreDatabase(backupPath, restorePath string) (*chronicle.DB, error) {
    err := chronicle.Restore(context.Background(), chronicle.RestoreOptions{
        SourcePath: backupPath,
        DestPath:   restorePath,
    })
    if err != nil {
        return nil, err
    }
    return chronicle.Open(restorePath, chronicle.DefaultConfig(restorePath))
}
```

### Export to CSV

Export data for analysis in other tools:

```go
func exportToCSV(db *chronicle.DB, metric string, w io.Writer) error {
    exporter := chronicle.NewExporter(db)
    return exporter.Export(context.Background(), chronicle.ExportOptions{
        Format:  chronicle.ExportCSV,
        Metrics: []string{metric},
        Start:   time.Now().Add(-24 * time.Hour),
        End:     time.Now(),
        Output:  w,
    })
}

// Usage
file, _ := os.Create("export.csv")
defer file.Close()
exportToCSV(db, "temperature", file)
```

## Monitoring Chronicle Itself

### Health Check Endpoint

Monitor Chronicle's health:

```go
package main

import (
    "encoding/json"
    "log"
    "net/http"
    "time"

    "github.com/chronicle-db/chronicle"
)

var db *chronicle.DB

func healthHandler(w http.ResponseWriter, r *http.Request) {
    stats := db.Stats()
    
    health := map[string]interface{}{
        "status":       "healthy",
        "series_count": stats.SeriesCount,
        "points_count": stats.PointsCount,
        "disk_usage":   stats.DiskBytes,
        "memory_usage": stats.MemoryBytes,
        "uptime":       stats.Uptime.String(),
    }
    
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(health)
}

func main() {
    var err error
    db, err = chronicle.Open("monitored.db", chronicle.Config{
        Path:        "monitored.db",
        HTTPEnabled: true,
        HTTPPort:    8086,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Custom health endpoint
    http.HandleFunc("/healthz", healthHandler)
    
    log.Println("Health endpoint: http://localhost:8087/healthz")
    http.ListenAndServe(":8087", nil)
}
```

### Cardinality Monitoring

Track and limit series cardinality:

```go
// Set up cardinality tracking
tracker := chronicle.NewCardinalityTracker(db, chronicle.CardinalityConfig{
    MaxTotalSeries:     100_000,
    MaxSeriesPerMetric: 10_000,
    OnLimitReached: func(metric string, count int) {
        log.Printf("WARNING: Cardinality limit reached for %s: %d series", metric, count)
    },
})

// Check cardinality stats
stats := tracker.Stats()
fmt.Printf("Total series: %d/%d\n", stats.TotalSeries, stats.MaxTotalSeries)
for metric, count := range stats.SeriesPerMetric {
    fmt.Printf("  %s: %d series\n", metric, count)
}
```

## Integration Patterns

### Grafana Data Source

Chronicle works with Grafana via Prometheus-compatible API:

```yaml
# grafana/provisioning/datasources/chronicle.yaml
apiVersion: 1
datasources:
  - name: Chronicle
    type: prometheus
    url: http://chronicle:8086
    access: proxy
    isDefault: true
```

### OpenTelemetry Integration

Receive OTLP metrics:

```go
db, _ := chronicle.Open("otel.db", chronicle.Config{
    Path:        "otel.db",
    HTTPEnabled: true,
    HTTPPort:    4318,  // OTLP HTTP port
})

// OTLP endpoint: http://localhost:4318/v1/metrics
// Configure your OTel collector to send metrics here
```

### Multi-Tenant Setup

Isolate data by tenant:

```go
// Create tenant manager
tenants := chronicle.NewTenantManager(chronicle.TenantConfig{
    BasePath: "/data/tenants",
    DefaultConfig: chronicle.Config{
        RetentionDuration: 7 * 24 * time.Hour,
    },
})

// Get or create tenant database
db, err := tenants.GetTenant("customer-123")
if err != nil {
    log.Fatal(err)
}

// Write with tenant isolation
db.Write(chronicle.Point{
    Metric:    "api_requests",
    Tags:      map[string]string{"endpoint": "/users"},
    Value:     1,
    Timestamp: time.Now().UnixNano(),
})
```

## Edge & IoT

### Memory-Constrained Device

Configuration for Raspberry Pi or similar:

```go
db, _ := chronicle.Open("/data/metrics.db", chronicle.Config{
    Path:              "/data/metrics.db",
    MaxMemory:         16 * 1024 * 1024,    // 16MB only
    BufferSize:        500,                  // Small buffer
    PartitionDuration: 15 * time.Minute,    // Frequent partitions
    RetentionDuration: 6 * time.Hour,       // Short retention
    SyncInterval:      30 * time.Second,    // Balance durability/performance
})
```

### Offline-First with Sync

Store locally, sync when connected:

```go
// Local edge database
localDB, _ := chronicle.Open("local.db", chronicle.Config{
    Path:              "local.db",
    RetentionDuration: 7 * 24 * time.Hour,
})

// Sync to central server when online
replicator := chronicle.NewReplicator(localDB, chronicle.ReplicatorConfig{
    RemoteURL:     "https://central.example.com/api/v1/prom/write",
    BatchSize:     1000,
    RetryInterval: time.Minute,
    OnSync: func(count int) {
        log.Printf("Synced %d points to central server", count)
    },
})

replicator.Start()
defer replicator.Stop()
```

---

:::tip
All recipes are tested with Chronicle v1.x. Adjust configuration values based on your specific requirements and hardware constraints.
:::
