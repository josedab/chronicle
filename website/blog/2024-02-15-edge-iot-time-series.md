---
slug: edge-iot-time-series
title: "Building IoT Solutions with Embedded Time-Series Storage"
authors: [chronicle]
tags: [tutorial, iot, edge-computing]
---

Edge computing and IoT present unique challenges for time-series data: limited resources, unreliable connectivity, and the need for local intelligence. Here's how Chronicle addresses these challenges.

<!-- truncate -->

## The Edge Computing Challenge

Traditional architectures send all sensor data to the cloud for processing. This works until:

- **Bandwidth is limited or expensive** — Cellular connections on industrial equipment
- **Latency matters** — Real-time control systems can't wait for round-trips
- **Connectivity is unreliable** — Remote sites with intermittent internet
- **Data volumes are massive** — High-frequency sensors generating gigabytes per day

The solution? Process data at the edge, store it locally, and sync intelligently.

## Chronicle for Edge Deployments

Chronicle was designed with edge constraints in mind:

### Minimal Resource Requirements

```go
// Configuration for a Raspberry Pi or industrial gateway
db, _ := chronicle.Open("/data/sensors.db", chronicle.Config{
    Path:              "/data/sensors.db",
    MaxMemory:         16 * 1024 * 1024,  // Just 16MB
    BufferSize:        500,               // Small buffer
    PartitionDuration: 15 * time.Minute,  // Frequent partitions
    RetentionDuration: 24 * time.Hour,    // Auto-cleanup
})
```

Chronicle runs comfortably on devices with 32MB of RAM.

### Local Analytics

Don't just store data — analyze it locally:

```go
// Detect anomalies without cloud connectivity
anomalyDetector := chronicle.NewAnomalyDetector(db, chronicle.AnomalyConfig{
    Metric:     "motor_vibration",
    Method:     chronicle.AnomalyMAD,
    Window:     time.Hour,
    Threshold:  3.0,
})

anomalies := anomalyDetector.Detect(ctx, time.Now().Add(-time.Hour), time.Now())
if len(anomalies) > 0 {
    triggerLocalAlert(anomalies)
}
```

### Smart Sync

When connectivity is available, sync only what matters:

```go
replicator := chronicle.NewReplicator(db, chronicle.ReplicatorConfig{
    RemoteURL:     "https://cloud.example.com/ingest",
    BatchSize:     1000,
    RetryInterval: 5 * time.Minute,
    
    // Only sync aggregated data, keep raw locally
    Filter: func(p chronicle.Point) bool {
        return p.Tags["aggregated"] == "true"
    },
})
```

## Real-World Example: Industrial Motor Monitoring

Let's build a complete edge monitoring solution:

```go
package main

import (
    "log"
    "math/rand"
    "time"
    
    "github.com/chronicle-db/chronicle"
)

func main() {
    // Initialize edge database
    db, err := chronicle.Open("/data/motors.db", chronicle.Config{
        Path:              "/data/motors.db",
        MaxMemory:         32 * 1024 * 1024,
        PartitionDuration: 15 * time.Minute,
        RetentionDuration: 7 * 24 * time.Hour,
        HTTPEnabled:       true,
        HTTPPort:          8086,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Set up recording rules for efficient storage
    rules := chronicle.NewRecordingRules(db)
    rules.Add(chronicle.RecordingRule{
        Name:     "motor_vibration_5m_avg",
        Query:    "avg(motor_vibration) by (motor_id)",
        Interval: 5 * time.Minute,
    })
    rules.Start()
    defer rules.Stop()

    // Set up local alerting
    alerts := chronicle.NewAlertEngine(db, chronicle.AlertConfig{
        EvaluationInterval: 30 * time.Second,
    })
    
    alerts.AddRule(chronicle.AlertRule{
        Name:       "HighVibration",
        Expression: "max(motor_vibration) > 10",
        Duration:   time.Minute,
        Labels:     map[string]string{"severity": "warning"},
    })
    
    alerts.SetNotificationHandler(func(alerts []chronicle.Alert) error {
        for _, a := range alerts {
            // Trigger local PLC action, light, or buzzer
            log.Printf("LOCAL ALERT: %s - %s", a.Labels["alertname"], a.Status)
        }
        return nil
    })
    alerts.Start()
    defer alerts.Stop()

    // Simulate motor data collection
    go func() {
        for {
            for motorID := 1; motorID <= 4; motorID++ {
                vibration := 2.0 + rand.Float64()*3.0
                if rand.Float64() < 0.01 {
                    vibration += 10 // Occasional spike
                }
                
                db.Write(chronicle.Point{
                    Metric:    "motor_vibration",
                    Tags:      map[string]string{"motor_id": fmt.Sprintf("M%d", motorID)},
                    Value:     vibration,
                    Timestamp: time.Now().UnixNano(),
                })
            }
            time.Sleep(100 * time.Millisecond)
        }
    }()

    log.Println("Edge monitoring running on http://localhost:8086")
    select {}
}
```

## Benefits Over Cloud-Only Approaches

| Aspect | Cloud-Only | Edge + Chronicle |
|--------|------------|------------------|
| Latency | 100-500ms | Under 1ms |
| Connectivity dependency | Required | Optional |
| Bandwidth cost | High | Minimal |
| Local intelligence | None | Full analytics |
| Data ownership | Cloud provider | Local |

## Deployment Tips

### 1. Size Your Retention

Match retention to your sync frequency:

```go
// If you sync daily, keep 3 days locally for safety
RetentionDuration: 3 * 24 * time.Hour,
```

### 2. Use Recording Rules

Pre-aggregate data to reduce storage and sync volume:

```go
rules.Add(chronicle.RecordingRule{
    Name:     "sensor_hourly_summary",
    Query:    "avg(sensor_value) by (sensor_id)",
    Interval: time.Hour,
    Labels:   map[string]string{"aggregated": "true"},
})
```

### 3. Handle Restarts Gracefully

Chronicle's WAL ensures no data loss on power failures:

```go
// Data is automatically recovered on restart
db, _ := chronicle.Open("/data/sensors.db", config)
// WAL replay happens automatically
```

## Conclusion

Edge computing doesn't mean giving up on sophisticated time-series analysis. With Chronicle, you get the full power of a time-series database in a package that fits on an industrial gateway or Raspberry Pi.

**Ready to try it?** Check out our [IoT Sensor Monitoring example](/docs/getting-started/quick-start#real-world-example-iot-sensor-monitoring) in the Quick Start guide.
