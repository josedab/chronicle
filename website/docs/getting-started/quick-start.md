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

## Real-World Example: IoT Sensor Monitoring

Here's a complete example showing how to build a temperature monitoring system for a smart building:

```go
package main

import (
    "fmt"
    "log"
    "math"
    "math/rand"
    "time"

    "github.com/chronicle-db/chronicle"
)

func main() {
    // Configure for IoT: small memory footprint, automatic cleanup
    db, err := chronicle.Open("sensors.db", chronicle.Config{
        Path:              "sensors.db",
        MaxMemory:         32 * 1024 * 1024,   // 32MB - suitable for edge devices
        PartitionDuration: 15 * time.Minute,   // Smaller partitions for faster queries
        RetentionDuration: 24 * time.Hour,     // Keep only 24 hours of data
        BufferSize:        1000,               // Flush every 1000 points
        HTTPEnabled:       true,
        HTTPPort:          8086,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Define sensors across different rooms
    sensors := []struct {
        id       string
        room     string
        floor    string
        baseline float64
    }{
        {"temp-001", "lobby", "1", 21.0},
        {"temp-002", "office-a", "2", 22.0},
        {"temp-003", "office-b", "2", 22.5},
        {"temp-004", "server-room", "basement", 18.0},
        {"temp-005", "conference", "3", 21.5},
    }

    // Simulate 24 hours of sensor data (1 reading per minute)
    fmt.Println("Generating 24 hours of sensor data...")
    startTime := time.Now().Add(-24 * time.Hour)
    
    for minute := 0; minute < 24*60; minute++ {
        timestamp := startTime.Add(time.Duration(minute) * time.Minute)
        hour := timestamp.Hour()
        
        for _, sensor := range sensors {
            // Simulate realistic temperature patterns
            // - Warmer during business hours (9-18)
            // - Server room stays cool but spikes occasionally
            var temp float64
            if sensor.room == "server-room" {
                temp = sensor.baseline + rand.Float64()*2
                if rand.Float64() < 0.02 { // 2% chance of spike
                    temp += 5 + rand.Float64()*3
                }
            } else {
                // Normal rooms: warmer during day, cooler at night
                dayBonus := 0.0
                if hour >= 9 && hour <= 18 {
                    dayBonus = 2.0 + math.Sin(float64(hour-9)/9*math.Pi)*1.5
                }
                temp = sensor.baseline + dayBonus + (rand.Float64()-0.5)*1.5
            }

            db.Write(chronicle.Point{
                Metric: "temperature",
                Tags: map[string]string{
                    "sensor_id": sensor.id,
                    "room":      sensor.room,
                    "floor":     sensor.floor,
                },
                Value:     temp,
                Timestamp: timestamp.UnixNano(),
            })
        }
    }
    db.Flush()
    fmt.Println("✓ Generated data for 5 sensors over 24 hours")

    // Query 1: Current temperature in all rooms
    fmt.Println("\n📊 Current temperatures by room:")
    result, _ := db.Execute(&chronicle.Query{
        Metric:  "temperature",
        Start:   time.Now().Add(-5 * time.Minute).UnixNano(),
        End:     time.Now().UnixNano(),
        GroupBy: []string{"room"},
        Aggregation: &chronicle.Aggregation{
            Function: chronicle.AggLast,
        },
    })
    for _, p := range result.Points {
        fmt.Printf("   %s: %.1f°C\n", p.Tags["room"], p.Value)
    }

    // Query 2: Hourly averages for the server room
    fmt.Println("\n📈 Server room hourly averages (last 6 hours):")
    result, _ = db.Execute(&chronicle.Query{
        Metric: "temperature",
        Tags:   map[string]string{"room": "server-room"},
        Start:  time.Now().Add(-6 * time.Hour).UnixNano(),
        End:    time.Now().UnixNano(),
        Aggregation: &chronicle.Aggregation{
            Function: chronicle.AggMean,
            Window:   time.Hour,
        },
    })
    for _, p := range result.Points {
        t := time.Unix(0, p.Timestamp)
        fmt.Printf("   %s: %.1f°C\n", t.Format("15:04"), p.Value)
    }

    // Query 3: Find temperature anomalies (max values)
    fmt.Println("\n🔥 Peak temperatures by floor:")
    result, _ = db.Execute(&chronicle.Query{
        Metric:  "temperature",
        Start:   time.Now().Add(-24 * time.Hour).UnixNano(),
        End:     time.Now().UnixNano(),
        GroupBy: []string{"floor"},
        Aggregation: &chronicle.Aggregation{
            Function: chronicle.AggMax,
        },
    })
    for _, p := range result.Points {
        fmt.Printf("   Floor %s: %.1f°C (max)\n", p.Tags["floor"], p.Value)
    }

    // Set up alerting for high temperatures
    fmt.Println("\n🚨 Setting up temperature alerts...")
    engine := chronicle.NewAlertEngine(db, chronicle.AlertConfig{
        EvaluationInterval: time.Minute,
    })
    
    engine.AddRule(chronicle.AlertRule{
        Name:       "ServerRoomOverheat",
        Expression: `max(temperature{room="server-room"}) > 25`,
        Duration:   2 * time.Minute,
        Labels:     map[string]string{"severity": "critical", "team": "facilities"},
        Annotations: map[string]string{
            "summary":     "Server room temperature exceeds safe threshold",
            "description": "Temperature in server room has been above 25°C for 2 minutes",
        },
    })

    engine.AddRule(chronicle.AlertRule{
        Name:       "OfficeTooCold",
        Expression: `min(temperature{floor="2"}) < 18`,
        Duration:   5 * time.Minute,
        Labels:     map[string]string{"severity": "warning", "team": "facilities"},
        Annotations: map[string]string{
            "summary": "Office temperature below comfort threshold",
        },
    })
    
    fmt.Println("✓ Alert rules configured")
    fmt.Println("\n🌐 HTTP API available at http://localhost:8086")
    fmt.Println("   Try: curl 'http://localhost:8086/api/v1/query?query=temperature'")
    fmt.Println("\nPress Ctrl+C to exit")
    select {}
}
```

Run this example:

```bash
go run main.go
```

Expected output:

```
Generating 24 hours of sensor data...
✓ Generated data for 5 sensors over 24 hours

📊 Current temperatures by room:
   lobby: 21.3°C
   office-a: 23.1°C
   office-b: 23.8°C
   server-room: 18.4°C
   conference: 22.7°C

📈 Server room hourly averages (last 6 hours):
   08:00: 18.2°C
   09:00: 18.5°C
   10:00: 18.1°C
   11:00: 18.9°C
   12:00: 18.3°C
   13:00: 18.6°C

🔥 Peak temperatures by floor:
   Floor 1: 23.2°C (max)
   Floor 2: 25.1°C (max)
   Floor 3: 24.3°C (max)
   Floor basement: 26.8°C (max)

🚨 Setting up temperature alerts...
✓ Alert rules configured

🌐 HTTP API available at http://localhost:8086
   Try: curl 'http://localhost:8086/api/v1/query?query=temperature'
```

## What's Next?

- [First Queries](/docs/getting-started/first-queries) - Learn the query language
- [Data Model](/docs/core-concepts/data-model) - Understand metrics, tags, and series
- [HTTP API Guide](/docs/guides/http-api) - Complete HTTP endpoint reference
