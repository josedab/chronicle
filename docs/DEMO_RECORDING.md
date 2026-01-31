# Recording a Demo Video

This guide helps you create a compelling demo video for Chronicle.

## Prerequisites

- Terminal with good contrast (dark theme recommended)
- Screen recording software:
  - macOS: QuickTime Player or OBS
  - Linux: OBS or SimpleScreenRecorder
  - Windows: OBS or built-in Game Bar

## Demo Script

### 1. Introduction (30 seconds)

```
"Chronicle is an embedded time-series database for Go.
Unlike Prometheus or InfluxDB, it runs inside your application
with zero external dependencies."
```

### 2. Installation (15 seconds)

```bash
# Show installation
go get github.com/chronicle-db/chronicle
```

### 3. Basic Usage (60 seconds)

Create and run this file:

```go
// demo.go
package main

import (
    "fmt"
    "time"
    "github.com/chronicle-db/chronicle"
)

func main() {
    // Open database - single file, no server needed
    db, _ := chronicle.Open("demo.db", chronicle.DefaultConfig("demo.db"))
    defer db.Close()

    // Write some data
    for i := 0; i < 100; i++ {
        db.Write(chronicle.Point{
            Metric:    "temperature",
            Tags:      map[string]string{"room": "office"},
            Value:     20.0 + float64(i%10),
            Timestamp: time.Now().Add(-time.Duration(100-i) * time.Minute).UnixNano(),
        })
    }
    db.Flush()

    // Query with aggregation
    result, _ := db.Execute(&chronicle.Query{
        Metric: "temperature",
        Aggregation: &chronicle.Aggregation{
            Function: chronicle.AggMean,
            Window:   time.Hour,
        },
    })

    fmt.Printf("Found %d hourly averages\n", len(result.Points))
    for _, p := range result.Points {
        fmt.Printf("  %.1f°C at %s\n", p.Value, time.Unix(0, p.Timestamp).Format("15:04"))
    }
}
```

Run it:

```bash
go run demo.go
```

### 4. HTTP API (45 seconds)

```go
// http_demo.go
package main

import (
    "github.com/chronicle-db/chronicle"
)

func main() {
    db, _ := chronicle.Open("demo.db", chronicle.Config{
        Path:        "demo.db",
        HTTPEnabled: true,
        HTTPPort:    8086,
    })
    defer db.Close()

    println("Server running on http://localhost:8086")
    select {} // Keep running
}
```

In another terminal:

```bash
# Write data
curl -X POST http://localhost:8086/write \
  -d 'cpu_usage,host=server1 value=45.7'

# Query
curl -X POST http://localhost:8086/query \
  -H "Content-Type: application/json" \
  -d '{"metric":"cpu_usage"}'

# PromQL
curl "http://localhost:8086/api/v1/query?query=cpu_usage"
```

### 5. Key Features Highlights (45 seconds)

Show these one-liners:

```go
// SQL-like queries
parser.Parse("SELECT mean(value) FROM cpu WHERE host='server1' GROUP BY time(5m)")

// PromQL support
executor.Query(`rate(http_requests_total[5m])`, time.Now())

// Forecasting
forecaster.Forecast(ForecastRequest{Metric: "cpu", Horizon: 24*time.Hour})

// Native histograms
db.WriteHistogram(HistogramPoint{Metric: "latency", Buckets: buckets})
```

### 6. Closing (15 seconds)

```
"Chronicle - embedded time-series for Go.
Install with: go get github.com/chronicle-db/chronicle
Documentation at: chronicle-db.github.io/chronicle"
```

## Recording Tips

1. **Font size**: Use 18-20pt font for readability
2. **Speed**: Type at a moderate pace, pause after important commands
3. **Terminal size**: 120x30 characters works well
4. **Clean environment**: Clear terminal history, use clean directory
5. **Audio**: Record narration separately for better quality

## Recommended Tools

### Terminal Recording (Text-based)

```bash
# asciinema - records terminal sessions
brew install asciinema
asciinema rec demo.cast

# Convert to GIF
# https://github.com/asciinema/agg
agg demo.cast demo.gif
```

### Video Editing

- **DaVinci Resolve** (free) - Professional editing
- **iMovie** (macOS) - Simple editing
- **Kdenlive** (Linux) - Good free option

## Output Formats

1. **GIF** - For README/docs (< 30 seconds, no audio)
2. **MP4** - For YouTube/website
3. **WebM** - For web embedding

## Example Upload Locations

- GitHub README (GIF via GitHub CDN)
- YouTube (unlisted or public)
- Vimeo (for embedding)
- Website `/static/demo.mp4`

## Demo Files

After recording, add the demo to the repository:

```
website/
└── static/
    └── demo/
        ├── demo.gif       # For README
        ├── demo.mp4       # Full video
        └── thumbnail.png  # Video thumbnail
```

Update README.md:

```markdown
## Demo

![Chronicle Demo](website/static/demo/demo.gif)

[Watch full demo on YouTube](https://youtube.com/...)
```
