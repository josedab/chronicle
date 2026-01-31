# Chronicle Configuration Reference

This document describes all configuration options for Chronicle.

## Quick Start

```go
import "github.com/chronicle-db/chronicle"

// Use defaults
cfg := chronicle.DefaultConfig("data.db")

// Or customize
cfg := chronicle.Config{
    Path:              "data.db",
    MaxMemory:         128 * 1024 * 1024,  // 128MB
    PartitionDuration: 2 * time.Hour,
    RetentionDuration: 30 * 24 * time.Hour,  // 30 days
    HTTPEnabled:       true,
    HTTPPort:          8086,
}

db, err := chronicle.Open(cfg.Path, cfg)
```

---

## Core Configuration

### Path

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `Path` | `string` | (required) | File path for the database |

```go
cfg.Path = "/var/lib/chronicle/data.db"
```

### Memory

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `MaxMemory` | `int64` | 64MB | Maximum memory for buffers and caches |
| `BufferSize` | `int` | 10,000 | Points to buffer before flushing |

```go
cfg.MaxMemory = 128 * 1024 * 1024  // 128MB
cfg.BufferSize = 50_000
```

**Guidelines:**
- IoT/Edge: 16-64MB
- Development: 64-128MB
- Production server: 256MB-1GB

### Storage

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `MaxStorageBytes` | `int64` | 0 (unlimited) | Maximum database size |
| `PartitionDuration` | `time.Duration` | 1 hour | Time span per partition |

```go
cfg.MaxStorageBytes = 10 * 1024 * 1024 * 1024  // 10GB
cfg.PartitionDuration = 6 * time.Hour
```

**Partition duration guidelines:**
- High-frequency data (>1000 pts/sec): 15-30 minutes
- Medium frequency (10-1000 pts/sec): 1 hour (default)
- Low frequency (<10 pts/sec): 6-24 hours

---

## Write-Ahead Log (WAL)

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `SyncInterval` | `time.Duration` | 1 second | How often to sync WAL to disk |
| `WALMaxSize` | `int64` | 128MB | Max size before WAL rotation |
| `WALRetain` | `int` | 3 | Number of old WAL files to keep |

```go
cfg.SyncInterval = 100 * time.Millisecond  // More durable
cfg.WALMaxSize = 64 * 1024 * 1024  // 64MB
cfg.WALRetain = 5
```

**Trade-offs:**
- Lower `SyncInterval` = better durability, higher latency
- Higher `WALMaxSize` = fewer rotations, longer recovery

---

## Retention & Compaction

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `RetentionDuration` | `time.Duration` | 0 (forever) | How long to keep data |
| `CompactionWorkers` | `int` | 1 | Background compaction threads |
| `CompactionInterval` | `time.Duration` | 30 minutes | How often to run compaction |

```go
cfg.RetentionDuration = 7 * 24 * time.Hour  // 7 days
cfg.CompactionWorkers = 2
cfg.CompactionInterval = 15 * time.Minute
```

---

## Downsampling

Reduce storage by aggregating old data:

```go
cfg.DownsampleRules = []chronicle.DownsampleRule{
    {
        After:    24 * time.Hour,      // After 1 day
        Window:   5 * time.Minute,     // 5-minute buckets
        Function: chronicle.AggMean,
    },
    {
        After:    7 * 24 * time.Hour,  // After 1 week
        Window:   1 * time.Hour,       // 1-hour buckets
        Function: chronicle.AggMean,
    },
}
```

| Field | Type | Description |
|-------|------|-------------|
| `After` | `time.Duration` | Age threshold for downsampling |
| `Window` | `time.Duration` | Aggregation window size |
| `Function` | `AggFunc` | Aggregation function |

---

## Query

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `QueryTimeout` | `time.Duration` | 30 seconds | Maximum query execution time |

```go
cfg.QueryTimeout = 1 * time.Minute
```

---

## HTTP API

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `HTTPEnabled` | `bool` | false | Enable HTTP server |
| `HTTPPort` | `int` | 8086 | HTTP server port |
| `PrometheusRemoteWriteEnabled` | `bool` | false | Enable Prometheus remote write |

```go
cfg.HTTPEnabled = true
cfg.HTTPPort = 9090
cfg.PrometheusRemoteWriteEnabled = true
```

When enabled, the following endpoints are available:
- `/health` - Health check
- `/write` - InfluxDB line protocol
- `/query` - SQL-like queries
- `/api/v1/query` - PromQL instant queries
- `/api/v1/query_range` - PromQL range queries
- `/api/v1/prom/write` - Prometheus remote write
- `/v1/metrics` - OpenTelemetry OTLP
- `/schemas` - Schema registry
- `/api/v1/alerts` - Active alerts
- `/api/v1/rules` - Alert rules
- `/stream` - WebSocket streaming

See [API.md](./API.md) for details.

---

## Continuous Queries

Materialized views with periodic refresh:

```go
cfg.ContinuousQueries = []chronicle.ContinuousQuery{
    {
        Name:     "cpu_5m",
        Source:   "cpu_usage",
        Target:   "cpu_usage_5m",
        Window:   5 * time.Minute,
        Function: chronicle.AggMean,
        Interval: 1 * time.Minute,
        GroupBy:  []string{"host"},
    },
}
```

| Field | Type | Description |
|-------|------|-------------|
| `Name` | `string` | Unique identifier |
| `Source` | `string` | Source metric name |
| `Target` | `string` | Destination metric name |
| `Window` | `time.Duration` | Aggregation window |
| `Function` | `AggFunc` | Aggregation function |
| `Interval` | `time.Duration` | Refresh interval |
| `GroupBy` | `[]string` | Grouping tags |

---

## Replication

Replicate data to a remote Chronicle instance:

```go
cfg.Replication = &chronicle.ReplicationConfig{
    Enabled:     true,
    Endpoint:    "https://central.example.com/api/v1/write",
    BatchSize:   1000,
    Interval:    10 * time.Second,
    Headers:     map[string]string{"Authorization": "Bearer token"},
    Compression: true,
}
```

| Field | Type | Description |
|-------|------|-------------|
| `Enabled` | `bool` | Enable replication |
| `Endpoint` | `string` | Remote write endpoint URL |
| `BatchSize` | `int` | Points per batch |
| `Interval` | `time.Duration` | Send interval |
| `Headers` | `map[string]string` | HTTP headers |
| `Compression` | `bool` | Enable gzip compression |

---

## Schema Validation

Define schemas to validate incoming data:

```go
cfg.StrictSchema = true  // Reject unknown metrics
cfg.Schemas = []chronicle.MetricSchema{
    {
        Name: "temperature",
        Tags: []chronicle.TagSchema{
            {Name: "sensor_id", Required: true},
            {Name: "unit", AllowedVals: []string{"celsius", "fahrenheit"}},
        },
        Fields: []chronicle.FieldSchema{
            {Name: "value", MinValue: ptr(-273.15), MaxValue: ptr(1000.0)},
        },
    },
}
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `StrictSchema` | `bool` | false | Reject points without schema |
| `Schemas` | `[]MetricSchema` | nil | Schema definitions |

### MetricSchema

| Field | Type | Description |
|-------|------|-------------|
| `Name` | `string` | Metric name |
| `Tags` | `[]TagSchema` | Tag definitions |
| `Fields` | `[]FieldSchema` | Field definitions |

### TagSchema

| Field | Type | Description |
|-------|------|-------------|
| `Name` | `string` | Tag name |
| `Required` | `bool` | Tag must be present |
| `AllowedVals` | `[]string` | Valid values (empty = any) |

### FieldSchema

| Field | Type | Description |
|-------|------|-------------|
| `Name` | `string` | Field name |
| `MinValue` | `*float64` | Minimum value (nil = no min) |
| `MaxValue` | `*float64` | Maximum value (nil = no max) |

---

## Encryption

Enable AES-256-GCM encryption at rest:

```go
cfg.Encryption = &chronicle.EncryptionConfig{
    Enabled:     true,
    KeyPassword: os.Getenv("CHRONICLE_ENCRYPTION_KEY"),
}
```

| Field | Type | Description |
|-------|------|-------------|
| `Enabled` | `bool` | Enable encryption |
| `KeyPassword` | `string` | Password for key derivation |

**Security notes:**
- Key is derived using PBKDF2 with 100,000 iterations
- Random salt is stored in the database header
- Each block uses a unique IV/nonce

---

## Storage Backend Configuration

### S3BackendConfig

```go
s3Cfg := chronicle.S3BackendConfig{
    Bucket:          "my-bucket",
    Region:          "us-west-2",
    Endpoint:        "",  // Use for MinIO, etc.
    AccessKeyID:     "",  // Optional, uses default chain
    SecretAccessKey: "",
    Prefix:          "chronicle/",
    UsePathStyle:    false,
    CacheSize:       100,
}
backend, err := chronicle.NewS3Backend(s3Cfg)
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `Bucket` | `string` | (required) | S3 bucket name |
| `Region` | `string` | us-east-1 | AWS region |
| `Endpoint` | `string` | "" | Custom endpoint (MinIO) |
| `AccessKeyID` | `string` | "" | AWS access key |
| `SecretAccessKey` | `string` | "" | AWS secret key |
| `Prefix` | `string` | "" | Key prefix for objects |
| `UsePathStyle` | `bool` | false | Use path-style addressing |
| `CacheSize` | `int` | 100 | Partitions to cache locally |

---

## Environment Variables

Chronicle also reads configuration from environment variables:

| Variable | Description |
|----------|-------------|
| `CHRONICLE_PATH` | Database file path |
| `CHRONICLE_HTTP_PORT` | HTTP server port |
| `CHRONICLE_ENCRYPTION_KEY` | Encryption password |
| `AWS_ACCESS_KEY_ID` | S3 access key |
| `AWS_SECRET_ACCESS_KEY` | S3 secret key |
| `AWS_REGION` | S3 region |

---

## Example Configurations

### IoT Edge Device

```go
cfg := chronicle.Config{
    Path:              "/data/metrics.db",
    MaxMemory:         16 * 1024 * 1024,  // 16MB
    BufferSize:        1000,
    PartitionDuration: 1 * time.Hour,
    RetentionDuration: 24 * time.Hour,
    SyncInterval:      5 * time.Second,
}
```

### Development

```go
cfg := chronicle.DefaultConfig("dev.db")
cfg.HTTPEnabled = true
cfg.MaxMemory = 256 * 1024 * 1024
```

### Production Server

```go
cfg := chronicle.Config{
    Path:                         "/var/lib/chronicle/data.db",
    MaxMemory:                    1024 * 1024 * 1024,  // 1GB
    BufferSize:                   100_000,
    PartitionDuration:            1 * time.Hour,
    RetentionDuration:            30 * 24 * time.Hour,
    CompactionWorkers:            4,
    HTTPEnabled:                  true,
    HTTPPort:                     8086,
    PrometheusRemoteWriteEnabled: true,
    Encryption: &chronicle.EncryptionConfig{
        Enabled:     true,
        KeyPassword: os.Getenv("CHRONICLE_ENCRYPTION_KEY"),
    },
}
```

---

## See Also

- [README](../README.md) - Quick start
- [FEATURES](./FEATURES.md) - Feature guide
- [ARCHITECTURE](./ARCHITECTURE.md) - System design
- [API](./API.md) - HTTP endpoints
