---
sidebar_position: 2
---

# Configuration

Complete reference for Chronicle configuration options.

## Basic Usage

```go
// Use defaults
db, _ := chronicle.Open("data.db", chronicle.DefaultConfig("data.db"))

// Custom configuration
db, _ := chronicle.Open("data.db", chronicle.Config{
    Path:              "data.db",
    MaxMemory:         128 * 1024 * 1024,
    PartitionDuration: time.Hour,
    RetentionDuration: 7 * 24 * time.Hour,
})
```

## Config Struct

```go
type Config struct {
    // Path is the file path for the database. Required.
    Path string

    // MaxMemory is the maximum memory budget in bytes.
    // Default: 64MB
    MaxMemory int64

    // MaxStorageBytes is the maximum database file size.
    // When exceeded, oldest partitions are removed.
    // Default: 0 (unlimited)
    MaxStorageBytes int64

    // SyncInterval is how often the WAL syncs to disk.
    // Default: 1 second
    SyncInterval time.Duration

    // PartitionDuration is the time span per partition.
    // Default: 1 hour
    PartitionDuration time.Duration

    // WALMaxSize is the max WAL file size before rotation.
    // Default: 128MB
    WALMaxSize int64

    // WALRetain is the number of old WAL files to keep.
    // Default: 3
    WALRetain int

    // RetentionDuration is how long data is kept.
    // Default: 0 (infinite)
    RetentionDuration time.Duration

    // DownsampleRules defines automatic downsampling.
    DownsampleRules []DownsampleRule

    // BufferSize is points to buffer before flushing.
    // Default: 10,000
    BufferSize int

    // CompactionWorkers is background compaction workers.
    // Default: 1
    CompactionWorkers int

    // CompactionInterval is how often compaction runs.
    // Default: 30 minutes
    CompactionInterval time.Duration

    // QueryTimeout is max query execution time.
    // Default: 30 seconds
    QueryTimeout time.Duration

    // HTTPEnabled enables the HTTP API server.
    // Default: false
    HTTPEnabled bool

    // HTTPPort is the HTTP server port.
    // Default: 8086
    HTTPPort int

    // PrometheusRemoteWriteEnabled enables Prometheus remote write.
    // Default: false
    PrometheusRemoteWriteEnabled bool

    // ContinuousQueries defines materialized views.
    ContinuousQueries []ContinuousQuery

    // Replication configures outbound replication.
    Replication *ReplicationConfig

    // Schemas defines metric schemas for validation.
    Schemas []MetricSchema

    // StrictSchema rejects points without a schema.
    // Default: false
    StrictSchema bool

    // Encryption configures encryption at rest.
    Encryption *EncryptionConfig
}
```

## Configuration Options

### Storage Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `Path` | `string` | Required | Database file path |
| `MaxMemory` | `int64` | 64MB | Memory budget for buffers |
| `MaxStorageBytes` | `int64` | 0 (unlimited) | Max database size |
| `PartitionDuration` | `Duration` | 1 hour | Time span per partition |

```go
config := chronicle.Config{
    Path:              "/var/lib/chronicle/data.db",
    MaxMemory:         256 * 1024 * 1024,  // 256MB
    MaxStorageBytes:   50 * 1024 * 1024 * 1024, // 50GB
    PartitionDuration: 6 * time.Hour,
}
```

### WAL Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `SyncInterval` | `Duration` | 1s | WAL sync frequency |
| `WALMaxSize` | `int64` | 128MB | Max WAL file size |
| `WALRetain` | `int` | 3 | Old WAL files to keep |

```go
config := chronicle.Config{
    SyncInterval: 100 * time.Millisecond, // More durable
    WALMaxSize:   64 * 1024 * 1024,       // 64MB
    WALRetain:    5,
}
```

### Retention Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `RetentionDuration` | `Duration` | 0 (infinite) | Data retention period |
| `DownsampleRules` | `[]DownsampleRule` | nil | Downsampling policies |

```go
config := chronicle.Config{
    RetentionDuration: 30 * 24 * time.Hour,
    DownsampleRules: []chronicle.DownsampleRule{
        {
            SourceResolution: time.Minute,
            TargetResolution: time.Hour,
            Aggregations:     []chronicle.AggFunc{chronicle.AggMean},
            Retention:        365 * 24 * time.Hour,
        },
    },
}
```

### Performance Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `BufferSize` | `int` | 10,000 | Write buffer capacity |
| `CompactionWorkers` | `int` | 1 | Parallel compaction |
| `CompactionInterval` | `Duration` | 30min | Compaction frequency |
| `QueryTimeout` | `Duration` | 30s | Max query time |

```go
// High-throughput configuration
config := chronicle.Config{
    BufferSize:         100_000,
    CompactionWorkers:  4,
    CompactionInterval: 15 * time.Minute,
    QueryTimeout:       60 * time.Second,
}
```

### HTTP Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `HTTPEnabled` | `bool` | false | Enable HTTP API |
| `HTTPPort` | `int` | 8086 | HTTP listen port |
| `PrometheusRemoteWriteEnabled` | `bool` | false | Enable Prometheus write |

```go
config := chronicle.Config{
    HTTPEnabled:                  true,
    HTTPPort:                     8086,
    PrometheusRemoteWriteEnabled: true,
}
```

### Schema Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `Schemas` | `[]MetricSchema` | nil | Metric schemas |
| `StrictSchema` | `bool` | false | Require schemas |

```go
config := chronicle.Config{
    StrictSchema: true,
    Schemas: []chronicle.MetricSchema{
        {
            Name:         "cpu_usage",
            Type:         chronicle.SchemaTypeGauge,
            RequiredTags: []string{"host"},
        },
    },
}
```

### Encryption Options

```go
config := chronicle.Config{
    Encryption: &chronicle.EncryptionConfig{
        Enabled:   true,
        Algorithm: chronicle.EncryptionAES256GCM,
        Key:       []byte("32-byte-key-for-aes-256-encrypt"),
    },
}
```

### Replication Options

```go
config := chronicle.Config{
    Replication: &chronicle.ReplicationConfig{
        Enabled:       true,
        RemoteURL:     "http://replica:8086/write",
        BatchSize:     1000,
        FlushInterval: 5 * time.Second,
    },
}
```

## DefaultConfig

`DefaultConfig` returns sensible defaults:

```go
func DefaultConfig(path string) Config {
    return Config{
        Path:              path,
        MaxMemory:         64 * 1024 * 1024,  // 64MB
        SyncInterval:      time.Second,
        PartitionDuration: time.Hour,
        WALMaxSize:        128 * 1024 * 1024, // 128MB
        WALRetain:         3,
        BufferSize:        10_000,
        CompactionWorkers: 1,
        CompactionInterval: 30 * time.Minute,
        QueryTimeout:       30 * time.Second,
        HTTPPort:          8086,
    }
}
```

## Configuration Recipes

### Development

```go
config := chronicle.Config{
    Path:              "dev.db",
    MaxMemory:         32 * 1024 * 1024,
    PartitionDuration: 15 * time.Minute,
    HTTPEnabled:       true,
    HTTPPort:          8086,
}
```

### Production - High Throughput

```go
config := chronicle.Config{
    Path:              "/var/lib/chronicle/data.db",
    MaxMemory:         512 * 1024 * 1024,
    MaxStorageBytes:   100 * 1024 * 1024 * 1024,
    BufferSize:        100_000,
    SyncInterval:      time.Second,
    PartitionDuration: 6 * time.Hour,
    RetentionDuration: 30 * 24 * time.Hour,
    CompactionWorkers: 4,
    QueryTimeout:      60 * time.Second,
    HTTPEnabled:       true,
    HTTPPort:          8086,
}
```

### Production - Durability Focus

```go
config := chronicle.Config{
    Path:              "/var/lib/chronicle/data.db",
    MaxMemory:         256 * 1024 * 1024,
    BufferSize:        10_000,
    SyncInterval:      100 * time.Millisecond,
    WALRetain:         5,
    PartitionDuration: time.Hour,
    RetentionDuration: 90 * 24 * time.Hour,
    HTTPEnabled:       true,
}
```

### IoT / Edge

```go
config := chronicle.Config{
    Path:              "/data/metrics.db",
    MaxMemory:         16 * 1024 * 1024,   // 16MB
    MaxStorageBytes:   1024 * 1024 * 1024, // 1GB
    BufferSize:        1000,
    PartitionDuration: 30 * time.Minute,
    RetentionDuration: 24 * time.Hour,
}
