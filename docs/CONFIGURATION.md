# Chronicle Configuration Reference

This document describes all configuration options for Chronicle.

## Quick Start

```go
import "github.com/chronicle-db/chronicle"

// Use defaults
cfg := chronicle.DefaultConfig("data.db")

// Or use the builder for a fluent API with validation
cfg, err := chronicle.NewConfigBuilder("data.db").
    WithMaxMemory(128 * 1024 * 1024).       // 128MB
    WithRetention(30 * 24 * time.Hour).      // 30 days
    WithHTTP(8086).
    Build()

// Or customize the struct directly
cfg = chronicle.Config{
    Path: "data.db",
    Storage: chronicle.StorageConfig{
        MaxMemory:         128 * 1024 * 1024, // 128MB
        PartitionDuration: 2 * time.Hour,
    },
    Retention: chronicle.RetentionConfig{
        RetentionDuration: 30 * 24 * time.Hour, // 30 days
    },
    HTTP: chronicle.HTTPConfig{
        HTTPEnabled: true,
        HTTPPort:    8086,
    },
}

db, err := chronicle.Open(cfg.Path, cfg)
```

Chronicle groups related settings under `Storage`, `WAL`, `Retention`, `Query`, and `HTTP`. Legacy flat fields (for example, `MaxMemory`, `RetentionDuration`) are still supported for backward compatibility.

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
| `Storage.MaxMemory` | `int64` | 64MB | Maximum memory for buffers and caches |
| `Storage.BufferSize` | `int` | 10,000 | Points to buffer before flushing |

```go
cfg.Storage.MaxMemory = 128 * 1024 * 1024  // 128MB
cfg.Storage.BufferSize = 50_000
```

**Guidelines:**
- IoT/Edge: 16-64MB
- Development: 64-128MB
- Production server: 256MB-1GB

### Storage

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `Storage.MaxStorageBytes` | `int64` | 0 (unlimited) | Maximum database size |
| `Storage.PartitionDuration` | `time.Duration` | 1 hour | Time span per partition |

```go
cfg.Storage.MaxStorageBytes = 10 * 1024 * 1024 * 1024  // 10GB
cfg.Storage.PartitionDuration = 6 * time.Hour
```

**Partition duration guidelines:**
- High-frequency data (>1000 pts/sec): 15-30 minutes
- Medium frequency (10-1000 pts/sec): 1 hour (default)
- Low frequency (<10 pts/sec): 6-24 hours

---

## Write-Ahead Log (WAL)

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `WAL.SyncInterval` | `time.Duration` | 1 second | How often to sync WAL to disk |
| `WAL.WALMaxSize` | `int64` | 128MB | Max size before WAL rotation |
| `WAL.WALRetain` | `int` | 3 | Number of old WAL files to keep |

```go
cfg.WAL.SyncInterval = 100 * time.Millisecond  // More durable
cfg.WAL.WALMaxSize = 64 * 1024 * 1024  // 64MB
cfg.WAL.WALRetain = 5
```

**Trade-offs:**
- Lower `SyncInterval` = better durability, higher latency
- Higher `WALMaxSize` = fewer rotations, longer recovery

---

## Retention & Compaction

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `Retention.RetentionDuration` | `time.Duration` | 0 (forever) | How long to keep data |
| `Retention.CompactionWorkers` | `int` | 1 | Background compaction threads |
| `Retention.CompactionInterval` | `time.Duration` | 30 minutes | How often to run compaction |

```go
cfg.Retention.RetentionDuration = 7 * 24 * time.Hour  // 7 days
cfg.Retention.CompactionWorkers = 2
cfg.Retention.CompactionInterval = 15 * time.Minute
```

---

## Downsampling

Reduce storage by aggregating old data:

```go
cfg.Retention.DownsampleRules = []chronicle.DownsampleRule{
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
| `Query.QueryTimeout` | `time.Duration` | 30 seconds | Maximum query execution time |

```go
cfg.Query.QueryTimeout = 1 * time.Minute
```

---

## HTTP API

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `HTTP.HTTPEnabled` | `bool` | false | Enable HTTP server |
| `HTTP.HTTPPort` | `int` | 8086 | HTTP server port |
| `HTTP.PrometheusRemoteWriteEnabled` | `bool` | false | Enable Prometheus remote write |

```go
cfg.HTTP.HTTPEnabled = true
cfg.HTTP.HTTPPort = 9090
cfg.HTTP.PrometheusRemoteWriteEnabled = true
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
    Path: "/data/metrics.db",
    Storage: chronicle.StorageConfig{
        MaxMemory:         16 * 1024 * 1024, // 16MB
        BufferSize:        1000,
        PartitionDuration: 1 * time.Hour,
    },
    Retention: chronicle.RetentionConfig{
        RetentionDuration: 24 * time.Hour,
    },
    WAL: chronicle.WALConfig{
        SyncInterval: 5 * time.Second,
    },
}
```

### Development

```go
cfg := chronicle.DefaultConfig("dev.db")
cfg.HTTP.HTTPEnabled = true
cfg.Storage.MaxMemory = 256 * 1024 * 1024
```

### Production Server

```go
cfg := chronicle.Config{
    Path: "/var/lib/chronicle/data.db",
    Storage: chronicle.StorageConfig{
        MaxMemory:         1024 * 1024 * 1024, // 1GB
        BufferSize:        100_000,
        PartitionDuration: 1 * time.Hour,
    },
    Retention: chronicle.RetentionConfig{
        RetentionDuration: 30 * 24 * time.Hour,
        CompactionWorkers: 4,
    },
    HTTP: chronicle.HTTPConfig{
        HTTPEnabled:                  true,
        HTTPPort:                     8086,
        PrometheusRemoteWriteEnabled: true,
    },
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
- [TESTING](./TESTING.md) - Testing guide

---

## Validation

Call `Validate()` to check a configuration for logical errors before opening the database:

```go
cfg := chronicle.Config{
    Path: "data.db",
    Storage: chronicle.StorageConfig{
        BufferSize: -1,  // invalid
    },
    HTTP: chronicle.HTTPConfig{
        HTTPPort: 99999,  // invalid
    },
}

if err := cfg.Validate(); err != nil {
    log.Fatal(err)
    // Output: Storage.BufferSize must be non-negative, got -1
    //         HTTP.HTTPPort must be 0-65535, got 99999
}
```

`Validate()` normalizes legacy fields first, then checks all values. It returns a combined error listing every problem found (using `errors.Join`).

---

## Legacy Fields

Chronicle previously used flat config fields. These are deprecated but still
supported — they are automatically copied into the grouped equivalents via
`normalize()` during `Open()`:

| Legacy Field | Replacement |
|-------------|-------------|
| `MaxMemory` | `Storage.MaxMemory` |
| `BufferSize` | `Storage.BufferSize` |
| `PartitionDuration` | `Storage.PartitionDuration` |
| `MaxStorageBytes` | `Storage.MaxStorageBytes` |
| `SyncInterval` | `WAL.SyncInterval` |
| `WALMaxSize` | `WAL.WALMaxSize` |
| `WALRetain` | `WAL.WALRetain` |
| `RetentionDuration` | `Retention.RetentionDuration` |
| `CompactionWorkers` | `Retention.CompactionWorkers` |
| `CompactionInterval` | `Retention.CompactionInterval` |
| `QueryTimeout` | `Query.QueryTimeout` |
| `HTTPEnabled` | `HTTP.HTTPEnabled` |
| `HTTPPort` | `HTTP.HTTPPort` |

**Migration**: Replace flat fields with their grouped equivalents. Use
`DefaultConfig(path)` to get a fully-populated Config with sensible defaults.

---

## Feature Configuration Reference

This section documents all feature-specific configuration structs across the codebase.
Chronicle defines **257** configuration types in total. The core types are documented
above; the remaining feature configs are organized by domain below.

Each config struct typically has a `Default*Config()` constructor that returns
sensible defaults. Use the feature-specific `New*()` constructor to initialize
the feature with your custom configuration.

### Core Database

#### `BackupConfig`
**Source:** `backup.go`

| Field | Type |
|-------|------|
| `DestinationPath` | `string` |
| `StorageBackend` | `StorageBackend` |
| `Compression` | `bool` |
| `Encryption` | `bool` |
| `RetentionCount` | `int` |
| `IncrementalEnabled` | `bool` |

#### `CardinalityConfig`
**Source:** `cardinality.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxTotalSeries` | `int64` |
| `MaxSeriesPerMetric` | `int64` |
| `MaxLabelValues` | `int64` |
| `AlertThresholdPercent` | `int` |
| `CheckInterval` | `time.Duration` |
| `OnAlert` | `func(alert` |

#### `DataRehydrationConfig`
**Source:** `data_rehydration.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `CacheSizeMB` | `int64` |
| `CacheTTL` | `time.Duration` |
| `MaxConcurrentFetches` | `int` |
| `PrefetchEnabled` | `bool` |

#### `HotBackupConfig`
**Source:** `hot_backup.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `BackupDir` | `string` |
| `MaxBackups` | `int` |
| `CompressionEnabled` | `bool` |

#### `IncrementalBackupConfig`
**Source:** `incremental_backup.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `BackupDir` | `string` |
| `MaxChains` | `int` |
| `FullBackupEvery` | `int` |
| `VerifyChecksum` | `bool` |

#### `RetentionOptimizerConfig`
**Source:** `retention_optimizer.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `StorageBudgetMB` | `int64` |
| `CheckInterval` | `time.Duration` |
| `MinRetention` | `time.Duration` |
| `MaxRetention` | `time.Duration` |

#### `TierCostConfig`
**Source:** `retention_optimizer.go`

| Field | Type |
|-------|------|
| `TierName` | `string` |
| `CostPerGBMonth` | `float64` |
| `ReadCostPer1K` | `float64` |
| `WriteCostPer1K` | `float64` |
| `TransitionCost` | `float64` |
| `MinStorageDays` | `int` |

#### `LifecycleAgentConfig`
**Source:** `retention_optimizer.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `DryRun` | `bool` |
| `Interval` | `time.Duration` |
| `HotThreshold` | `float64` |
| `ColdThreshold` | `float64` |
| `TargetSavings` | `float64` |

#### `SeriesDedupConfig`
**Source:** `series_dedup.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `DeduplicateWindow` | `time.Duration` |
| `MaxTracked` | `int` |
| `MergeStrategy` | `string` |

#### `SmartCompactionConfig`
**Source:** `smart_compaction.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `IOBudgetMBps` | `int` |
| `ColdThreshold` | `time.Duration` |
| `MinPartitionSize` | `int64` |
| `MaxPartitionSize` | `int64` |
| `CompactInterval` | `time.Duration` |

#### `SmartRetentionConfig`
**Source:** `smart_retention.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `EvaluationInterval` | `time.Duration` |
| `MinRetention` | `time.Duration` |
| `HotTierMaxAge` | `time.Duration` |
| `WarmTierMaxAge` | `time.Duration` |
| `WarmDownsampleWindow` | `time.Duration` |
| `ColdDownsampleWindow` | `time.Duration` |
| `AccessWeightDecay` | `float64` |
| `HighValueThreshold` | `float64` |
| `AutoMigrate` | `bool` |

#### `StorageEngineConfig`
**Source:** `storage_engine.go`

| Field | Type |
|-------|------|
| `Path` | `string` |
| `Storage` | `StorageConfig` |
| `WAL` | `WALConfig` |
| `PartitionDuration` | `time.Duration` |
| `BufferSize` | `int` |
| `SyncInterval` | `time.Duration` |
| `WALMaxSize` | `int64` |
| `WALRetain` | `int` |

#### `StorageStatsConfig`
**Source:** `storage_stats.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `CollectionInterval` | `time.Duration` |

#### `StorageTierConfig`
**Source:** `tiered_storage.go`

| Field | Type |
|-------|------|
| `Level` | `StorageTierLevel` |
| `Backend` | `StorageBackend` |
| `CostPerGBMonth` | `float64` |
| `MaxCapacityBytes` | `int64` |
| `ReadLatencySLA` | `time.Duration` |

#### `AccessTrackerConfig`
**Source:** `tiered_storage.go`

| Field | Type |
|-------|------|
| `TrackingWindowDuration` | `time.Duration` |
| `DecayFactor` | `float64` |
| `HistogramBuckets` | `int` |

#### `MigrationEngineConfig`
**Source:** `tiered_storage.go`

| Field | Type |
|-------|------|
| `CheckInterval` | `time.Duration` |
| `MigrationCooldown` | `time.Duration` |
| `MaxConcurrentMigrations` | `int` |
| `DryRun` | `bool` |

#### `CostOptimizerConfig`
**Source:** `tiered_storage.go`

| Field | Type |
|-------|------|
| `BudgetPerMonth` | `float64` |
| `OptimizationInterval` | `time.Duration` |
| `PrefetchEnabled` | `bool` |
| `PrefetchThreshold` | `float64` |

#### `AdaptiveTieredConfig`
**Source:** `tiered_storage.go`

| Field | Type |
|-------|------|
| `Tiers` | `[]*StorageTierConfig` |
| `AccessTracker` | `AccessTrackerConfig` |
| `Migration` | `MigrationEngineConfig` |
| `CostOptimizer` | `CostOptimizerConfig` |

#### `LifecycleConfig`
**Source:** `tiered_storage_lifecycle.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `EvaluationInterval` | `time.Duration` |
| `HotToWarmAge` | `time.Duration` |
| `WarmToColdAge` | `time.Duration` |
| `ColdToArchiveAge` | `time.Duration` |
| `MaxHotSizeBytes` | `int64` |
| `MaxWarmSizeBytes` | `int64` |
| `CostBudgetMonthly` | `float64` |
| `DryRun` | `bool` |

#### `WALSnapshotConfig`
**Source:** `wal_snapshot.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `SnapshotInterval` | `time.Duration` |
| `MaxSnapshots` | `int` |
| `CompactAfterSnapshot` | `bool` |

### Storage & Backends

#### `AdaptiveCompressionConfig`
**Source:** `adaptive_compression.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `AnalysisWindow` | `int` |
| `ReanalysisInterval` | `time.Duration` |
| `MinCompressionRatio` | `float64` |
| `PreferSpeed` | `bool` |
| `EnableLearning` | `bool` |
| `MaxMemoryUsage` | `int64` |

#### `AdaptiveCompressionV3Config`
**Source:** `adaptive_compression_v3.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `Strategy` | `BanditStrategy` |
| `ExplorationRate` | `float64` |
| `MinTrialsBeforeExploit` | `int` |
| `DecayFactor` | `float64` |
| `FallbackCodec` | `CodecType` |
| `FeatureWindowSize` | `int` |
| `RewardWeight` | `float64` |

#### `ClickHouseConfig`
**Source:** `clickhouse.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `DefaultDatabase` | `string` |
| `DefaultFormat` | `ClickHouseFormat` |
| `MaxQuerySize` | `int64` |
| `MaxResultRows` | `int` |

#### `CompressionAdvisorConfig`
**Source:** `compression_advisor.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `AnalysisWindow` | `int` |
| `ReanalysisInterval` | `time.Duration` |
| `BenchmarkSamples` | `int` |
| `AutoApply` | `bool` |
| `MinImprovementPct` | `float64` |

#### `CompressionPluginConfig`
**Source:** `compression_plugin.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxPlugins` | `int` |
| `AllowOverride` | `bool` |
| `DefaultPlugin` | `string` |
| `BenchmarkOnRegister` | `bool` |

#### `DuckDBBackendConfig`
**Source:** `duckdb_backend.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxMemoryMB` | `int` |
| `WorkerThreads` | `int` |
| `EnableCaching` | `bool` |
| `CacheMaxEntries` | `int` |
| `CacheTTL` | `time.Duration` |
| `AutoRoute` | `bool` |
| `ComplexThreshold` | `int` |

#### `ParquetConfig`
**Source:** `parquet.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `CompressionCodec` | `string` |
| `RowGroupSize` | `int` |
| `PageSize` | `int` |
| `EnableDictionaryEncoding` | `bool` |
| `EnableStatistics` | `bool` |
| `EnableBloomFilter` | `bool` |
| `DataPageVersion` | `int` |

#### `ParquetBridgeConfig`
**Source:** `parquet_bridge.go`

| Field | Type |
|-------|------|
| `DataDir` | `string` |
| `ArchiveDir` | `string` |
| `PartitionBy` | `ParquetPartition` |
| `RowGroupSize` | `int` |
| `CompressionCodec` | `string` |
| `MaxFileSize` | `int64` |
| `AutoArchiveInterval` | `time.Duration` |

#### `SQLiteBackendConfig`
**Source:** `sqlite_backend.go`

| Field | Type |
|-------|------|
| `Path` | `string` |
| `CacheSize` | `int` |
| `JournalMode` | `string` |
| `Synchronous` | `string` |
| `BusyTimeout` | `int` |
| `EnableFTS` | `bool` |
| `EnableMetricIndex` | `bool` |
| `MaxConnections` | `int` |

#### `S3TieringConfig`
**Source:** `storage_backend_s3.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `Policies` | `[]S3TieringPolicy` |

### Query & Optimization

#### `CollaborativeQueryConfig`
**Source:** `collaborative_query.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxSessionDuration` | `time.Duration` |
| `MaxParticipants` | `int` |
| `SyncInterval` | `time.Duration` |
| `BufferSize` | `int` |
| `EnableConflictResolution` | `bool` |
| `HistoryRetention` | `int` |

#### `ContinuousAggConfig`
**Source:** `continuous_agg.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxAggregations` | `int` |
| `CheckInterval` | `time.Duration` |
| `RetainWindows` | `int` |

#### `DistributedQueryConfig`
**Source:** `distributed_query.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxFanOut` | `int` |
| `PartialTimeout` | `time.Duration` |
| `MergeStrategy` | `string` |
| `MaxResultSize` | `int` |
| `RetryAttempts` | `int` |

#### `HWAcceleratedQueryConfig`
**Source:** `hw_accelerated_query.go`

| Field | Type |
|-------|------|
| `EnableSIMD` | `bool` |
| `EnableBatchProcessing` | `bool` |
| `BatchSize` | `int` |
| `VectorWidth` | `int` |
| `EnablePrefetch` | `bool` |
| `CacheLineSize` | `int` |
| `EnableParallelScan` | `bool` |
| `MaxParallelWorkers` | `int` |
| `ForceSoftwareFallback` | `bool` |

#### `VectorizedScanConfig`
**Source:** `hw_accelerated_query.go`

| Field | Type |
|-------|------|
| `BatchSize` | `int` |
| `UseColumnar` | `bool` |
| `PushPredicates` | `bool` |
| `ParallelDecode` | `bool` |
| `PrefetchPages` | `int` |

#### `HybridIndexConfig`
**Source:** `hybrid_index.go`

| Field | Type |
|-------|------|
| `MaxDimensions` | `int` |
| `DefaultDimension` | `int` |
| `MaxVectorsPerPartition` | `int` |
| `DistanceMetric` | `DistanceMetric` |
| `HNSWMaxLevel` | `int` |
| `HNSWEfConstruction` | `int` |
| `HNSWEfSearch` | `int` |
| `HNSWMaxConnections` | `int` |
| `BuildIndexThreshold` | `int` |
| `TemporalBucketDuration` | `time.Duration` |

#### `MaterializedViewConfig`
**Source:** `materialized_views.go`

| Field | Type |
|-------|------|
| `MaxViews` | `int` |
| `DefaultStaleness` | `time.Duration` |
| `CheckInterval` | `time.Duration` |
| `MaxMemoryBytes` | `int64` |
| `EnableShadowMode` | `bool` |

#### `MaterializedViewV2Config`
**Source:** `materialized_views_v2.go`

| Field | Type |
|-------|------|
| `MaxViews` | `int` |
| `CheckpointInterval` | `time.Duration` |
| `MaxAllowedLateness` | `time.Duration` |
| `DefaultLatePolicy` | `LateDataPolicy` |
| `EnableExactlyOnce` | `bool` |
| `EnableShadowVerification` | `bool` |
| `ShadowVerificationRate` | `float64` |

#### `AssistantConfig`
**Source:** `query_assistant.go`

| Field | Type |
|-------|------|
| `Provider` | `string` |
| `APIKey` | `string` |
| `Endpoint` | `string` |
| `Model` | `string` |
| `Temperature` | `float64` |
| `MaxTokens` | `int` |
| `Timeout` | `time.Duration` |
| `EnableCache` | `bool` |
| `CacheTTL` | `time.Duration` |

#### `QueryBuilderConfig`
**Source:** `query_builder.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxSavedQueries` | `int` |
| `EnableSharing` | `bool` |
| `AutocompleteLimit` | `int` |
| `QueryHistorySize` | `int` |

#### `VisualQueryBuilderConfig`
**Source:** `query_builder_sdk.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `AutocompleteLimit` | `int` |
| `CacheMetadata` | `bool` |
| `MetadataCacheTTL` | `time.Duration` |
| `MaxQueryComplexity` | `int` |
| `EnableValidation` | `bool` |

#### `QueryCacheConfig`
**Source:** `query_cache.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxEntries` | `int` |
| `MaxMemoryBytes` | `int64` |
| `DefaultTTL` | `time.Duration` |
| `EvictionPolicy` | `CacheEvictionPolicy` |
| `CompressionEnabled` | `bool` |
| `PartialReuse` | `bool` |
| `InvalidateOnWrite` | `bool` |

#### `QueryCompilerConfig`
**Source:** `query_compiler.go`

| Field | Type |
|-------|------|
| `EnablePushDown` | `bool` |
| `EnableVectorized` | `bool` |
| `EnableCostOptimizer` | `bool` |
| `MaxIRNodes` | `int` |
| `CacheSize` | `int` |
| `DefaultTimeout` | `time.Duration` |

#### `QueryConsoleConfig`
**Source:** `query_console.go`

| Field | Type |
|-------|------|
| `Bind` | `string` |
| `ReadTimeout` | `time.Duration` |
| `WriteTimeout` | `time.Duration` |
| `MaxQueryLen` | `int` |
| `EnableCORS` | `bool` |
| `AllowedOrigins` | `[]string` |

#### `QueryCostConfig`
**Source:** `query_cost.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxCostBudget` | `float64` |
| `WarnThreshold` | `float64` |
| `RejectThreshold` | `float64` |
| `CostPerPartition` | `float64` |
| `CostPerPoint` | `float64` |
| `CostPerByte` | `float64` |

#### `QueryFederationConfig`
**Source:** `query_federation.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxConcurrentQueries` | `int` |
| `QueryTimeout` | `time.Duration` |
| `CacheEnabled` | `bool` |
| `CacheTTL` | `time.Duration` |
| `PushdownEnabled` | `bool` |
| `RetryCount` | `int` |
| `RetryBackoff` | `time.Duration` |

#### `ClickHouseFederationConfig`
**Source:** `query_federation.go`

| Field | Type |
|-------|------|
| `Name` | `string` |
| `Host` | `string` |
| `Port` | `int` |
| `Database` | `string` |
| `Username` | `string` |
| `Password` | `string` |

#### `DuckDBConfig`
**Source:** `query_federation.go`

| Field | Type |
|-------|------|
| `Name` | `string` |
| `Path` | `string` |

#### `PostgresConfig`
**Source:** `query_federation.go`

| Field | Type |
|-------|------|
| `Name` | `string` |
| `Host` | `string` |
| `Port` | `int` |
| `Database` | `string` |
| `Username` | `string` |
| `Password` | `string` |
| `SSLMode` | `string` |

#### `HTTPSourceConfig`
**Source:** `query_federation.go`

| Field | Type |
|-------|------|
| `Name` | `string` |
| `BaseURL` | `string` |
| `Headers` | `map[string]string` |
| `Timeout` | `time.Duration` |

#### `QueryMiddlewareConfig`
**Source:** `query_middleware.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxMiddlewares` | `int` |

#### `QueryOptimizerConfig`
**Source:** `query_optimizer.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `ProfilingEnabled` | `bool` |
| `ProfilingWindow` | `time.Duration` |
| `CostBasedEnabled` | `bool` |
| `IndexRecommendations` | `bool` |
| `PlanCacheEnabled` | `bool` |
| `PlanCacheTTL` | `time.Duration` |
| `PlanCacheMaxSize` | `int` |
| `FeedbackEnabled` | `bool` |
| `MinSamples` | `int` |
| `LearningRate` | `float64` |

#### `QueryPlanVizConfig`
**Source:** `query_plan_viz.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `IncludeTimings` | `bool` |
| `MaxPlanDepth` | `int` |

#### `QueryPlannerConfig`
**Source:** `query_planner.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `StatsRefreshInterval` | `time.Duration` |
| `MaxParallelScans` | `int` |
| `EnablePartitionPruning` | `bool` |
| `EnablePredicatePushdown` | `bool` |
| `EnableParallelExec` | `bool` |
| `CostModelWeight` | `float64` |
| `MaxStatsAge` | `time.Duration` |
| `MinRowsForParallel` | `int` |
| `StaleStatsThreshold` | `float64` |

#### `QueryProfilerConfig`
**Source:** `query_profiler.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `SlowQueryThreshold` | `time.Duration` |
| `MaxProfiles` | `int` |
| `EnableTracing` | `bool` |
| `RecordAllQueries` | `bool` |

#### `RecordingRulesConfig`
**Source:** `recording_rules.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `Rules` | `[]RecordingRule` |
| `DefaultInterval` | `time.Duration` |
| `ConcurrencyLimit` | `int` |

#### `ResultCacheConfig`
**Source:** `result_cache.go`

| Field | Type |
|-------|------|
| `MaxEntries` | `int` |
| `TTL` | `time.Duration` |
| `Enabled` | `bool` |

#### `SQLPipelineConfig`
**Source:** `sql_pipelines.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxConcurrentPipes` | `int` |
| `CheckpointInterval` | `time.Duration` |
| `BackfillRateLimit` | `int` |
| `EnableExactlyOnce` | `bool` |
| `MaxPipelines` | `int` |
| `DefaultBatchSize` | `int` |

### Streaming & ETL

#### `CDCConfig`
**Source:** `cdc.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `BufferSize` | `int` |
| `MaxSubscribers` | `int` |
| `FlushInterval` | `time.Duration` |
| `IncludeMetrics` | `[]string` |
| `ExcludeMetrics` | `[]string` |

#### `ETLPipelineManagerConfig`
**Source:** `etl_pipeline_manager.go`

| Field | Type |
|-------|------|
| `MaxPipelines` | `int` |
| `HealthInterval` | `time.Duration` |
| `MetricsRetention` | `time.Duration` |

#### `WindowedJoinConfig`
**Source:** `etl_pipeline_manager.go`

| Field | Type |
|-------|------|
| `LeftMetric` | `string` |
| `RightMetric` | `string` |
| `WindowSize` | `time.Duration` |
| `OutputMetric` | `string` |
| `JoinType` | `WindowedJoinType` |

#### `EnrichmentLookupConfig`
**Source:** `etl_pipeline_manager.go`

| Field | Type |
|-------|------|
| `LookupMetric` | `string` |
| `KeyTag` | `string` |
| `ValueTag` | `string` |
| `CacheTTL` | `time.Duration` |
| `MaxCacheSize` | `int` |

#### `ExportConfig`
**Source:** `export.go`

| Field | Type |
|-------|------|
| `Format` | `ExportFormat` |
| `OutputPath` | `string` |
| `Metrics` | `[]string` |
| `Tags` | `map[string]string` |
| `Start` | `int64` |
| `End` | `int64` |
| `Compression` | `bool` |
| `BatchSize` | `int` |
| `IncludeHeaders` | `bool` |
| `TimestampFormat` | `string` |

#### `ImportConfig`
**Source:** `migration_tool.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `BatchSize` | `int` |
| `MaxErrors` | `int` |
| `SkipInvalid` | `bool` |
| `DryRun` | `bool` |

#### `StreamDSLV2Config`
**Source:** `stream_dsl_v2.go`

| Field | Type |
|-------|------|
| `MaxConcurrentQueries` | `int` |
| `DefaultWindowSize` | `time.Duration` |
| `MaxWindowSize` | `time.Duration` |
| `EnableCEP` | `bool` |
| `EnableJoins` | `bool` |
| `StateBackend` | `string` |
| `CheckpointInterval` | `time.Duration` |
| `MaxStateSize` | `int64` |

#### `StreamProcessingConfig`
**Source:** `stream_processing.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxPipelines` | `int` |
| `CheckpointInterval` | `time.Duration` |
| `StateBackendType` | `string` |
| `MaxBufferSize` | `int` |
| `WorkerCount` | `int` |

#### `StreamReplayConfig`
**Source:** `stream_replay.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxReplays` | `int` |
| `DefaultSpeed` | `float64` |
| `BufferSize` | `int` |

#### `StreamConfig`
**Source:** `streaming.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `BufferSize` | `int` |
| `PingInterval` | `time.Duration` |
| `WriteTimeout` | `time.Duration` |

#### `ETLPipelineConfig`
**Source:** `streaming_etl.go`

| Field | Type |
|-------|------|
| `Name` | `string` |
| `MaxBufferSize` | `int` |
| `CheckpointInterval` | `time.Duration` |
| `Workers` | `int` |
| `BackpressureStrategy` | `BackpressureStrategy` |
| `ErrorHandler` | `ETLErrorHandler` |

#### `ETLAggregateConfig`
**Source:** `streaming_etl.go`

| Field | Type |
|-------|------|
| `Window` | `time.Duration` |
| `Function` | `AggFunc` |
| `GroupBy` | `[]string` |
| `EmitMode` | `string` |

#### `StreamingSQLConfig`
**Source:** `streaming_sql.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `BufferSize` | `int` |
| `WindowTimeout` | `time.Duration` |
| `MaxConcurrentQueries` | `int` |
| `StateStoreSize` | `int` |
| `EmitInterval` | `time.Duration` |

#### `WritePipelineConfig`
**Source:** `write_hooks.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxHooks` | `int` |

### Observability & Monitoring

#### `AlertBuilderConfig`
**Source:** `alert_builder.go`

| Field | Type |
|-------|------|
| `MaxRulesPerUser` | `int` |
| `MaxConditionsPerRule` | `int` |
| `EnablePreview` | `bool` |
| `PreviewLookback` | `time.Duration` |
| `DefaultEvalInterval` | `time.Duration` |

#### `AnomalyCorrelationConfig`
**Source:** `anomaly_correlation.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `CorrelationWindow` | `time.Duration` |
| `MinConfidence` | `float64` |
| `MaxCausalDepth` | `int` |
| `EvaluationInterval` | `time.Duration` |
| `MaxIncidents` | `int` |
| `TagOverlapThreshold` | `float64` |
| `TemporalProximityMs` | `int64` |
| `EnableCausalGraph` | `bool` |
| `WebhookURL` | `string` |

#### `AnomalyDetectionV2Config`
**Source:** `anomaly_detection_v2.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `SeasonalityPeriod` | `time.Duration` |
| `MinDataPoints` | `int` |
| `BaselineWindow` | `int` |
| `AdaptiveRate` | `float64` |
| `MaxFalsePositiveRate` | `float64` |
| `FeedbackEnabled` | `bool` |
| `CorrelationWindow` | `time.Duration` |
| `AlertCooldown` | `time.Duration` |

#### `AnomalyExplainabilityConfig`
**Source:** `anomaly_explainability.go`

| Field | Type |
|-------|------|
| `LLMProvider` | `ExplainLLMProvider` |
| `LLMModel` | `string` |
| `APIKey` | `string` |
| `MaxContextWindow` | `int` |
| `ExplanationDepth` | `ExplainDepth` |
| `IncludeHistorical` | `bool` |
| `IncludeCorrelations` | `bool` |
| `IncludeRecommendations` | `bool` |
| `MaxRecommendations` | `int` |
| `CacheExplanations` | `bool` |
| `CacheTTL` | `time.Duration` |

#### `AnomalyPipelineConfig`
**Source:** `anomaly_pipeline.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `Sensitivity` | `float64` |
| `WindowSize` | `int` |
| `EvaluationInterval` | `time.Duration` |
| `ZScoreThreshold` | `float64` |
| `IQRMultiplier` | `float64` |
| `MinDataPoints` | `int` |
| `EnableAutoAlert` | `bool` |
| `AlertWebhookURL` | `string` |

#### `BenchRunnerConfig`
**Source:** `bench_runner.go`

| Field | Type |
|-------|------|
| `WriteCount` | `int` |
| `QueryCount` | `int` |
| `PointsPerBatch` | `int` |
| `Concurrency` | `int` |

#### `CapacityPlanningConfig`
**Source:** `capacity_planning.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MetricsCollectionInterval` | `time.Duration` |
| `ForecastHorizon` | `time.Duration` |
| `HistoryWindow` | `time.Duration` |
| `RecommendationInterval` | `time.Duration` |
| `AutoTuneEnabled` | `bool` |
| `SafetyMargin` | `float64` |
| `AlertThreshold` | `float64` |
| `MinDataPoints` | `int` |

#### `CrossAlertConfig`
**Source:** `cross_alert.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxRules` | `int` |
| `EvalInterval` | `time.Duration` |

#### `DataQualityConfig`
**Source:** `data_quality.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxGapDuration` | `time.Duration` |
| `DuplicateWindow` | `time.Duration` |
| `OutlierStddevs` | `float64` |
| `MaxClockSkew` | `time.Duration` |
| `CheckInterval` | `time.Duration` |
| `MaxIssues` | `int` |

#### `DeclarativeAlertingConfig`
**Source:** `declarative_alerting.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `WatchPaths` | `[]string` |
| `ReloadInterval` | `time.Duration` |
| `ValidationStrict` | `bool` |
| `DryRunMode` | `bool` |
| `MaxAlertRules` | `int` |

#### `ForecastConfig`
**Source:** `forecast.go`

| Field | Type |
|-------|------|
| `Method` | `ForecastMethod` |
| `SeasonalPeriods` | `int` |
| `Alpha` | `float64` |
| `Beta` | `float64` |
| `Gamma` | `float64` |
| `AnomalyThreshold` | `float64` |

#### `ForecastV2Config`
**Source:** `forecast_v2.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `DefaultHorizon` | `int` |
| `SeasonalityMode` | `string` |
| `ChangepointThreshold` | `float64` |
| `ConfidenceLevel` | `float64` |

#### `HealthCheckConfig`
**Source:** `health_check.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `CheckInterval` | `time.Duration` |

#### `MetricCorrelationConfig`
**Source:** `metric_correlation.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MinDataPoints` | `int` |
| `MinCorrelation` | `float64` |
| `MaxPairs` | `int` |
| `AnalysisWindow` | `time.Duration` |

#### `MetricLifecycleConfig`
**Source:** `metric_lifecycle.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `EvaluationInterval` | `time.Duration` |
| `MaxPolicies` | `int` |

#### `MetricMetadataStoreConfig`
**Source:** `metric_metadata_store.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxEntries` | `int` |

#### `MetricsCatalogConfig`
**Source:** `metrics_catalog.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `ScanInterval` | `time.Duration` |
| `MaxMetrics` | `int` |
| `TrackLineage` | `bool` |
| `TrackQueryUsage` | `bool` |
| `DeprecationGracePeriod` | `time.Duration` |

#### `MetricsSDKConfig`
**Source:** `metrics_sdk.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `AppID` | `string` |
| `Platform` | `string` |
| `MaxBufferSize` | `int` |
| `FlushInterval` | `time.Duration` |
| `SyncEndpoint` | `string` |
| `SyncEnabled` | `bool` |
| `SamplingRate` | `float64` |
| `PrivacyMode` | `bool` |
| `MaxMemoryBytes` | `int64` |
| `BatchUploadSize` | `int` |
| `OfflineBufferDays` | `int` |

#### `InternalMetricsConfig`
**Source:** `observability.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `CollectionInterval` | `time.Duration` |
| `RetentionDuration` | `time.Duration` |
| `MetricPrefix` | `string` |
| `MaxRingBufferSize` | `int` |
| `ExposeHTTP` | `bool` |
| `EmitToSelf` | `bool` |

#### `HealthCheckerConfig`
**Source:** `observability.go`

| Field | Type |
|-------|------|
| `CheckInterval` | `time.Duration` |
| `Timeout` | `time.Duration` |
| `FailureThreshold` | `int` |
| `RecoveryThreshold` | `int` |

#### `ObservabilitySuiteConfig`
**Source:** `observability.go`

| Field | Type |
|-------|------|
| `Metrics` | `InternalMetricsConfig` |
| `Health` | `HealthCheckerConfig` |

#### `OTLPConfig`
**Source:** `otel.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxBodySize` | `int64` |

#### `OTelCollectorConfig`
**Source:** `otel_collector.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `Endpoint` | `string` |
| `APIKey` | `string` |
| `BatchSize` | `int` |
| `FlushInterval` | `time.Duration` |
| `Timeout` | `time.Duration` |
| `RetryEnabled` | `bool` |
| `MaxRetries` | `int` |
| `RetryBackoff` | `time.Duration` |
| `Headers` | `map[string]string` |
| `ResourceToTags` | `bool` |
| `ScopeToTags` | `bool` |
| `MetricPrefix` | `string` |

#### `OTelCollectorReceiverConfig`
**Source:** `otel_collector.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `ListenAddr` | `string` |
| `TLSEnabled` | `bool` |
| `TLSCertFile` | `string` |
| `TLSKeyFile` | `string` |
| `MaxBatchSize` | `int` |
| `CompressionType` | `string` |

#### `OTelDistroConfig`
**Source:** `otel_distribution.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `ListenAddr` | `string` |
| `GRPCPort` | `int` |
| `HTTPPort` | `int` |
| `BatchSize` | `int` |
| `FlushInterval` | `time.Duration` |
| `MaxQueueSize` | `int` |
| `EnableMetrics` | `bool` |
| `EnableTraces` | `bool` |
| `EnableLogs` | `bool` |
| `ResourceAttrs` | `map[string]string` |
| `DefaultAlertRules` | `bool` |

#### `OTelPipelineConfig`
**Source:** `otel_distribution.go`

| Field | Type |
|-------|------|
| `Name` | `string` |
| `SignalType` | `string` |
| `Receivers` | `[]string` |
| `Processors` | `[]string` |
| `Exporters` | `[]string` |
| `Enabled` | `bool` |

#### `OTLPProtoConfig`
**Source:** `otlp_proto_ingest.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxBatchSize` | `int` |
| `EnableHistograms` | `bool` |
| `EnableSummaries` | `bool` |
| `EnableExponentialHistograms` | `bool` |

#### `PointValidatorConfig`
**Source:** `point_validator.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `RejectNaN` | `bool` |
| `RejectInf` | `bool` |
| `MaxTagKeys` | `int` |
| `MaxTagValueLen` | `int` |
| `MaxMetricLen` | `int` |
| `MaxTimestampSkew` | `time.Duration` |

#### `ProfileConfig`
**Source:** `profile.go`

| Field | Type |
|-------|------|
| `MaxProfiles` | `int` |
| `RetentionDuration` | `time.Duration` |
| `CompressionEnabled` | `bool` |
| `MaxProfileSize` | `int64` |

#### `PromScraperConfig`
**Source:** `prom_scraper.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `ScrapeInterval` | `time.Duration` |
| `ScrapeTimeout` | `time.Duration` |
| `MaxTargets` | `int` |

#### `RootCauseAnalysisConfig`
**Source:** `root_cause_analysis.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `CausalWindowSize` | `time.Duration` |
| `MinConfidence` | `float64` |
| `MaxCausalDepth` | `int` |
| `GrangerLagSteps` | `int` |
| `CorrelationThreshold` | `float64` |
| `TopKCauses` | `int` |
| `ExplanationEnabled` | `bool` |

#### `SelfInstrumentationConfig`
**Source:** `self_instrumentation.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `CollectInterval` | `time.Duration` |
| `EmitOTLP` | `bool` |

#### `TracingConfig`
**Source:** `tracing.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `JaegerEndpoint` | `string` |
| `ZipkinEndpoint` | `string` |
| `OTLPEndpoint` | `string` |
| `TraceIDTagKey` | `string` |
| `SpanIDTagKey` | `string` |
| `ServiceNameTagKey` | `string` |
| `MaxTracesPerQuery` | `int` |
| `TraceRetention` | `time.Duration` |
| `CacheSize` | `int` |
| `HTTPClient` | `HTTPDoer` |

### Kubernetes & Deployment

#### `AutoShardingConfig`
**Source:** `auto_sharding.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `VirtualNodes` | `int` |
| `RebalanceThreshold` | `float64` |
| `RebalanceInterval` | `time.Duration` |
| `MaxShardsPerNode` | `int` |
| `ReplicationFactor` | `int` |
| `MigrationBatchSize` | `int` |

#### `AutoscaleConfig`
**Source:** `autoscale.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `CollectionInterval` | `time.Duration` |
| `ForecastHorizon` | `time.Duration` |
| `EvaluationInterval` | `time.Duration` |
| `ScaleUpThreshold` | `float64` |
| `ScaleDownThreshold` | `float64` |
| `CooldownPeriod` | `time.Duration` |
| `MinBufferPool` | `int` |
| `MaxBufferPool` | `int` |
| `MinWriteWorkers` | `int` |
| `MaxWriteWorkers` | `int` |
| `MinCompactionWorkers` | `int` |
| `MaxCompactionWorkers` | `int` |
| `ForecastConfig` | `ForecastConfig` |

#### `AutoScalingConfig`
**Source:** `autoscaling.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `PredictionHorizon` | `time.Duration` |
| `HistoryWindow` | `time.Duration` |
| `SampleInterval` | `time.Duration` |
| `MinDataPoints` | `int` |
| `ScaleUpThreshold` | `float64` |
| `ScaleDownThreshold` | `float64` |
| `CooldownPeriod` | `time.Duration` |
| `TargetUtilization` | `float64` |

#### `CloudSaaSConfig`
**Source:** `cloud_saas.go`

| Field | Type |
|-------|------|
| `Region` | `string` |
| `Tier` | `CloudTier` |
| `MaxStorageBytes` | `int64` |
| `MaxInstances` | `int` |
| `BillingEnabled` | `bool` |
| `AutoDeploy` | `bool` |
| `ControlPlaneURL` | `string` |
| `APIKey` | `string` |
| `HeartbeatInterval` | `time.Duration` |
| `MetricRetention` | `time.Duration` |

#### `EdgeCloudFabricConfig`
**Source:** `edge_cloud_fabric.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `SyncInterval` | `time.Duration` |
| `MaxBandwidthMBps` | `int` |
| `RetryBackoff` | `time.Duration` |
| `MaxRetries` | `int` |
| `ConflictResolution` | `string` |
| `CompressionEnabled` | `bool` |
| `BatchSize` | `int` |
| `ResumableUploads` | `bool` |

#### `EdgeMeshConfig`
**Source:** `edge_mesh.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `NodeID` | `string` |
| `ListenAddr` | `string` |
| `AdvertiseAddr` | `string` |
| `SeedNodes` | `[]string` |
| `GossipInterval` | `time.Duration` |
| `HealthCheckInterval` | `time.Duration` |
| `HealthCheckTimeout` | `time.Duration` |
| `ReplicationFactor` | `int` |
| `VirtualNodes` | `int` |
| `MaxPeers` | `int` |
| `QueryTimeout` | `time.Duration` |
| `EnableAutoRebalance` | `bool` |

#### `EdgeSyncConfig`
**Source:** `edge_sync.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `Endpoint` | `string` |
| `Provider` | `EdgeSyncProvider` |
| `SyncInterval` | `time.Duration` |
| `BatchSize` | `int` |
| `MaxRetries` | `int` |
| `RetryBackoff` | `time.Duration` |
| `CompressionEnabled` | `bool` |
| `ConflictResolution` | `ConflictResolution` |
| `QueuePath` | `string` |
| `MaxQueueSize` | `int64` |
| `BandwidthLimitBps` | `int64` |
| `Auth` | `*EdgeSyncAuth` |
| `MetricFilter` | `[]string` |
| `TagFilter` | `map[string][]string` |

#### `FleetConfig`
**Source:** `fleet_manager.go`

| Field | Type |
|-------|------|
| `HeartbeatInterval` | `time.Duration` |
| `HeartbeatTimeout` | `time.Duration` |
| `MaxNodes` | `int` |
| `SyncInterval` | `time.Duration` |
| `EnableAutoRecovery` | `bool` |

#### `K8sConfig`
**Source:** `k8s_operator.go`

| Field | Type |
|-------|------|
| `Namespace` | `string` |
| `Name` | `string` |
| `Labels` | `map[string]string` |
| `Annotations` | `map[string]string` |
| `Replicas` | `int` |
| `Resources` | `K8sResourceSpec` |
| `Storage` | `K8sStorageSpec` |
| `Backup` | `K8sBackupSpec` |
| `Monitoring` | `K8sMonitoringSpec` |
| `HealthCheckPort` | `int` |
| `MetricsPort` | `int` |
| `AutoScaling` | `K8sAutoScalingSpec` |

#### `HelmChartConfig`
**Source:** `k8s_operator_crd.go`

| Field | Type |
|-------|------|
| `ReleaseName` | `string` |
| `Namespace` | `string` |
| `ChartVersion` | `string` |

#### `ReconcilerConfig`
**Source:** `k8s_reconciler.go`

| Field | Type |
|-------|------|
| `ReconcileInterval` | `time.Duration` |
| `MaxRetries` | `int` |
| `RetryBackoff` | `time.Duration` |
| `HealthCheckTimeout` | `time.Duration` |
| `EnableAutoHealing` | `bool` |
| `EnableAutoScaling` | `bool` |
| `BackupBeforeUpgrade` | `bool` |
| `MaxConcurrentReconciles` | `int` |

#### `K8sSidecarConfig`
**Source:** `k8s_sidecar.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `ScrapeInterval` | `time.Duration` |
| `ScrapeTimeout` | `time.Duration` |
| `MetricsPort` | `int` |
| `MetricsPath` | `string` |
| `DiscoveryEnabled` | `bool` |
| `NamespaceFilter` | `[]string` |
| `LabelSelector` | `map[string]string` |
| `AnnotationPrefix` | `string` |
| `AddPodLabels` | `bool` |
| `AddNamespace` | `bool` |
| `AddPodName` | `bool` |
| `AddNodeName` | `bool` |
| `RetentionDuration` | `time.Duration` |
| `MaxStorageBytes` | `int64` |
| `RemoteWriteEnabled` | `bool` |
| `RemoteWriteURL` | `string` |
| `HealthPort` | `int` |

#### `PredictiveAutoscalingConfig`
**Source:** `predictive_autoscaling.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `EvaluationInterval` | `time.Duration` |
| `ForecastHorizon` | `time.Duration` |
| `HistoryWindow` | `time.Duration` |
| `ScaleUpThreshold` | `float64` |
| `ScaleDownThreshold` | `float64` |
| `CooldownPeriod` | `time.Duration` |
| `MinReplicas` | `int` |
| `MaxReplicas` | `int` |
| `TargetUtilization` | `float64` |
| `SafetyMargin` | `float64` |
| `LeadTimeMinutes` | `int` |
| `EnableHPAMetrics` | `bool` |

#### `SaaSControlPlaneConfig`
**Source:** `saas_control_plane.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxTenants` | `int` |
| `DefaultQuota` | `TenantQuota` |
| `MeteringInterval` | `time.Duration` |
| `BillingWebhookURL` | `string` |
| `EnableRBAC` | `bool` |
| `EnableRateLimiting` | `bool` |
| `APIKeyRotationDays` | `int` |

#### `SaaSFleetConfig`
**Source:** `saas_fleet.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `HeartbeatInterval` | `time.Duration` |
| `HeartbeatTimeout` | `time.Duration` |
| `MaxAgents` | `int` |
| `EnableRemoteUpgrade` | `bool` |
| `EnableConfigPush` | `bool` |
| `MeteringInterval` | `time.Duration` |

### Security & Compliance

#### `AuditLogConfig`
**Source:** `audit_log.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxEntries` | `int` |
| `LogWrites` | `bool` |
| `LogQueries` | `bool` |
| `LogAdmin` | `bool` |

#### `BlockchainAuditConfig`
**Source:** `blockchain_audit.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `HashAlgorithm` | `string` |
| `AnchorInterval` | `time.Duration` |
| `MaxBatchSize` | `int` |
| `RetentionPeriod` | `time.Duration` |
| `EnableBlockchain` | `bool` |
| `BlockchainProvider` | `string` |
| `BlockchainEndpoint` | `string` |
| `LegalHoldEnabled` | `bool` |
| `VerificationCacheTTL` | `time.Duration` |

#### `ComplianceAutomationConfig`
**Source:** `compliance_automation.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `PIIDetectionEnabled` | `bool` |
| `AutoMaskEnabled` | `bool` |
| `RetentionEnforcement` | `bool` |
| `AuditLogEnabled` | `bool` |
| `AuditLogMaxSize` | `int` |
| `DSAREnabled` | `bool` |
| `DSARSLADuration` | `time.Duration` |
| `Standards` | `[]ComplianceStandard` |

#### `CompliancePacksConfig`
**Source:** `compliance_packs.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `AuditLogEnabled` | `bool` |
| `AuditLogPath` | `string` |
| `AutoRemediate` | `bool` |

#### `ConfidentialConfig`
**Source:** `confidential.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `TEEType` | `TEEType` |
| `AttestationEnabled` | `bool` |
| `AttestationInterval` | `time.Duration` |
| `SealingKeyPolicy` | `SealingPolicy` |
| `EncryptInMemory` | `bool` |
| `AllowedOperations` | `[]ConfidentialOp` |
| `VerifyBeforeDecrypt` | `bool` |

#### `SMPCConfig`
**Source:** `confidential.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `Threshold` | `int` |
| `TotalParties` | `int` |

#### `DataContractConfig`
**Source:** `data_contracts.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `ValidateOnWrite` | `bool` |
| `AsyncValidation` | `bool` |
| `SamplingRate` | `float64` |
| `MaxViolationsPerMin` | `int` |
| `ProfilingEnabled` | `bool` |
| `ProfilingWindow` | `time.Duration` |
| `AlertOnViolation` | `bool` |
| `EnforcementMode` | `ContractEnforcementMode` |

#### `DataMaskingConfig`
**Source:** `data_masking.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `DefaultPolicy` | `string` |
| `MaxRules` | `int` |

#### `PolicyEngineConfig`
**Source:** `policy_engine.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `DefaultAction` | `PolicyAction` |
| `AuditEnabled` | `bool` |
| `AuditRetention` | `time.Duration` |
| `CacheEnabled` | `bool` |
| `CacheTTL` | `time.Duration` |
| `RateLimitEnabled` | `bool` |
| `RateLimitWindow` | `time.Duration` |
| `AsyncEvaluation` | `bool` |

#### `PrivacyFederationConfig`
**Source:** `privacy_federation.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `Epsilon` | `float64` |
| `Delta` | `float64` |
| `NoiseType` | `NoiseType` |
| `MinAggregationSize` | `int` |
| `SensitivityBound` | `float64` |
| `PrivacyBudgetPerQuery` | `float64` |
| `TotalPrivacyBudget` | `float64` |
| `BudgetRefreshInterval` | `time.Duration` |

#### `RegulatoryComplianceConfig`
**Source:** `regulatory_compliance.go`

| Field | Type |
|-------|------|
| `EnabledFrameworks` | `[]ComplianceFramework` |
| `AuditLogRetention` | `time.Duration` |
| `ImmutableAuditTrail` | `bool` |
| `AutoRemediate` | `bool` |
| `AlertOnViolations` | `bool` |
| `ReportFormat` | `ReportFormat` |
| `DataLineageEnabled` | `bool` |
| `EncryptionRequired` | `bool` |
| `AccessReviewInterval` | `time.Duration` |

#### `ZKQueryConfig`
**Source:** `zk_query.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `CommitmentScheme` | `CommitmentSchemeType` |
| `MerkleTreeDepth` | `int` |
| `ProofType` | `ZKProofType` |
| `BatchCommitmentSize` | `int` |
| `CacheCommitments` | `bool` |
| `CommitmentCacheTTL` | `time.Duration` |
| `EnableAuditLog` | `bool` |

### Replication & Sync

#### `CloudRelayConfig`
**Source:** `cloud_relay.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `TargetURL` | `string` |
| `NodeID` | `string` |
| `Region` | `string` |
| `BatchSize` | `int` |
| `FlushInterval` | `time.Duration` |
| `MaxQueueSize` | `int` |
| `MaxRetries` | `int` |
| `RetryBackoff` | `time.Duration` |
| `MaxBandwidthBps` | `int64` |
| `CompressionEnabled` | `bool` |
| `HeartbeatInterval` | `time.Duration` |
| `ConflictStrategy` | `string` |
| `AuthToken` | `string` |

#### `CloudSyncConfig`
**Source:** `cloud_sync.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `CloudEndpoint` | `string` |
| `SyncMode` | `SyncMode` |
| `BatchSize` | `int` |
| `MaxBatchBytes` | `int` |
| `SyncInterval` | `time.Duration` |
| `RetryInterval` | `time.Duration` |
| `MaxRetries` | `int` |
| `OfflineQueuePath` | `string` |
| `MaxOfflineQueueSize` | `int64` |
| `ConflictResolution` | `ConflictResolutionStrategy` |
| `MetricFilters` | `[]string` |
| `TimeRangeStart` | `int64` |
| `TimeRangeEnd` | `int64` |
| `EnableCompression` | `bool` |
| `BandwidthLimit` | `int64` |
| `SyncFormat` | `CloudSyncFormat` |
| `CloudProvider` | `CloudProvider` |
| `S3Config` | `*S3SyncConfig` |
| `HTTPClient` | `HTTPDoer` |

#### `S3SyncConfig`
**Source:** `cloud_sync.go`

| Field | Type |
|-------|------|
| `Bucket` | `string` |
| `Prefix` | `string` |
| `Region` | `string` |
| `AccessKeyID` | `string` |
| `SecretAccessKey` | `string` |

#### `RecordingRuleConfig`
**Source:** `cloud_sync.go`

| Field | Type |
|-------|------|
| `Name` | `string` |
| `Query` | `string` |
| `Interval` | `string` |

#### `CloudSyncFabricConfig`
**Source:** `cloud_sync_fabric.go`

| Field | Type |
|-------|------|
| `MaxConnectors` | `int` |
| `SyncInterval` | `time.Duration` |
| `BandwidthLimitBytes` | `int64` |
| `CompressTransfers` | `bool` |
| `MerkleDepth` | `int` |
| `PriorityLevels` | `int` |
| `MaxRetries` | `int` |
| `RetryBackoff` | `time.Duration` |

#### `CloudConnectorConfig`
**Source:** `cloud_sync_fabric.go`

| Field | Type |
|-------|------|
| `Name` | `string` |
| `Type` | `CloudConnectorType` |
| `Endpoint` | `string` |
| `Region` | `string` |
| `Bucket` | `string` |
| `Prefix` | `string` |
| `Priority` | `int` |
| `Enabled` | `bool` |
| `Config` | `map[string]string` |

#### `ConnectionPoolConfig`
**Source:** `connection_pool.go`

| Field | Type |
|-------|------|
| `MaxSize` | `int` |
| `MinIdle` | `int` |
| `MaxIdleTime` | `time.Duration` |
| `HealthCheckInterval` | `time.Duration` |

#### `DeltaSyncConfig`
**Source:** `delta_sync.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `CloudEndpoint` | `string` |
| `DeviceID` | `string` |
| `SyncDirection` | `DeltaSyncDirection` |
| `SyncInterval` | `time.Duration` |
| `BatchSize` | `int` |
| `MaxBatchBytes` | `int64` |
| `DeltaCompressionEnabled` | `bool` |
| `VectorClockEnabled` | `bool` |
| `ConflictStrategy` | `DeltaConflictStrategy` |
| `OfflineQueuePath` | `string` |
| `MaxOfflineQueueBytes` | `int64` |
| `BandwidthLimitBps` | `int64` |
| `MaxRetries` | `int` |
| `RetryBackoff` | `time.Duration` |
| `MaxRetryBackoff` | `time.Duration` |
| `FilterMetrics` | `[]string` |
| `FilterTags` | `map[string][]string` |
| `EnableChecksum` | `bool` |
| `CompressPayload` | `bool` |
| `Auth` | `*DeltaSyncAuth` |
| `InsecureSkipVerify` | `bool` |

#### `FederationConfig`
**Source:** `federation.go`

| Field | Type |
|-------|------|
| `Timeout` | `time.Duration` |
| `RetryCount` | `int` |
| `HealthCheckInterval` | `time.Duration` |
| `MaxConcurrentQueries` | `int` |
| `MergeStrategy` | `MergeStrategy` |

#### `MultiRegionReplicationConfig`
**Source:** `multi_region_replication.go`

| Field | Type |
|-------|------|
| `RegionName` | `string` |
| `PeerAddresses` | `[]string` |
| `ConsistencyLevel` | `MRConsistencyLevel` |
| `ReplicationFactor` | `int` |
| `HeartbeatInterval` | `time.Duration` |
| `ConflictResolution` | `MRConflictResolution` |
| `MaxReplicationLag` | `time.Duration` |
| `SyncBatchSize` | `int` |

#### `OfflineSyncConfig`
**Source:** `offline_sync.go`

| Field | Type |
|-------|------|
| `NodeID` | `string` |
| `MaxOfflineDuration` | `time.Duration` |
| `SyncBatchSize` | `int` |
| `BloomFilterSize` | `uint` |
| `BloomFilterHashes` | `uint` |
| `MaxDeltaSize` | `int64` |
| `CompressDeltas` | `bool` |
| `ConflictStrategy` | `CRDTConflictStrategy` |
| `SchemaEvolutionEnabled` | `bool` |
| `BandwidthBudgetBps` | `int64` |

#### `RateControllerConfig`
**Source:** `rate_controller.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `GlobalRateLimit` | `int64` |
| `PerMetricLimit` | `int64` |
| `BurstSize` | `int64` |
| `WindowDuration` | `time.Duration` |

#### `WireProtocolConfig`
**Source:** `wire_protocol.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `ListenAddr` | `string` |
| `MaxConnections` | `int` |
| `MaxMessageSize` | `int` |
| `ReadTimeout` | `time.Duration` |
| `WriteTimeout` | `time.Duration` |

### ML & Analytics

#### `AdaptiveOptimizerConfig`
**Source:** `adaptive_optimizer.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `LearningRate` | `float64` |
| `MinSamplesForRecommend` | `int` |
| `PlanCacheSize` | `int` |
| `PlanCacheTTL` | `time.Duration` |
| `StabilityThreshold` | `float64` |
| `MaxHistorySize` | `int` |
| `IndexRecommendEnabled` | `bool` |
| `CostModelUpdateFreq` | `time.Duration` |

#### `AdaptiveSamplingConfig`
**Source:** `adaptive_sampling.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `DefaultRate` | `float64` |
| `MinRate` | `float64` |
| `MaxRate` | `float64` |
| `VolatilityWindow` | `int` |
| `StorageBudgetMB` | `int64` |
| `AdjustInterval` | `time.Duration` |

#### `AutoMLConfig`
**Source:** `auto_ml.go`

| Field | Type |
|-------|------|
| `EnableAutoSelection` | `bool` |
| `EnableEnsemble` | `bool` |
| `CrossValidationFolds` | `int` |
| `HoldoutRatio` | `float64` |
| `MaxModelsToEvaluate` | `int` |
| `MinDataPoints` | `int` |
| `CacheModels` | `bool` |
| `CacheTTL` | `time.Duration` |
| `SeasonalityDetection` | `bool` |
| `TrendDetection` | `bool` |

#### `DataLineageConfig`
**Source:** `data_lineage.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxEntries` | `int` |
| `TrackWrites` | `bool` |
| `TrackQueries` | `bool` |
| `TrackTransforms` | `bool` |

#### `FeatureStoreConfig`
**Source:** `feature_store.go`

| Field | Type |
|-------|------|
| `MaxFeatures` | `int` |
| `DefaultTTL` | `time.Duration` |
| `EnableVersioning` | `bool` |
| `MaxVersions` | `int` |
| `CacheEnabled` | `bool` |
| `CacheTTL` | `time.Duration` |
| `OnlineServingEnabled` | `bool` |
| `BatchServingEnabled` | `bool` |
| `PointInTimeEnabled` | `bool` |

#### `MaterializationConfig`
**Source:** `feature_store.go`

| Field | Type |
|-------|------|
| `FeatureName` | `string` |
| `SourceQuery` | `string` |
| `Schedule` | `string` |
| `TTL` | `time.Duration` |

#### `FederatedLearningConfig`
**Source:** `federated_learning.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `Role` | `FederatedRole` |
| `CoordinatorURL` | `string` |
| `MinParticipants` | `int` |
| `RoundTimeout` | `time.Duration` |
| `AggregationStrategy` | `AggregationStrategy` |
| `EnableDifferentialPrivacy` | `bool` |
| `Epsilon` | `float64` |
| `Delta` | `float64` |
| `ClippingNorm` | `float64` |
| `EnableSecureAggregation` | `bool` |
| `EnableCompression` | `bool` |
| `CompressionThreshold` | `float64` |
| `SyncInterval` | `time.Duration` |
| `MaxRetries` | `int` |
| `HeartbeatInterval` | `time.Duration` |

#### `RoundConfig`
**Source:** `federated_learning.go`

| Field | Type |
|-------|------|
| `LocalEpochs` | `int` |
| `BatchSize` | `int` |
| `LearningRate` | `float64` |
| `MinSamples` | `int` |

#### `FederatedMLConfig`
**Source:** `federated_ml_training.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `Role` | `FederatedMLRole` |
| `CoordinatorURL` | `string` |
| `MinParticipants` | `int` |
| `MaxRounds` | `int` |
| `RoundTimeout` | `time.Duration` |
| `ConvergenceThreshold` | `float64` |
| `EnableDifferentialPrivacy` | `bool` |
| `Epsilon` | `float64` |
| `Delta` | `float64` |
| `ClippingNorm` | `float64` |
| `EnableSecureAggregation` | `bool` |
| `SecretShares` | `int` |
| `ShareThreshold` | `int` |
| `ModelType` | `string` |
| `LearningRate` | `float64` |

#### `FoundationModelConfig`
**Source:** `foundation_model.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `ModelPath` | `string` |
| `ModelVersion` | `string` |
| `MaxBatchSize` | `int` |
| `InferenceTimeout` | `time.Duration` |
| `ForecastHorizon` | `int` |
| `AnomalyThreshold` | `float64` |
| `EnableFineTuning` | `bool` |
| `FineTuningLearningRate` | `float64` |
| `FineTuningEpochs` | `int` |
| `CacheEnabled` | `bool` |
| `CacheTTL` | `time.Duration` |
| `MaxConcurrentInferences` | `int` |

#### `MLInferenceConfig`
**Source:** `ml_inference.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxModels` | `int` |
| `MaxModelSizeBytes` | `int64` |
| `InferenceTimeoutMs` | `int64` |
| `BatchSize` | `int` |
| `FeatureWindowSize` | `int` |
| `EnableRealTimeScoring` | `bool` |
| `AnomalyScoreThreshold` | `float64` |
| `ModelStorePath` | `string` |

#### `InferenceAutoMLConfig`
**Source:** `ml_inference.go`

| Field | Type |
|-------|------|
| `CandidateModels` | `[]InferenceModelType` |
| `ValidationSplit` | `float64` |
| `MaxTrainingTime` | `time.Duration` |
| `OptimizeFor` | `string` |

#### `MultiModalConfig`
**Source:** `multi_modal.go`

| Field | Type |
|-------|------|
| `LogsEnabled` | `bool` |
| `TracesEnabled` | `bool` |
| `CorrelationEnabled` | `bool` |
| `LogRetention` | `time.Duration` |
| `TraceRetention` | `time.Duration` |
| `MaxLogEntries` | `int` |
| `MaxSpans` | `int` |
| `FullTextIndexing` | `bool` |
| `NGramSize` | `int` |
| `CorrelationWindow` | `time.Duration` |

#### `NLDashboardConfig`
**Source:** `nl_dashboard.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `DefaultTimeRange` | `string` |
| `DefaultRefreshInterval` | `string` |
| `MaxPanelsPerRow` | `int` |
| `EnableAutoComplete` | `bool` |
| `EnableFeedback` | `bool` |

#### `FieldConfig`
**Source:** `nl_dashboard.go`

| Field | Type |
|-------|------|
| `Defaults` | `*FieldDefaults` |
| `Overrides` | `[]any` |

#### `ColorConfig`
**Source:** `nl_dashboard.go`

| Field | Type |
|-------|------|
| `Mode` | `string` |
| `FixedColor` | `string` |

#### `NLQueryConfig`
**Source:** `nl_query.go`

| Field | Type |
|-------|------|
| `Provider` | `string` |
| `APIKey` | `string` |
| `Endpoint` | `string` |
| `Model` | `string` |
| `EnableConversation` | `bool` |
| `MaxConversationTurns` | `int` |
| `EnableOptimization` | `bool` |
| `EnableExplanation` | `bool` |
| `EnableVisualization` | `bool` |
| `EnableAlerts` | `bool` |
| `Timeout` | `time.Duration` |
| `FallbackToLocal` | `bool` |

#### `SemanticSearchConfig`
**Source:** `semantic_search.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `WindowSize` | `int` |
| `EmbeddingDimension` | `int` |
| `MaxPatterns` | `int` |
| `SimilarityThreshold` | `float64` |
| `IndexType` | `SearchIndexType` |
| `HNSWEfConstruction` | `int` |
| `HNSWEfSearch` | `int` |
| `HNSWM` | `int` |
| `AutoIndexInterval` | `time.Duration` |

#### `TinyMLConfig`
**Source:** `tinyml.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxModelSize` | `int64` |
| `MaxModels` | `int` |
| `DefaultThreshold` | `float64` |
| `InferenceTimeout` | `int64` |

#### `TSBranchConfig`
**Source:** `ts_branching.go`

| Field | Type |
|-------|------|
| `MaxBranches` | `int` |
| `DefaultBranch` | `string` |
| `CopyOnWrite` | `bool` |
| `DeletedRetention` | `time.Duration` |
| `AutoCleanup` | `bool` |
| `DefaultMergeStrategy` | `TSMergeStrategy` |
| `BranchCompression` | `bool` |

#### `TSDiffConfig`
**Source:** `ts_diff.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxComparePoints` | `int` |
| `SignificanceLevel` | `float64` |

#### `TSDiffMergeConfig`
**Source:** `ts_diff_merge.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxDiffSize` | `int` |
| `MergeTimeout` | `time.Duration` |
| `ConflictStrategy` | `string` |
| `AutoCleanupBranches` | `bool` |
| `BranchTTL` | `time.Duration` |

#### `TSRAGConfig`
**Source:** `ts_rag.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `EmbeddingDim` | `int` |
| `MaxPatternLength` | `int` |
| `SimilarityThreshold` | `float64` |
| `MaxRetrievedPatterns` | `int` |
| `LLMProvider` | `string` |
| `LLMEndpoint` | `string` |
| `LLMAPIKey` | `string` |
| `LLMModel` | `string` |
| `MaxConversationHistory` | `int` |
| `EnableAutoEmbed` | `bool` |
| `SegmentDuration` | `time.Duration` |

#### `LLMConfig`
**Source:** `ts_rag_llm.go`

| Field | Type |
|-------|------|
| `Provider` | `string` |
| `ModelPath` | `string` |
| `MaxTokensLimit` | `int` |
| `Temperature` | `float64` |
| `TopP` | `float64` |
| `SystemPrompt` | `string` |

#### `VectorConfig`
**Source:** `vector.go`

| Field | Type |
|-------|------|
| `MaxDimensions` | `int` |
| `DefaultDimension` | `int` |
| `MaxVectorsPerSeries` | `int` |
| `DistanceMetric` | `DistanceMetric` |
| `NormalizeVectors` | `bool` |

### Integrations & Protocols

#### `ArrowFlightConfig`
**Source:** `arrow_flight.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `BindAddr` | `string` |
| `MaxBatchSize` | `int` |
| `MaxMessageSize` | `int64` |
| `EnableCompression` | `bool` |

#### `FFIConfig`
**Source:** `cffi.go`

| Field | Type |
|-------|------|
| `EnableThreadSafety` | `bool` |
| `MaxHandles` | `int` |

#### `ConnectorHubConfig`
**Source:** `connector_hub.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxConnectors` | `int` |
| `HealthCheckInterval` | `time.Duration` |
| `DeadLetterEnabled` | `bool` |
| `DeadLetterMax` | `int` |
| `DefaultBatchSize` | `int` |
| `DefaultFlushMs` | `int` |

#### `ConnectorConfig`
**Source:** `connector_hub.go`

| Field | Type |
|-------|------|
| `Name` | `string` |
| `Type` | `ConnectorType` |
| `Driver` | `string` |
| `BatchSize` | `int` |
| `FlushIntervalMs` | `int` |
| `Properties` | `map[string]string` |
| `Filters` | `ConnectorFilters` |

#### `DenoRuntimeConfig`
**Source:** `deno_runtime.go`

| Field | Type |
|-------|------|
| `ProjectID` | `string` |
| `KVDatabaseID` | `string` |
| `Region` | `string` |
| `MaxMemoryMB` | `int` |
| `EnableStreaming` | `bool` |
| `CacheEnabled` | `bool` |
| `CacheTTL` | `time.Duration` |
| `BatchSize` | `int` |
| `ConsistencyLevel` | `string` |
| `APIToken` | `string` |

#### `EdgeDeployConfig`
**Source:** `deno_runtime.go`

| Field | Type |
|-------|------|
| `Platform` | `EdgePlatform` |
| `Workers` | `*WorkersRuntimeConfig` |
| `Deno` | `*DenoRuntimeConfig` |
| `FeatureFlags` | `map[string]bool` |

#### `EBPFConfig`
**Source:** `ebpf.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `CollectionInterval` | `time.Duration` |
| `EnableCPUMetrics` | `bool` |
| `EnableMemoryMetrics` | `bool` |
| `EnableDiskMetrics` | `bool` |
| `EnableNetworkMetrics` | `bool` |
| `EnableProcessMetrics` | `bool` |
| `EnableSyscallTracing` | `bool` |
| `ProcessFilter` | `func(pid` |
| `TargetPIDs` | `[]int` |
| `HistogramBuckets` | `[]float64` |
| `MaxEventsPerSecond` | `int` |

#### `GrafanaBackendConfig`
**Source:** `grafana_backend.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `HTTPPort` | `int` |
| `MaxDataPoints` | `int` |
| `DefaultInterval` | `time.Duration` |
| `EnableStreaming` | `bool` |
| `EnableAnnotations` | `bool` |
| `EnableVariables` | `bool` |
| `CacheDuration` | `time.Duration` |
| `AllowedOrigins` | `[]string` |

#### `GrafanaDatasourcePluginConfig`
**Source:** `grafana_plugin.go`

| Field | Type |
|-------|------|
| `URL` | `string` |
| `Database` | `string` |
| `BasicAuth` | `bool` |
| `TLSEnabled` | `bool` |
| `MaxRetries` | `int` |
| `Timeout` | `int` |

#### `GRPCIngestionConfig`
**Source:** `grpc_ingestion.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `ListenAddr` | `string` |
| `MaxMessageSize` | `int` |
| `MaxConcurrent` | `int` |
| `WriteTimeout` | `time.Duration` |
| `QueryTimeout` | `time.Duration` |
| `EnableReflection` | `bool` |
| `TLSCertFile` | `string` |
| `TLSKeyFile` | `string` |

#### `IoTDeviceSDKConfig`
**Source:** `iot_device_sdk.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxDevices` | `int` |
| `HeartbeatInterval` | `time.Duration` |
| `OfflineQueueSize` | `int` |
| `BatchSize` | `int` |
| `BatchFlushInterval` | `time.Duration` |
| `EnableAutoDiscovery` | `bool` |
| `EnableOTA` | `bool` |
| `MaxPayloadSize` | `int` |
| `ProtocolVersion` | `string` |
| `EnableCompression` | `bool` |
| `RetryAttempts` | `int` |
| `RetryBackoff` | `time.Duration` |

#### `JupyterKernelConfig`
**Source:** `jupyter_kernel.go`

| Field | Type |
|-------|------|
| `KernelName` | `string` |
| `DisplayName` | `string` |
| `Language` | `string` |
| `MagicsEnabled` | `bool` |
| `VisualizationEnabled` | `bool` |
| `MaxOutputSize` | `int` |
| `ExecutionTimeout` | `time.Duration` |
| `ConnectionFile` | `string` |

#### `LSPConfig`
**Source:** `lsp.go`

| Field | Type |
|-------|------|
| `Port` | `int` |
| `EnableDiagnostics` | `bool` |
| `EnableCompletion` | `bool` |
| `EnableHover` | `bool` |
| `EnableFormatting` | `bool` |
| `DiagnosticDelay` | `time.Duration` |

#### `LSPEnhancedConfig`
**Source:** `lsp_enhanced.go`

| Field | Type |
|-------|------|
| `EnableCQL` | `bool` |
| `EnablePromQL` | `bool` |
| `EnableSQL` | `bool` |
| `MetricCacheSize` | `int` |
| `TagCacheSize` | `int` |
| `EnableSignatureHelp` | `bool` |
| `EnableCodeActions` | `bool` |

#### `MarketplaceConfig`
**Source:** `marketplace.go`

| Field | Type |
|-------|------|
| `MarketplaceURL` | `string` |
| `CacheTTL` | `time.Duration` |
| `AutoUpdate` | `bool` |
| `VerifySignatures` | `bool` |
| `RevenueSharePct` | `float64` |
| `MaxPluginSize` | `int64` |
| `EnableRatings` | `bool` |
| `EnableReviews` | `bool` |
| `SandboxEnabled` | `bool` |

#### `MobileSDKConfig`
**Source:** `mobile_sdk.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxOfflineQueueSize` | `int` |
| `SyncInterval` | `time.Duration` |
| `BatchSize` | `int` |
| `CompressionEnabled` | `bool` |
| `RetryMaxAttempts` | `int` |
| `RetryBackoff` | `time.Duration` |
| `MaxConnectedDevices` | `int` |
| `ConflictResolution` | `MobileConflictStrategy` |

#### `OpenAPIGeneratorConfig`
**Source:** `openapi_spec.go`

| Field | Type |
|-------|------|
| `Title` | `string` |
| `Description` | `string` |
| `Version` | `string` |
| `ServerURL` | `string` |
| `IncludeExperimental` | `bool` |

#### `SDKGeneratorConfig`
**Source:** `openapi_spec.go`

| Field | Type |
|-------|------|
| `Language` | `SDKLanguage` |
| `OutputDir` | `string` |
| `PackageName` | `string` |
| `Version` | `string` |
| `IncludeTests` | `bool` |
| `IncludeExamples` | `bool` |

#### `PrometheusDropInConfig`
**Source:** `prometheus_dropin.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `EnableAlertmanager` | `bool` |
| `AlertmanagerURL` | `string` |
| `EnableRecordingRules` | `bool` |
| `EnableServiceDiscovery` | `bool` |
| `MaxSamples` | `int` |
| `LookbackDelta` | `time.Duration` |
| `QueryTimeout` | `time.Duration` |

#### `TFProviderConfig`
**Source:** `terraform_provider.go`

| Field | Type |
|-------|------|
| `Endpoint` | `string` |
| `APIKey` | `string` |
| `Timeout` | `int` |

#### `UniversalSDKConfig`
**Source:** `universal_sdk.go`

| Field | Type |
|-------|------|
| `OutputDir` | `string` |
| `Languages` | `[]UniversalSDKLanguage` |
| `APIBaseURL` | `string` |
| `PackageName` | `string` |
| `Version` | `string` |
| `IncludeHTTPClient` | `bool` |
| `IncludeFFI` | `bool` |
| `GenerateTests` | `bool` |
| `DocFormat` | `string` |

#### `WASMConsoleConfig`
**Source:** `wasm_console.go`

| Field | Type |
|-------|------|
| `Title` | `string` |
| `MaxQueryHistory` | `int` |
| `EnableAutoComplete` | `bool` |
| `EnableChartSuggestions` | `bool` |
| `Theme` | `string` |
| `MaxResultRows` | `int` |
| `SupportedLanguages` | `[]string` |

#### `PlaygroundConfig`
**Source:** `wasm_playground.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `Title` | `string` |
| `Theme` | `string` |
| `MaxQueryLength` | `int` |
| `DefaultQuery` | `string` |
| `EnableCharts` | `bool` |
| `EnableExport` | `bool` |
| `WASMPath` | `string` |
| `CDNBaseURL` | `string` |
| `AllowedOrigins` | `string` |
| `AutocompleteOn` | `bool` |
| `MaxResultRows` | `int` |
| `SessionTimeout` | `time.Duration` |

#### `WASMPluginConfig`
**Source:** `wasm_runtime.go`

| Field | Type |
|-------|------|
| `Name` | `string` |
| `Path` | `string` |
| `WASMBytes` | `[]byte` |
| `MaxMemoryMB` | `int` |
| `MaxExecTimeMs` | `int64` |
| `Permissions` | `WASMPermissions` |

#### `WASMRuntimeConfig`
**Source:** `wasm_runtime.go`

| Field | Type |
|-------|------|
| `MaxPlugins` | `int` |
| `DefaultMemoryMB` | `int` |
| `DefaultTimeoutMs` | `int64` |
| `EnableSandbox` | `bool` |

#### `WASMUDFConfig`
**Source:** `wasm_udf.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxExecutionTime` | `time.Duration` |
| `MaxMemoryBytes` | `int64` |
| `SandboxEnabled` | `bool` |
| `MaxUDFs` | `int` |
| `CacheCompiled` | `bool` |

#### `WebhookConfig`
**Source:** `webhook_system.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxWebhooks` | `int` |
| `RetryAttempts` | `int` |
| `RetryDelay` | `time.Duration` |

#### `WorkersRuntimeConfig`
**Source:** `workers_runtime.go`

| Field | Type |
|-------|------|
| `D1DatabaseID` | `string` |
| `R2BucketName` | `string` |
| `KVNamespace` | `string` |
| `AccountID` | `string` |
| `APIToken` | `string` |
| `MaxMemoryMB` | `int` |
| `EnableStreaming` | `bool` |
| `CacheEnabled` | `bool` |
| `CacheTTL` | `time.Duration` |
| `BatchSize` | `int` |
| `Region` | `string` |

### Developer Tools

#### `AutoRemediationConfig`
**Source:** `auto_remediation.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `EvaluationInterval` | `time.Duration` |
| `MaxActionsPerHour` | `int` |
| `RequireApproval` | `bool` |
| `ApprovalTimeout` | `time.Duration` |
| `EnableMLRecommendations` | `bool` |
| `RollbackOnFailure` | `bool` |
| `CircuitBreakerThreshold` | `int` |
| `CooldownPeriod` | `time.Duration` |

#### `BranchConfig`
**Source:** `branching.go`

| Field | Type |
|-------|------|
| `MaxBranches` | `int` |
| `MaxSnapshots` | `int` |
| `AutoSnapshot` | `bool` |
| `SnapshotInterval` | `time.Duration` |
| `RetainSnapshots` | `time.Duration` |

#### `ChaosConfig`
**Source:** `chaos.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `Seed` | `int64` |
| `MaxConcurrentFaults` | `int` |
| `SafetyChecks` | `bool` |
| `LogEvents` | `bool` |

#### `FaultConfig`
**Source:** `chaos.go`

| Field | Type |
|-------|------|
| `Type` | `FaultType` |
| `Duration` | `time.Duration` |
| `Probability` | `float64` |
| `Target` | `string` |
| `Parameters` | `map[string]any` |

#### `ChaosRecoveryConfig`
**Source:** `chaos_recovery.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxScenarios` | `int` |
| `DefaultDuration` | `time.Duration` |
| `AutoRecover` | `bool` |

#### `ChronicleStudioConfig`
**Source:** `chronicle_studio.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxProjects` | `int` |
| `MaxNotebooksPerProject` | `int` |
| `MaxCellsPerNotebook` | `int` |
| `AutoSaveInterval` | `time.Duration` |
| `MaxQueryTimeout` | `time.Duration` |
| `EnableCollaboration` | `bool` |
| `MaxCollaborators` | `int` |
| `EnableVersioning` | `bool` |
| `MaxVersionHistory` | `int` |
| `EnableExport` | `bool` |
| `ExportFormats` | `[]string` |

#### `AxisConfig`
**Source:** `chronicle_studio.go`

| Field | Type |
|-------|------|
| `Label` | `string` |
| `Field` | `string` |
| `Format` | `string` |
| `Min` | `*float64` |
| `Max` | `*float64` |

#### `SeriesConfig`
**Source:** `chronicle_studio.go`

| Field | Type |
|-------|------|
| `Name` | `string` |
| `Field` | `string` |
| `Color` | `string` |
| `Type` | `VisualizationType` |

#### `ConfigReloadConfig`
**Source:** `config_hot_reload.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `WatchInterval` | `time.Duration` |

#### `DataMeshConfig`
**Source:** `data_mesh.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `NodeID` | `string` |
| `ListenAddr` | `string` |
| `SeedNodes` | `[]string` |
| `GossipInterval` | `time.Duration` |
| `HeartbeatInterval` | `time.Duration` |
| `HeartbeatTimeout` | `time.Duration` |
| `MaxConcurrentQueries` | `int` |
| `QueryTimeout` | `time.Duration` |
| `ReplicationFactor` | `int` |
| `EnableLocalityRouting` | `bool` |
| `MaxPeers` | `int` |
| `EnableMTLS` | `bool` |

#### `DeprecationConfig`
**Source:** `deprecation.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `WarnOnUse` | `bool` |
| `MinWarningVersions` | `int` |

#### `DashboardConfig`
**Source:** `embeddable_dashboard.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `Title` | `string` |
| `RefreshInterval` | `time.Duration` |
| `MaxPanels` | `int` |
| `Theme` | `string` |
| `DefaultTimeRange` | `time.Duration` |
| `EnableExport` | `bool` |

#### `WebComponentConfig`
**Source:** `embeddable_dashboard.go`

| Field | Type |
|-------|------|
| `TagName` | `string` |
| `ShadowDOM` | `bool` |
| `Attributes` | `[]string` |

#### `FeatureFlagConfig`
**Source:** `feature_flags.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `DefaultPolicy` | `string` |
| `DisabledFeatures` | `[]string` |

#### `FeatureManagerConfig`
**Source:** `feature_manager.go`

| Field | Type |
|-------|------|
| `ExemplarConfig` | `ExemplarConfig` |
| `CardinalityConfig` | `CardinalityConfig` |
| `StrictSchema` | `bool` |
| `Schemas` | `[]MetricSchema` |
| `CQL` | `CQLConfig` |
| `Observability` | `ObservabilitySuiteConfig` |
| `MaterializedViews` | `MaterializedViewConfig` |

#### `GitOpsConfig`
**Source:** `gitops.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `WatchPaths` | `[]string` |
| `ReconcileInterval` | `time.Duration` |
| `DryRunByDefault` | `bool` |
| `EnableAuditLog` | `bool` |
| `MaxAuditEntries` | `int` |
| `StrictValidation` | `bool` |

#### `NotebookConfig`
**Source:** `notebooks.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxCells` | `int` |
| `MaxOutputRows` | `int` |
| `ExecutionTimeout` | `time.Duration` |
| `AutoSaveInterval` | `time.Duration` |
| `MaxNotebooks` | `int` |

#### `PluginLifecycleConfig`
**Source:** `plugin_lifecycle.go`

| Field | Type |
|-------|------|
| `PluginDir` | `string` |
| `CacheDir` | `string` |
| `AutoUpdate` | `bool` |
| `SandboxEnabled` | `bool` |
| `MaxPlugins` | `int` |
| `HealthCheckInterval` | `time.Duration` |
| `InstallTimeout` | `time.Duration` |

#### `PluginSDKConfig`
**Source:** `plugin_sdk.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `PluginDir` | `string` |
| `MaxPlugins` | `int` |
| `MaxMemoryPerPlugin` | `int64` |
| `ExecutionTimeout` | `time.Duration` |
| `MarketplaceURL` | `string` |
| `EnableSandbox` | `bool` |
| `AutoUpdate` | `bool` |

#### `SchemaDesignerConfig`
**Source:** `schema_designer.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxSchemas` | `int` |
| `MaxFieldsPerSchema` | `int` |
| `EnableCodeGen` | `bool` |
| `EnableValidation` | `bool` |

#### `SchemaInferenceConfig`
**Source:** `schema_inference.go`

| Field | Type |
|-------|------|
| `SampleSize` | `int` |
| `ConfidenceThreshold` | `float64` |
| `AutoSuggestIndexes` | `bool` |
| `AutoSuggestCompress` | `bool` |
| `AutoSuggestRetention` | `bool` |
| `MaxFieldCount` | `int` |
| `EnableMigration` | `bool` |
| `MigrationBatchSize` | `int` |

#### `StudioEnhancedConfig`
**Source:** `studio_enhanced.go`

| Field | Type |
|-------|------|
| `EnableCollaboration` | `bool` |
| `MaxSessions` | `int` |
| `SessionTimeout` | `time.Duration` |
| `EnableAutocomplete` | `bool` |
| `EnableSyntaxHighlight` | `bool` |
| `Theme` | `string` |
| `EnableRealtimePreview` | `bool` |
| `MaxQueryHistory` | `int` |
| `EnableDataExport` | `bool` |

#### `TimeTravelDebugConfig`
**Source:** `time_travel_debug.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxSnapshots` | `int` |
| `DiffTimeout` | `time.Duration` |
| `MaxDiffResults` | `int` |
| `ReplayBufferSize` | `int` |

#### `TimeTravelConfig`
**Source:** `timetravel.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxRetention` | `time.Duration` |
| `SnapshotInterval` | `time.Duration` |
| `ChangeDataCaptureEnabled` | `bool` |
| `MaxSnapshots` | `int` |
| `CompactAfter` | `time.Duration` |

### Other

#### `GossipClusterConfig`
**Source:** `cluster_engine.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `NodeID` | `string` |
| `ListenAddr` | `string` |
| `Seeds` | `[]string` |
| `ReplicationFactor` | `int` |
| `VirtualNodes` | `int` |
| `GossipInterval` | `time.Duration` |
| `FailureTimeout` | `time.Duration` |
| `RebalanceDelay` | `time.Duration` |

#### `AntiEntropyConfig`
**Source:** `cluster_engine.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `Interval` | `time.Duration` |
| `MaxRepairKeys` | `int` |
| `Concurrency` | `int` |

#### `CrossCloudTieringConfig`
**Source:** `cross_cloud_tiering.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `EvaluationInterval` | `time.Duration` |
| `DefaultPolicy` | `string` |
| `CostOptimizationEnabled` | `bool` |
| `EgressAware` | `bool` |
| `MaxConcurrentMigrations` | `int` |
| `DryRunMode` | `bool` |

#### `EmbeddedClusterConfig`
**Source:** `embedded_cluster.go`

| Field | Type |
|-------|------|
| `NodeID` | `string` |
| `BindAddr` | `string` |
| `BindPort` | `int` |
| `Peers` | `[]string` |
| `ReplicationFactor` | `int` |
| `DefaultConsistency` | `ReadConsistency` |
| `ElectionTimeout` | `time.Duration` |
| `HeartbeatInterval` | `time.Duration` |
| `SnapshotInterval` | `time.Duration` |
| `MaxStaleness` | `time.Duration` |
| `ReadOnly` | `bool` |
| `AutoFailover` | `bool` |
| `SplitBrainDetection` | `bool` |

#### `ExemplarConfig`
**Source:** `exemplar.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxExemplarsPerSeries` | `int` |
| `MaxTotalExemplars` | `int64` |
| `RetentionDuration` | `time.Duration` |

#### `GeoConfig`
**Source:** `geo.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `IndexResolution` | `int` |
| `MaxGeofences` | `int` |
| `GeofenceCheckInterval` | `time.Duration` |
| `EnableSpatialIndex` | `bool` |
| `MaxPointsPerQuery` | `int` |

#### `MultiModelConfig`
**Source:** `multi_model.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `KeyValueTTL` | `time.Duration` |
| `MaxDocumentSize` | `int` |
| `MaxLogRetention` | `time.Duration` |
| `IndexPaths` | `[]string` |
| `CompactionInterval` | `time.Duration` |

#### `SLOConfig`
**Source:** `multi_model_extensions.go`

| Field | Type |
|-------|------|
| `Name` | `string` |
| `Metric` | `string` |
| `Target` | `float64` |
| `Window` | `time.Duration` |
| `BadThreshold` | `float64` |
| `Operator` | `SLOOperator` |

#### `IntegratedMultiModelStoreConfig`
**Source:** `multi_model_store.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `EnableCrossModel` | `bool` |
| `MaxCrossModelJoins` | `int` |

#### `PartitionPrunerConfig`
**Source:** `partition_pruner.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MinPartitions` | `int` |

#### `HardeningConfig`
**Source:** `production_hardening.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `FuzzIterations` | `int` |
| `ChaosRounds` | `int` |
| `PropertyTests` | `int` |
| `ComplianceLevel` | `string` |

#### `RetryConfig`
**Source:** `retry.go`

| Field | Type |
|-------|------|
| `MaxAttempts` | `int` |
| `InitialBackoff` | `time.Duration` |
| `MaxBackoff` | `time.Duration` |
| `BackoffMultiplier` | `float64` |
| `Jitter` | `float64` |
| `RetryIf` | `func(error)` |

#### `SchemaEvolutionConfig`
**Source:** `schema_evolution.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `AutoDetect` | `bool` |
| `AutoMigrate` | `bool` |
| `MaxVersions` | `int` |
| `BreakingChangeAlerts` | `bool` |
| `AlertWebhookURL` | `string` |
| `LazyMigration` | `bool` |

#### `TagIndexConfig`
**Source:** `tag_index.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxCardinality` | `int` |
| `MaxTagKeys` | `int` |

#### `TenantConfig`
**Source:** `tenant.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `DefaultTenant` | `string` |
| `AllowedTenants` | `[]string` |
| `IsolateQueries` | `bool` |

#### `TenantIsolationConfig`
**Source:** `tenant_isolation.go`

| Field | Type |
|-------|------|
| `Enabled` | `bool` |
| `MaxTenants` | `int` |
| `DefaultMemoryBudgetMB` | `int64` |
| `DefaultQueryQuota` | `int` |
| `DefaultStorageLimitMB` | `int64` |
