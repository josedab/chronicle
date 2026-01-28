---
sidebar_position: 1
---

# Storage Backends

Chronicle supports pluggable storage backends for flexibility in deployment and data management.

## Available Backends

| Backend | Use Case | Durability | Cost |
|---------|----------|------------|------|
| Local File | Development, small deployments | Local disk | Low |
| S3 | Cloud, scalable storage | High | Medium |
| Custom | Special requirements | Varies | Varies |

## Local File (Default)

The default backend stores data in a single file:

```go
db, _ := chronicle.Open("data.db", chronicle.DefaultConfig("data.db"))
```

### Characteristics
- Simple, no external dependencies
- Good performance on SSD/NVMe
- Limited by local disk capacity
- Single-machine only

## S3 Backend

Store partitions in Amazon S3 or compatible object storage:

```go
backend, err := chronicle.NewS3Backend(chronicle.S3Config{
    Bucket:          "my-chronicle-bucket",
    Prefix:          "metrics/",
    Region:          "us-west-2",
    Endpoint:        "",  // Leave empty for AWS
    AccessKeyID:     "",  // Uses default credential chain if empty
    SecretAccessKey: "",
})
if err != nil {
    log.Fatal(err)
}

// Use backend for partition storage
// Local file still used for WAL and hot data
```

### S3 Configuration

```go
type S3Config struct {
    // Bucket is the S3 bucket name (required)
    Bucket string
    
    // Prefix for all keys (optional, e.g., "chronicle/prod/")
    Prefix string
    
    // Region for the bucket (required for AWS)
    Region string
    
    // Endpoint for S3-compatible services (MinIO, DigitalOcean, etc.)
    Endpoint string
    
    // Credentials (uses default chain if empty)
    AccessKeyID     string
    SecretAccessKey string
    
    // Upload settings
    PartSize    int64 // Multipart upload part size (default: 5MB)
    Concurrency int   // Upload concurrency (default: 5)
}
```

### S3-Compatible Services

#### MinIO

```go
backend, _ := chronicle.NewS3Backend(chronicle.S3Config{
    Bucket:          "chronicle",
    Region:          "us-east-1",
    Endpoint:        "http://minio:9000",
    AccessKeyID:     "minioadmin",
    SecretAccessKey: "minioadmin",
})
```

#### DigitalOcean Spaces

```go
backend, _ := chronicle.NewS3Backend(chronicle.S3Config{
    Bucket:          "my-space",
    Region:          "nyc3",
    Endpoint:        "https://nyc3.digitaloceanspaces.com",
    AccessKeyID:     os.Getenv("DO_ACCESS_KEY"),
    SecretAccessKey: os.Getenv("DO_SECRET_KEY"),
})
```

#### Google Cloud Storage (S3 Compatible)

```go
backend, _ := chronicle.NewS3Backend(chronicle.S3Config{
    Bucket:   "my-bucket",
    Region:   "auto",
    Endpoint: "https://storage.googleapis.com",
    // Uses HMAC keys for auth
})
```

### Tiered Storage

Combine local and S3 for cost-effective storage:

```go
// Hot data: local disk (fast)
// Cold data: S3 (cheap)
db, _ := chronicle.Open("data.db", chronicle.Config{
    Path:              "data.db",
    PartitionDuration: time.Hour,
    
    // Keep last 24 hours locally
    LocalRetention: 24 * time.Hour,
    
    // Move older data to S3
    ColdStorage: backend,
    ColdThreshold: 24 * time.Hour,
})
```

## Custom Backend

Implement the `StorageBackend` interface for custom storage:

```go
type StorageBackend interface {
    // Read reads data from the given key
    Read(ctx context.Context, key string) (io.ReadCloser, error)
    
    // Write writes data to the given key
    Write(ctx context.Context, key string, data io.Reader) error
    
    // Delete deletes the given key
    Delete(ctx context.Context, key string) error
    
    // List lists keys with the given prefix
    List(ctx context.Context, prefix string) ([]string, error)
}
```

### Example: Custom Backend

```go
type MyBackend struct {
    client *myclient.Client
}

func (b *MyBackend) Read(ctx context.Context, key string) (io.ReadCloser, error) {
    data, err := b.client.Get(ctx, key)
    if err != nil {
        return nil, err
    }
    return io.NopCloser(bytes.NewReader(data)), nil
}

func (b *MyBackend) Write(ctx context.Context, key string, data io.Reader) error {
    content, err := io.ReadAll(data)
    if err != nil {
        return err
    }
    return b.client.Put(ctx, key, content)
}

func (b *MyBackend) Delete(ctx context.Context, key string) error {
    return b.client.Delete(ctx, key)
}

func (b *MyBackend) List(ctx context.Context, prefix string) ([]string, error) {
    return b.client.List(ctx, prefix)
}

// Register and use
backend := &MyBackend{client: myclient.New()}
```

## Performance Considerations

### Local vs Remote

| Aspect | Local | S3 |
|--------|-------|-----|
| Latency | ~1ms | ~50-100ms |
| Throughput | Disk-limited | Network-limited |
| Concurrent access | Good | Excellent |
| Cost | Fixed | Per-request |

### Optimization Tips

1. **Use local cache for hot data**
   ```go
   config.LocalRetention = 24 * time.Hour
   ```

2. **Batch operations**
   ```go
   config.S3BatchSize = 100
   ```

3. **Enable compression**
   ```go
   config.S3Compression = true
   ```

4. **Use appropriate regions**
   - Deploy Chronicle near the S3 region
   - Use S3 Transfer Acceleration for cross-region

## Migration

### Local to S3

```go
// 1. Create S3 backend
backend, _ := chronicle.NewS3Backend(s3Config)

// 2. Export from local
localDB, _ := chronicle.Open("data.db", localConfig)
migrator := chronicle.NewMigrator(localDB, backend)

// 3. Migrate
err := migrator.Migrate(context.Background())
if err != nil {
    log.Fatal(err)
}

// 4. Verify
stats := migrator.Stats()
fmt.Printf("Migrated %d partitions, %d bytes\n", stats.Partitions, stats.Bytes)
```

### S3 to Local

```go
// Reverse: download from S3 to local
migrator := chronicle.NewMigrator(s3Backend, localBackend)
migrator.Migrate(context.Background())
```

## Monitoring

Track storage backend health:

```go
stats := backend.Stats()
fmt.Printf("Requests: %d\n", stats.Requests)
fmt.Printf("Errors: %d\n", stats.Errors)
fmt.Printf("Bytes read: %d\n", stats.BytesRead)
fmt.Printf("Bytes written: %d\n", stats.BytesWritten)
fmt.Printf("Avg latency: %v\n", stats.AvgLatency)
```
