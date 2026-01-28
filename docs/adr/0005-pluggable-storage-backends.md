# ADR-0005: Pluggable Storage Backend Abstraction

## Status

Accepted

## Context

Chronicle targets diverse deployment environments:

1. **Edge devices**: Local filesystem storage, limited connectivity
2. **Cloud environments**: Object storage (S3, GCS) for durability and scalability
3. **Development/testing**: In-memory storage for fast tests
4. **Hybrid deployments**: Local storage with cloud backup/overflow

A hardcoded storage implementation would limit Chronicle's applicability. We needed an abstraction that:

- Supports multiple storage technologies without code changes
- Maintains consistent behavior across backends
- Enables new backends without modifying core logic
- Allows composition (e.g., tiered storage combining local + remote)

## Decision

Chronicle defines a **`StorageBackend` interface** that abstracts all storage operations:

```go
type StorageBackend interface {
    // Read retrieves data from the given path
    Read(ctx context.Context, path string) ([]byte, error)
    
    // Write stores data at the given path
    Write(ctx context.Context, path string, data []byte) error
    
    // Delete removes data at the given path
    Delete(ctx context.Context, path string) error
    
    // List returns all paths with the given prefix
    List(ctx context.Context, prefix string) ([]string, error)
    
    // Exists checks if a path exists
    Exists(ctx context.Context, path string) (bool, error)
}
```

### Provided Implementations

| Backend | Use Case | Characteristics |
|---------|----------|-----------------|
| **FileBackend** | Production edge deployment | Local filesystem, fastest for local access |
| **MemoryBackend** | Testing, ephemeral workloads | In-memory map, no persistence |
| **S3Backend** | Cloud deployment, archival | AWS S3, high durability, higher latency |
| **TieredBackend** | Hybrid deployment | Combines local (hot) + remote (cold) storage |

### Tiered Storage

The `TieredBackend` composes multiple backends with policies:

- **Hot tier**: Fast local storage for recent data
- **Cold tier**: Remote storage for older data
- **Promotion/demotion**: Automatic data movement based on age or access patterns

## Consequences

### Positive

- **Deployment flexibility**: Same Chronicle binary runs on edge or cloud
- **Testing simplicity**: MemoryBackend enables fast, isolated tests
- **Future extensibility**: New backends (Azure Blob, GCS, MinIO) easily added
- **Operational patterns**: Tiered storage enables cost optimization

### Negative

- **Abstraction overhead**: Interface calls add indirection vs direct file operations
- **Lowest common denominator**: Interface limited to operations all backends support
- **Consistency variations**: Different backends have different consistency guarantees
- **Error handling complexity**: Backend-specific errors must be normalized

### Interface Design Trade-offs

**Included in interface:**
- Basic CRUD operations (Read, Write, Delete)
- Listing with prefix (enables partition enumeration)
- Existence check (avoids read for existence queries)

**Excluded from interface:**
- Atomic rename (not supported by all object stores)
- Append operations (object stores are immutable)
- Directory operations (object stores have flat namespace)

### Backend-Specific Considerations

**FileBackend**:
- Uses atomic write-to-temp-then-rename for crash safety
- Supports concurrent reads
- Directory creation handled automatically

**S3Backend**:
- Requires AWS credentials configuration
- Higher latency, suitable for cold data
- Strongly consistent for read-after-write (since 2020)
- Multipart upload for large objects

**MemoryBackend**:
- Data lost on process exit
- Useful for benchmarking without I/O variance
- Concurrent access protected by mutex

### Enabled

- Multi-cloud deployment strategies
- Cost optimization via storage tiering
- Disaster recovery with remote replication
- Simple testing without filesystem setup

### Prevented

- Backend-specific optimizations (e.g., S3 multipart streaming)
- Transactions across multiple keys
- Backend-specific features (S3 versioning, lifecycle policies managed externally)
