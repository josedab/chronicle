---
sidebar_position: 1
slug: /api-reference
---

# API Reference

Complete reference for the Chronicle Go API.

## Quick Links

| Section | Description |
|---------|-------------|
| [Configuration](/docs/api-reference/configuration) | All configuration options |
| [DB](/docs/api-reference/db) | Database operations |
| [Query](/docs/api-reference/query) | Query types and execution |
| [HTTP Endpoints](/docs/api-reference/http-endpoints) | REST API reference |

## Import

```go
import "github.com/chronicle-db/chronicle"
```

## Core Types

### Point

```go
type Point struct {
    Metric    string            // Metric name
    Tags      map[string]string // Dimensional labels
    Value     float64           // Numeric value
    Timestamp int64             // Unix nanoseconds
}
```

### Series

```go
type Series struct {
    ID     uint64            // Unique series identifier
    Metric string            // Metric name
    Tags   map[string]string // Tag set
}
```

### Result

```go
type Result struct {
    Points []Point // Query results
}
```

## Error Handling

Chronicle returns standard Go errors. Common error patterns:

```go
db, err := chronicle.Open("data.db", config)
if err != nil {
    // Handle: path issues, WAL corruption, disk full
}

err = db.Write(point)
if err != nil {
    // Handle: schema validation, cardinality limit, disk full
}

result, err := db.Execute(query)
if err != nil {
    // Handle: query timeout, memory limit, invalid query
}
```

## Thread Safety

All Chronicle operations are thread-safe:
- Multiple goroutines can call `Write()` concurrently
- Multiple goroutines can call `Execute()` concurrently
- `Close()` waits for in-flight operations

## Memory Management

Chronicle manages its own memory. Key configuration:

```go
config := chronicle.Config{
    MaxMemory:  256 * 1024 * 1024, // Total memory budget
    BufferSize: 50_000,            // Write buffer size
}
```

## Versions

Chronicle follows semantic versioning. The current API is v1.
