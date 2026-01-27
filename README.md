# Chronicle

[![Go Reference](https://pkg.go.dev/badge/github.com/chronicle-db/chronicle.svg)](https://pkg.go.dev/github.com/chronicle-db/chronicle)
[![Go Report Card](https://goreportcard.com/badge/github.com/chronicle-db/chronicle)](https://goreportcard.com/report/github.com/chronicle-db/chronicle)
[![codecov](https://codecov.io/gh/chronicle-db/chronicle/branch/main/graph/badge.svg)](https://codecov.io/gh/chronicle-db/chronicle)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![CI](https://github.com/chronicle-db/chronicle/actions/workflows/ci.yml/badge.svg)](https://github.com/chronicle-db/chronicle/actions/workflows/ci.yml)

Chronicle is an embedded time-series database for Go designed for constrained and edge environments. It provides compressed columnar storage, SQL-like queries, retention, and downsampling in a single-file format.

## Installation

```bash
go get github.com/chronicle-db/chronicle
```

**Requirements:** Go 1.23 or later

## Quick Start

```go
package main

import (
    "log"
    "time"

    "github.com/chronicle-db/chronicle"
)

func main() {
    db, err := chronicle.Open("sensors.db", chronicle.Config{
        MaxMemory:         64 * 1024 * 1024,
        PartitionDuration: time.Hour,
        RetentionDuration: 7 * 24 * time.Hour,
        BufferSize:        10_000,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    _ = db.Write(chronicle.Point{
        Metric:    "temperature",
        Tags:      map[string]string{"room": "living"},
        Value:     22.5,
        Timestamp: time.Now().UnixNano(),
    })

    result, err := db.Execute(&chronicle.Query{
        Metric: "temperature",
        Tags:   map[string]string{"room": "living"},
        Start:  time.Now().Add(-time.Hour).UnixNano(),
        End:    time.Now().UnixNano(),
        Aggregation: &chronicle.Aggregation{
            Function: chronicle.AggMean,
            Window:   5 * time.Minute,
        },
    })
    if err != nil {
        log.Fatal(err)
    }

    _ = result
}
```

## Features

### Core Storage
- **Single-file storage** with append-only partitions
- **Gorilla float compression** and delta timestamp encoding
- **Dictionary tag compression** and per-column encoding selection
- **WAL-based crash recovery** with rotation
- **Pluggable storage backends** (file, memory, S3, tiered)

### Query & Analytics
- **SQL-like query parser** (limited subset)
- **PromQL subset support** for Prometheus compatibility
- **GraphQL API** with interactive playground
- **Time-series forecasting** (Holt-Winters, exponential smoothing, anomaly detection)
- **Recording rules** for pre-computed queries
- **Native histograms** with exponential bucketing
- **Retention policies** - time-based and size-based limits
- **Downsampling** background workers
- **Continuous queries** (materialized views)
- **Query assistant** - natural language to SQL/PromQL

### Integrations
- **HTTP API** with Influx line protocol, Prometheus remote write
- **OpenTelemetry receiver** for OTLP metric ingestion
- **Grafana data source plugin** for visualization
- **WebAssembly compilation** for browser/edge runtime
- **Admin UI dashboard** for monitoring and exploration
- **Query federation** across multiple instances
- **Data export** to CSV, JSON, Parquet formats

### Enterprise Features
- **Encryption at rest** with AES-256-GCM
- **Schema registry** for metric validation
- **Multi-tenancy** with namespace isolation
- **Alerting engine** with webhook notifications
- **Streaming API** for real-time subscriptions
- **Exemplar support** for trace correlation
- **Cardinality management** with limits and alerts
- **Delta/incremental backups** with retention
- **Outbound replication** to a central endpoint
- **Vector embeddings** for ML/semantic search
- **Continuous profiling** with metric correlation

## Configuration

```go
cfg := chronicle.Config{
    Path:              "data.db",        // Database file path
    MaxMemory:         64 * 1024 * 1024, // Max memory for buffers (64MB)
    MaxStorageBytes:   0,                // Max storage size (0 = unlimited)
    PartitionDuration: time.Hour,        // Time span per partition
    RetentionDuration: 7 * 24 * time.Hour, // Data retention period
    BufferSize:        10_000,           // Write buffer size
    QueryTimeout:      30 * time.Second, // Query timeout
    HTTPEnabled:       false,            // Enable HTTP API
    HTTPPort:          8086,             // HTTP port
    StrictSchema:      false,            // Enforce schema validation
    Encryption: &chronicle.EncryptionConfig{
        Enabled:     false,              // Enable encryption at rest
        KeyPassword: "",                 // Password for key derivation
    },
}
```

## HTTP API

When `HTTPEnabled: true`, the following endpoints are available:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/write` | POST | Write points (Influx line protocol) |
| `/query` | POST | Execute SQL-like query |
| `/metrics` | GET | List all metrics |
| `/api/v1/query` | GET/POST | Prometheus instant query |
| `/api/v1/query_range` | GET/POST | Prometheus range query |
| `/api/v1/forecast` | POST | Time-series forecasting |
| `/api/v1/histogram` | GET/POST | Native histogram operations |
| `/v1/metrics` | POST | OpenTelemetry OTLP JSON ingestion |
| `/schemas` | GET/POST/DELETE | Schema registry CRUD |
| `/api/v1/alerts` | GET | Get active alerts |
| `/api/v1/rules` | GET/POST | Alerting rules management |
| `/stream` | GET | WebSocket streaming subscription |
| `/graphql` | POST | GraphQL API |
| `/graphql/playground` | GET | Interactive GraphQL playground |
| `/admin` | GET | Admin UI dashboard |
| `/api/v1/prom/write` | POST | Prometheus remote write |

## Project Structure

```
chronicle/
├── internal/           # Private implementation details
│   ├── bits/           # Bit-level I/O utilities
│   ├── encoding/       # Compression codecs (Gorilla, Delta, Dictionary)
│   └── query/          # Query parsing and aggregation
├── docs/             # Documentation
│   ├── ARCHITECTURE.md # System design overview
│   └── BENCHMARKS.md   # Performance benchmarks
├── examples/           # Example applications
│   └── simple/         # Simple usage example
├── .github/            # GitHub configuration
│   └── workflows/      # CI/CD workflows
├── *.go                # Main package source files
├── *_test.go           # Test files
└── doc.go              # Package documentation
```

The main `chronicle` package provides the public API. Internal packages under `internal/`
contain implementation details that should not be imported directly.

## Documentation

- [API Documentation](https://pkg.go.dev/github.com/chronicle-db/chronicle)
- [HTTP API Reference](docs/API.md)
- [Features Guide](docs/FEATURES.md)
- [Configuration Reference](docs/CONFIGURATION.md)
- [Architecture Overview](docs/ARCHITECTURE.md)
- [Benchmarks](docs/BENCHMARKS.md)

## Development

```bash
# Run all checks
make all

# Run tests with race detector
make test

# Run benchmarks
make bench

# Run linters
make lint

# See all commands
make help
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Security

For reporting security vulnerabilities, see [SECURITY.md](SECURITY.md). Chronicle includes:

- AES-256-GCM encryption at rest (opt-in)
- Request body limits and rate limiting on HTTP endpoints
- Query timeout limits to prevent DoS
- No `unsafe` package usage in core code
- Path traversal protection in storage backends

## License

Apache 2.0 - see [LICENSE](LICENSE) for details.
