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

**Requirements:** Go 1.24 or later — Chronicle uses recent Go features (range-over-func, enhanced generics). If Go 1.24 is not yet packaged for your OS, install from [go.dev/dl](https://go.dev/dl/) or use `go install golang.org/dl/go1.24@latest`.

## Getting Started

New to Chronicle? Start with the **[Getting Started Guide](docs/GETTING_STARTED.md)** for a 10-minute walkthrough.

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
        Storage: chronicle.StorageConfig{
            MaxMemory:         64 * 1024 * 1024,
            PartitionDuration: time.Hour,
            BufferSize:        10_000,
        },
        Retention: chronicle.RetentionConfig{
            RetentionDuration: 7 * 24 * time.Hour,
        },
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

## Core API

These are the essential types and functions for most use cases:

| Symbol | Purpose |
|--------|---------|
| `Open(path, config)` | Create or open a database |
| `DB.Write(point)` | Write a single point |
| `DB.WriteBatch(points)` | Write multiple points |
| `DB.Execute(query)` | Run a query and get results |
| `DB.Close()` | Close the database |
| `Point` | A metric name, float64 value, nanosecond timestamp, and tags |
| `Query` | Metric, time range, tag filters, aggregation |
| `Config` / `DefaultConfig()` | Database configuration with sensible defaults |
| `ConfigBuilder` | Fluent API: `NewConfigBuilder(path).WithRetention(...).Build()` |
| `Result` | Query output containing matched `Points` |

For the full API surface, see the [Go reference](https://pkg.go.dev/github.com/chronicle-db/chronicle) or [`api_stability.go`](api_stability.go).

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

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  API Layer                                                   │
│  /write  /query  /api/v1/query  /v1/metrics  /stream         │
├──────────────────────────────────────────────────────────────┤
│  Core Engine                                                 │
│  Write Buffer → Schema Validation → Stream Hub               │
│  Query Engine (SQL + PromQL) → Alert Manager                 │
├──────────────────────────────────────────────────────────────┤
│  Partition Manager                                           │
│  [Partition 0: t0-t1] [Partition 1: t1-t2] ... [Active]     │
├──────────────────────────────────────────────────────────────┤
│  Storage Backend (pluggable)                                 │
│  FileBackend │ MemoryBackend │ S3Backend │ TieredBackend     │
├──────────────────────────────────────────────────────────────┤
│  Background: WAL Recovery │ Retention │ Downsampling │ Compaction │
└──────────────────────────────────────────────────────────────┘
```

For a detailed walkthrough, see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## API Maturity

Chronicle is pre-1.0. Not all features have the same stability level.

| Tier | Meaning | Examples |
|------|---------|----------|
| **Stable** | Covered by semver. Safe for production. | `DB`, `Point`, `Query`, `Open()`, `Write()`, `Execute()`, `Config` |
| **Beta** | May change between minor versions. | PromQL, HTTP API, Grafana plugin, replication |
| **Experimental** | May change or be removed without notice. | Jupyter kernel, WASM runtime, TinyML, ZK proofs |

See [`api_stability.go`](api_stability.go) for the full classification of every exported symbol.

## Configuration

Legacy flat fields (for example, `MaxMemory`) remain supported, but new grouped config is preferred.

```go
cfg := chronicle.Config{
    Path: "data.db", // Database file path
    Storage: chronicle.StorageConfig{
        MaxMemory:         64 * 1024 * 1024, // Max memory for buffers (64MB)
        MaxStorageBytes:   0,                // Max storage size (0 = unlimited)
        PartitionDuration: time.Hour,        // Time span per partition
        BufferSize:        10_000,           // Write buffer size
    },
    Retention: chronicle.RetentionConfig{
        RetentionDuration: 7 * 24 * time.Hour, // Data retention period
    },
    Query: chronicle.QueryConfig{
        QueryTimeout: 30 * time.Second,
    },
    HTTP: chronicle.HTTPConfig{
        HTTPEnabled: false, // Enable HTTP API
        HTTPPort:    8086,  // HTTP port
    },
    StrictSchema: false, // Enforce schema validation
    Encryption: &chronicle.EncryptionConfig{
        Enabled:     false, // Enable encryption at rest
        KeyPassword: "",    // Password for key derivation
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
├── internal/              # Private implementation details
│   ├── adminui/           # Admin UI dashboard components
│   ├── anomaly/           # Anomaly detection and classification
│   ├── bits/              # Bit-level I/O utilities
│   ├── cep/               # Complex event processing engine
│   ├── chprotocol/        # ClickHouse-compatible protocol
│   ├── cluster/           # Cluster coordination and write routing
│   ├── continuousquery/   # Continuous query execution engine
│   ├── cql/               # CQL query engine and parser
│   ├── digitaltwin/       # Digital twin synchronization
│   ├── edgemesh/          # Edge mesh networking (CRDT, mDNS)
│   ├── encoding/          # Compression codecs (Gorilla, Delta, Dictionary)
│   ├── gpucompression/    # GPU-accelerated compression
│   ├── hardwareaccel/     # Hardware acceleration (SIMD, FPGA)
│   ├── oteldistro/        # OpenTelemetry distribution helpers
│   ├── pluginmkt/         # Plugin marketplace and registry
│   ├── query/             # Query parsing and aggregation
│   └── raft/              # Raft consensus implementation
├── docs/                  # Documentation
│   ├── ARCHITECTURE.md    # System design overview
│   ├── BENCHMARKS.md      # Performance benchmarks
│   └── adr/               # Architecture Decision Records
├── examples/              # Example applications
│   ├── simple/            # Basic write and query
│   ├── http-server/       # Full HTTP API server
│   ├── iot-collector/     # IoT sensor data collection
│   ├── prometheus-compatible/ # Prometheus backend
│   └── analytics-dashboard/   # Forecasting and alerting
├── .github/               # GitHub configuration
│   └── workflows/         # CI/CD workflows
├── *.go                   # Main package source files
├── *_test.go              # Test files
└── doc.go                 # Package documentation
```

The main `chronicle` package provides the public API. Internal packages under `internal/`
contain implementation details that should not be imported directly.

## Documentation

- **[Getting Started](docs/GETTING_STARTED.md)** — 10-minute tutorial from install to query
- [API Documentation](https://pkg.go.dev/github.com/chronicle-db/chronicle)
- [HTTP API Reference](docs/API.md)
- [Features Guide](docs/FEATURES.md)
- [Configuration Reference](docs/CONFIGURATION.md)
- [Plugin Development](docs/PLUGIN_DEVELOPMENT.md)
- [Testing Guide](docs/TESTING.md)
- [Architecture Overview](docs/ARCHITECTURE.md)
- [Benchmarks](docs/BENCHMARKS.md)

## Development

```bash
# Install development tools
make setup

# Run all checks
make all

# Run tests with race detector
make test

# Run short tests only (fast iteration)
make test-short

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
