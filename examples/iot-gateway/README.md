# IoT Edge Gateway Example

A complete IoT edge gateway demonstrating Chronicle in a real-world scenario.

## What It Does

- **Simulates 10 sensors** reporting temperature, humidity, pressure, and battery voltage
- **Stores locally** in a single Chronicle database file
- **HTTP API** for querying sensor data
- **Health checks** for Kubernetes readiness/liveness probes
- **Automatic retention** (24 hours) and memory management (64MB budget)

## Running

```bash
cd examples/iot-gateway
go run main.go
```

## Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET :8080/health` | Health check (Chronicle built-in) |
| `GET :8080/health/ready` | Readiness probe |
| `GET :8080/health/live` | Liveness probe |
| `GET :8080/metrics` | List all metric names |
| `GET :8081/api/v1/gateway/sensors` | Sensor fleet status |
| `GET :8081/api/v1/gateway/latest?metric=temperature` | Latest readings |

## Configuration for Edge Devices

The example uses edge-optimized settings:

```go
cfg.Storage.MaxMemory = 64 * 1024 * 1024       // 64MB RAM budget
cfg.Storage.BufferSize = 1000                    // Small buffer
cfg.Storage.PartitionDuration = 30 * time.Minute // Short partitions
cfg.Retention.RetentionDuration = 24 * time.Hour // 24h retention
```

See [Edge Deployment Cookbook](../../docs/EDGE_DEPLOYMENT.md) for device-specific tuning.
