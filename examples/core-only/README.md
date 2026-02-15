# Core-Only Example

The simplest possible Chronicle program: open a database, write points, query them, and close.

No HTTP server, no advanced config, no optional features.

## Run

```bash
cd examples/core-only
go run .
```

## What It Demonstrates

| API | Purpose |
|-----|---------|
| `chronicle.Open()` | Create or open a database |
| `chronicle.DefaultConfig()` | Sensible default configuration |
| `db.Write()` | Write a single data point |
| `db.Flush()` | Flush buffer so queries can find data |
| `db.Execute()` | Run a query |
| `db.Metrics()` | List known metric names |
| `db.Close()` | Close the database |

## Next Steps

- [Getting Started Guide](../../docs/GETTING_STARTED.md) — Full 10-minute tutorial
- [Core API Reference](../../docs/CORE_API.md) — All essential types and functions
- [`examples/http-server/`](../http-server/) — Serve data over HTTP
- [`examples/iot-collector/`](../iot-collector/) — IoT sensor data collection
