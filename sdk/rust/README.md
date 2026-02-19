# Chronicle Rust SDK

Rust FFI bindings for the [Chronicle](https://github.com/chronicle-db/chronicle) time-series database.

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
chronicle-db = "0.5.0"
```

**Requirements:** Rust 2021 edition, Chronicle shared library (`libchronicle.so` / `libchronicle.dylib`).

Build the shared library from the main repository:

```bash
cd /path/to/chronicle
make build-ffi
```

## Quick Start

```rust
use chronicle::ChronicleDB;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open or create a database
    let db = ChronicleDB::open("sensors.db", Default::default())?;

    // Write data points
    db.write("cpu_usage", 42.5, &[("host", "server1")])?;
    db.write("memory_mb", 1024.0, &[("host", "server1")])?;

    // Query data
    let results = db.query("cpu_usage", 0, 0, None)?;
    for point in &results {
        println!("{}: {} at {}", point.metric, point.value, point.timestamp);
    }

    // Close the database
    db.close()?;
    Ok(())
}
```

## API Reference

### `ChronicleDB::open(path, config) -> Result<Self>`

Opens or creates a Chronicle database.

| Config Field | Type | Description |
|-------------|------|-------------|
| `retention_hours` | `i64` | Data retention period (0 = unlimited) |
| `enable_compression` | `bool` | Enable compression (default: `true`) |
| `enable_wal` | `bool` | Enable write-ahead log (default: `true`) |

### `db.write(metric, value, tags) -> Result<()>`

Write a single data point. Tags are provided as a slice of `(&str, &str)` pairs.

### `db.query(metric, start, end, limit) -> Result<Vec<Point>>`

Query data points for a metric within a time range.

### `db.close() -> Result<()>`

Close the database and release resources.

## Documentation

- [Main Documentation](../../docs/GETTING_STARTED.md)
- [API Reference](https://pkg.go.dev/github.com/chronicle-db/chronicle)
