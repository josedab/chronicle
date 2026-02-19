# Chronicle Python SDK

Python client for the [Chronicle](https://github.com/chronicle-db/chronicle) time-series database, using ctypes FFI bindings.

## Installation

```bash
pip install chronicle-db
```

**Requirements:** Python 3.8+, Chronicle shared library (`libchronicle.so` / `libchronicle.dylib`).

Build the shared library from the main repository:

```bash
cd /path/to/chronicle
make build-ffi
```

## Quick Start

```python
from chronicle import ChronicleDB

# Open or create a database
db = ChronicleDB("sensors.db")

# Write data points
db.write("cpu_usage", 42.5, tags={"host": "server1"})
db.write("memory_mb", 1024.0, tags={"host": "server1"})

# Query data
results = db.query("cpu_usage", start=0, end=0)
for point in results:
    print(f"{point.metric}: {point.value} at {point.timestamp}")

# Close the database
db.close()
```

### Pandas Integration

```python
from chronicle import ChronicleDB

db = ChronicleDB("sensors.db")

# Export query results as a DataFrame
df = db.query_dataframe("cpu_usage", start=0, end=0)
print(df.describe())

db.close()
```

Requires the `pandas` extra: `pip install chronicle-db[pandas]`

## API Reference

### `ChronicleDB(path, **config)`

Opens or creates a Chronicle database.

| Parameter | Type | Description |
|-----------|------|-------------|
| `path` | `str` | Path to the database file |
| `retention_hours` | `int` | Data retention period (0 = unlimited) |
| `enable_compression` | `bool` | Enable compression (default: `True`) |
| `enable_wal` | `bool` | Enable write-ahead log (default: `True`) |

### `db.write(metric, value, tags=None, timestamp=None)`

Write a single data point.

### `db.query(metric, start=0, end=0, tags=None)`

Query data points for a metric within a time range. Returns a list of `Point` objects.

### `db.close()`

Close the database and release resources.

## Documentation

- [Main Documentation](../../docs/GETTING_STARTED.md)
- [Python Client Guide](../../docs/PYTHON_CLIENT.md)
- [API Reference](https://pkg.go.dev/github.com/chronicle-db/chronicle)
