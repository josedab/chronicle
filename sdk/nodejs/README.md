# Chronicle Node.js SDK

Node.js client for the [Chronicle](https://github.com/chronicle-db/chronicle) time-series database, using FFI bindings via `ffi-napi` or CLI fallback.

## Installation

```bash
npm install chronicle-db
```

**Requirements:** Node.js 14+, Chronicle shared library (`libchronicle.so` / `libchronicle.dylib`) or Chronicle CLI in PATH.

Build the shared library from the main repository:

```bash
cd /path/to/chronicle
make build-ffi
```

## Quick Start

```javascript
const { ChronicleDB } = require('chronicle-db');

async function main() {
  // Open or create a database
  const db = new ChronicleDB('sensors.db');

  // Write data points
  await db.write('cpu_usage', 42.5, { host: 'server1' });
  await db.write('memory_mb', 1024.0, { host: 'server1' });

  // Query data
  const results = await db.query('cpu_usage');
  results.forEach(point => {
    console.log(`${point.metric}: ${point.value} at ${point.timestamp}`);
  });

  // Close the database
  db.close();
}

main().catch(console.error);
```

## API Reference

### `new ChronicleDB(path, options?)`

Opens or creates a Chronicle database.

| Parameter | Type | Description |
|-----------|------|-------------|
| `path` | `string` | Path to the database file |
| `options.retentionHours` | `number` | Data retention period (0 = unlimited) |
| `options.enableCompression` | `boolean` | Enable compression (default: `true`) |
| `options.enableWal` | `boolean` | Enable write-ahead log (default: `true`) |

### `db.write(metric, value, tags?, timestamp?)`

Write a single data point. Returns a `Promise`.

### `db.query(metric, options?)`

Query data points. Options: `start`, `end`, `tags`, `limit`. Returns a `Promise<Point[]>`.

### `db.close()`

Close the database and release resources.

## Documentation

- [Main Documentation](../../docs/GETTING_STARTED.md)
- [API Reference](https://pkg.go.dev/github.com/chronicle-db/chronicle)
