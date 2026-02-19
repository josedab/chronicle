# Chronicle Java SDK

Java client for the [Chronicle](https://github.com/chronicle-db/chronicle) time-series database, using JNA/FFI bindings.

## Installation

### Maven

```xml
<dependency>
    <groupId>io.chronicle</groupId>
    <artifactId>chronicle-db</artifactId>
    <version>0.5.0</version>
</dependency>
```

### Gradle

```groovy
implementation 'io.chronicle:chronicle-db:0.5.0'
```

**Requirements:** Java 11+, Chronicle shared library (`libchronicle.so` / `libchronicle.dylib`).

Build the shared library from the main repository:

```bash
cd /path/to/chronicle
make build-ffi
```

## Quick Start

```java
import io.chronicle.ChronicleDB;
import io.chronicle.ChronicleDB.Point;
import java.util.List;
import java.util.Map;

public class Example {
    public static void main(String[] args) {
        // Open or create a database
        try (ChronicleDB db = ChronicleDB.open("sensors.db")) {
            // Write data points
            db.write("cpu_usage", 42.5, Map.of("host", "server1"));
            db.write("memory_mb", 1024.0, Map.of("host", "server1"));

            // Query data
            List<Point> results = db.query("cpu_usage", 0, 0);
            for (Point point : results) {
                System.out.printf("%s: %.2f at %d%n",
                    point.metric, point.value, point.timestamp);
            }
        }
    }
}
```

## API Reference

### `ChronicleDB.open(path)`

Opens or creates a Chronicle database. Implements `AutoCloseable` for use with try-with-resources.

### `ChronicleDB.open(path, config)`

Opens with custom configuration.

| Config Field | Type | Description |
|-------------|------|-------------|
| `retention_hours` | `long` | Data retention period (0 = unlimited) |
| `enable_compression` | `boolean` | Enable compression (default: `true`) |
| `enable_wal` | `boolean` | Enable write-ahead log (default: `true`) |

### `db.write(metric, value, tags)`

Write a single data point.

### `db.query(metric, start, end)`

Query data points for a metric within a time range. Returns `List<Point>`.

### `db.close()`

Close the database and release resources. Called automatically with try-with-resources.

## Documentation

- [Main Documentation](../../docs/GETTING_STARTED.md)
- [API Reference](https://pkg.go.dev/github.com/chronicle-db/chronicle)
