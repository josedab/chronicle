# Chronicle CLI Reference

The `chronicle` command-line tool provides utilities for managing Chronicle time-series databases.

## Synopsis

```
chronicle <command> [options]
```

Run `chronicle <command> -h` for command-specific help.

---

## Commands

### `serve`

Start a Chronicle HTTP server.

```bash
chronicle serve [options]
```

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `-db` | string | `chronicle.db` | Database file path |
| `-port` | int | `8086` | HTTP listen port |
| `-retention` | duration | `0` (unlimited) | Data retention duration |
| `-max-memory` | int64 | `67108864` (64 MB) | Max memory budget in bytes |
| `-partition` | duration | `1h` | Partition duration |

**Example:**

```bash
chronicle serve -db /var/lib/chronicle/data.db -port 9090 -retention 720h
```

The server exposes endpoints for writes (`/write`), queries (`/query`), Prometheus remote write (`/api/v1/prom/write`), and health checks (`/health`). Press Ctrl-C for graceful shutdown.

---

### `query`

Execute a CQL/SQL query against a database (directly or via HTTP).

```bash
chronicle query [options] "QUERY"
```

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `-db` | string | *(none)* | Database file path (direct mode) |
| `-server` | string | *(none)* | HTTP server URL (remote mode) |
| `-format` | string | `table` | Output format: `table`, `json`, `csv` |

Exactly one of `-db` or `-server` must be provided.

**Examples:**

```bash
# Direct query against a local database
chronicle query -db data.db "SELECT * FROM temperature WHERE time > now() - 1h"

# Remote query against a running server
chronicle query -server http://localhost:8086 -format json "SELECT avg(value) FROM cpu"
```

---

### `write`

Write a single data point to a database.

```bash
chronicle write [options]
```

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `-db` | string | `chronicle.db` | Database file path |
| `-metric` | string | *(required)* | Metric name |
| `-value` | float64 | `0` | Metric value |
| `-tags` | string | *(none)* | Comma-separated tags (`key=value,key2=value2`) |

**Example:**

```bash
chronicle write -db sensors.db -metric temperature -value 22.5 -tags "room=living,floor=1"
```

---

### `import`

Import data from CSV or JSON Lines files.

```bash
chronicle import [options] <file>
```

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `-db` | string | `chronicle.db` | Database file path |
| `-format` | string | `csv` | Input format: `csv`, `jsonl` |
| `-metric` | string | *(none)* | Default metric name (if not in data) |

**Examples:**

```bash
# Import from CSV
chronicle import -db data.db -format csv -metric cpu_usage measurements.csv

# Import from JSON Lines
chronicle import -db data.db -format jsonl data.jsonl
```

**CSV format:** The first row must be a header. Expected columns: `timestamp` (RFC 3339 or Unix nanos), `metric`, `value`, and any additional columns are treated as tags.

---

### `export`

Export data to CSV, JSON, or JSON Lines.

```bash
chronicle export [options]
```

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `-db` | string | `chronicle.db` | Database file path |
| `-format` | string | `csv` | Output format: `csv`, `json`, `jsonl` |
| `-metric` | string | *(required)* | Metric to export |
| `-start` | string | *(none)* | Start time (RFC 3339) |
| `-end` | string | *(none)* | End time (RFC 3339) |
| `-output` | string | `-` (stdout) | Output file path |

**Examples:**

```bash
# Export to CSV file
chronicle export -db data.db -metric temperature -output temps.csv

# Export a time range as JSON to stdout
chronicle export -db data.db -format json -metric cpu \
  -start 2025-01-01T00:00:00Z -end 2025-01-02T00:00:00Z
```

---

### `inspect`

Show database statistics.

```bash
chronicle inspect [options] [db-path]
```

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `-db` | string | `chronicle.db` | Database file path |

The database path can also be passed as a positional argument.

**Example:**

```bash
chronicle inspect data.db
```

Displays metrics count, total points, partition count, and database file size.

---

### `init`

Initialize a new database with default configuration.

```bash
chronicle init [options]
```

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `-db` | string | `chronicle.db` | Database file path |
| `-retention` | duration | `168h` (7 days) | Retention period |
| `-max-memory` | int64 | `67108864` (64 MB) | Max memory budget in bytes |

**Example:**

```bash
chronicle init -db /var/lib/chronicle/data.db -retention 720h
```

---

### `compact`

Compact and optimize a database by merging partitions and removing expired data.

```bash
chronicle compact [options]
```

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `-db` | string | `chronicle.db` | Database file path |

**Example:**

```bash
chronicle compact -db data.db
```

---

### `watch`

Stream live data from a metric, printing new values at a regular interval.

```bash
chronicle watch [options]
```

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `-db` | string | `chronicle.db` | Database file path |
| `-metric` | string | *(required)* | Metric to watch |
| `-interval` | duration | `2s` | Poll interval |

**Example:**

```bash
chronicle watch -db sensors.db -metric temperature -interval 5s
```

Press Ctrl-C to stop.

---

### `doctor`

Run health checks on a database.

```bash
chronicle doctor [options]
```

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `-db` | string | `chronicle.db` | Database file path |

**Example:**

```bash
chronicle doctor -db data.db
```

Reports WAL integrity, partition health, storage usage, and other diagnostics.

---

### `benchmark`

Run performance benchmarks (writes and queries).

```bash
chronicle benchmark [options]
```

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `-db` | string | *(temp file)* | Database file path |
| `-points` | int | `10000` | Number of points to benchmark |

**Example:**

```bash
chronicle benchmark -points 100000
```

---

### `diff`

Compare data across two time ranges for the same metric.

```bash
chronicle diff [options]
```

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `-db` | string | `chronicle.db` | Database file path |
| `-metric` | string | *(required)* | Metric to compare |
| `-from-a` | string | *(required)* | Range A start (RFC 3339) |
| `-to-a` | string | *(required)* | Range A end (RFC 3339) |
| `-from-b` | string | *(required)* | Range B start (RFC 3339) |
| `-to-b` | string | *(required)* | Range B end (RFC 3339) |

**Example:**

```bash
chronicle diff -db data.db -metric cpu \
  -from-a 2025-01-01T00:00:00Z -to-a 2025-01-02T00:00:00Z \
  -from-b 2025-01-08T00:00:00Z -to-b 2025-01-09T00:00:00Z
```

---

### `completion`

Generate shell completion scripts.

```bash
chronicle completion [options]
```

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `-shell` | string | *(required)* | Shell type: `bash`, `zsh`, `fish` |

**Usage:**

```bash
# Bash
eval "$(chronicle completion -shell bash)"

# Zsh
chronicle completion -shell zsh > "${fpath[1]}/_chronicle"

# Fish
chronicle completion -shell fish | source
```

---

### `version`

Print version information.

```bash
chronicle version
```

---

### `help`

Show usage information.

```bash
chronicle help
```

---

## See Also

- [README](../README.md) — Quick start guide
- [CONFIGURATION](./CONFIGURATION.md) — Configuration reference
- [API](./API.md) — HTTP API documentation
