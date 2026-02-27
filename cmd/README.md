# Chronicle CLI Commands

This directory contains the executable entry points for Chronicle.

## Commands

### `chronicle`

The main Chronicle server and CLI tool.

```bash
go build -o chronicle ./cmd/chronicle
```

**Subcommands:**

| Command | Description |
|---------|-------------|
| `serve` | Start the Chronicle HTTP server |
| `query` | Execute a CQL/SQL query against a database |
| `import` | Import data from CSV or other formats |
| `export` | Export data to Parquet, CSV, or JSON |
| `inspect` | Inspect a Chronicle database file |
| `version` | Print the version string |

**Examples:**

```bash
# Start the server with a config file
chronicle serve -config config.yaml

# Run an ad-hoc query
chronicle query "SELECT mean(value) FROM cpu WHERE host='server01' GROUP BY time(5m)"

# Import CSV data
chronicle import -format csv data.csv

# Export data to Parquet
chronicle export -format parquet -metric cpu -output cpu_data.parquet
```

### `chronicle-agent`

A zero-code observability agent that collects system metrics (CPU, memory, disk, network) using eBPF with `/proc` fallback and ships them to a Chronicle instance.

```bash
go build -o chronicle-agent ./cmd/chronicle-agent
```

**Usage:**

```bash
# Send metrics to a remote Chronicle server
chronicle-agent --target http://localhost:8428 --interval 10s

# Use eBPF HTTP tracing with container enrichment
chronicle-agent --target /path/to/db --ebpf-http --container-enrichment
```

## Building

Build all commands from the repository root:

```bash
make build
# or individually:
go build -o bin/chronicle ./cmd/chronicle
go build -o bin/chronicle-agent ./cmd/chronicle-agent
```
