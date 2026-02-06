# Prometheus-Compatible Example

Uses Chronicle as a Prometheus-compatible backend with PromQL queries.

## Run

```bash
go run main.go
```

Then configure Prometheus to `remote_write` to `http://localhost:8086/prometheus/write`.

## Features Demonstrated

- Prometheus remote write ingestion
- PromQL-style queries (rate, sum, etc.)
- Histogram metrics
- Prometheus-format metric export
