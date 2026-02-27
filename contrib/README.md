# Community Contributions

This directory contains community-maintained integrations and extensions for Chronicle.

## Contents

| Directory | Description |
|-----------|-------------|
| `grafana/` | Grafana data source plugin and dashboards for Chronicle |
| `otel-exporter/` | OpenTelemetry exporter that ships metrics to Chronicle |

## Grafana Integration

The `grafana/` directory provides a Grafana data source plugin that allows querying and visualizing Chronicle time-series data directly in Grafana dashboards.

## OpenTelemetry Exporter

The `otel-exporter/` directory provides an OpenTelemetry Collector exporter component that forwards metrics to a Chronicle instance.

## Contributing

To add a new integration:

1. Create a new directory under `contrib/`
2. Include a `README.md` with setup and usage instructions
3. Follow the [Contributing Guide](../CONTRIBUTING.md)
