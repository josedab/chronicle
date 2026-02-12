# Chronicle OTel Collector Exporter

A standalone OpenTelemetry Collector exporter that sends metrics to a Chronicle time-series database.

## Overview

This exporter receives metrics from the OpenTelemetry Collector pipeline and writes them to Chronicle via its HTTP API. It supports:

- All OTLP metric types (gauge, sum, histogram, summary)
- Configurable batching and retry
- Resource and scope attributes as tags
- TLS and authentication

## Configuration

```yaml
exporters:
  chronicle:
    endpoint: "http://localhost:8086"
    batch_size: 1000
    flush_interval: 10s
    timeout: 30s
    retry:
      enabled: true
      max_attempts: 3
      initial_interval: 1s
    headers:
      Authorization: "Bearer your-api-key"
```

## Usage with OTel Collector

1. Build a custom collector with this exporter:

```go
package main

import (
    "go.opentelemetry.io/collector/component"
    "go.opentelemetry.io/collector/otelcol"
    chronicleexporter "github.com/chronicle-db/chronicle/contrib/otel-exporter"
)

func main() {
    factories, _ := otelcol.Factories(
        otelcol.WithExporters(chronicleexporter.NewFactory()),
    )
    // ... start collector with factories
}
```

2. Or generate a collector config with Chronicle:

```go
import "github.com/chronicle-db/chronicle"

config := chronicle.DefaultOTelCollectorConfig()
config.Endpoint = "http://chronicle:8086"
yaml := chronicle.GenerateOTelCollectorYAML(config)
```

## Development

```bash
go test ./...
```

## License

Apache 2.0 - see [LICENSE](../../LICENSE) for details.
