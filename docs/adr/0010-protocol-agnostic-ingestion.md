# ADR-0010: Protocol-Agnostic Ingestion Layer

## Status

Accepted

## Context

Monitoring ecosystems are heterogeneous. Organizations use multiple tools and protocols:

1. **Prometheus ecosystem**: Remote write protocol, PromQL queries
2. **InfluxDB ecosystem**: Line protocol, Flux queries
3. **OpenTelemetry**: OTLP protocol, vendor-neutral standard
4. **Custom applications**: JSON APIs, various formats

Requiring clients to adopt a Chronicle-specific protocol creates friction:

- Existing exporters and agents must be modified
- Teams must learn new tooling
- Migration from other systems becomes a project

## Decision

Chronicle implements a **protocol-agnostic ingestion layer** supporting multiple formats:

### Supported Protocols

| Protocol | Endpoint | Use Case |
|----------|----------|----------|
| JSON | `POST /api/v1/write` | Custom applications, simple integration |
| Line Protocol | `POST /api/v1/write/line` | InfluxDB compatibility |
| Prometheus Remote Write | `POST /api/v1/prom/write` | Prometheus/Grafana Agent |
| OTLP | `POST /v1/metrics` | OpenTelemetry collectors |

### JSON Format

```json
{
  "points": [
    {
      "metric": "cpu_usage",
      "tags": {"host": "server1"},
      "value": 0.85,
      "timestamp": 1704067200000000000
    }
  ]
}
```

### Line Protocol

```
cpu_usage,host=server1 value=0.85 1704067200000000000
```

### Prometheus Remote Write

Standard protobuf format as defined by Prometheus remote write specification.

### OTLP

OpenTelemetry Protocol for metrics, supporting:
- Gauge, Sum, Histogram, Summary metric types
- Resource and scope attributes flattened to tags
- Both JSON and protobuf encodings

### Protocol Detection

```go
func (s *Server) handleWrite(w http.ResponseWriter, r *http.Request) {
    contentType := r.Header.Get("Content-Type")
    
    switch {
    case strings.Contains(contentType, "x-protobuf"):
        s.handleProtoWrite(w, r)
    case strings.Contains(contentType, "application/json"):
        s.handleJSONWrite(w, r)
    case r.URL.Path == "/api/v1/write/line":
        s.handleLineProtocol(w, r)
    case r.URL.Path == "/api/v1/prom/write":
        s.handlePromRemoteWrite(w, r)
    case r.URL.Path == "/v1/metrics":
        s.handleOTLP(w, r)
    }
}
```

### Normalization

All protocols normalize to the internal Point model:

```go
type Point struct {
    Metric    string
    Tags      map[string]string
    Value     float64
    Timestamp int64
}
```

Protocol-specific metadata is preserved as tags where meaningful.

## Consequences

### Positive

- **Drop-in replacement**: Works with existing Prometheus, OTLP, InfluxDB tooling
- **Gradual migration**: Teams can migrate one system at a time
- **Ecosystem integration**: Grafana, Telegraf, OTel Collector work out-of-box
- **Flexibility**: Choose protocol based on existing infrastructure

### Negative

- **Multiple parsers to maintain**: Each protocol needs parsing, validation, error handling
- **Feature disparity**: Not all protocols support all features (e.g., native histograms)
- **Conversion overhead**: Protocol normalization has CPU cost
- **Documentation burden**: Must document each protocol's specifics

### Protocol-Specific Considerations

**Prometheus Remote Write**:
- Compressed with Snappy (required)
- Batched writes (efficient)
- Metadata labels preserved

**OTLP**:
- Multiple metric types (Gauge, Sum, Histogram, Summary)
- Histograms expanded to bucket metrics with `le` tag
- Resource attributes become tags with `resource_` prefix
- Supports both gzip and identity encoding

**Line Protocol**:
- Whitespace-sensitive parsing
- Field types inferred
- Nanosecond timestamp precision

**JSON**:
- Most flexible, least efficient
- Good for debugging and simple integrations
- No compression by default

### Semantic Mapping

| Concept | Prometheus | OTLP | Line Protocol | JSON |
|---------|------------|------|---------------|------|
| Metric name | `__name__` label | Metric.Name | Measurement | `metric` field |
| Labels/Tags | Labels | Attributes | Tags | `tags` object |
| Value | Sample.Value | NumberDataPoint | Field value | `value` field |
| Timestamp | Sample.Timestamp | TimeUnixNano | Timestamp | `timestamp` field |

### Enabled

- Migration from Prometheus without agent changes
- OpenTelemetry adoption with standard collectors
- Mixed environments with multiple metric sources
- Future protocol additions without client impact

### Prevented

- Protocol-specific optimizations (must normalize everything)
- Type fidelity (all values become float64 internally)
- Protocol-specific features (e.g., Prometheus exemplars stored separately)
