# Plugin Development Guide

This guide explains how to build, test, and distribute plugins for Chronicle.

## Plugin Types

Chronicle supports six plugin types:

| Type | Interface | Purpose |
|------|-----------|---------|
| Aggregator | `AggregatorPlugin` | Custom aggregation functions (e.g., percentile, EWMA) |
| Ingestor | `IngestorPlugin` | Custom data ingestion formats |
| Transformer | `TransformerPlugin` | Point transformation pipelines |
| Alert Handler | `AlertHandlerPlugin` | Custom alert delivery (Slack, PagerDuty, etc.) |
| Compression | `CompressionPlugin` | Custom compression algorithms |
| Storage | `StoragePlugin` | Custom storage backends |

## Quick Start: Writing an Aggregator Plugin

### 1. Implement the interface

```go
package main

import "github.com/chronicle-db/chronicle"

// EWMAPlugin implements exponentially weighted moving average.
type EWMAPlugin struct {
    alpha float64
}

func (p *EWMAPlugin) Name() string { return "ewma" }

func (p *EWMAPlugin) Aggregate(values []float64) (float64, error) {
    if len(values) == 0 {
        return 0, nil
    }
    result := values[0]
    for _, v := range values[1:] {
        result = p.alpha*v + (1-p.alpha)*result
    }
    return result, nil
}

func (p *EWMAPlugin) Reset() {
    // No state to reset for this implementation
}
```

### 2. Create a manifest

```go
manifest := chronicle.PluginManifest{
    ID:          "ewma-aggregator",
    Name:        "EWMA Aggregator",
    Version:     "1.0.0",
    Type:        chronicle.PluginTypeAggregator,
    Description: "Exponentially weighted moving average",
    Author:      "Your Name",
    License:     "Apache-2.0",
}
```

### 3. Register with the SDK

```go
sdk := chronicle.NewPluginSDK(chronicle.DefaultPluginSDKConfig())

err := sdk.RegisterAggregator(manifest, &EWMAPlugin{alpha: 0.3})
if err != nil {
    log.Fatal(err)
}
```

### 4. Invoke the plugin

```go
result, err := sdk.InvokeAggregator(ctx, "ewma-aggregator", []float64{1, 2, 3, 4, 5})
```

---

## Plugin Interfaces

### AggregatorPlugin

```go
type AggregatorPlugin interface {
    Name() string
    Aggregate(values []float64) (float64, error)
    Reset()
}
```

- `Name()` — Unique identifier for the aggregation function
- `Aggregate(values)` — Compute the aggregated result from a slice of values
- `Reset()` — Clear any accumulated state between invocations

### IngestorPlugin

```go
type IngestorPlugin interface {
    Name() string
    Parse(data []byte) ([]Point, error)
    ContentType() string
}
```

- `Name()` — Plugin identifier
- `Parse(data)` — Convert raw bytes into Chronicle Points
- `ContentType()` — HTTP Content-Type this plugin handles (e.g., `"application/x-msgpack"`)

### TransformerPlugin

```go
type TransformerPlugin interface {
    Name() string
    Transform(points []Point) ([]Point, error)
}
```

- `Name()` — Plugin identifier
- `Transform(points)` — Process points in a pipeline (filter, enrich, modify)

### AlertHandlerPlugin

```go
type AlertHandlerPlugin interface {
    Name() string
    Handle(alert PluginAlertNotification) error
}
```

- `Name()` — Plugin identifier
- `Handle(alert)` — Deliver an alert notification to an external system

---

## Plugin Manifest

Every plugin requires a `PluginManifest` that describes it:

```go
type PluginManifest struct {
    ID           string     `json:"id"`           // Unique identifier (e.g., "my-org.ewma")
    Name         string     `json:"name"`          // Human-readable name
    Version      string     `json:"version"`       // Semantic version
    Type         PluginType `json:"type"`           // Plugin type constant
    Description  string     `json:"description"`    // Brief description
    Author       string     `json:"author"`         // Author name
    License      string     `json:"license"`        // SPDX license identifier
    Homepage     string     `json:"homepage"`       // Project URL (optional)
    MinVersion   string     `json:"min_chronicle_version"` // Minimum Chronicle version (optional)
    Dependencies []string   `json:"dependencies"`   // Other plugin IDs required (optional)
}
```

**Naming convention**: Use reverse-domain notation for IDs (e.g., `"com.example.my-plugin"`).

---

## Plugin Lifecycle

Plugins move through these states:

```
registered → loaded → running → stopped
                   ↘ failed
```

| State | Description |
|-------|-------------|
| `registered` | Manifest accepted, plugin stored in registry |
| `loaded` | Plugin initialized and ready |
| `running` | Plugin actively processing requests |
| `stopped` | Plugin gracefully shut down |
| `failed` | Plugin encountered an unrecoverable error |

---

## SDK Configuration

```go
config := chronicle.PluginSDKConfig{
    Enabled:            true,
    PluginDir:          "./plugins",        // Directory for plugin files
    MaxPlugins:         50,                 // Maximum registered plugins
    MaxMemoryPerPlugin: 64 * 1024 * 1024,   // 64MB per plugin
    ExecutionTimeout:   30 * time.Second,   // Timeout per invocation
    EnableSandbox:      true,               // Enable isolation
    AutoUpdate:         false,              // Auto-update from marketplace
}

sdk := chronicle.NewPluginSDK(config)
```

---

## Testing Plugins

Test plugins directly without a database:

```go
func TestEWMAPlugin(t *testing.T) {
    plugin := &EWMAPlugin{alpha: 0.3}

    result, err := plugin.Aggregate([]float64{10, 20, 30, 40, 50})
    if err != nil {
        t.Fatalf("Aggregate error: %v", err)
    }
    if result < 30 || result > 50 {
        t.Errorf("unexpected EWMA result: %f", result)
    }
}
```

Test with the SDK registry for integration coverage:

```go
func TestEWMARegistration(t *testing.T) {
    sdk := chronicle.NewPluginSDK(chronicle.DefaultPluginSDKConfig())

    manifest := chronicle.PluginManifest{
        ID:      "test-ewma",
        Name:    "Test EWMA",
        Version: "1.0.0",
        Type:    chronicle.PluginTypeAggregator,
    }

    err := sdk.RegisterAggregator(manifest, &EWMAPlugin{alpha: 0.5})
    if err != nil {
        t.Fatalf("register: %v", err)
    }

    result, err := sdk.InvokeAggregator(context.Background(), "test-ewma", []float64{1, 2, 3})
    if err != nil {
        t.Fatalf("invoke: %v", err)
    }
    t.Logf("EWMA result: %f", result)

    // Verify plugin appears in listing
    plugins := sdk.ListByType(chronicle.PluginTypeAggregator)
    if len(plugins) == 0 {
        t.Error("expected plugin in listing")
    }
}
```

---

## Compression Plugins

Compression plugins use a separate manager (`CompressionPluginManager`):

```go
type CompressionPlugin interface {
    Name() string
    Version() string
    Description() string
    Compress(data []byte) ([]byte, error)
    Decompress(data []byte) ([]byte, error)
    Metadata() PluginMetadata
}
```

Register and use:

```go
manager := chronicle.NewCompressionPluginManager()
manager.RegisterPlugin(&MyCompressor{}, true) // true = run benchmark

compressed, err := manager.Compress("my-compressor", data)
original, err := manager.Decompress("my-compressor", compressed)
```

---

## Best Practices

1. **Return meaningful errors** — Wrap errors with context: `fmt.Errorf("ewma: %w", err)`
2. **Handle empty inputs** — `Aggregate([]float64{})` should return a zero value, not panic
3. **Be idempotent** — `Reset()` should leave the plugin in a clean initial state
4. **Respect timeouts** — Long-running operations should check `ctx.Done()`
5. **Version carefully** — Follow semver; breaking changes require a major version bump
6. **Document configuration** — If your plugin accepts config via `Init(map[string]interface{})`, document the expected keys

---

## See Also

- [API Reference](./API.md) — HTTP endpoints for plugin management
- [Architecture](./ARCHITECTURE.md) — System design overview
- [Configuration](./CONFIGURATION.md) — SDK configuration reference
