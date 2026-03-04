# Custom Plugin Example

Demonstrates how to create and register custom plugins with Chronicle's plugin system.

## Run

```bash
go run main.go
```

## Features Demonstrated

- Creating an EWMA (Exponentially Weighted Moving Average) aggregator plugin
- Creating a unit-conversion transformer plugin (Celsius → Fahrenheit)
- Registering plugins with `PluginRegistry` using `PluginManifest`
- Listing registered plugins
- Invoking an aggregator plugin on time-series data
- Writing data points with the Chronicle database
