---
sidebar_position: 3
---

# Grafana Integration

Chronicle integrates seamlessly with Grafana for visualization and dashboarding.

## Connection Methods

Chronicle supports two Grafana connection methods:

1. **Prometheus data source** - Use Chronicle's PromQL-compatible API
2. **JSON data source** - Use Chronicle's native API

## Method 1: Prometheus Data Source (Recommended)

### Add Data Source

1. Open Grafana → Configuration → Data Sources
2. Click "Add data source"
3. Select **Prometheus**
4. Configure:

| Setting | Value |
|---------|-------|
| Name | Chronicle |
| URL | `http://chronicle:8086` |
| Access | Server (default) |

5. Click "Save & Test"

### Create Dashboards

Use standard PromQL queries:

```promql
# CPU usage gauge
cpu_usage{host="server-01"}

# Request rate graph
rate(http_requests_total[5m])

# 95th percentile latency
histogram_quantile(0.95, rate(request_duration_bucket[5m]))

# Error rate percentage
sum(rate(http_requests_total{status=~"5.."}[5m])) / sum(rate(http_requests_total[5m])) * 100
```

### Dashboard Example

```json
{
  "panels": [
    {
      "title": "CPU Usage",
      "type": "gauge",
      "targets": [
        {
          "expr": "avg(cpu_usage)",
          "legendFormat": "CPU %"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "min": 0,
          "max": 100,
          "unit": "percent"
        }
      }
    },
    {
      "title": "Request Rate",
      "type": "timeseries",
      "targets": [
        {
          "expr": "sum by (method) (rate(http_requests_total[5m]))",
          "legendFormat": "{{method}}"
        }
      ]
    }
  ]
}
```

## Method 2: JSON Data Source

For advanced use cases, use the JSON data source plugin.

### Install Plugin

```bash
grafana-cli plugins install simpod-json-datasource
```

### Configure Data Source

1. Add data source → **JSON**
2. URL: `http://chronicle:8086`

### Query Format

```json
{
  "metric": "cpu_usage",
  "tags": {"host": "server-01"},
  "start": "$__from",
  "end": "$__to"
}
```

## Pre-built Dashboards

### System Metrics Dashboard

Import this JSON for basic system monitoring:

```json
{
  "title": "Chronicle - System Metrics",
  "panels": [
    {
      "title": "CPU Usage by Host",
      "type": "timeseries",
      "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0},
      "targets": [{
        "expr": "cpu_usage",
        "legendFormat": "{{host}}"
      }]
    },
    {
      "title": "Memory Usage",
      "type": "timeseries",
      "gridPos": {"h": 8, "w": 12, "x": 12, "y": 0},
      "targets": [{
        "expr": "memory_used / memory_total * 100",
        "legendFormat": "{{host}}"
      }]
    },
    {
      "title": "Disk I/O",
      "type": "timeseries",
      "gridPos": {"h": 8, "w": 24, "x": 0, "y": 8},
      "targets": [{
        "expr": "rate(disk_bytes_read[5m])",
        "legendFormat": "Read - {{device}}"
      }, {
        "expr": "rate(disk_bytes_written[5m])",
        "legendFormat": "Write - {{device}}"
      }]
    }
  ]
}
```

### HTTP Metrics Dashboard

```json
{
  "title": "Chronicle - HTTP Metrics",
  "panels": [
    {
      "title": "Request Rate",
      "type": "stat",
      "targets": [{
        "expr": "sum(rate(http_requests_total[5m]))",
        "legendFormat": "req/s"
      }]
    },
    {
      "title": "Error Rate",
      "type": "gauge",
      "targets": [{
        "expr": "sum(rate(http_requests_total{status=~\"5..\"}[5m])) / sum(rate(http_requests_total[5m])) * 100"
      }],
      "fieldConfig": {
        "defaults": {
          "thresholds": {
            "steps": [
              {"value": 0, "color": "green"},
              {"value": 1, "color": "yellow"},
              {"value": 5, "color": "red"}
            ]
          }
        }
      }
    },
    {
      "title": "Latency Percentiles",
      "type": "timeseries",
      "targets": [
        {"expr": "histogram_quantile(0.50, rate(request_duration_bucket[5m]))", "legendFormat": "p50"},
        {"expr": "histogram_quantile(0.95, rate(request_duration_bucket[5m]))", "legendFormat": "p95"},
        {"expr": "histogram_quantile(0.99, rate(request_duration_bucket[5m]))", "legendFormat": "p99"}
      ]
    }
  ]
}
```

## Variables

Use Grafana variables for dynamic dashboards:

### Host Variable

1. Dashboard Settings → Variables → New
2. Configure:

| Setting | Value |
|---------|-------|
| Name | host |
| Type | Query |
| Data source | Chronicle |
| Query | `label_values(cpu_usage, host)` |

3. Use in queries: `cpu_usage{host="$host"}`

### Time Range Variable

Grafana's built-in `$__range` works with Chronicle:

```promql
rate(http_requests_total[$__rate_interval])
```

## Alerting

### Create Alert Rule

1. Panel → Alert → Create alert rule
2. Configure condition:

```promql
# Alert when CPU > 90%
avg(cpu_usage) > 90

# Alert when error rate > 5%
sum(rate(http_requests_total{status=~"5.."}[5m])) / sum(rate(http_requests_total[5m])) * 100 > 5
```

3. Set evaluation interval and notification channels

### Alert Examples

```yaml
# High CPU Alert
- alert: HighCPU
  expr: avg by (host) (cpu_usage) > 90
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "High CPU on {{ $labels.host }}"

# High Error Rate
- alert: HighErrorRate  
  expr: |
    sum(rate(http_requests_total{status=~"5.."}[5m])) 
    / sum(rate(http_requests_total[5m])) * 100 > 5
  for: 2m
  labels:
    severity: critical
```

## Performance Tips

### Query Optimization

```promql
# ❌ Slow: No time filter
sum(http_requests_total)

# ✅ Fast: With rate and time window
sum(rate(http_requests_total[5m]))
```

### Dashboard Settings

- **Max data points**: Set to 1000 for large time ranges
- **Min interval**: Match your scrape interval (e.g., `15s`)
- **Relative time**: Use relative ranges to leverage caching

### Caching

Configure caching in Grafana:

```ini
# grafana.ini
[caching]
enabled = true

# or per-data-source
```

## Troubleshooting

### No Data

1. Verify Chronicle is running: `curl http://chronicle:8086/health`
2. Check metric exists: `curl http://chronicle:8086/metrics`
3. Test query directly: `curl "http://chronicle:8086/api/v1/query?query=up"`

### Slow Queries

1. Add time bounds to queries
2. Reduce query resolution
3. Use `rate()` instead of raw counters
4. Check Chronicle's query timeout setting

### Connection Refused

1. Verify network connectivity
2. Check Chronicle's `HTTPPort` config
3. Ensure Chronicle is listening on correct interface
