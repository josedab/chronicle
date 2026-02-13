# Chronicle Grafana Dashboard

Pre-built Grafana dashboard for monitoring a Chronicle TSDB instance.

## Quick Import

1. Open Grafana → Dashboards → Import
2. Upload `chronicle-dashboard.json` or paste its contents
3. Select your Chronicle datasource
4. Click Import

## Panels

| Panel | Type | Shows |
|-------|------|-------|
| Write Throughput | Time series | Points written per second |
| Query Latency (p95) | Time series | 95th percentile query duration |
| Storage Size | Stat | Current database file size |
| Active Metrics | Stat | Number of unique metric names |
| Partition Count | Stat | Number of active partitions |
| WAL Size | Stat | Current write-ahead log size |
| Points Written | Time series | Cumulative points over time |
| Active Alerts | Table | Currently firing alert rules |

## Prerequisites

- Grafana 10.0+
- Chronicle Grafana datasource plugin installed
- Chronicle HTTP API enabled (`config.HTTP.HTTPEnabled = true`)

## Customization

Edit the dashboard JSON to add panels for your specific metrics. Common additions:
- Per-metric cardinality tracking
- Retention policy status
- Replication lag (if multi-region is enabled)
- Query cache hit rate
