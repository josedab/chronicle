# Docker Compose Monitoring Stack for Chronicle

This example demonstrates how to deploy a complete monitoring stack for Chronicle using Docker Compose.

## Components

| Service | Port | Description |
|---|---|---|
| **Chronicle** | 8086 | Time-series database engine exposing metrics endpoint |
| **Prometheus** | 9090 | Scrapes Chronicle metrics and evaluates alerting rules |
| **Alertmanager** | 9093 | Manages and routes alert notifications |
| **Grafana** | 3000 | Visualizes time-series and system health with pre-provisioned dashboards |

## Quickstart

1. Navigate to this directory:
   ```bash
   cd examples/docker-compose-monitoring
   ```

2. Start the stack:
   ```bash
   docker compose up -d
   ```

3. Verify service endpoints:
   - Chronicle Health: http://localhost:8086/health
   - Prometheus UI: http://localhost:9090
   - Alertmanager UI: http://localhost:9093
   - Grafana Dashboard: http://localhost:3000 (credentials: `admin` / `chronicle`)

## Pre-provisioned Dashboards

Grafana automatically provisions:
- **Datasources:** Both Chronicle (`http://chronicle:8086`) and Prometheus (`http://prometheus:9090`)
- **Dashboards:** Folder `Chronicle` -> `Chronicle Monitoring Overview`

## Alerting Rules

Pre-configured rules in `prometheus/alerts.yml` track:
- `ChronicleDown`: Triggers critical alert if the Chronicle instance is unreachable for >1m.
- `PrometheusScrapeError`: Triggers warning alert on out-of-order sample ingestion.
