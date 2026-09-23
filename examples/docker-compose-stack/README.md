# Docker Compose Monitoring Stack: Chronicle + Grafana + Alertmanager

This example provides a production-ready Docker Compose environment running **Chronicle TSDB**, **Grafana**, and **Prometheus Alertmanager**.

## Services

| Service | Port | Description |
|---------|------|-------------|
| `chronicle` | `8086` | Embedded Time-Series Database with Prometheus API support |
| `grafana` | `3000` | Real-time visualization pre-configured with Chronicle Datasource |
| `alertmanager`| `9093` | Alert routing and dispatch |

## Quick Start

1. Start the stack:
   ```bash
   docker compose up -d
   ```

2. Verify services are up:
   ```bash
   docker compose ps
   curl -s http://localhost:8086/health
   ```

3. Ingest sample metrics:
   ```bash
   curl -i -XPOST 'http://localhost:8086/write' \
     --data-binary 'cpu_usage,host=server01,region=us-east value=42.5'
   ```

4. View in Grafana:
   - Navigate to `http://localhost:3000` (Default credentials: `admin` / `admin`).
   - The **Chronicle Prometheus** datasource is automatically provisioned and ready for PromQL / CQL dashboards.

## Teardown

```bash
docker compose down -v
```
