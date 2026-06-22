# Docker Compose Monitoring Stack

Complete monitoring stack: **Chronicle TSDB** + **Grafana** + **Auto-seeded IoT data**.

Demonstrates Bounty #12 from [josedab/chronicle](https://github.com/josedab/chronicle).

## Architecture

```
┌─────────────┐     ┌─────────────┐
│  Chronicle  │────▶│   Grafana   │
│  (TSDB)     │     │  (Dashboard)│
│  :8086      │     │  :3000      │
└──────┬──────┘     └─────────────┘
       │
       ▼
┌─────────────┐
│  Seed Data  │
│  Generator  │
└─────────────┘
```

## Quick Start

```bash
# 1. Start the stack
docker compose up -d

# 2. Wait for Chronicle to be healthy (10-30s)
docker compose ps

# 3. Seed sample IoT data (5 minutes, 10s interval)
docker compose run --rm seed-data 5 10

# Or seed with defaults (5 min, 10s interval)
docker compose run --rm seed-data

# 4. Open Grafana
open http://localhost:3000
# Login: admin / chronicle
```

## Services

| Service | URL | Port | Purpose |
|---------|-----|------|---------|
| Chronicle | `http://localhost:8086/health` | 8086 | Time-series database |
| Chronicle GraphQL | `http://localhost:8087` | 8087 | GraphQL playground |
| Grafana | `http://localhost:3000` | 3000 | Dashboard & visualization |

## Sample Data

The seed script generates realistic IoT sensor data:
- **Temperature**: 17-27°C range
- **Humidity**: 45-65% range
- **Pressure**: 1008-1018 hPa range
- **5 sensors**: sensor-01 through sensor-05

## Write Your Own Data

```bash
# Prometheus-style line protocol
curl -X POST http://localhost:8086/write \
  -d 'cpu_usage,host=myserver value=42.5'

# HTTP API query
curl 'http://localhost:8086/api/v1/query?query=cpu_usage'

# Range query
curl 'http://localhost:8086/api/v1/query_range?query=cpu_usage&start=-1h'
```

## Grafana Dashboard

Pre-configured dashboard: **Chronicle IoT Monitoring**
- Real-time temperature & humidity stats
- Rate-of-change trend panels
- Sensor filter templating
- Auto-refresh every 15 seconds

## Cleanup

```bash
# Stop services (keep data)
docker compose down

# Stop and remove all data
docker compose down -v
```

## Configuration

Edit `seed-data.sh` parameters:
- `$1` = duration in minutes (default: 5)
- `$2` = interval in seconds (default: 10)

```bash
# Generate 10 minutes of data at 30-second intervals
docker compose run --rm seed-data 10 30
```

## Files

```
docker-monitoring-stack/
├── docker-compose.yml        # Stack definition
├── seed-data.sh              # IoT data generator
├── grafana/
│   ├── provisioning/
│   │   ├── datasources/
│   │   │   └── chronicle.yml  # Prometheus datasource
│   │   └── dashboards/
│   │       └── dashboards.yml # Dashboard provider config
│   └── dashboards/
│       └── iot-monitoring.json # Pre-built dashboard
└── README.md
```

## Requirements

- Docker 24+
- Docker Compose v2+
- ~256MB RAM minimum
