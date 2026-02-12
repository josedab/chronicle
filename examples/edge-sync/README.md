# Edge Sync Demo

Demonstrates Chronicle DB running as independent edge nodes collecting IoT sensor data, with a cloud aggregation node.

## Architecture

```
┌──────────┐  ┌──────────┐  ┌──────────┐
│  edge-1  │  │  edge-2  │  │  edge-3  │
│  :8086   │  │  :8087   │  │  :8088   │
│ temp/hum │  │ temp/hum │  │ temp/hum │
└────┬─────┘  └────┬─────┘  └────┬─────┘
     │             │             │
     └─────────────┼─────────────┘
                   │
            ┌──────┴──────┐
            │    cloud    │
            │    :8089    │
            │ aggregator  │
            └─────────────┘
```

Each edge node generates simulated temperature and humidity readings every second. The cloud node serves as an aggregation endpoint.

## Run

```bash
docker compose up --build
```

## Query Nodes

```bash
# Check each edge node
curl http://localhost:8086/status
curl http://localhost:8087/status
curl http://localhost:8088/status

# Check the cloud aggregator
curl http://localhost:8089/status
```

## Stop

```bash
docker compose down -v
```
