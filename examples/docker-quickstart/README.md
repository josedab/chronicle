# Docker Quickstart: Chronicle + Grafana

Spin up Chronicle with a pre-configured Grafana instance in one command.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/)

## Usage

```bash
docker-compose up --build
```

Once healthy:

| Service    | URL                           | Credentials        |
|------------|-------------------------------|---------------------|
| Chronicle  | http://localhost:8086/health   | —                   |
| Grafana    | http://localhost:3000          | admin / chronicle   |

Grafana is pre-provisioned with Chronicle as a Prometheus-compatible datasource.

## Write sample data

```bash
curl -s -X POST http://localhost:8086/write \
  -d 'cpu_usage,host=server1 value=42.5'
```

## Query via PromQL

```bash
curl -s 'http://localhost:8086/api/v1/query?query=cpu_usage'
```

## Cleanup

```bash
docker-compose down -v
```
