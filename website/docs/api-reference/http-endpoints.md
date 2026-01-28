---
sidebar_position: 5
---

# HTTP Endpoints

Complete reference for Chronicle's REST API.

## Overview

Enable the HTTP API:

```go
db, _ := chronicle.Open("data.db", chronicle.Config{
    HTTPEnabled: true,
    HTTPPort:    8086,
})
```

Base URL: `http://localhost:8086`

## Write Endpoints

### POST /write

Write data points.

**InfluxDB Line Protocol:**

```bash
curl -X POST http://localhost:8086/write \
  -d 'cpu_usage,host=server-01,region=us-west value=45.7 1706400000000000000'
```

**JSON Format:**

```bash
curl -X POST http://localhost:8086/write \
  -H "Content-Type: application/json" \
  -d '{
    "points": [
      {
        "metric": "cpu_usage",
        "tags": {"host": "server-01"},
        "value": 45.7,
        "timestamp": 1706400000000000000
      }
    ]
  }'
```

**Response:** `202 Accepted`

**Gzip Support:**

```bash
curl -X POST http://localhost:8086/write \
  -H "Content-Encoding: gzip" \
  --data-binary @data.gz
```

### POST /prometheus/write

Prometheus remote write endpoint.

```bash
# Requires PrometheusRemoteWriteEnabled: true
curl -X POST http://localhost:8086/prometheus/write \
  -H "Content-Type: application/x-protobuf" \
  -H "Content-Encoding: snappy" \
  --data-binary @prometheus-data
```

### POST /v1/metrics

OTLP metrics endpoint (OpenTelemetry).

```bash
curl -X POST http://localhost:8086/v1/metrics \
  -H "Content-Type: application/x-protobuf" \
  --data-binary @otlp-metrics
```

## Query Endpoints

### POST /query

Execute a query.

**JSON Query:**

```bash
curl -X POST http://localhost:8086/query \
  -H "Content-Type: application/json" \
  -d '{
    "metric": "cpu_usage",
    "tags": {"host": "server-01"},
    "start": 1706400000000000000,
    "end": 1706486400000000000
  }'
```

**SQL Query:**

```bash
curl -X POST http://localhost:8086/query \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT mean(value) FROM cpu_usage WHERE host = '\''server-01'\'' GROUP BY time(5m)"}'
```

**Response:**

```json
{
  "points": [
    {
      "metric": "cpu_usage",
      "tags": {"host": "server-01"},
      "value": 45.7,
      "timestamp": 1706400000000000000
    }
  ]
}
```

### GET /api/v1/query

Prometheus-compatible instant query.

```bash
curl "http://localhost:8086/api/v1/query?query=cpu_usage{host='server-01'}&time=2024-01-28T12:00:00Z"
```

**Parameters:**
- `query` - PromQL expression
- `time` - Evaluation timestamp (optional, defaults to now)

**Response:**

```json
{
  "status": "success",
  "data": {
    "resultType": "vector",
    "result": [
      {
        "metric": {"__name__": "cpu_usage", "host": "server-01"},
        "value": [1706443200, "45.7"]
      }
    ]
  }
}
```

### GET /api/v1/query_range

Prometheus-compatible range query.

```bash
curl "http://localhost:8086/api/v1/query_range?query=rate(http_requests_total[5m])&start=2024-01-28T00:00:00Z&end=2024-01-28T12:00:00Z&step=60"
```

**Parameters:**
- `query` - PromQL expression
- `start` - Start timestamp
- `end` - End timestamp
- `step` - Query resolution in seconds

### GET /api/v1/forecast

Time-series forecasting.

```bash
curl "http://localhost:8086/api/v1/forecast?metric=cpu_usage&horizon=24h&tags=host:server-01"
```

**Parameters:**
- `metric` - Metric name
- `horizon` - Forecast horizon (e.g., "1h", "24h")
- `tags` - Tag filters (key:value format)

## GraphQL Endpoint

### POST /graphql

GraphQL queries.

```bash
curl -X POST http://localhost:8086/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "query { query(metric: \"cpu_usage\", start: \"2024-01-01T00:00:00Z\", end: \"2024-01-02T00:00:00Z\") { points { timestamp value } } }"
  }'
```

### GET /graphql/playground

GraphQL Playground UI.

Open `http://localhost:8086/graphql/playground` in a browser.

## Metadata Endpoints

### GET /metrics

List all metric names.

```bash
curl http://localhost:8086/metrics
```

**Response:**

```json
["cpu_usage", "memory_used", "http_requests_total"]
```

### GET /health

Health check.

```bash
curl http://localhost:8086/health
```

**Response:**

```json
{"status": "ok"}
```

## Schema Endpoints

### GET /schemas

List all schemas.

```bash
curl http://localhost:8086/schemas
```

### POST /schemas

Register a schema.

```bash
curl -X POST http://localhost:8086/schemas \
  -H "Content-Type: application/json" \
  -d '{
    "name": "cpu_usage",
    "type": "gauge",
    "unit": "percent",
    "required_tags": ["host"]
  }'
```

### DELETE /schemas?name=\{name\}

Delete a schema.

```bash
curl -X DELETE "http://localhost:8086/schemas?name=cpu_usage"
```

## Alerting Endpoints

### GET /api/v1/alerts

List active alerts.

```bash
curl http://localhost:8086/api/v1/alerts
```

### GET /api/v1/rules

List alerting rules.

```bash
curl http://localhost:8086/api/v1/rules
```

### POST /api/v1/rules

Create an alerting rule.

```bash
curl -X POST http://localhost:8086/api/v1/rules \
  -H "Content-Type: application/json" \
  -d '{
    "name": "HighCPU",
    "expr": "cpu_usage > 90",
    "for": "5m",
    "labels": {"severity": "warning"},
    "annotations": {"summary": "High CPU usage"}
  }'
```

## Histogram Endpoint

### POST /api/v1/histogram

Write histogram data.

```bash
curl -X POST http://localhost:8086/api/v1/histogram \
  -H "Content-Type: application/json" \
  -d '{
    "metric": "request_duration",
    "tags": {"endpoint": "/api"},
    "buckets": [
      {"upper_bound": 0.1, "count": 100},
      {"upper_bound": 0.5, "count": 450},
      {"upper_bound": 1.0, "count": 500}
    ],
    "sum": 45.2,
    "count": 500
  }'
```

## Admin UI

### GET /admin

Admin dashboard UI.

Open `http://localhost:8086/admin` in a browser.

Features:
- Database statistics
- Active queries
- Cardinality metrics
- Configuration view

## WebSocket Streaming

### WS /stream

Real-time data streaming.

```javascript
const ws = new WebSocket('ws://localhost:8086/stream');

ws.onopen = () => {
  // Subscribe to metrics
  ws.send(JSON.stringify({
    type: 'subscribe',
    metrics: ['cpu_usage', 'memory_used'],
    tags: {host: 'server-01'}
  }));
};

ws.onmessage = (event) => {
  const point = JSON.parse(event.data);
  console.log(point);
};
```

## Error Responses

All endpoints return errors in this format:

```json
{
  "error": "error message"
}
```

**HTTP Status Codes:**
- `200 OK` - Success
- `202 Accepted` - Write accepted
- `204 No Content` - Empty write
- `400 Bad Request` - Invalid request
- `404 Not Found` - Endpoint not found
- `405 Method Not Allowed` - Wrong HTTP method
- `500 Internal Server Error` - Server error

## Rate Limiting

Chronicle doesn't include built-in rate limiting. Use a reverse proxy (nginx, HAProxy) for production deployments.

## Authentication

Chronicle doesn't include built-in authentication. For production:
- Use a reverse proxy with authentication
- Deploy behind a VPN
- Use network-level security
