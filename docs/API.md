# Chronicle HTTP API Reference

This document describes the Chronicle HTTP API endpoints available when `HTTPEnabled: true`.

## Endpoints Overview

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/write` | POST | Write points (InfluxDB line protocol) |
| `/query` | POST | Execute SQL-like query |
| `/api/v1/query` | GET/POST | Prometheus-compatible instant query |
| `/api/v1/query_range` | GET/POST | Prometheus-compatible range query |
| `/v1/metrics` | POST | OpenTelemetry OTLP JSON ingestion |
| `/schemas` | GET/POST/DELETE | Schema registry CRUD |
| `/api/v1/alerts` | GET | Get active alerts |
| `/api/v1/rules` | GET/POST | Alerting rules management |
| `/stream` | WebSocket | Real-time streaming subscription |
| `/api/v1/prom/write` | POST | Prometheus remote write |

---

## Health Check

### GET /health

Returns the health status of the Chronicle instance.

**Response:**
```json
{
  "status": "ok"
}
```

---

## Write Endpoints

### POST /write

Write data points using InfluxDB line protocol format.

**Content-Type:** `text/plain`

**Request Body:**
```
cpu,host=server01,region=us-west value=0.64 1609459200000000000
cpu,host=server02,region=us-east value=0.55 1609459200000000000
```

**Response:** `204 No Content` on success

### POST /v1/metrics

Write metrics using OpenTelemetry OTLP JSON format.

**Content-Type:** `application/json`

**Request Body:**
```json
{
  "resourceMetrics": [
    {
      "resource": {
        "attributes": [
          {"key": "service.name", "value": {"stringValue": "my-service"}}
        ]
      },
      "scopeMetrics": [
        {
          "scope": {"name": "my-library"},
          "metrics": [
            {
              "name": "http_requests",
              "gauge": {
                "dataPoints": [
                  {
                    "attributes": [{"key": "method", "value": {"stringValue": "GET"}}],
                    "asDouble": 42.0,
                    "timeUnixNano": "1609459200000000000"
                  }
                ]
              }
            }
          ]
        }
      ]
    }
  ]
}
```

**Response:** `200 OK` with accepted count

### POST /api/v1/prom/write

Accept Prometheus remote write format (snappy-compressed protobuf).

**Content-Type:** `application/x-protobuf`  
**Content-Encoding:** `snappy`

**Response:** `204 No Content` on success

---

## Query Endpoints

### POST /query

Execute a Chronicle SQL-like query.

**Content-Type:** `application/json`

**Request Body:**
```json
{
  "query": "SELECT mean(value) FROM cpu WHERE host='server01' GROUP BY time(5m)"
}
```

**Response:**
```json
{
  "series": [
    {
      "metric": "cpu",
      "tags": {"host": "server01"},
      "points": [
        {"timestamp": 1609459200000000000, "value": 0.64}
      ]
    }
  ]
}
```

### GET/POST /api/v1/query

Prometheus-compatible instant query endpoint.

**Query Parameters:**
- `query` (required): PromQL expression
- `time` (optional): Evaluation timestamp (Unix timestamp)

**Request:**
```
GET /api/v1/query?query=http_requests_total{method="GET"}&time=1609459200
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "resultType": "vector",
    "result": [
      {
        "metric": {"__name__": "http_requests_total", "method": "GET"},
        "value": [1609459200, "42"]
      }
    ]
  }
}
```

### GET/POST /api/v1/query_range

Prometheus-compatible range query endpoint.

**Query Parameters:**
- `query` (required): PromQL expression
- `start` (required): Start timestamp (Unix timestamp)
- `end` (required): End timestamp (Unix timestamp)
- `step` (required): Query resolution step (e.g., "15s", "1m", "1h")

**Request:**
```
GET /api/v1/query_range?query=rate(http_requests_total[5m])&start=1609459200&end=1609462800&step=60
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "resultType": "matrix",
    "result": [
      {
        "metric": {"__name__": "http_requests_total", "method": "GET"},
        "values": [
          [1609459200, "1.2"],
          [1609459260, "1.5"]
        ]
      }
    ]
  }
}
```

---

## Schema Registry

### GET /schemas

List all registered schemas.

**Response:**
```json
{
  "schemas": [
    {
      "name": "cpu",
      "tags": [{"name": "host", "required": true}],
      "fields": [{"name": "value", "min_value": 0, "max_value": 100}]
    }
  ]
}
```

### POST /schemas

Register a new schema.

**Request Body:**
```json
{
  "name": "temperature",
  "tags": [
    {"name": "sensor_id", "required": true},
    {"name": "location", "allowed_values": ["indoor", "outdoor"]}
  ],
  "fields": [
    {"name": "value", "min_value": -50, "max_value": 100}
  ]
}
```

**Response:** `201 Created`

### DELETE /schemas?name=temperature

Delete a schema.

**Query Parameters:**
- `name` (required): Schema name to delete

**Response:** `204 No Content`

---

## Alerting

### GET /api/v1/alerts

Get all active alerts.

**Response:**
```json
{
  "alerts": [
    {
      "rule_name": "high_cpu",
      "metric": "cpu",
      "labels": {"host": "server01"},
      "value": 95.5,
      "threshold": 90.0,
      "triggered_at": "2024-01-15T10:30:00Z"
    }
  ]
}
```

### GET /api/v1/rules

List all alerting rules.

**Response:**
```json
{
  "rules": [
    {
      "name": "high_cpu",
      "metric": "cpu",
      "condition": "> 90",
      "duration": "5m",
      "labels": {"severity": "critical"}
    }
  ]
}
```

### POST /api/v1/rules

Create a new alerting rule.

**Request Body:**
```json
{
  "name": "high_cpu",
  "metric": "cpu",
  "condition": "> 90",
  "for_duration": "5m",
  "labels": {"severity": "critical"},
  "annotations": {"summary": "CPU usage above 90%"},
  "webhooks": ["https://alerts.example.com/webhook"]
}
```

**Response:** `201 Created`

---

## Streaming

### WebSocket /stream

Real-time streaming subscription over WebSocket.

**Connection:**
```
ws://localhost:8086/stream?metric=cpu&filter=host:server01
```

**Query Parameters:**
- `metric` (required): Metric name to subscribe to
- `filter` (optional): Tag filter in format `key:value`

**Messages (server → client):**
```json
{
  "metric": "cpu",
  "tags": {"host": "server01"},
  "value": 0.64,
  "timestamp": 1609459200000000000
}
```

---

## Multi-Tenancy

For multi-tenant deployments, include the `X-Tenant-ID` header in requests:

```
X-Tenant-ID: tenant-123
```

All operations (write, query, schema) will be isolated to the specified tenant namespace.

---

## Error Responses

All endpoints return errors in this format:

```json
{
  "error": "error message description"
}
```

Common HTTP status codes:
- `400 Bad Request`: Invalid request syntax or parameters
- `404 Not Found`: Resource not found
- `500 Internal Server Error`: Server-side error

---

## GraphQL

### POST /graphql

GraphQL endpoint for flexible querying.

**Content-Type:** `application/json`

**Request Body:**
```json
{
  "query": "{ metrics }",
  "operationName": null,
  "variables": {}
}
```

**Response:**
```json
{
  "data": {
    "metrics": ["cpu", "memory", "disk"]
  }
}
```

**Example Queries:**

List metrics:
```graphql
{ metrics }
```

Query points:
```graphql
{
  points(metric: "cpu", start: "1h") {
    timestamp
    value
    tags { key value }
  }
}
```

Get stats:
```graphql
{
  stats {
    metrics
    uptime
    version
  }
}
```

### GET /graphql/playground

Interactive GraphQL playground for testing queries.

---

## Forecast API

### POST /api/v1/forecast

Generate time-series forecasts.

**Request Body:**
```json
{
  "metric": "cpu",
  "start": 1609459200000000000,
  "end": 1609462800000000000,
  "periods": 10,
  "method": "holt_winters"
}
```

**Parameters:**
- `metric`: Metric name
- `start`: Start timestamp (nanoseconds)
- `end`: End timestamp (nanoseconds)
- `periods`: Number of periods to forecast
- `method`: `holt_winters`, `double`, `simple`, `moving_average`

**Response:**
```json
{
  "predictions": [
    {
      "timestamp": 1609466400000000000,
      "value": 45.2,
      "lower_bound": 42.1,
      "upper_bound": 48.3,
      "confidence": 0.95
    }
  ],
  "anomalies": [],
  "rmse": 1.23,
  "mae": 0.98
}
```

---

## Histogram API

### GET /api/v1/histogram

Get histogram data.

**Query Parameters:**
- `name`: Histogram metric name

**Response:**
```json
{
  "name": "request_duration",
  "count": 1000,
  "sum": 125.5,
  "buckets": [10, 25, 50, 100, 200]
}
```

### POST /api/v1/histogram

Record a histogram observation.

**Request Body:**
```json
{
  "name": "request_duration",
  "value": 0.125
}
```

**Response:** `202 Accepted`

---

## Admin UI

### GET /admin

Web-based administration dashboard.

Features:
- System stats (uptime, memory, goroutines)
- Database stats (metrics, partitions)
- Interactive query explorer
- Configuration view

### GET /admin/api/stats

JSON endpoint for system statistics.

**Response:**
```json
{
  "uptime": 3600.5,
  "version": "1.0.0",
  "go_version": "go1.21",
  "num_cpu": 8,
  "num_goroutine": 42,
  "metric_count": 15,
  "partition_count": 24,
  "memory": {
    "alloc": 52428800,
    "total_alloc": 104857600,
    "sys": 75497472,
    "num_gc": 12
  }
}
```

### GET /admin/api/metrics

List all metrics.

**Response:**
```json
[
  {"name": "cpu"},
  {"name": "memory"},
  {"name": "disk"}
]
```

### GET /admin/api/health

Health check with details.

**Response:**
```json
{
  "status": "healthy",
  "uptime": 3600.5
}
```

---

## Metrics List

### GET /metrics

List all registered metric names.

**Response:**
```json
["cpu", "memory", "disk", "network"]
```
