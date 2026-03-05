# Chronicle HTTP API Reference

This document describes the Chronicle HTTP API endpoints available when `HTTPEnabled: true`.

## Endpoints Overview

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/health/ready` | GET | Readiness probe |
| `/health/live` | GET | Liveness probe |
| `/write` | POST | Write points (InfluxDB line protocol) |
| `/query` | POST | Execute SQL-like query |
| `/api/v1/query` | GET/POST | Prometheus-compatible instant query |
| `/api/v1/query_range` | GET/POST | Prometheus-compatible range query |
| `/v1/metrics` | POST | OpenTelemetry OTLP JSON ingestion |
| `/schemas` | GET/POST/DELETE | Schema registry CRUD |
| `/api/v1/alerts` | GET | Get active alerts |
| `/api/v1/rules` | GET/POST | Alerting rules management |
| `/stream` | WebSocket | Real-time streaming subscription |
| `/prometheus/write` | POST | Prometheus remote write |
| `/graphql` | POST | GraphQL API |
| `/graphql/playground` | GET | Interactive GraphQL playground |
| `/api/v1/studio/export` | POST | Export data from Chronicle Studio |
| `/api/v1/catalog/export` | GET | Export metrics catalog |
| `/api/v1/iceberg/export` | POST | Export data in Iceberg format |
| `/api/v1/forecast` | POST | Generate time-series forecasts |
| `/api/v1/histogram` | GET/POST | Histogram data and observations |
| `/api/v1/prom/labels` | GET | List label names (Prometheus-compatible) |
| `/api/v1/cql` | POST | Execute a CQL query |
| `/api/v1/cql/validate` | POST | Validate CQL syntax |
| `/api/v1/cql/explain` | POST | Explain a CQL query plan |
| `/api/v1/anomalies` | GET | List detected anomalies |
| `/api/v1/anomalies/stats` | GET | Anomaly detection statistics |
| `/api/v1/anomalies/baseline/{metric}` | GET | Get baseline for a metric |
| `/api/v1/incidents` | GET | List correlated incidents |
| `/metrics` | GET | List registered metric names |
| `/api/v1/views` | GET | List materialized views |
| `/api/v2/views` | GET | List materialized views (v2) |
| `/api/v1/planner/stats` | GET | Query planner statistics |
| `/api/v1/connectors` | GET | List connector hub connectors |
| `/api/v1/connectors/drivers` | GET | List available connector drivers |
| `/api/v1/notebooks` | GET | List notebooks |
| `/api/v1/compile` | POST | Compile a query to an execution plan |
| `/api/v1/compile/stats` | GET | Query compiler statistics |
| `/api/v1/rag/ask` | POST | Ask a natural-language question (RAG) |
| `/api/v1/rag/stats` | GET | RAG engine statistics |
| `/api/v1/plugins` | GET | List registered plugins |
| `/api/v1/fleet/agents` | GET | List fleet agents |
| `/api/v1/fleet/stats` | GET | Fleet manager statistics |
| `/api/v1/retention/stats` | GET | Smart retention statistics |
| `/api/v1/retention/profiles` | GET | List retention profiles |
| `/api/v1/retention/evaluate` | POST | Evaluate retention recommendations |
| `/api/v1/hardening/run` | POST | Run production hardening checks |
| `/api/v1/hardening/summary` | GET | Hardening suite summary |
| `/openapi.json` | GET | OpenAPI 3.0 specification (auto-generated) |
| `/swagger` | GET | Swagger UI for interactive API exploration |

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

### GET /health/ready

Readiness probe — indicates whether the instance is ready to accept traffic. Used by Kubernetes and load balancers to determine when to route requests to this instance.

**Response (200 OK):**
```json
{
  "status": "ready"
}
```

**Response (503 Service Unavailable):**
```json
{
  "status": "not_ready"
}
```

**Example:**
```bash
curl -s http://localhost:8086/health/ready
```

### GET /health/live

Liveness probe — indicates whether the instance is alive and should not be restarted. Used by Kubernetes to detect deadlocked or unresponsive instances.

**Response (200 OK):**
```json
{
  "status": "live"
}
```

**Response (503 Service Unavailable):**
```json
{
  "status": "not_live"
}
```

**Example:**
```bash
curl -s http://localhost:8086/health/live
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

### POST /prometheus/write

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

## Export API

Export endpoints are available through different subsystems:

### POST /api/v1/studio/export

Export time-series data via Chronicle Studio.

**Content-Type:** `application/json`

**Request Body:**
```json
{
  "metric": "cpu",
  "start": 1609459200000000000,
  "end": 1609462800000000000,
  "format": "csv",
  "tags": {"host": "server01"},
  "limit": 10000
}
```

**Parameters:**
- `metric` (required): Metric name to export
- `start` (optional): Start timestamp (nanoseconds)
- `end` (optional): End timestamp (nanoseconds)
- `format` (optional): Output format — `json`, `csv`, `parquet` (default: `json`)
- `tags` (optional): Tag filters
- `limit` (optional): Maximum number of points

**Response (200 OK):**
```json
{
  "format": "csv",
  "rows": 1500,
  "data": "timestamp,value,host\n1609459200000000000,0.64,server01\n..."
}
```

**Example:**
```bash
curl -X POST http://localhost:8086/api/v1/studio/export \
  -H "Content-Type: application/json" \
  -d '{"metric":"cpu","format":"json","limit":100}'
```

### GET /api/v1/catalog/export

Export the metrics catalog.

**Query Parameters:**
- `format` (optional): Output format — `json`, `csv` (default: `json`)

**Response (200 OK):**
```json
{
  "metrics": [
    {
      "name": "cpu",
      "type": "gauge",
      "labels": ["host", "region"],
      "description": "CPU usage percentage",
      "unit": "percent"
    }
  ],
  "total": 15
}
```

**Example:**
```bash
curl "http://localhost:8086/api/v1/catalog/export?format=json"
```

### POST /api/v1/iceberg/export

Export data in Apache Iceberg format for lakehouse integration.

**Content-Type:** `application/json`

**Request Body:**
```json
{
  "metric": "cpu",
  "start": 1609459200000000000,
  "end": 1609462800000000000,
  "catalog": "default",
  "namespace": "chronicle",
  "table": "cpu_metrics"
}
```

**Parameters:**
- `metric` (required): Metric name to export
- `start` / `end` (optional): Time range (nanosecond timestamps)
- `catalog` (optional): Iceberg catalog name (default: `default`)
- `namespace` (optional): Iceberg namespace
- `table` (optional): Target table name

**Response (200 OK):**
```json
{
  "status": "completed",
  "records_exported": 50000,
  "bytes_written": 2048576,
  "table": "chronicle.cpu_metrics",
  "snapshot_id": "snap-001"
}
```

**Example:**
```bash
curl -X POST http://localhost:8086/api/v1/iceberg/export \
  -H "Content-Type: application/json" \
  -d '{"metric":"cpu","catalog":"default","table":"cpu_export"}'
```

All export endpoints return `200 OK` on success or `400 Bad Request` for invalid parameters.

---

## Labels API

### GET /api/v1/prom/labels

List all known label names (Prometheus-compatible).

**Response:**
```json
{
  "status": "success",
  "data": ["__name__", "host", "region", "instance"]
}
```

---

## CQL Query Engine

Chronicle Query Language (CQL) provides a SQL-like interface for querying time-series data with time-series-specific extensions.

### POST /api/v1/cql

Execute a CQL query.

**Content-Type:** `text/plain`

**Request Body:**
```
SELECT metric, avg(value) FROM metrics WHERE time > now() - 1h GROUP BY metric
```

**Response (200 OK):**
```json
{
  "columns": ["metric", "avg"],
  "rows": [
    ["cpu", 42.5],
    ["memory", 78.2]
  ]
}
```

**Status Codes:**
- `200 OK`: Query executed successfully
- `400 Bad Request`: Invalid CQL syntax or query error
- `405 Method Not Allowed`: Only POST is accepted

**Example:**
```bash
curl -X POST http://localhost:8086/api/v1/cql \
  -d "SELECT * FROM cpu WHERE host = 'server01' LIMIT 10"
```

### POST /api/v1/cql/validate

Validate CQL query syntax without executing it.

**Content-Type:** `text/plain`

**Request Body:**
```
SELECT metric FROM metrics WHERE time > now() - 1h
```

**Response (valid):**
```json
{
  "valid": true
}
```

**Response (invalid):**
```json
{
  "valid": false,
  "error": "unexpected token at position 12"
}
```

**Example:**
```bash
curl -X POST http://localhost:8086/api/v1/cql/validate \
  -d "SELECT * FROM cpu"
```

### POST /api/v1/cql/explain

Get the query execution plan for a CQL query.

**Content-Type:** `text/plain`

**Request Body:**
```
SELECT avg(value) FROM cpu WHERE host = 'server01' GROUP BY time(5m)
```

**Response (200 OK):**
```json
{
  "plan": "Scan(cpu) → Filter(host=server01) → GroupBy(5m) → Aggregate(avg)",
  "estimated_cost": 150
}
```

**Example:**
```bash
curl -X POST http://localhost:8086/api/v1/cql/explain \
  -d "SELECT avg(value) FROM cpu GROUP BY time(5m)"
```

---

## Anomaly Detection

### GET /api/v1/anomalies

List detected anomalies from the streaming anomaly pipeline.

**Query Parameters:**
- `metric` (optional): Filter by metric name
- `since` (optional): Return anomalies after this timestamp (RFC 3339 format)
- `limit` (optional): Maximum number of results (default: 100)

**Response (200 OK):**
```json
[
  {
    "metric": "cpu",
    "value": 98.5,
    "expected": 45.0,
    "score": 3.2,
    "timestamp": "2026-01-15T10:30:00Z",
    "tags": {"host": "server01"}
  }
]
```

**Example:**
```bash
curl "http://localhost:8086/api/v1/anomalies?metric=cpu&since=2026-01-15T00:00:00Z&limit=50"
```

### GET /api/v1/anomalies/stats

Get anomaly detection pipeline statistics.

**Response (200 OK):**
```json
{
  "total_anomalies": 42,
  "metrics_monitored": 15,
  "detection_rate": 0.03
}
```

**Example:**
```bash
curl http://localhost:8086/api/v1/anomalies/stats
```

### GET /api/v1/anomalies/baseline/{metric}

Get the statistical baseline for a specific metric.

**Path Parameters:**
- `metric` (required): Metric name

**Response (200 OK):**
```json
{
  "metric": "cpu",
  "mean": 45.2,
  "stddev": 12.3,
  "q1": 35.0,
  "median": 44.5,
  "q3": 55.0
}
```

**Status Codes:**
- `200 OK`: Baseline returned
- `400 Bad Request`: Metric name not provided
- `404 Not Found`: No baseline exists for the metric

**Example:**
```bash
curl http://localhost:8086/api/v1/anomalies/baseline/cpu
```

---

## Incidents

### GET /api/v1/incidents

List correlated incidents from the anomaly correlation engine. Incidents group related anomalies across metrics that share a common root cause.

**Response (200 OK):**
```json
[
  {
    "id": "inc-001",
    "anomalies": ["cpu_spike", "memory_pressure"],
    "severity": "critical",
    "created_at": "2026-01-15T10:30:00Z",
    "status": "active"
  }
]
```

**Example:**
```bash
curl http://localhost:8086/api/v1/incidents
```

---

## Metrics List

### GET /metrics

List all registered metric names.

**Response:**
```json
["cpu", "memory", "disk", "network"]
```

---

## Materialized Views

### GET /api/v1/views

List all materialized views (v1).

**Response (200 OK):**
```json
[
  {
    "name": "avg_cpu_5m",
    "query": "SELECT avg(value) FROM cpu GROUP BY time(5m)",
    "status": "active",
    "last_refresh": "2026-01-15T10:30:00Z"
  }
]
```

**Example:**
```bash
curl http://localhost:8086/api/v1/views
```

### GET /api/v2/views

List all materialized views (v2 engine with incremental refresh support).

**Response (200 OK):**
```json
[
  {
    "name": "avg_cpu_5m",
    "query": "SELECT avg(value) FROM cpu GROUP BY time(5m)",
    "status": "active",
    "refresh_mode": "incremental",
    "last_refresh": "2026-01-15T10:30:00Z"
  }
]
```

**Example:**
```bash
curl http://localhost:8086/api/v2/views
```

---

## Query Planner

### GET /api/v1/planner/stats

Get query planner statistics including cache hit rates and optimization metrics.

**Response (200 OK):**
```json
{
  "queries_planned": 1500,
  "cache_hits": 1200,
  "cache_misses": 300,
  "avg_plan_time_ms": 2.5
}
```

**Example:**
```bash
curl http://localhost:8086/api/v1/planner/stats
```

---

## Connector Hub

### GET /api/v1/connectors

List all registered connectors.

**Response (200 OK):**
```json
[
  {
    "id": "pg-source-1",
    "driver": "postgres",
    "status": "running",
    "direction": "source"
  }
]
```

**Example:**
```bash
curl http://localhost:8086/api/v1/connectors
```

### GET /api/v1/connectors/drivers

List available connector drivers.

**Response (200 OK):**
```json
[
  {
    "name": "postgres",
    "type": "source",
    "version": "1.0.0"
  },
  {
    "name": "kafka",
    "type": "sink",
    "version": "1.0.0"
  }
]
```

**Example:**
```bash
curl http://localhost:8086/api/v1/connectors/drivers
```

---

## Notebooks

### GET /api/v1/notebooks

List all notebooks.

**Response (200 OK):**
```json
[
  {
    "id": "nb-001",
    "title": "CPU Analysis",
    "created_at": "2026-01-15T10:00:00Z",
    "cells": 5
  }
]
```

**Example:**
```bash
curl http://localhost:8086/api/v1/notebooks
```

---

## Query Compiler

### POST /api/v1/compile

Compile a query into an execution plan without executing it.

**Content-Type:** `text/plain`

**Request Body:**
```
SELECT avg(value) FROM cpu WHERE host = 'server01' GROUP BY time(5m)
```

**Response (200 OK):**
```json
{
  "plan": "Scan(cpu) → Filter(host=server01) → GroupBy(5m) → Aggregate(avg)",
  "estimated_cost": 120,
  "optimizations_applied": ["predicate_pushdown", "projection_pruning"]
}
```

**Status Codes:**
- `200 OK`: Compilation successful
- `400 Bad Request`: Invalid query syntax
- `405 Method Not Allowed`: Only POST is accepted

**Example:**
```bash
curl -X POST http://localhost:8086/api/v1/compile \
  -d "SELECT avg(value) FROM cpu GROUP BY time(5m)"
```

### GET /api/v1/compile/stats

Get query compiler statistics.

**Response (200 OK):**
```json
{
  "total_compilations": 500,
  "cache_hits": 350,
  "avg_compile_time_ms": 1.2
}
```

**Example:**
```bash
curl http://localhost:8086/api/v1/compile/stats
```

---

## Time-Series RAG

### POST /api/v1/rag/ask

Ask a natural-language question about your time-series data using Retrieval-Augmented Generation.

**Content-Type:** `application/json`

**Request Body:**
```json
{
  "question": "What caused the CPU spike yesterday at 3pm?",
  "context_window": "24h",
  "max_results": 5
}
```

**Response (200 OK):**
```json
{
  "answer": "The CPU spike at 15:00 correlates with a deployment event...",
  "sources": [
    {
      "metric": "cpu",
      "timestamp": "2026-01-14T15:00:00Z",
      "value": 98.5
    }
  ],
  "confidence": 0.85
}
```

**Status Codes:**
- `200 OK`: Answer generated
- `400 Bad Request`: Invalid request body
- `405 Method Not Allowed`: Only POST is accepted

**Example:**
```bash
curl -X POST http://localhost:8086/api/v1/rag/ask \
  -H "Content-Type: application/json" \
  -d '{"question":"What is the average CPU usage today?"}'
```

### GET /api/v1/rag/stats

Get RAG engine statistics.

**Response (200 OK):**
```json
{
  "total_queries": 200,
  "avg_response_time_ms": 450,
  "cache_hit_rate": 0.3
}
```

**Example:**
```bash
curl http://localhost:8086/api/v1/rag/stats
```

---

## Plugin Registry

### GET /api/v1/plugins

List all registered plugins.

**Response (200 OK):**
```json
[
  {
    "name": "custom-aggregator",
    "version": "1.2.0",
    "status": "active",
    "type": "aggregation"
  }
]
```

**Example:**
```bash
curl http://localhost:8086/api/v1/plugins
```

---

## Fleet Manager

### GET /api/v1/fleet/agents

List all fleet agents.

**Response (200 OK):**
```json
[
  {
    "id": "agent-001",
    "hostname": "edge-node-1",
    "status": "healthy",
    "last_heartbeat": "2026-01-15T10:29:55Z",
    "version": "1.0.0"
  }
]
```

**Example:**
```bash
curl http://localhost:8086/api/v1/fleet/agents
```

### GET /api/v1/fleet/stats

Get fleet manager statistics.

**Response (200 OK):**
```json
{
  "total_agents": 50,
  "healthy_agents": 48,
  "unhealthy_agents": 2,
  "avg_heartbeat_latency_ms": 120
}
```

**Example:**
```bash
curl http://localhost:8086/api/v1/fleet/stats
```

---

## Smart Retention

### GET /api/v1/retention/stats

Get smart retention engine statistics.

**Response (200 OK):**
```json
{
  "metrics_managed": 15,
  "profiles_active": 3,
  "bytes_reclaimed": 1073741824,
  "last_evaluation": "2026-01-15T10:00:00Z"
}
```

**Example:**
```bash
curl http://localhost:8086/api/v1/retention/stats
```

### GET /api/v1/retention/profiles

List all retention profiles.

**Response (200 OK):**
```json
[
  {
    "name": "default",
    "retention": "30d",
    "downsample_after": "7d",
    "metrics_matched": 10
  }
]
```

**Example:**
```bash
curl http://localhost:8086/api/v1/retention/profiles
```

### POST /api/v1/retention/evaluate

Evaluate retention recommendations for the current data.

**Response (200 OK):**
```json
{
  "recommendations": [
    {
      "metric": "cpu",
      "current_retention": "90d",
      "suggested_retention": "30d",
      "estimated_savings_bytes": 536870912
    }
  ]
}
```

**Status Codes:**
- `200 OK`: Evaluation complete
- `405 Method Not Allowed`: Only POST is accepted

**Example:**
```bash
curl -X POST http://localhost:8086/api/v1/retention/evaluate
```

---

## Production Hardening

### POST /api/v1/hardening/run

Run all production hardening checks.

**Response (200 OK):**
```json
{
  "checks": [
    {
      "name": "wal_sync",
      "status": "pass",
      "details": "WAL sync is enabled"
    },
    {
      "name": "tls_enabled",
      "status": "fail",
      "details": "TLS is not configured"
    }
  ],
  "passed": 8,
  "failed": 2,
  "total": 10
}
```

**Status Codes:**
- `200 OK`: Checks completed
- `405 Method Not Allowed`: Only POST is accepted

**Example:**
```bash
curl -X POST http://localhost:8086/api/v1/hardening/run
```

### GET /api/v1/hardening/summary

Get a summary of the last hardening check results.

**Response (200 OK):**
```json
{
  "score": 80,
  "passed": 8,
  "failed": 2,
  "last_run": "2026-01-15T10:30:00Z"
}
```

**Example:**
```bash
curl http://localhost:8086/api/v1/hardening/summary
```

---

## API Discovery

### GET /openapi.json

Returns the auto-generated OpenAPI 3.0 specification for the Chronicle API. Use this to generate client SDKs, import into Postman, or integrate with API gateways.

**Response:** `200 OK` with `application/json` OpenAPI 3.0.3 document.

**Example:**
```bash
curl http://localhost:8086/openapi.json
```

### GET /swagger

Serves an interactive Swagger UI for exploring and testing the API.

**Example:**

Open `http://localhost:8086/swagger` in a browser.

> **Note:** Both `/openapi.json` and `/swagger` are registered automatically when the HTTP server starts. No additional configuration is required.
