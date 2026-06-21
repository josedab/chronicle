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

---

## PromQL Examples

Chronicle supports PromQL via the `/api/v1/query` (instant) and `/api/v1/query_range` (range) endpoints.
These examples assume metrics are ingested via the Prometheus remote write endpoint (`/prometheus/write`).

---

### Basic Selectors

#### 1. Select a metric
```promql
http_requests_total
```
Returns the current value of all time series with the metric name `http_requests_total`.

```bash
curl "http://localhost:8086/api/v1/query?query=http_requests_total"
```

#### 2. Filter by label
```promql
http_requests_total{method="GET"}
```
Returns only time series where the `method` label equals `GET`.

```bash
curl "http://localhost:8086/api/v1/query?query=http_requests_total%7Bmethod%3D%22GET%22%7D"
```

#### 3. Negative label matcher
```promql
http_requests_total{method!="OPTIONS"}
```
Returns all time series except those with `method="OPTIONS"`.

#### 4. Regex label matcher
```promql
http_requests_total{path=~"/api/.*"}
```
Matches time series where `path` starts with `/api/`.

#### 5. Negative regex matcher
```promql
http_requests_total{path!~"/health.*"}
```
Excludes time series matching the `/health.*` pattern.

---

### Rates and Counters

#### 6. Rate of increase (per-second average)
```promql
rate(http_requests_total[5m])
```
Computes the per-second average rate of increase over the last 5 minutes. Ideal for counters.

```bash
curl "http://localhost:8086/api/v1/query?query=rate(http_requests_total%5B5m%5D)"
```

#### 7. Increase over time window
```promql
increase(http_requests_total[1h])
```
Returns the total increase in the counter over the last hour.

#### 8. irate — instant rate
```promql
irate(http_requests_total[5m])
```
Computes the per-second rate using only the two most recent samples. More responsive to changes than `rate`.

---

### Aggregation

#### 9. Sum across all instances
```promql
sum(rate(http_requests_total[5m]))
```
Sums the rate across all label dimensions, giving the aggregate request rate.

#### 10. Sum by label
```promql
sum by (method) (rate(http_requests_total[5m]))
```
Sums the rate grouped by `method`, showing per-method aggregate rates.

```bash
curl "http://localhost:8086/api/v1/query?query=sum+by+(method)+(rate(http_requests_total%5B5m%5D))"
```

#### 11. Top-k
```promql
topk(5, rate(http_requests_total[5m]))
```
Returns the 5 time series with the highest per-second rate.

#### 12. Bottom-k
```promql
bottomk(5, rate(http_requests_total[5m]))
```
Returns the 5 time series with the lowest per-second rate.

---

### Gauge Operations

#### 13. Average over time
```promql
avg_over_time(cpu_usage_percent[10m])
```
Computes the average of all samples in the last 10 minutes for a gauge metric.

#### 14. Max / Min over time
```promql
max_over_time(memory_usage_bytes[1h])
```
Returns the maximum value observed in the last hour.

```promql
min_over_time(memory_usage_bytes[1h])
```
Returns the minimum value observed in the last hour.

---

### Histograms

#### 15. Histogram quantile
```promql
histogram_quantile(0.95, rate(request_duration_seconds_bucket[5m]))
```
Computes the 95th percentile of request duration over the last 5 minutes.

```bash
curl "http://localhost:8086/api/v1/query?query=histogram_quantile(0.95%2C+rate(request_duration_seconds_bucket%5B5m%5D))"
```

---

### Arithmetic and Prediction

#### 16. Ratio / percentage
```promql
rate(http_errors_total[5m]) / rate(http_requests_total[5m])
```
Computes the error rate as a fraction of total requests.

#### 17. Predict linear growth
```promql
predict_linear(disk_usage_bytes[1h], 86400)
```
Predicts disk usage 24 hours (86400 seconds) from now based on the last hour's trend.

---

### Range Queries

#### 18. Range query with step
```promql
rate(http_requests_total[5m])
```
When used with `/api/v1/query_range`, returns a matrix of rate values over time:

```bash
curl "http://localhost:8086/api/v1/query_range?query=rate(http_requests_total%5B5m%5D)&start=$(date -d '1 hour ago' +%s)&end=$(date +%s)&step=60"
```

#### 19. Offset modifier
```promql
rate(http_requests_total[5m] offset 1h)
```
Compares the current 5-minute rate against the rate from 1 hour ago.

#### 20. Arithmetic between offset series
```promql
rate(http_requests_total[5m]) - rate(http_requests_total[5m] offset 1h)
```
Shows the delta in request rate compared to 1 hour ago.

---

### Modifiers

#### 21. @ modifier — evaluate at specific time
```promql
http_requests_total @ 1717200000
```
Evaluates the instant vector at the specified Unix timestamp.

---

## Schema Registry
