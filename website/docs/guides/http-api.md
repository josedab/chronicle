---
sidebar_position: 1
---

# HTTP API Guide

This guide covers practical usage of Chronicle's HTTP API for common scenarios.

## Enabling the HTTP API

```go
db, _ := chronicle.Open("data.db", chronicle.Config{
    HTTPEnabled: true,
    HTTPPort:    8086,
})
```

The server starts automatically and listens on `127.0.0.1:8086`.

## Writing Data

### Line Protocol

Chronicle supports InfluxDB line protocol:

```
<measurement>,<tag_key>=<tag_value> <field_key>=<field_value> <timestamp>
```

**Examples:**

```bash
# Single point
curl -X POST http://localhost:8086/write \
  -d 'cpu_usage,host=server-01,region=us-west value=45.7'

# Multiple points
curl -X POST http://localhost:8086/write \
  -d 'cpu_usage,host=server-01 value=45.7
cpu_usage,host=server-02 value=32.1
memory_used,host=server-01 value=8589934592'

# With explicit timestamp (nanoseconds)
curl -X POST http://localhost:8086/write \
  -d 'cpu_usage,host=server-01 value=45.7 1706400000000000000'
```

### JSON Format

For programmatic access:

```bash
curl -X POST http://localhost:8086/write \
  -H "Content-Type: application/json" \
  -d '{
    "points": [
      {
        "metric": "cpu_usage",
        "tags": {"host": "server-01", "region": "us-west"},
        "value": 45.7,
        "timestamp": 1706400000000000000
      },
      {
        "metric": "memory_used",
        "tags": {"host": "server-01"},
        "value": 8589934592
      }
    ]
  }'
```

### Compression

For large payloads, use gzip:

```bash
# Compress data
echo 'cpu_usage,host=server-01 value=45.7' | gzip > data.gz

# Send compressed
curl -X POST http://localhost:8086/write \
  -H "Content-Encoding: gzip" \
  --data-binary @data.gz
```

## Querying Data

### Basic Queries

```bash
# All points for a metric
curl -X POST http://localhost:8086/query \
  -H "Content-Type: application/json" \
  -d '{"metric": "cpu_usage"}'

# With time range
curl -X POST http://localhost:8086/query \
  -H "Content-Type: application/json" \
  -d '{
    "metric": "cpu_usage",
    "start": 1706400000000000000,
    "end": 1706486400000000000
  }'

# With tag filters
curl -X POST http://localhost:8086/query \
  -H "Content-Type: application/json" \
  -d '{
    "metric": "cpu_usage",
    "tags": {"host": "server-01"}
  }'
```

### SQL-Like Queries

```bash
# Simple select
curl -X POST http://localhost:8086/query \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM cpu_usage WHERE time > now() - 1h"}'

# With aggregation
curl -X POST http://localhost:8086/query \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT mean(value) FROM cpu_usage GROUP BY time(5m), host"}'

# Complex query
curl -X POST http://localhost:8086/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "SELECT max(value) FROM cpu_usage WHERE host = '\''server-01'\'' AND region IN ('\''us-west'\'', '\''us-east'\'') GROUP BY time(1h) LIMIT 24"
  }'
```

### PromQL Queries

Use Prometheus-compatible endpoints:

```bash
# Instant query
curl "http://localhost:8086/api/v1/query?query=cpu_usage{host='server-01'}"

# Range query
curl "http://localhost:8086/api/v1/query_range?query=rate(http_requests_total[5m])&start=2024-01-28T00:00:00Z&end=2024-01-28T12:00:00Z&step=60"
```

## Client Examples

### Python

```python
import requests
import time

# Write data
def write_point(metric, tags, value):
    data = {
        "points": [{
            "metric": metric,
            "tags": tags,
            "value": value,
            "timestamp": int(time.time() * 1e9)
        }]
    }
    response = requests.post(
        "http://localhost:8086/write",
        json=data
    )
    return response.status_code == 202

# Query data
def query(metric, start_hours_ago=1):
    start = int((time.time() - start_hours_ago * 3600) * 1e9)
    end = int(time.time() * 1e9)
    
    response = requests.post(
        "http://localhost:8086/query",
        json={
            "metric": metric,
            "start": start,
            "end": end
        }
    )
    return response.json()

# Usage
write_point("cpu_usage", {"host": "server-01"}, 45.7)
result = query("cpu_usage")
for point in result["points"]:
    print(f"{point['metric']}: {point['value']}")
```

### JavaScript/Node.js

```javascript
const axios = require('axios');

const BASE_URL = 'http://localhost:8086';

// Write data
async function writePoint(metric, tags, value) {
  const data = {
    points: [{
      metric,
      tags,
      value,
      timestamp: Date.now() * 1e6  // Convert to nanoseconds
    }]
  };
  
  const response = await axios.post(`${BASE_URL}/write`, data);
  return response.status === 202;
}

// Query data
async function query(metric, startHoursAgo = 1) {
  const now = Date.now() * 1e6;
  const start = now - (startHoursAgo * 3600 * 1e9);
  
  const response = await axios.post(`${BASE_URL}/query`, {
    metric,
    start,
    end: now
  });
  
  return response.data;
}

// Usage
(async () => {
  await writePoint('cpu_usage', { host: 'server-01' }, 45.7);
  const result = await query('cpu_usage');
  result.points.forEach(p => console.log(`${p.metric}: ${p.value}`));
})();
```

### Go

```go
package main

import (
    "bytes"
    "encoding/json"
    "fmt"
    "net/http"
    "time"
)

type Point struct {
    Metric    string            `json:"metric"`
    Tags      map[string]string `json:"tags"`
    Value     float64           `json:"value"`
    Timestamp int64             `json:"timestamp"`
}

type WriteRequest struct {
    Points []Point `json:"points"`
}

type QueryRequest struct {
    Metric string `json:"metric"`
    Start  int64  `json:"start"`
    End    int64  `json:"end"`
}

func writePoint(metric string, tags map[string]string, value float64) error {
    req := WriteRequest{
        Points: []Point{{
            Metric:    metric,
            Tags:      tags,
            Value:     value,
            Timestamp: time.Now().UnixNano(),
        }},
    }
    
    body, _ := json.Marshal(req)
    resp, err := http.Post(
        "http://localhost:8086/write",
        "application/json",
        bytes.NewReader(body),
    )
    if err != nil {
        return err
    }
    defer resp.Body.Close()
    return nil
}

func main() {
    writePoint("cpu_usage", map[string]string{"host": "server-01"}, 45.7)
}
```

### cURL Cheat Sheet

```bash
# Write single point
curl -X POST localhost:8086/write -d 'cpu,host=srv1 value=45.7'

# Write JSON
curl -X POST localhost:8086/write -H "Content-Type: application/json" \
  -d '{"points":[{"metric":"cpu","tags":{"host":"srv1"},"value":45.7}]}'

# Query last hour
curl -X POST localhost:8086/query -H "Content-Type: application/json" \
  -d '{"query":"SELECT * FROM cpu WHERE time > now() - 1h"}'

# PromQL query
curl "localhost:8086/api/v1/query?query=cpu{host='srv1'}"

# Health check
curl localhost:8086/health

# List metrics
curl localhost:8086/metrics
```

## Batch Operations

For high-throughput scenarios, batch your writes:

```bash
# Generate 1000 points and write in one request
for i in $(seq 1 1000); do
  echo "cpu_usage,host=server-$((i % 10)) value=$((RANDOM % 100))"
done | curl -X POST http://localhost:8086/write --data-binary @-
```

## Error Handling

Handle common errors:

```python
import requests

def safe_write(data):
    try:
        response = requests.post(
            "http://localhost:8086/write",
            json=data,
            timeout=5
        )
        if response.status_code == 202:
            return True
        elif response.status_code == 400:
            print(f"Invalid data: {response.text}")
        elif response.status_code == 500:
            print(f"Server error: {response.text}")
        return False
    except requests.exceptions.Timeout:
        print("Request timed out")
        return False
    except requests.exceptions.ConnectionError:
        print("Connection failed")
        return False
```

## Best Practices

1. **Batch writes** - Send multiple points per request
2. **Use compression** - Enable gzip for large payloads
3. **Set timeouts** - Avoid hanging on network issues
4. **Retry with backoff** - Handle transient failures
5. **Monitor response codes** - Watch for 5xx errors
