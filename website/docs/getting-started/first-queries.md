---
sidebar_position: 3
---

# First Queries

Learn Chronicle's query capabilities, from basic data retrieval to complex aggregations.

## Basic Query

Retrieve all points for a metric within a time range:

```go
result, err := db.Execute(&chronicle.Query{
    Metric: "cpu_usage",
    Start:  time.Now().Add(-time.Hour).UnixNano(),
    End:    time.Now().UnixNano(),
})

for _, p := range result.Points {
    fmt.Printf("%s: %.2f\n", time.Unix(0, p.Timestamp), p.Value)
}
```

## Filter by Tags

Filter to specific tag values:

```go
result, _ := db.Execute(&chronicle.Query{
    Metric: "cpu_usage",
    Tags:   map[string]string{"host": "server-01", "region": "us-west"},
    Start:  time.Now().Add(-time.Hour).UnixNano(),
    End:    time.Now().UnixNano(),
})
```

Use tag filters for more complex matching:

```go
result, _ := db.Execute(&chronicle.Query{
    Metric: "cpu_usage",
    TagFilters: []chronicle.TagFilter{
        {Key: "host", Op: chronicle.TagOpEq, Values: []string{"server-01"}},
        {Key: "env", Op: chronicle.TagOpNotEq, Values: []string{"dev"}},
        {Key: "region", Op: chronicle.TagOpIn, Values: []string{"us-west", "us-east"}},
    },
    Start: time.Now().Add(-time.Hour).UnixNano(),
    End:   time.Now().UnixNano(),
})
```

**Tag Operators:**
| Operator | Description |
|----------|-------------|
| `TagOpEq` | Equals |
| `TagOpNotEq` | Not equals |
| `TagOpIn` | In list of values |

## Aggregations

Aggregate data over time windows:

```go
result, _ := db.Execute(&chronicle.Query{
    Metric: "cpu_usage",
    Start:  time.Now().Add(-24 * time.Hour).UnixNano(),
    End:    time.Now().UnixNano(),
    Aggregation: &chronicle.Aggregation{
        Function: chronicle.AggMean,
        Window:   time.Hour,  // Hourly buckets
    },
})
```

**Available Aggregation Functions:**

| Function | Description |
|----------|-------------|
| `AggCount` | Count of points |
| `AggSum` | Sum of values |
| `AggMean` | Average value |
| `AggMin` | Minimum value |
| `AggMax` | Maximum value |
| `AggStddev` | Standard deviation |
| `AggRate` | Rate of change per second |
| `AggFirst` | First value in window |
| `AggLast` | Last value in window |
| `AggPercentile` | Percentile calculation |

## Group By Tags

Group results by tag values:

```go
result, _ := db.Execute(&chronicle.Query{
    Metric:  "cpu_usage",
    Start:   time.Now().Add(-time.Hour).UnixNano(),
    End:     time.Now().UnixNano(),
    GroupBy: []string{"host"},
    Aggregation: &chronicle.Aggregation{
        Function: chronicle.AggMean,
        Window:   5 * time.Minute,
    },
})

// Results include host tag in each point
for _, p := range result.Points {
    fmt.Printf("[%s] %.2f\n", p.Tags["host"], p.Value)
}
```

## Limit Results

Limit the number of returned points:

```go
result, _ := db.Execute(&chronicle.Query{
    Metric: "cpu_usage",
    Start:  time.Now().Add(-24 * time.Hour).UnixNano(),
    End:    time.Now().UnixNano(),
    Limit:  100,  // Return at most 100 points
})
```

## SQL-Like Syntax

Chronicle supports a SQL-like query language:

```go
parser := &chronicle.QueryParser{}

// Basic select
query, _ := parser.Parse(`SELECT * FROM cpu_usage WHERE time > now() - 1h`)

// With aggregation
query, _ = parser.Parse(`
    SELECT mean(value) 
    FROM cpu_usage 
    WHERE host = 'server-01' AND region = 'us-west'
    GROUP BY time(5m)
`)

// With tag grouping
query, _ = parser.Parse(`
    SELECT max(value) 
    FROM cpu_usage 
    GROUP BY time(1h), host
    LIMIT 100
`)

result, _ := db.Execute(query)
```

**Supported SQL syntax:**
- `SELECT` - `*`, `mean()`, `sum()`, `min()`, `max()`, `count()`, `rate()`, `stddev()`, `first()`, `last()`
- `FROM` - metric name
- `WHERE` - tag filters and time conditions
- `GROUP BY` - `time(interval)` and tag names
- `LIMIT` - max results

## PromQL Queries

For Prometheus compatibility, use the PromQL executor:

```go
executor := chronicle.NewPromQLExecutor(db)

// Instant query
result, _ := executor.Query(`cpu_usage{host="server-01"}`, time.Now())

// Range query with rate
result, _ = executor.QueryRange(
    `rate(http_requests_total[5m])`,
    time.Now().Add(-time.Hour),
    time.Now(),
    time.Minute,
)

// Aggregation across series
result, _ = executor.Query(`sum by (region) (cpu_usage)`, time.Now())
```

**Supported PromQL features:**
- Label matchers: `=`, `!=`, `=~`, `!~`
- Range vectors: `[5m]`, `[1h]`
- Functions: `rate()`, `sum()`, `avg()`, `min()`, `max()`, `count()`
- Aggregation operators: `by`, `without`

## HTTP Query API

Query via HTTP:

```bash
# JSON query
curl -X POST http://localhost:8086/query \
  -H "Content-Type: application/json" \
  -d '{
    "metric": "cpu_usage",
    "tags": {"host": "server-01"},
    "start": 1700000000000000000,
    "end": 1700100000000000000
  }'

# SQL-like query
curl -X POST http://localhost:8086/query \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT mean(value) FROM cpu_usage GROUP BY time(5m)"}'

# PromQL (Prometheus-compatible endpoint)
curl "http://localhost:8086/api/v1/query?query=cpu_usage{host='server-01'}"
curl "http://localhost:8086/api/v1/query_range?query=rate(cpu_usage[5m])&start=2024-01-01T00:00:00Z&end=2024-01-02T00:00:00Z&step=60"
```

## GraphQL Queries

Chronicle also provides a GraphQL API:

```graphql
query {
  query(
    metric: "cpu_usage"
    start: "2024-01-01T00:00:00Z"
    end: "2024-01-02T00:00:00Z"
    tags: {host: "server-01"}
  ) {
    points {
      timestamp
      value
      tags
    }
  }
}
```

Access the GraphQL playground at `http://localhost:8086/graphql/playground`.

## What's Next?

- [Data Model](/docs/core-concepts/data-model) - Understand metrics, tags, and series
- [Queries Deep Dive](/docs/core-concepts/queries) - Advanced query patterns
- [HTTP API Reference](/docs/api-reference/http-endpoints) - Complete endpoint documentation
