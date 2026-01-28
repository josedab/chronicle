---
sidebar_position: 4
---

# Queries

Chronicle provides multiple query interfaces: a native Go API, SQL-like syntax, PromQL, and GraphQL. This page covers query capabilities in depth.

## Query Structure

Every query has these components:

```go
type Query struct {
    Metric      string             // Required: metric name
    Tags        map[string]string  // Simple tag equality filters
    TagFilters  []TagFilter        // Advanced tag filters
    Start       int64              // Start time (unix nanos)
    End         int64              // End time (unix nanos)
    Aggregation *Aggregation       // Optional aggregation
    GroupBy     []string           // Group by tag keys
    Limit       int                // Max results
}
```

## Time Ranges

Specify time ranges in Unix nanoseconds:

```go
// Last hour
query := &chronicle.Query{
    Metric: "cpu_usage",
    Start:  time.Now().Add(-time.Hour).UnixNano(),
    End:    time.Now().UnixNano(),
}

// Specific date range
start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
end := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
query := &chronicle.Query{
    Metric: "cpu_usage",
    Start:  start.UnixNano(),
    End:    end.UnixNano(),
}
```

:::tip
Use `0` for Start to query from the beginning of time, and `time.Now().UnixNano()` for End to include the latest data.
:::

## Tag Filtering

### Simple Equality

```go
query := &chronicle.Query{
    Metric: "http_requests",
    Tags: map[string]string{
        "method": "GET",
        "status": "200",
    },
}
```

### Advanced Filters

Use `TagFilters` for more complex matching:

```go
query := &chronicle.Query{
    Metric: "http_requests",
    TagFilters: []chronicle.TagFilter{
        // Equals
        {Key: "method", Op: chronicle.TagOpEq, Values: []string{"GET"}},
        
        // Not equals
        {Key: "env", Op: chronicle.TagOpNotEq, Values: []string{"dev"}},
        
        // In list
        {Key: "region", Op: chronicle.TagOpIn, Values: []string{"us-west", "us-east", "eu-west"}},
    },
}
```

## Aggregation Functions

Aggregate data over time windows:

```go
query := &chronicle.Query{
    Metric: "cpu_usage",
    Start:  time.Now().Add(-24 * time.Hour).UnixNano(),
    End:    time.Now().UnixNano(),
    Aggregation: &chronicle.Aggregation{
        Function: chronicle.AggMean,
        Window:   time.Hour,
    },
}
```

### Available Functions

| Function | Description | Example Use |
|----------|-------------|-------------|
| `AggCount` | Number of points | Request count |
| `AggSum` | Sum of values | Total bytes |
| `AggMean` | Average value | Average latency |
| `AggMin` | Minimum value | Lowest temperature |
| `AggMax` | Maximum value | Peak CPU |
| `AggStddev` | Standard deviation | Variance analysis |
| `AggRate` | Per-second rate | Requests/sec from counter |
| `AggFirst` | First value in window | Opening price |
| `AggLast` | Last value in window | Closing price |

### Rate Calculation

For counter metrics, use `AggRate`:

```go
// Get requests per second from a counter
query := &chronicle.Query{
    Metric: "http_requests_total",
    Aggregation: &chronicle.Aggregation{
        Function: chronicle.AggRate,
        Window:   time.Minute,
    },
}
```

## Grouping

Group results by tag values:

```go
// Average CPU per host
query := &chronicle.Query{
    Metric:  "cpu_usage",
    GroupBy: []string{"host"},
    Aggregation: &chronicle.Aggregation{
        Function: chronicle.AggMean,
        Window:   5 * time.Minute,
    },
}

result, _ := db.Execute(query)
for _, p := range result.Points {
    fmt.Printf("Host %s: %.2f%%\n", p.Tags["host"], p.Value)
}
```

### Multiple Group Keys

```go
// Average latency per endpoint and method
query := &chronicle.Query{
    Metric:  "request_latency",
    GroupBy: []string{"endpoint", "method"},
    Aggregation: &chronicle.Aggregation{
        Function: chronicle.AggMean,
        Window:   time.Minute,
    },
}
```

## SQL-Like Queries

Chronicle supports SQL-like syntax:

```go
parser := &chronicle.QueryParser{}

// Basic query
q, _ := parser.Parse(`SELECT * FROM cpu_usage WHERE time > now() - 1h`)

// With aggregation
q, _ = parser.Parse(`
    SELECT mean(value) 
    FROM cpu_usage 
    WHERE host = 'server-01'
    GROUP BY time(5m)
`)

// Multiple conditions
q, _ = parser.Parse(`
    SELECT max(value) 
    FROM http_latency 
    WHERE status != '500' AND region IN ('us-west', 'eu-west')
    GROUP BY time(1h), endpoint
    LIMIT 100
`)

result, _ := db.Execute(q)
```

### SQL Syntax Reference

```sql
SELECT <function>(<field>) | *
FROM <metric>
WHERE <conditions>
GROUP BY time(<interval>), <tags>
LIMIT <n>
```

**Functions:** `mean()`, `sum()`, `min()`, `max()`, `count()`, `rate()`, `stddev()`, `first()`, `last()`

**Conditions:** `=`, `!=`, `>`, `<`, `>=`, `<=`, `IN`, `AND`, `OR`

**Time expressions:** `now()`, `now() - 1h`, `now() - 7d`

## PromQL Queries

For Prometheus compatibility:

```go
executor := chronicle.NewPromQLExecutor(db)

// Instant query
result, _ := executor.Query(
    `http_requests_total{method="GET"}`,
    time.Now(),
)

// Range query
result, _ = executor.QueryRange(
    `rate(http_requests_total[5m])`,
    time.Now().Add(-time.Hour),
    time.Now(),
    time.Minute,  // Step
)

// Aggregation
result, _ = executor.Query(
    `sum by (region) (rate(http_requests_total[5m]))`,
    time.Now(),
)
```

### Supported PromQL Features

**Selectors:**
- `metric_name` - Select by name
- `{label="value"}` - Label equality
- `{label!="value"}` - Label inequality
- `{label=~"regex"}` - Regex match
- `{label!~"regex"}` - Negative regex

**Range vectors:**
- `[5m]` - 5 minutes
- `[1h]` - 1 hour
- `[7d]` - 7 days

**Functions:**
- `rate()`, `irate()` - Per-second rate
- `sum()`, `avg()`, `min()`, `max()`, `count()`
- `increase()` - Total increase
- `histogram_quantile()` - Percentiles

**Aggregation:**
- `by (label)` - Group by labels
- `without (label)` - Group excluding labels

## Query Optimization

### Use Time Bounds

Always specify time ranges to limit partition scans:

```go
// ❌ Scans all partitions
query := &chronicle.Query{Metric: "cpu_usage"}

// ✅ Only scans relevant partitions
query := &chronicle.Query{
    Metric: "cpu_usage",
    Start:  time.Now().Add(-time.Hour).UnixNano(),
    End:    time.Now().UnixNano(),
}
```

### Use Aggregation Server-Side

Aggregate in the database, not in your code:

```go
// ❌ Fetches all points, aggregates in app
result, _ := db.Execute(&Query{Metric: "cpu_usage", ...})
sum := 0.0
for _, p := range result.Points {
    sum += p.Value
}
avg := sum / float64(len(result.Points))

// ✅ Aggregates in database
result, _ := db.Execute(&Query{
    Metric: "cpu_usage",
    Aggregation: &Aggregation{Function: AggMean, Window: time.Hour},
})
```

### Limit Results

Use `Limit` to cap result size:

```go
query := &chronicle.Query{
    Metric: "cpu_usage",
    Limit:  1000,  // At most 1000 points
}
```

### Use Tags Effectively

Queries with tag filters are faster than full scans:

```go
// ✅ Fast: Uses tag index
query := &chronicle.Query{
    Metric: "cpu_usage",
    Tags:   map[string]string{"host": "server-01"},
}

// Slower: Must scan all series
query := &chronicle.Query{
    Metric: "cpu_usage",
}
```

## Query Timeouts

Configure query timeout to prevent runaway queries:

```go
db, _ := chronicle.Open("data.db", chronicle.Config{
    QueryTimeout: 30 * time.Second,
})

// Query that takes too long
result, err := db.Execute(expensiveQuery)
// err: "query timeout"
```

## What's Next?

- [Retention](/docs/core-concepts/retention) - Configure data retention
- [HTTP API](/docs/guides/http-api) - Query via HTTP
- [PromQL Integration](/docs/guides/prometheus-integration) - Prometheus compatibility
