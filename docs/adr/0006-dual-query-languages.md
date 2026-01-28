# ADR-0006: Dual Query Language Support (SQL-like + PromQL Subset)

## Status

Accepted

## Context

Chronicle serves users from different backgrounds:

1. **Operations/SRE teams**: Familiar with Prometheus and PromQL
2. **Data engineers**: Familiar with SQL and relational databases
3. **Application developers**: May know either or neither

Choosing a single query language would create adoption barriers:

- **PromQL only**: Powerful for time-series but unfamiliar to SQL users, limited for ad-hoc analysis
- **SQL only**: Familiar but verbose for time-series patterns, no existing dashboard compatibility
- **Custom DSL**: Maximum flexibility but zero familiarity, documentation burden

We also needed to consider integration requirements:
- Grafana supports both PromQL and SQL datasources
- Prometheus alerting rules use PromQL
- Business intelligence tools expect SQL

## Decision

Chronicle implements **both a SQL-like query syntax and a PromQL subset**:

### SQL-like Syntax

```sql
SELECT mean(value) FROM cpu_usage 
WHERE host = 'server1' 
  AND time >= now() - 1h
GROUP BY time(5m), datacenter
```

Features supported:
- `SELECT` with aggregation functions (sum, mean, min, max, count, stddev, percentile)
- `FROM` clause for metric selection
- `WHERE` clause with tag filters and time predicates
- `GROUP BY` with time buckets and tag dimensions
- `ORDER BY` and `LIMIT` for result control

### PromQL Subset

```promql
rate(http_requests_total{status="500"}[5m])
sum by (instance) (node_cpu_seconds_total{mode="idle"})
```

Features supported:
- Instant and range vectors
- Label matchers (=, !=, =~, !~)
- Aggregation operators (sum, avg, min, max, count, stddev)
- Functions (rate, irate, increase, delta, deriv)
- Binary operators and grouping modifiers

### Query Router

The parser auto-detects query language based on syntax:
- Queries starting with `SELECT` route to SQL parser
- Other queries route to PromQL parser

```go
func (db *DB) Query(query string) ([]Point, error) {
    if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(query)), "SELECT") {
        return db.querySQL(query)
    }
    return db.queryPromQL(query)
}
```

## Consequences

### Positive

- **Lower adoption barrier**: Users can start with familiar syntax
- **Grafana compatibility**: Works with both Prometheus and SQL datasource plugins
- **Migration path**: Teams can migrate from Prometheus without rewriting all queries
- **Flexibility**: SQL for ad-hoc analysis, PromQL for monitoring dashboards

### Negative

- **Dual maintenance burden**: Two parsers, two query planners to maintain
- **Feature parity challenges**: Not all features available in both languages
- **Documentation complexity**: Must document both query languages
- **Subtle semantic differences**: Same concept may behave slightly differently

### Feature Comparison

| Feature | SQL-like | PromQL |
|---------|----------|--------|
| Time range filter | `WHERE time >= ...` | `[5m]` range selector |
| Tag filter | `WHERE tag = 'value'` | `{tag="value"}` |
| Aggregation | `GROUP BY time(5m)` | `sum by (label)` |
| Rate calculation | Custom function | `rate()`, `irate()` |
| Regex matching | `LIKE` (limited) | `=~` operator |

### PromQL Limitations

Not implemented (to manage scope):
- Subqueries
- `offset` modifier
- Recording rules evaluation
- Full function library (histogram_quantile, etc.)

These can be added based on user demand.

### Enabled

- Grafana dashboards with mixed SQL and PromQL panels
- Alerting rules in PromQL syntax
- Ad-hoc analysis in SQL
- Gradual migration from Prometheus

### Prevented

- Single, unified query optimization path
- Complete PromQL compatibility (subset only)
- Strongly-typed query results (both return Point slices)
