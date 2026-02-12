package cql

import (
	"context"
	"time"
)

// AggFunc enumerates aggregation functions.
type AggFunc int

const (
	AggNone AggFunc = iota
	AggCount
	AggSum
	AggMean
	AggMin
	AggMax
	AggStddev
	AggPercentile
	AggRate
	AggFirst
	AggLast
)

// Aggregation defines an aggregation operation.
type Aggregation struct {
	Function AggFunc
	Window   time.Duration
}

// TagFilter represents a tag predicate.
type TagFilter struct {
	Key    string
	Values []string
}

// Query represents a translated query.
type Query struct {
	Metric      string
	Tags        map[string]string
	TagFilters  []TagFilter
	Start       int64
	End         int64
	Aggregation *Aggregation
	GroupBy     []string
	Limit       int
}

// Point represents a single time-series data point.
type Point struct {
	Metric    string
	Tags      map[string]string
	Value     float64
	Timestamp int64
}

// Result holds query results.
type Result struct {
	Points []Point
}

// QueryExecutor executes queries against the storage engine.
type QueryExecutor interface {
	ExecuteContext(ctx context.Context, q *Query) (*Result, error)
}

func aggFuncName(f AggFunc) string {
	switch f {
	case AggCount:
		return "count"
	case AggSum:
		return "sum"
	case AggMean:
		return "mean"
	case AggMin:
		return "min"
	case AggMax:
		return "max"
	case AggStddev:
		return "stddev"
	case AggPercentile:
		return "percentile"
	case AggRate:
		return "rate"
	case AggFirst:
		return "first"
	case AggLast:
		return "last"
	default:
		return "unknown"
	}
}
