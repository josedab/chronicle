package chronicle

import (
	"sort"
	"strings"
)

// Point represents a single time-series data point with a metric name, optional tags,
// a float64 value, and a Unix nanosecond timestamp.
type Point struct {
	// Metric is the series name (e.g., "cpu.usage", "http.request_count").
	Metric string
	// Tags are optional key-value labels for filtering and grouping (e.g., {"host": "web-1"}).
	Tags map[string]string
	// Value is the numeric measurement.
	Value float64
	// Timestamp is the observation time in Unix nanoseconds.
	Timestamp int64
}

// Series represents a unique time series identified by a numeric ID,
// a metric name, and a set of distinguishing tags.
type Series struct {
	ID     uint64
	Metric string
	Tags   map[string]string
}

// SeriesKey uniquely identifies a time series by its metric name and tags.
// It provides a consistent string representation for use as a map key.
type SeriesKey struct {
	Metric string
	Tags   map[string]string
}

// NewSeriesKey creates a SeriesKey from a metric name and tags.
func NewSeriesKey(metric string, tags map[string]string) SeriesKey {
	return SeriesKey{Metric: metric, Tags: tags}
}

// SeriesKeyFromPoint extracts a SeriesKey from a Point.
func SeriesKeyFromPoint(p Point) SeriesKey {
	return SeriesKey{Metric: p.Metric, Tags: p.Tags}
}

// SeriesKeyFromSeries extracts a SeriesKey from a Series.
func SeriesKeyFromSeries(s Series) SeriesKey {
	return SeriesKey{Metric: s.Metric, Tags: s.Tags}
}

// String returns a canonical string representation of the series key.
// The format is "metric|tag1=val1,tag2=val2" with tags sorted alphabetically.
// This method is used for map keys and should produce consistent output.
func (sk SeriesKey) String() string {
	if len(sk.Tags) == 0 {
		return sk.Metric
	}

	parts := make([]string, 0, len(sk.Tags))
	for k, v := range sk.Tags {
		parts = append(parts, k+"="+v)
	}
	sort.Strings(parts)

	return sk.Metric + "|" + strings.Join(parts, ",")
}

// PrometheusString returns a Prometheus-style string representation.
// The format is "metric{tag1=val1,tag2=val2}" with tags sorted alphabetically.
// This format is used for cardinality tracking and external APIs.
func (sk SeriesKey) PrometheusString() string {
	if len(sk.Tags) == 0 {
		return sk.Metric
	}

	keys := make([]string, 0, len(sk.Tags))
	for k := range sk.Tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	b.WriteString(sk.Metric)
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(sk.Tags[k])
	}
	b.WriteByte('}')
	return b.String()
}

// Equals checks if two SeriesKeys are equal.
func (sk SeriesKey) Equals(other SeriesKey) bool {
	if sk.Metric != other.Metric {
		return false
	}
	if len(sk.Tags) != len(other.Tags) {
		return false
	}
	for k, v := range sk.Tags {
		if other.Tags[k] != v {
			return false
		}
	}
	return true
}

// Result holds the output of a query execution, containing the matching data points.
type Result struct {
	Points []Point
}
