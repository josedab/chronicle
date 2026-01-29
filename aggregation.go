package chronicle

import (
	"time"

	"github.com/chronicle-db/chronicle/internal/query"
)

// aggBuckets wraps internal query.AggBuckets.
type aggBuckets struct {
	*query.AggBuckets
}

func newAggBuckets(maxBytes int64) *aggBuckets {
	return &aggBuckets{query.NewAggBuckets(maxBytes)}
}

func (b *aggBuckets) add(tags map[string]string, timestamp int64, value float64, window time.Duration, groupBy []string, agg AggFunc) error {
	return b.Add(tags, timestamp, value, window, groupBy, query.AggFunc(agg))
}

func (b *aggBuckets) finalize(agg AggFunc, window time.Duration) []Point {
	internalPoints := b.Finalize(query.AggFunc(agg), window)
	points := make([]Point, len(internalPoints))
	for i, p := range internalPoints {
		points[i] = Point{
			Metric:    p.Metric,
			Tags:      p.Tags,
			Value:     p.Value,
			Timestamp: p.Timestamp,
		}
	}
	return points
}

func makeGroupKey(tags map[string]string, groupBy []string) string {
	return query.MakeGroupKey(tags, groupBy)
}

func parseGroupKey(key string) map[string]string {
	return query.ParseGroupKey(key)
}
