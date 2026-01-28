package query

import (
	"errors"
	"math"
	"sort"
	"strings"
	"time"
)

// Point represents a single time-series data point.
type Point struct {
	Metric    string
	Tags      map[string]string
	Value     float64
	Timestamp int64
}

// AggState holds aggregation state for a single bucket.
type AggState struct {
	Count   int
	Sum     float64
	Min     float64
	Max     float64
	First   float64
	Last    float64
	FirstTs int64
	LastTs  int64
	Mean    float64
	M2      float64
	Values  []float64
}

// AggBuckets manages aggregation buckets by group key and time window.
type AggBuckets struct {
	Buckets   map[string]map[int64]*AggState
	MaxBytes  int64
	UsedBytes int64
}

// NewAggBuckets creates a new aggregation bucket manager.
func NewAggBuckets(maxBytes int64) *AggBuckets {
	return &AggBuckets{
		Buckets:  make(map[string]map[int64]*AggState),
		MaxBytes: maxBytes,
	}
}

// Add adds a value to the appropriate bucket.
func (b *AggBuckets) Add(tags map[string]string, timestamp int64, value float64, window time.Duration, groupBy []string, agg AggFunc) error {
	groupKey := MakeGroupKey(tags, groupBy)
	bucket := timestamp / int64(window)

	bucketMap, ok := b.Buckets[groupKey]
	if !ok {
		bucketMap = make(map[int64]*AggState)
		b.Buckets[groupKey] = bucketMap
	}

	state, ok := bucketMap[bucket]
	if !ok {
		state = &AggState{Min: value, Max: value, First: value, Last: value, FirstTs: timestamp, LastTs: timestamp}
		bucketMap[bucket] = state
	}

	state.Count++
	state.Sum += value
	if value < state.Min {
		state.Min = value
	}
	if value > state.Max {
		state.Max = value
	}
	if timestamp < state.FirstTs {
		state.FirstTs = timestamp
		state.First = value
	}
	if timestamp >= state.LastTs {
		state.LastTs = timestamp
		state.Last = value
	}

	// Welford for stddev
	delta := value - state.Mean
	state.Mean += delta / float64(state.Count)
	state.M2 += delta * (value - state.Mean)

	if agg == AggPercentile {
		state.Values = append(state.Values, value)
		b.UsedBytes += 8
		if b.MaxBytes > 0 && b.UsedBytes > b.MaxBytes {
			return errors.New("query memory budget exceeded")
		}
	}
	return nil
}

// Finalize computes final aggregated values and returns points.
func (b *AggBuckets) Finalize(agg AggFunc, window time.Duration) []Point {
	var result []Point
	for groupKey, bucketMap := range b.Buckets {
		tags := ParseGroupKey(groupKey)
		for bucket, state := range bucketMap {
			value := ApplyAggState(agg, state)
			result = append(result, Point{
				Tags:      tags,
				Value:     value,
				Timestamp: bucket * int64(window),
			})
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Timestamp < result[j].Timestamp
	})

	return result
}

// ApplyAggState computes the final value for an aggregation state.
func ApplyAggState(fn AggFunc, state *AggState) float64 {
	switch fn {
	case AggCount:
		return float64(state.Count)
	case AggSum:
		return state.Sum
	case AggMean:
		if state.Count == 0 {
			return 0
		}
		return state.Sum / float64(state.Count)
	case AggMin:
		return state.Min
	case AggMax:
		return state.Max
	case AggFirst:
		return state.First
	case AggLast:
		return state.Last
	case AggStddev:
		if state.Count <= 1 {
			return 0
		}
		return math.Sqrt(state.M2 / float64(state.Count-1))
	case AggRate:
		if state.LastTs == state.FirstTs {
			return 0
		}
		seconds := float64(state.LastTs-state.FirstTs) / float64(time.Second)
		if seconds == 0 {
			return 0
		}
		return (state.Last - state.First) / seconds
	case AggPercentile:
		if len(state.Values) == 0 {
			return 0
		}
		sort.Float64s(state.Values)
		idx := int(math.Round(0.95 * float64(len(state.Values)-1)))
		if idx < 0 {
			idx = 0
		}
		if idx >= len(state.Values) {
			idx = len(state.Values) - 1
		}
		return state.Values[idx]
	default:
		return 0
	}
}

// MakeGroupKey creates a group key from tags and group by fields.
func MakeGroupKey(tags map[string]string, groupBy []string) string {
	if len(groupBy) == 0 {
		return ""
	}

	parts := make([]string, 0, len(groupBy))
	for _, key := range groupBy {
		if v, ok := tags[key]; ok {
			parts = append(parts, key+"="+v)
		}
	}
	return strings.Join(parts, "|")
}

// ParseGroupKey parses a group key back into tags.
func ParseGroupKey(key string) map[string]string {
	if key == "" {
		return nil
	}

	out := make(map[string]string)
	pairs := strings.Split(key, "|")
	for _, pair := range pairs {
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) == 2 {
			out[kv[0]] = kv[1]
		}
	}
	return out
}
