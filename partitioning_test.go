package chronicle

import (
	"testing"
	"time"
)

func TestPartitioning(t *testing.T) {
	t.Run("single_point_single_batch", func(t *testing.T) {
		batches := groupByPartition(
			[]Point{{Metric: "m", Value: 1, Timestamp: 1000}},
			time.Hour,
		)
		if len(batches) != 1 {
			t.Fatalf("expected 1 batch, got %d", len(batches))
		}
		if len(batches[0].points) != 1 {
			t.Errorf("expected 1 point in batch, got %d", len(batches[0].points))
		}
	})

	t.Run("same_partition_grouped", func(t *testing.T) {
		dur := time.Hour
		base := int64(dur) * 5 // start at partition boundary
		points := []Point{
			{Metric: "m", Value: 1, Timestamp: base + 1},
			{Metric: "m", Value: 2, Timestamp: base + 2},
			{Metric: "m", Value: 3, Timestamp: base + 3},
		}
		batches := groupByPartition(points, dur)
		if len(batches) != 1 {
			t.Fatalf("expected 1 batch for same-partition points, got %d", len(batches))
		}
		if len(batches[0].points) != 3 {
			t.Errorf("expected 3 points, got %d", len(batches[0].points))
		}
	})

	t.Run("different_partitions_split", func(t *testing.T) {
		dur := time.Hour
		bucketSize := int64(dur)
		points := []Point{
			{Metric: "m", Value: 1, Timestamp: bucketSize * 0},
			{Metric: "m", Value: 2, Timestamp: bucketSize * 1},
			{Metric: "m", Value: 3, Timestamp: bucketSize * 2},
		}
		batches := groupByPartition(points, dur)
		if len(batches) != 3 {
			t.Errorf("expected 3 batches for 3 different partitions, got %d", len(batches))
		}
	})

	t.Run("zero_duration_defaults_to_hour", func(t *testing.T) {
		batches := groupByPartition(
			[]Point{{Metric: "m", Value: 1, Timestamp: 1000}},
			0,
		)
		if len(batches) == 0 {
			t.Error("expected at least 1 batch with zero duration")
		}
	})

	t.Run("empty_points", func(t *testing.T) {
		batches := groupByPartition(nil, time.Hour)
		if len(batches) != 0 {
			t.Errorf("expected 0 batches for nil points, got %d", len(batches))
		}
	})
}
