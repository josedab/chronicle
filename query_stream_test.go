package chronicle

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestExecuteStream_Basic(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	now := time.Now().UnixNano()
	points := []Point{
		{Metric: "cpu_usage", Tags: map[string]string{"host": "server1"}, Value: 10.0, Timestamp: now - 3000},
		{Metric: "cpu_usage", Tags: map[string]string{"host": "server1"}, Value: 20.0, Timestamp: now - 2000},
		{Metric: "cpu_usage", Tags: map[string]string{"host": "server2"}, Value: 30.0, Timestamp: now - 1000},
		{Metric: "mem_usage", Tags: map[string]string{"host": "server1"}, Value: 80.0, Timestamp: now},
	}

	for _, p := range points {
		if err := db.Write(p); err != nil {
			t.Fatalf("write failed: %v", err)
		}
	}
	_ = db.Flush()

	// Stream cpu_usage
	var collected []Point
	err := db.ExecuteStream(&Query{Metric: "cpu_usage"}, func(p Point) error {
		collected = append(collected, p)
		return nil
	})
	if err != nil {
		t.Fatalf("ExecuteStream failed: %v", err)
	}

	if len(collected) != 3 {
		t.Fatalf("expected 3 points, got %d", len(collected))
	}

	// Stream with Limit
	var limited []Point
	err = db.ExecuteStream(&Query{Metric: "cpu_usage", Limit: 2}, func(p Point) error {
		limited = append(limited, p)
		return nil
	})
	if err != nil {
		t.Fatalf("ExecuteStream with limit failed: %v", err)
	}
	if len(limited) != 2 {
		t.Fatalf("expected 2 points, got %d", len(limited))
	}
}

func TestExecuteStream_Aggregation(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	now := time.Now().UnixNano()
	for i := 0; i < 5; i++ {
		_ = db.Write(Point{
			Metric:    "load",
			Value:     float64(i * 10),
			Timestamp: now - int64(i)*int64(time.Minute),
		})
	}
	_ = db.Flush()

	var aggPoints []Point
	err := db.ExecuteStream(&Query{
		Metric: "load",
		Aggregation: &Aggregation{
			Function: AggMean,
			Window:   5 * time.Minute,
		},
	}, func(p Point) error {
		aggPoints = append(aggPoints, p)
		return nil
	})
	if err != nil {
		t.Fatalf("ExecuteStream with aggregation failed: %v", err)
	}

	if len(aggPoints) == 0 {
		t.Fatalf("expected aggregated points, got 0")
	}
}

func TestExecuteStream_EarlyExitOnError(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	now := time.Now().UnixNano()
	for i := 0; i < 10; i++ {
		_ = db.Write(Point{
			Metric:    "req_count",
			Value:     float64(i),
			Timestamp: now + int64(i)*1000,
		})
	}
	_ = db.Flush()

	count := 0
	stopErr := errors.New("client stopped early")
	err := db.ExecuteStream(&Query{Metric: "req_count"}, func(p Point) error {
		count++
		if count >= 3 {
			return stopErr
		}
		return nil
	})

	if !errors.Is(err, stopErr) {
		t.Fatalf("expected stopErr, got %v", err)
	}
	if count != 3 {
		t.Fatalf("expected count 3 before early exit, got %d", count)
	}
}

func TestExecuteStream_ContextCancellation(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := db.ExecuteStreamContext(ctx, &Query{Metric: "any"}, func(p Point) error {
		return nil
	})

	if err == nil {
		t.Fatalf("expected error on canceled context, got nil")
	}
}
