package chronicle

import (
	"testing"
	"time"
)

func TestDbWrite(t *testing.T) {
	db := setupTestDB(t)

	t.Run("write_single_point", func(t *testing.T) {
		err := db.Write(Point{
			Metric:    "test.metric",
			Value:     42.0,
			Timestamp: time.Now().UnixNano(),
			Tags:      map[string]string{"host": "test"},
		})
		if err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	})

	t.Run("write_and_query_back", func(t *testing.T) {
		ts := time.Now().UnixNano()
		err := db.Write(Point{
			Metric:    "db.write.roundtrip",
			Value:     99.5,
			Timestamp: ts,
			Tags:      map[string]string{"env": "test"},
		})
		if err != nil {
			t.Fatalf("Write() error = %v", err)
		}
		if err := db.Flush(); err != nil {
			t.Fatalf("Flush() error = %v", err)
		}

		result, err := db.Execute(&Query{
			Metric: "db.write.roundtrip",
		})
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}
		if len(result.Points) == 0 {
			t.Fatal("expected at least 1 point")
		}
		if result.Points[0].Value != 99.5 {
			t.Errorf("expected value 99.5, got %f", result.Points[0].Value)
		}
	})

	t.Run("write_batch", func(t *testing.T) {
		now := time.Now().UnixNano()
		points := []Point{
			{Metric: "batch.test", Value: 1.0, Timestamp: now, Tags: map[string]string{"i": "0"}},
			{Metric: "batch.test", Value: 2.0, Timestamp: now + 1, Tags: map[string]string{"i": "1"}},
			{Metric: "batch.test", Value: 3.0, Timestamp: now + 2, Tags: map[string]string{"i": "2"}},
		}
		if err := db.WriteBatch(points); err != nil {
			t.Fatalf("WriteBatch() error = %v", err)
		}
	})

	t.Run("write_auto_timestamp", func(t *testing.T) {
		err := db.Write(Point{
			Metric: "auto.ts",
			Value:  1.0,
		})
		if err != nil {
			t.Fatalf("Write() error = %v", err)
		}
		if err := db.Flush(); err != nil {
			t.Fatalf("Flush() error = %v", err)
		}

		result, err := db.Execute(&Query{
			Metric: "auto.ts",
		})
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}
		if len(result.Points) == 0 {
			t.Error("expected at least 1 point with auto-assigned timestamp")
		}
	})
}
