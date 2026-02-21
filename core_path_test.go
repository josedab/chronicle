package chronicle

import (
	"testing"
	"time"
)

// TestCorePath_FullWriteQueryRoundtrip exercises the complete write→flush→query
// data path to ensure core coverage. This is the most important test in the project.
func TestCorePath_FullWriteQueryRoundtrip(t *testing.T) {
	db := setupTestDB(t)

	now := time.Now().UnixNano()

	// Phase 1: Single writes with various tag combinations
	for i := 0; i < 100; i++ {
		err := db.Write(Point{
			Metric:    "core.test",
			Value:     float64(i),
			Tags:      map[string]string{"host": "h1", "idx": "even"},
			Timestamp: now + int64(i)*int64(time.Second),
		})
		if err != nil {
			t.Fatalf("write %d failed: %v", i, err)
		}
	}

	// Phase 2: Flush to storage
	if err := db.Flush(); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	// Phase 3: Query all
	result, err := db.Execute(&Query{
		Metric: "core.test",
		Start:  0,
		End:    now + int64(200)*int64(time.Second),
	})
	if err != nil {
		t.Fatalf("query all failed: %v", err)
	}
	if result == nil || len(result.Points) == 0 {
		t.Fatal("expected points from query")
	}
	if len(result.Points) != 100 {
		t.Errorf("expected 100 points, got %d", len(result.Points))
	}

	// Phase 4: Query with tag filter
	result, err = db.Execute(&Query{
		Metric: "core.test",
		Tags:   map[string]string{"host": "h1"},
	})
	if err != nil {
		t.Fatalf("tag query failed: %v", err)
	}
	if result == nil || len(result.Points) == 0 {
		t.Fatal("tag query returned empty")
	}

	// Phase 5: Query with time range
	result, err = db.Execute(&Query{
		Metric: "core.test",
		Start:  now + int64(50)*int64(time.Second),
		End:    now + int64(60)*int64(time.Second),
	})
	if err != nil {
		t.Fatalf("range query failed: %v", err)
	}

	// Phase 6: Query with aggregation
	result, err = db.Execute(&Query{
		Metric:      "core.test",
		Aggregation: &Aggregation{Function: AggSum, Window: time.Minute},
	})
	if err != nil {
		t.Fatalf("agg query failed: %v", err)
	}
	if result == nil || len(result.Points) == 0 {
		t.Fatal("aggregation returned empty")
	}

	// Phase 7: Query with limit
	result, err = db.Execute(&Query{
		Metric: "core.test",
		Limit:  5,
	})
	if err != nil {
		t.Fatalf("limit query failed: %v", err)
	}
	if result != nil && len(result.Points) > 5 {
		t.Errorf("limit violated: got %d points", len(result.Points))
	}

	// Phase 8: Batch write
	batch := make([]Point, 50)
	for i := range batch {
		batch[i] = Point{
			Metric:    "core.batch",
			Value:     float64(i * 10),
			Tags:      map[string]string{"src": "batch"},
			Timestamp: now + int64(i)*int64(time.Second),
		}
	}
	if err := db.WriteBatch(batch); err != nil {
		t.Fatalf("batch write failed: %v", err)
	}
	db.Flush()

	result, err = db.Execute(&Query{Metric: "core.batch"})
	if err != nil {
		t.Fatalf("batch query failed: %v", err)
	}
	if result == nil || len(result.Points) != 50 {
		count := 0
		if result != nil { count = len(result.Points) }
		t.Errorf("expected 50 batch points, got %d", count)
	}

	// Phase 9: Metrics listing
	metrics := db.Metrics()
	if len(metrics) < 2 {
		t.Errorf("expected at least 2 metrics, got %d", len(metrics))
	}

	// Phase 10: Nil query handling
	result, err = db.Execute(nil)
	if err != nil {
		t.Fatalf("nil query should not error: %v", err)
	}
}
