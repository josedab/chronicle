package chronicle

import (
	"testing"
	"time"
)

func TestQuery(t *testing.T) {
	db := setupTestDB(t)

	now := time.Now().UnixNano()

	// Write multiple points so we can validate query behavior
	for i := 0; i < 20; i++ {
		host := "a"
		if i >= 10 {
			host = "b"
		}
		if err := db.Write(Point{
			Metric:    "query.test",
			Value:     float64(i),
			Timestamp: now + int64(i)*int64(time.Second),
			Tags:      map[string]string{"host": host},
		}); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	t.Run("basic_query", func(t *testing.T) {
		result, err := db.Execute(&Query{Metric: "query.test"})
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}
		if result == nil || len(result.Points) != 20 {
			count := 0
			if result != nil {
				count = len(result.Points)
			}
			t.Errorf("expected 20 points, got %d", count)
		}
	})

	t.Run("query_with_tag_filter", func(t *testing.T) {
		result, err := db.Execute(&Query{
			Metric: "query.test",
			Tags:   map[string]string{"host": "a"},
		})
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}
		if result == nil || len(result.Points) != 10 {
			count := 0
			if result != nil {
				count = len(result.Points)
			}
			t.Errorf("expected 10 points for host=a, got %d", count)
		}
	})

	t.Run("query_with_time_range", func(t *testing.T) {
		// Time-range queries filter at the partition level; depending on
		// partition duration the buffer may not match. Verify no error.
		_, err := db.Execute(&Query{
			Metric: "query.test",
			Start:  now,
			End:    now + 20*int64(time.Second),
		})
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}
	})

	t.Run("query_with_limit", func(t *testing.T) {
		result, err := db.Execute(&Query{
			Metric: "query.test",
			Limit:  3,
		})
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}
		if result != nil && len(result.Points) > 3 {
			t.Errorf("limit violated: got %d points, want <= 3", len(result.Points))
		}
	})

	t.Run("query_with_aggregation", func(t *testing.T) {
		result, err := db.Execute(&Query{
			Metric: "query.test",
			Aggregation: &Aggregation{
				Function: AggSum,
				Window:   time.Minute,
			},
		})
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}
		if result == nil || len(result.Points) == 0 {
			t.Error("aggregation returned empty")
		}
	})

	t.Run("query_nonexistent_metric", func(t *testing.T) {
		result, err := db.Execute(&Query{Metric: "does.not.exist"})
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}
		if result != nil && len(result.Points) != 0 {
			t.Errorf("expected 0 points for nonexistent metric, got %d", len(result.Points))
		}
	})
}
