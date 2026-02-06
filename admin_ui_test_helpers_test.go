package chronicle

import (
	"testing"
	"time"
)

func setupTestDB(t *testing.T) *DB {
	t.Helper()
	dir := t.TempDir()
	path := dir + "/test.db"
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		t.Fatalf("failed to open test db: %v", err)
	}
	return db
}

// writeTestPoints writes n points for the given metric starting at startTime,
// spaced 1 second apart, and flushes the buffer.
func writeTestPoints(t *testing.T, db *DB, metric string, n int, startTime time.Time) {
	t.Helper()
	for i := 0; i < n; i++ {
		if err := db.Write(Point{
			Metric:    metric,
			Value:     float64(i),
			Timestamp: startTime.Add(time.Duration(i) * time.Second).UnixNano(),
			Tags:      map[string]string{"host": "test"},
		}); err != nil {
			t.Fatalf("write point %d: %v", i, err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

// assertPointCount queries the metric and checks the result has exactly want points.
func assertPointCount(t *testing.T, db *DB, metric string, want int) {
	t.Helper()
	result, err := db.Execute(&Query{Metric: metric})
	if err != nil {
		t.Fatalf("query %q: %v", metric, err)
	}
	if len(result.Points) != want {
		t.Errorf("query %q: got %d points, want %d", metric, len(result.Points), want)
	}
}
