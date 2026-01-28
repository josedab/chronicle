package chronicle

import (
	"testing"
	"time"
)

func TestWriteRead(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/test.db"

	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	now := time.Now().UnixNano()
	points := []Point{
		{Metric: "temp", Tags: map[string]string{"room": "a"}, Value: 10.5, Timestamp: now},
		{Metric: "temp", Tags: map[string]string{"room": "b"}, Value: 12.0, Timestamp: now + int64(time.Second)},
	}

	if err := db.WriteBatch(points); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db, err = Open(path, DefaultConfig(path))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()

	result, err := db.Execute(&Query{Metric: "temp"})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(result.Points) != 2 {
		t.Fatalf("expected 2 points, got %d", len(result.Points))
	}
}

func TestQueryParserOperators(t *testing.T) {
	parser := &QueryParser{}
	q, err := parser.Parse("SELECT mean(value) FROM cpu WHERE host != 'a' AND region IN ('us','eu') GROUP BY time(5m), host")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if q.Aggregation == nil || q.Aggregation.Function != AggMean {
		t.Fatalf("expected mean aggregation")
	}
	if len(q.TagFilters) != 2 {
		t.Fatalf("expected 2 tag filters")
	}
}
