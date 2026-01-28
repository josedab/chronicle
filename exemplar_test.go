package chronicle

import (
	"testing"
	"time"
)

func TestExemplarStore_Write(t *testing.T) {
	store := NewExemplarStore(nil, DefaultExemplarConfig())

	p := ExemplarPoint{
		Metric:    "http_requests",
		Tags:      map[string]string{"method": "GET"},
		Value:     1.5,
		Timestamp: time.Now().UnixNano(),
		Exemplar: &Exemplar{
			Labels:    map[string]string{"trace_id": "abc123", "span_id": "def456"},
			Value:     1.5,
			Timestamp: time.Now().UnixNano(),
		},
	}

	if err := store.Write(p); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	stats := store.Stats()
	if stats.TotalExemplars != 1 {
		t.Errorf("expected 1 exemplar, got %d", stats.TotalExemplars)
	}
}

func TestExemplarStore_Query(t *testing.T) {
	store := NewExemplarStore(nil, DefaultExemplarConfig())

	now := time.Now().UnixNano()

	// Write multiple exemplars
	for i := 0; i < 5; i++ {
		store.Write(ExemplarPoint{
			Metric:    "http_requests",
			Tags:      map[string]string{"method": "GET"},
			Value:     float64(i),
			Timestamp: now + int64(i*1000),
			Exemplar: &Exemplar{
				Labels:    map[string]string{"trace_id": "trace-" + string(rune('a'+i))},
				Value:     float64(i),
				Timestamp: now + int64(i*1000),
			},
		})
	}

	results, err := store.Query("http_requests", map[string]string{"method": "GET"}, now, now+10000)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(results) != 5 {
		t.Errorf("expected 5 results, got %d", len(results))
	}
}

func TestExemplarStore_QueryByTraceID(t *testing.T) {
	store := NewExemplarStore(nil, DefaultExemplarConfig())

	now := time.Now().UnixNano()

	// Write exemplars with different trace IDs
	for i := 0; i < 3; i++ {
		store.Write(ExemplarPoint{
			Metric:    "http_requests",
			Tags:      map[string]string{"method": "GET"},
			Value:     float64(i),
			Timestamp: now,
			Exemplar: &Exemplar{
				Labels:    map[string]string{"trace_id": "trace-" + string(rune('a'+i))},
				Value:     float64(i),
			},
		})
	}

	results, err := store.QueryByTraceID("trace-b")
	if err != nil {
		t.Fatalf("QueryByTraceID failed: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("expected 1 result for trace-b, got %d", len(results))
	}

	if results[0].Exemplar.Labels["trace_id"] != "trace-b" {
		t.Error("wrong trace_id returned")
	}
}

func TestExemplarStore_PerSeriesLimit(t *testing.T) {
	store := NewExemplarStore(nil, ExemplarConfig{
		Enabled:               true,
		MaxExemplarsPerSeries: 3,
	})

	now := time.Now().UnixNano()

	// Write 5 exemplars to same series
	for i := 0; i < 5; i++ {
		store.Write(ExemplarPoint{
			Metric:    "http_requests",
			Tags:      map[string]string{"method": "GET"},
			Value:     float64(i),
			Timestamp: now + int64(i*1000),
			Exemplar: &Exemplar{
				Labels: map[string]string{"trace_id": "trace-" + string(rune('a'+i))},
				Value:  float64(i),
			},
		})
	}

	stats := store.Stats()
	if stats.TotalExemplars != 3 {
		t.Errorf("expected 3 exemplars (limited), got %d", stats.TotalExemplars)
	}
}

func TestExemplarStore_GlobalLimit(t *testing.T) {
	store := NewExemplarStore(nil, ExemplarConfig{
		Enabled:           true,
		MaxTotalExemplars: 5,
	})

	now := time.Now().UnixNano()

	// Write 10 exemplars to different series
	for i := 0; i < 10; i++ {
		store.Write(ExemplarPoint{
			Metric:    "http_requests",
			Tags:      map[string]string{"id": string(rune('a' + i))},
			Value:     float64(i),
			Timestamp: now + int64(i*1000),
			Exemplar: &Exemplar{
				Labels: map[string]string{"trace_id": "trace-" + string(rune('a'+i))},
				Value:  float64(i),
			},
		})
	}

	stats := store.Stats()
	if stats.TotalExemplars > 5 {
		t.Errorf("expected at most 5 exemplars, got %d", stats.TotalExemplars)
	}
}

func TestExemplarStore_Prune(t *testing.T) {
	store := NewExemplarStore(nil, ExemplarConfig{
		Enabled:           true,
		RetentionDuration: 100 * time.Millisecond,
	})

	oldTime := time.Now().Add(-time.Second).UnixNano()
	newTime := time.Now().UnixNano()

	// Write old exemplar
	store.Write(ExemplarPoint{
		Metric:    "http_requests",
		Tags:      map[string]string{"method": "GET"},
		Timestamp: oldTime,
		Exemplar: &Exemplar{
			Labels:    map[string]string{"trace_id": "old"},
			Timestamp: oldTime,
		},
	})

	// Write new exemplar
	store.Write(ExemplarPoint{
		Metric:    "http_requests",
		Tags:      map[string]string{"method": "POST"},
		Timestamp: newTime,
		Exemplar: &Exemplar{
			Labels:    map[string]string{"trace_id": "new"},
			Timestamp: newTime,
		},
	})

	pruned := store.Prune()
	if pruned != 1 {
		t.Errorf("expected 1 pruned, got %d", pruned)
	}

	stats := store.Stats()
	if stats.TotalExemplars != 1 {
		t.Errorf("expected 1 remaining exemplar, got %d", stats.TotalExemplars)
	}
}

func TestExemplarStore_Disabled(t *testing.T) {
	store := NewExemplarStore(nil, ExemplarConfig{
		Enabled: false,
	})

	err := store.Write(ExemplarPoint{
		Metric:   "http_requests",
		Exemplar: &Exemplar{Labels: map[string]string{"trace_id": "abc"}},
	})

	if err != nil {
		t.Errorf("disabled store should not error: %v", err)
	}

	stats := store.Stats()
	if stats.TotalExemplars != 0 {
		t.Error("disabled store should not store exemplars")
	}
}

func TestCloneExemplar(t *testing.T) {
	original := &Exemplar{
		Labels:    map[string]string{"trace_id": "abc"},
		Value:     1.5,
		Timestamp: 1000,
	}

	clone := cloneExemplar(original)

	if clone.Value != original.Value {
		t.Error("value mismatch")
	}
	if clone.Timestamp != original.Timestamp {
		t.Error("timestamp mismatch")
	}
	if clone.Labels["trace_id"] != original.Labels["trace_id"] {
		t.Error("labels mismatch")
	}

	// Modify clone
	clone.Labels["trace_id"] = "modified"
	if original.Labels["trace_id"] == "modified" {
		t.Error("clone should be independent")
	}
}

func TestCloneExemplarNil(t *testing.T) {
	clone := cloneExemplar(nil)
	if clone != nil {
		t.Error("clone of nil should be nil")
	}
}
