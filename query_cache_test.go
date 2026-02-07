package chronicle

import (
	"testing"
	"time"
)

func TestQueryCachePutGet(t *testing.T) {
	qc := NewQueryCache(nil, DefaultQueryCacheConfig())

	start := time.Now().Add(-time.Hour)
	end := time.Now()
	points := []Point{
		{Metric: "cpu", Value: 42.0, Timestamp: start.UnixNano()},
		{Metric: "cpu", Value: 43.0, Timestamp: end.UnixNano()},
	}

	qc.Put("cpu", start, end, nil, points)

	got, ok := qc.Get("cpu", start, end, nil)
	if !ok {
		t.Fatal("expected cache hit")
	}
	if len(got) != 2 {
		t.Errorf("expected 2 points, got %d", len(got))
	}
}

func TestQueryCacheMiss(t *testing.T) {
	qc := NewQueryCache(nil, DefaultQueryCacheConfig())

	_, ok := qc.Get("cpu", time.Now().Add(-time.Hour), time.Now(), nil)
	if ok {
		t.Fatal("expected cache miss")
	}
}

func TestQueryCacheTTLExpiry(t *testing.T) {
	cfg := DefaultQueryCacheConfig()
	cfg.DefaultTTL = time.Millisecond
	qc := NewQueryCache(nil, cfg)

	start := time.Now().Add(-time.Hour)
	end := time.Now()
	qc.Put("cpu", start, end, nil, []Point{{Metric: "cpu", Value: 1}})

	time.Sleep(5 * time.Millisecond)

	_, ok := qc.Get("cpu", start, end, nil)
	if ok {
		t.Fatal("expected cache miss after TTL expiry")
	}
}

func TestQueryCacheEviction(t *testing.T) {
	cfg := DefaultQueryCacheConfig()
	cfg.MaxEntries = 2
	qc := NewQueryCache(nil, cfg)

	t1 := time.Now().Add(-3 * time.Hour)
	t2 := time.Now().Add(-2 * time.Hour)
	t3 := time.Now().Add(-time.Hour)
	end := time.Now()

	qc.Put("m1", t1, end, nil, []Point{{Metric: "m1", Value: 1}})
	qc.Put("m2", t2, end, nil, []Point{{Metric: "m2", Value: 2}})
	qc.Put("m3", t3, end, nil, []Point{{Metric: "m3", Value: 3}})

	if qc.Entries() > 2 {
		t.Errorf("expected at most 2 entries, got %d", qc.Entries())
	}

	stats := qc.Stats()
	if stats.EvictionCount == 0 {
		t.Error("expected eviction to have occurred")
	}
}

func TestQueryCacheInvalidateMetric(t *testing.T) {
	qc := NewQueryCache(nil, DefaultQueryCacheConfig())

	start := time.Now().Add(-time.Hour)
	end := time.Now()
	qc.Put("cpu", start, end, nil, []Point{{Metric: "cpu", Value: 1}})
	qc.Put("mem", start, end, nil, []Point{{Metric: "mem", Value: 2}})

	count := qc.InvalidateMetric("cpu")
	if count != 1 {
		t.Errorf("expected 1 invalidated, got %d", count)
	}

	_, ok := qc.Get("cpu", start, end, nil)
	if ok {
		t.Fatal("expected cache miss after invalidation")
	}

	_, ok = qc.Get("mem", start, end, nil)
	if !ok {
		t.Fatal("expected cache hit for non-invalidated metric")
	}
}

func TestQueryCacheInvalidateAll(t *testing.T) {
	qc := NewQueryCache(nil, DefaultQueryCacheConfig())

	start := time.Now().Add(-time.Hour)
	end := time.Now()
	qc.Put("cpu", start, end, nil, []Point{{Metric: "cpu", Value: 1}})
	qc.Put("mem", start, end, nil, []Point{{Metric: "mem", Value: 2}})

	qc.InvalidateAll()

	if qc.Entries() != 0 {
		t.Errorf("expected 0 entries after invalidate all, got %d", qc.Entries())
	}
}

func TestQueryCacheStats(t *testing.T) {
	qc := NewQueryCache(nil, DefaultQueryCacheConfig())

	start := time.Now().Add(-time.Hour)
	end := time.Now()
	qc.Put("cpu", start, end, nil, []Point{{Metric: "cpu", Value: 1}})

	qc.Get("cpu", start, end, nil) // hit
	qc.Get("mem", start, end, nil) // miss

	stats := qc.Stats()
	if stats.HitCount != 1 {
		t.Errorf("expected 1 hit, got %d", stats.HitCount)
	}
	if stats.MissCount != 1 {
		t.Errorf("expected 1 miss, got %d", stats.MissCount)
	}
	if stats.HitRate != 0.5 {
		t.Errorf("expected 50%% hit rate, got %f", stats.HitRate)
	}
}

func TestQueryCacheWithTags(t *testing.T) {
	qc := NewQueryCache(nil, DefaultQueryCacheConfig())

	start := time.Now().Add(-time.Hour)
	end := time.Now()
	tags1 := map[string]string{"host": "a"}
	tags2 := map[string]string{"host": "b"}

	qc.Put("cpu", start, end, tags1, []Point{{Value: 1}})
	qc.Put("cpu", start, end, tags2, []Point{{Value: 2}})

	got1, ok := qc.Get("cpu", start, end, tags1)
	if !ok {
		t.Fatal("expected cache hit for tags1")
	}
	if got1[0].Value != 1 {
		t.Errorf("expected value 1, got %f", got1[0].Value)
	}

	got2, ok := qc.Get("cpu", start, end, tags2)
	if !ok {
		t.Fatal("expected cache hit for tags2")
	}
	if got2[0].Value != 2 {
		t.Errorf("expected value 2, got %f", got2[0].Value)
	}
}

func TestQueryCacheDisabled(t *testing.T) {
	cfg := DefaultQueryCacheConfig()
	cfg.Enabled = false
	qc := NewQueryCache(nil, cfg)

	start := time.Now().Add(-time.Hour)
	end := time.Now()
	qc.Put("cpu", start, end, nil, []Point{{Value: 1}})

	_, ok := qc.Get("cpu", start, end, nil)
	if ok {
		t.Fatal("expected cache miss when disabled")
	}
}
