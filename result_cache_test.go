package chronicle

import (
	"testing"
	"time"
)

func TestResultCacheConfig(t *testing.T) {
	cfg := DefaultResultCacheConfig()
	if !cfg.Enabled {
		t.Error("expected enabled")
	}
	if cfg.MaxEntries != 1000 {
		t.Errorf("expected 1000 max entries, got %d", cfg.MaxEntries)
	}
	if cfg.TTL != 5*time.Minute {
		t.Errorf("expected 5m TTL, got %v", cfg.TTL)
	}
}

func TestResultCachePutGet(t *testing.T) {
	db := setupTestDB(t)

	e := NewResultCacheEngine(db, DefaultResultCacheConfig())

	t.Run("miss on empty cache", func(t *testing.T) {
		r := e.Get("hash1")
		if r != nil {
			t.Error("expected nil on cache miss")
		}
	})

	t.Run("put and get", func(t *testing.T) {
		result := &Result{Points: []Point{{Metric: "cpu", Value: 42}}}
		e.Put("hash1", result, "cpu")

		r := e.Get("hash1")
		if r == nil {
			t.Fatal("expected cache hit")
		}
		if len(r.Points) != 1 {
			t.Errorf("expected 1 point, got %d", len(r.Points))
		}
		if r.Points[0].Value != 42 {
			t.Errorf("expected value 42, got %f", r.Points[0].Value)
		}
	})

	t.Run("update existing entry", func(t *testing.T) {
		result2 := &Result{Points: []Point{{Metric: "cpu", Value: 99}}}
		e.Put("hash1", result2, "cpu")
		r := e.Get("hash1")
		if r == nil {
			t.Fatal("expected cache hit")
		}
		if r.Points[0].Value != 99 {
			t.Errorf("expected updated value 99, got %f", r.Points[0].Value)
		}
	})
}

func TestResultCacheTTLExpiry(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultResultCacheConfig()
	cfg.TTL = 50 * time.Millisecond
	e := NewResultCacheEngine(db, cfg)

	e.Put("expiring", &Result{Points: []Point{{Metric: "m", Value: 1}}})

	r := e.Get("expiring")
	if r == nil {
		t.Fatal("expected cache hit before expiry")
	}

	time.Sleep(60 * time.Millisecond)

	r = e.Get("expiring")
	if r != nil {
		t.Error("expected cache miss after TTL expiry")
	}
}

func TestResultCacheInvalidation(t *testing.T) {
	db := setupTestDB(t)

	e := NewResultCacheEngine(db, DefaultResultCacheConfig())

	e.Put("h1", &Result{Points: []Point{{Metric: "cpu", Value: 1}}}, "cpu")
	e.Put("h2", &Result{Points: []Point{{Metric: "cpu", Value: 2}}}, "cpu")
	e.Put("h3", &Result{Points: []Point{{Metric: "mem", Value: 3}}}, "mem")

	removed := e.Invalidate("cpu")
	if removed != 2 {
		t.Errorf("expected 2 removed, got %d", removed)
	}

	if e.Get("h1") != nil {
		t.Error("expected h1 invalidated")
	}
	if e.Get("h3") == nil {
		t.Error("expected h3 still cached")
	}
}

func TestResultCacheLRUEviction(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultResultCacheConfig()
	cfg.MaxEntries = 3
	e := NewResultCacheEngine(db, cfg)

	e.Put("a", &Result{Points: []Point{{Value: 1}}})
	e.Put("b", &Result{Points: []Point{{Value: 2}}})
	e.Put("c", &Result{Points: []Point{{Value: 3}}})

	// Access "a" to make it recently used
	e.Get("a")

	// Adding "d" should evict "b" (least recently used)
	e.Put("d", &Result{Points: []Point{{Value: 4}}})

	if e.Get("b") != nil {
		t.Error("expected b to be evicted")
	}
	if e.Get("a") == nil {
		t.Error("expected a to still be cached")
	}
	if e.Get("d") == nil {
		t.Error("expected d to be cached")
	}

	stats := e.GetStats()
	if stats.TotalEvictions == 0 {
		t.Error("expected evictions > 0")
	}
}

func TestResultCacheClear(t *testing.T) {
	db := setupTestDB(t)

	e := NewResultCacheEngine(db, DefaultResultCacheConfig())

	e.Put("x", &Result{Points: []Point{{Value: 1}}})
	e.Put("y", &Result{Points: []Point{{Value: 2}}})
	e.Clear()

	if e.Get("x") != nil {
		t.Error("expected empty cache after clear")
	}
	stats := e.GetStats()
	if stats.EntryCount != 0 {
		t.Errorf("expected 0 entries, got %d", stats.EntryCount)
	}
}

func TestResultCacheStats(t *testing.T) {
	db := setupTestDB(t)

	e := NewResultCacheEngine(db, DefaultResultCacheConfig())

	e.Put("s1", &Result{Points: []Point{{Value: 1}}})
	e.Get("s1") // hit
	e.Get("s1") // hit
	e.Get("s2") // miss

	stats := e.GetStats()
	if stats.TotalHits != 2 {
		t.Errorf("expected 2 hits, got %d", stats.TotalHits)
	}
	if stats.TotalMisses != 1 {
		t.Errorf("expected 1 miss, got %d", stats.TotalMisses)
	}
	if stats.HitRate < 0.6 || stats.HitRate > 0.7 {
		t.Errorf("expected ~0.67 hit rate, got %f", stats.HitRate)
	}
}

func TestResultCacheStartStop(t *testing.T) {
	db := setupTestDB(t)

	e := NewResultCacheEngine(db, DefaultResultCacheConfig())
	e.Start()
	e.Start() // idempotent
	e.Stop()
}

func TestQueryCacheKey(t *testing.T) {
	t.Run("nil query returns empty", func(t *testing.T) {
		if k := QueryCacheKey(nil); k != "" {
			t.Errorf("expected empty, got %q", k)
		}
	})

	t.Run("deterministic", func(t *testing.T) {
		q := &Query{Metric: "cpu", Start: 1, End: 100}
		k1 := QueryCacheKey(q)
		k2 := QueryCacheKey(q)
		if k1 != k2 {
			t.Errorf("same query produced different keys: %q vs %q", k1, k2)
		}
	})

	t.Run("tag order independent", func(t *testing.T) {
		q1 := &Query{
			Metric: "cpu",
			Start:  1, End: 100,
			Tags: map[string]string{"host": "a", "region": "us"},
		}
		q2 := &Query{
			Metric: "cpu",
			Start:  1, End: 100,
			Tags: map[string]string{"region": "us", "host": "a"},
		}
		if QueryCacheKey(q1) != QueryCacheKey(q2) {
			t.Error("equivalent queries with different tag order produced different keys")
		}
	})

	t.Run("tag filter order independent", func(t *testing.T) {
		q1 := &Query{
			Metric: "cpu",
			Start:  1, End: 100,
			TagFilters: []TagFilter{
				{Key: "host", Op: TagOpEq, Values: []string{"a"}},
				{Key: "region", Op: TagOpEq, Values: []string{"us"}},
			},
		}
		q2 := &Query{
			Metric: "cpu",
			Start:  1, End: 100,
			TagFilters: []TagFilter{
				{Key: "region", Op: TagOpEq, Values: []string{"us"}},
				{Key: "host", Op: TagOpEq, Values: []string{"a"}},
			},
		}
		if QueryCacheKey(q1) != QueryCacheKey(q2) {
			t.Error("equivalent queries with different filter order produced different keys")
		}
	})

	t.Run("different queries produce different keys", func(t *testing.T) {
		q1 := &Query{Metric: "cpu", Start: 1, End: 100}
		q2 := &Query{Metric: "mem", Start: 1, End: 100}
		if QueryCacheKey(q1) == QueryCacheKey(q2) {
			t.Error("different queries produced same key")
		}
	})
}
