package chronicle

import (
	"testing"
	"time"
)

func TestDataRehydrationEngine(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultDataRehydrationConfig()
	engine := NewDataRehydrationEngine(db, cfg)
	engine.Start()
	defer engine.Stop()

	t.Run("FetchAndCache", func(t *testing.T) {
		req := RehydrationRequest{
			Metric:   "cpu.usage",
			Start:    1000,
			End:      2000,
			Source:   "s3",
			Priority: 1,
		}
		entry, err := engine.Fetch(req)
		if err != nil {
			t.Fatalf("Fetch failed: %v", err)
		}
		if entry.Status != "cached" {
			t.Errorf("expected status cached, got %s", entry.Status)
		}
		if entry.Metric != "cpu.usage" {
			t.Errorf("expected metric cpu.usage, got %s", entry.Metric)
		}
		if entry.Source != "s3" {
			t.Errorf("expected source s3, got %s", entry.Source)
		}
	})

	t.Run("CacheHit", func(t *testing.T) {
		req := RehydrationRequest{
			Metric: "mem.used",
			Start:  1000,
			End:    2000,
			Source: "local",
		}
		engine.Fetch(req)

		// second fetch should be a cache hit
		entry, err := engine.Fetch(req)
		if err != nil {
			t.Fatalf("Fetch failed: %v", err)
		}
		if entry.Status != "cached" {
			t.Errorf("expected status cached, got %s", entry.Status)
		}

		stats := engine.GetStats()
		if stats.CacheHits == 0 {
			t.Error("expected non-zero cache hits")
		}
	})

	t.Run("CacheMiss", func(t *testing.T) {
		cached := engine.GetCached("nonexistent.metric", 0, 1)
		if cached != nil {
			t.Error("expected nil for cache miss")
		}
	})

	t.Run("Eviction", func(t *testing.T) {
		req := RehydrationRequest{
			Metric: "evict.test",
			Start:  100,
			End:    200,
			Source: "gcs",
		}
		engine.Fetch(req)

		cached := engine.GetCached("evict.test", 100, 200)
		if cached == nil {
			t.Fatal("expected entry to be cached before eviction")
		}

		engine.Evict("evict.test")

		cached = engine.GetCached("evict.test", 100, 200)
		if cached != nil {
			t.Error("expected nil after eviction")
		}
	})

	t.Run("TTLExpiry", func(t *testing.T) {
		db2 := setupTestDB(t)
		defer db2.Close()
		shortCfg := DefaultDataRehydrationConfig()
		shortCfg.CacheTTL = time.Millisecond
		e2 := NewDataRehydrationEngine(db2, shortCfg)
		e2.Start()
		defer e2.Stop()

		req := RehydrationRequest{
			Metric: "ttl.test",
			Start:  1,
			End:    2,
			Source: "local",
		}
		e2.Fetch(req)
		time.Sleep(5 * time.Millisecond)

		cached := e2.GetCached("ttl.test", 1, 2)
		if cached != nil {
			t.Error("expected nil after TTL expiry")
		}
	})

	t.Run("MaxCacheSize", func(t *testing.T) {
		db3 := setupTestDB(t)
		defer db3.Close()

		// Write enough data so fetches produce meaningful sizes.
		for i := 0; i < 1000; i++ {
			db3.Write(Point{Metric: "a", Value: float64(i), Timestamp: int64(i + 1)})
			db3.Write(Point{Metric: "b", Value: float64(i), Timestamp: int64(i + 1)})
		}
		db3.Flush()

		smallCfg := DefaultDataRehydrationConfig()
		smallCfg.CacheSizeMB = 0 // 0 MB cache forces immediate eviction
		e3 := NewDataRehydrationEngine(db3, smallCfg)
		e3.Start()
		defer e3.Stop()

		e3.Fetch(RehydrationRequest{Metric: "a", Start: 0, End: 2000, Source: "s3"})
		e3.Fetch(RehydrationRequest{Metric: "b", Start: 0, End: 2000, Source: "s3"})

		stats := e3.GetStats()
		if stats.EvictionCount == 0 {
			t.Error("expected evictions when cache is full")
		}
	})

	t.Run("ListCached", func(t *testing.T) {
		entries := engine.ListCached()
		if len(entries) == 0 {
			t.Error("expected cached entries")
		}
	})

	t.Run("Stats", func(t *testing.T) {
		stats := engine.GetStats()
		if stats.TotalFetches == 0 {
			t.Error("expected non-zero total fetches")
		}
		if stats.CacheMisses == 0 {
			t.Error("expected non-zero cache misses")
		}
	})
}
