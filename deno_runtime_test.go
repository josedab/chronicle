package chronicle

import (
	"testing"
	"time"
)

func TestDefaultDenoRuntimeConfig(t *testing.T) {
	cfg := DefaultDenoRuntimeConfig()
	if cfg.MaxMemoryMB != 512 {
		t.Errorf("expected 512MB, got %d", cfg.MaxMemoryMB)
	}
	if cfg.BatchSize != 500 {
		t.Errorf("expected batch 500, got %d", cfg.BatchSize)
	}
	if cfg.ConsistencyLevel != "strong" {
		t.Errorf("expected strong, got %s", cfg.ConsistencyLevel)
	}
}

func TestDenoRuntimeCreation(t *testing.T) {
	config := DefaultDenoRuntimeConfig()
	config.KVDatabaseID = "test-db"
	config.APIToken = "test-token"

	dr, err := NewDenoRuntime(config)
	if err != nil {
		t.Fatalf("failed to create deno runtime: %v", err)
	}

	if dr.kv == nil {
		t.Fatal("expected KV backend to be initialized")
	}
	if dr.cache == nil {
		t.Fatal("expected cache to be initialized")
	}
}

func TestDenoRuntimeRequiresConfig(t *testing.T) {
	config := DenoRuntimeConfig{}
	_, err := NewDenoRuntime(config)
	if err == nil {
		t.Fatal("expected error for empty config")
	}
}

func TestDenoCacheHitMiss(t *testing.T) {
	cache := newDenoCache(time.Minute)

	result := &DenoQueryResult{
		Series: "cpu",
		Count:  10,
	}
	cache.set("key1", result)

	// Hit
	got := cache.get("key1")
	if got == nil {
		t.Fatal("expected cache hit")
	}
	if got.Count != 10 {
		t.Errorf("expected count 10, got %d", got.Count)
	}

	// Miss
	got = cache.get("nonexistent")
	if got != nil {
		t.Fatal("expected cache miss")
	}
}

func TestDenoCacheExpiry(t *testing.T) {
	cache := newDenoCache(1 * time.Millisecond)

	result := &DenoQueryResult{Series: "cpu", Count: 5}
	cache.set("key1", result)

	time.Sleep(5 * time.Millisecond)

	got := cache.get("key1")
	if got != nil {
		t.Fatal("expected expired entry to return nil")
	}
}

func TestEdgePlatformManager(t *testing.T) {
	epm := NewEdgePlatformManager()

	if epm.Workers() != nil {
		t.Fatal("expected nil workers before init")
	}
	if epm.Deno() != nil {
		t.Fatal("expected nil deno before init")
	}
}

func TestDenoMetrics(t *testing.T) {
	config := DefaultDenoRuntimeConfig()
	config.KVDatabaseID = "test"

	dr, _ := NewDenoRuntime(config)
	metrics := dr.Metrics()
	if metrics.RequestCount != 0 {
		t.Errorf("expected 0 requests, got %d", metrics.RequestCount)
	}
}
