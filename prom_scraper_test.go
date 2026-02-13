package chronicle

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestPromScraperAddRemoveTargets(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultPromScraperConfig()
	engine := NewPromScraperEngine(db, cfg)

	target := PromScrapeTarget{
		ID:      "target-1",
		URL:     "http://localhost:9090/metrics",
		Labels:  map[string]string{"job": "test"},
		Enabled: true,
	}

	if err := engine.AddTarget(target); err != nil {
		t.Fatalf("add target: %v", err)
	}

	targets := engine.ListTargets()
	if len(targets) != 1 {
		t.Errorf("target count: got %d, want 1", len(targets))
	}

	if err := engine.RemoveTarget("target-1"); err != nil {
		t.Fatalf("remove target: %v", err)
	}

	targets = engine.ListTargets()
	if len(targets) != 0 {
		t.Errorf("target count after remove: got %d, want 0", len(targets))
	}
}

func TestPromScraperDuplicateTarget(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultPromScraperConfig()
	engine := NewPromScraperEngine(db, cfg)

	target := PromScrapeTarget{ID: "dup-1", URL: "http://localhost/metrics", Enabled: true}
	if err := engine.AddTarget(target); err != nil {
		t.Fatalf("first add: %v", err)
	}

	if err := engine.AddTarget(target); err == nil {
		t.Error("expected error for duplicate target")
	}
}

func TestPromScraperMaxTargets(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultPromScraperConfig()
	cfg.MaxTargets = 2
	engine := NewPromScraperEngine(db, cfg)

	for i := 0; i < 2; i++ {
		target := PromScrapeTarget{
			ID:  fmt.Sprintf("target-%d", i),
			URL: fmt.Sprintf("http://host%d/metrics", i),
		}
		if err := engine.AddTarget(target); err != nil {
			t.Fatalf("add target %d: %v", i, err)
		}
	}

	extra := PromScrapeTarget{ID: "target-extra", URL: "http://extra/metrics"}
	if err := engine.AddTarget(extra); err == nil {
		t.Error("expected error when exceeding max targets")
	}
}

func TestPromScraperScrape(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultPromScraperConfig()
	engine := NewPromScraperEngine(db, cfg)

	target := PromScrapeTarget{
		ID:      "scrape-test",
		URL:     "http://localhost/metrics",
		Labels:  map[string]string{"env": "test"},
		Enabled: true,
	}
	engine.AddTarget(target)

	result, err := engine.Scrape("scrape-test")
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	if !result.Success {
		t.Error("scrape should succeed")
	}
	if result.SamplesCount <= 0 {
		t.Error("samples count should be positive")
	}
	if result.ScrapedAt.IsZero() {
		t.Error("scraped_at should be set")
	}

	stats := engine.Stats()
	if stats.TotalScrapes != 1 {
		t.Errorf("total scrapes: got %d, want 1", stats.TotalScrapes)
	}
	if stats.TotalSamplesScraped <= 0 {
		t.Error("total samples scraped should be positive")
	}
}

func TestPromScraperScrapeNotFound(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultPromScraperConfig()
	engine := NewPromScraperEngine(db, cfg)

	_, err := engine.Scrape("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent target")
	}
}

func TestPromScraperStartStop(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultPromScraperConfig()
	engine := NewPromScraperEngine(db, cfg)

	if err := engine.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := engine.Start(); err != nil {
		t.Fatalf("second start: %v", err)
	}
	if err := engine.Stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if err := engine.Stop(); err != nil {
		t.Fatalf("second stop: %v", err)
	}
}

func TestPromScraperDefaultConfig(t *testing.T) {
	cfg := DefaultPromScraperConfig()
	if cfg.ScrapeInterval != 15*time.Second {
		t.Errorf("scrape interval: got %v, want %v", cfg.ScrapeInterval, 15*time.Second)
	}
	if cfg.MaxTargets != 100 {
		t.Errorf("max targets: got %d, want 100", cfg.MaxTargets)
	}
}

func TestPromScraperHTTPHandlers(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultPromScraperConfig()
	engine := NewPromScraperEngine(db, cfg)

	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	req := httptest.NewRequest("GET", "/api/v1/scraper/stats", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("stats status: got %d, want %d", w.Code, http.StatusOK)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type: got %q, want %q", ct, "application/json")
	}
}
