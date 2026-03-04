package chronicle

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

// PromScraperConfig configures the embedded Prometheus scraper.
type PromScraperConfig struct {
	Enabled        bool          `json:"enabled"`
	ScrapeInterval time.Duration `json:"scrape_interval"`
	ScrapeTimeout  time.Duration `json:"scrape_timeout"`
	MaxTargets     int           `json:"max_targets"`
}

// DefaultPromScraperConfig returns sensible defaults.
func DefaultPromScraperConfig() PromScraperConfig {
	return PromScraperConfig{
		Enabled:        true,
		ScrapeInterval: 15 * time.Second,
		ScrapeTimeout:  10 * time.Second,
		MaxTargets:     100,
	}
}

// PromScrapeTarget represents a scrape target configuration.
type PromScrapeTarget struct {
	ID             string            `json:"id"`
	URL            string            `json:"url"`
	Labels         map[string]string `json:"labels"`
	Interval       time.Duration     `json:"interval"`
	Enabled        bool              `json:"enabled"`
	LastScrape     time.Time         `json:"last_scrape"`
	LastError      string            `json:"last_error"`
	ScrapeDuration time.Duration     `json:"scrape_duration"`
	SamplesScraped int64             `json:"samples_scraped"`
}

// PromScrapeResult represents the result of a single scrape.
type PromScrapeResult struct {
	TargetID     string        `json:"target_id"`
	Success      bool          `json:"success"`
	SamplesCount int           `json:"samples_count"`
	Duration     time.Duration `json:"duration"`
	Error        string        `json:"error"`
	ScrapedAt    time.Time     `json:"scraped_at"`
}

// PromScraperStats tracks scraper statistics.
type PromScraperStats struct {
	TotalTargets        int   `json:"total_targets"`
	TotalScrapes        int64 `json:"total_scrapes"`
	TotalSamplesScraped int64 `json:"total_samples_scraped"`
	FailedScrapes       int64 `json:"failed_scrapes"`
}

// PromScraperEngine manages Prometheus scraping.
type PromScraperEngine struct {
	db     *DB
	config PromScraperConfig

	targets map[string]*PromScrapeTarget
	stats   PromScraperStats
	running bool
	stopCh  chan struct{}

	mu sync.RWMutex
}

// NewPromScraperEngine creates a new Prometheus scraper engine.
func NewPromScraperEngine(db *DB, cfg PromScraperConfig) *PromScraperEngine {
	return &PromScraperEngine{
		db:      db,
		config:  cfg,
		targets: make(map[string]*PromScrapeTarget),
		stopCh:  make(chan struct{}),
	}
}

// Start begins the scraper engine.
func (e *PromScraperEngine) Start() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.running {
		return nil
	}
	e.running = true
	return nil
}

// Stop halts the scraper engine.
func (e *PromScraperEngine) Stop() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.running {
		return nil
	}
	e.running = false
	return nil
}

// AddTarget registers a new scrape target.
func (e *PromScraperEngine) AddTarget(target PromScrapeTarget) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.targets[target.ID]; exists {
		return fmt.Errorf("target %q already exists", target.ID)
	}

	if len(e.targets) >= e.config.MaxTargets {
		return fmt.Errorf("max targets (%d) reached", e.config.MaxTargets)
	}

	if target.Interval == 0 {
		target.Interval = e.config.ScrapeInterval
	}
	if target.Labels == nil {
		target.Labels = make(map[string]string)
	}

	e.targets[target.ID] = &target
	e.stats.TotalTargets = len(e.targets)
	return nil
}

// RemoveTarget removes a scrape target by ID.
func (e *PromScraperEngine) RemoveTarget(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.targets[id]; !exists {
		return fmt.Errorf("target %q not found", id)
	}

	delete(e.targets, id)
	e.stats.TotalTargets = len(e.targets)
	return nil
}

// ListTargets returns all registered targets.
func (e *PromScraperEngine) ListTargets() []PromScrapeTarget {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]PromScrapeTarget, 0, len(e.targets))
	for _, t := range e.targets {
		result = append(result, *t)
	}
	return result
}

// Scrape simulates scraping a target and writing results to the database.
func (e *PromScraperEngine) Scrape(targetID string) (*PromScrapeResult, error) {
	e.mu.Lock()
	target, exists := e.targets[targetID]
	if !exists {
		e.mu.Unlock()
		return nil, fmt.Errorf("target %q not found", targetID)
	}
	e.mu.Unlock()

	start := time.Now()
	samplesCount := 10
	now := time.Now()

	// Simulate writing scraped metrics
	for i := 0; i < samplesCount; i++ {
		metric := fmt.Sprintf("scrape_%s_metric_%d", targetID, i)
		if err := e.db.Write(Point{ //nolint:errcheck // best-effort scrape ingestion
			Metric:    metric,
			Value:     float64(i) * 1.5,
			Timestamp: now.UnixNano(),
			Tags:      target.Labels,
		}); err != nil {
			log.Printf("prom scraper: best-effort scrape write failed: %v", err)
		}
	}

	duration := time.Since(start)

	e.mu.Lock()
	target.LastScrape = now
	target.LastError = ""
	target.ScrapeDuration = duration
	target.SamplesScraped += int64(samplesCount)
	e.stats.TotalScrapes++
	e.stats.TotalSamplesScraped += int64(samplesCount)
	e.mu.Unlock()

	return &PromScrapeResult{
		TargetID:     targetID,
		Success:      true,
		SamplesCount: samplesCount,
		Duration:     duration,
		ScrapedAt:    now,
	}, nil
}

// Stats returns current scraper statistics.
func (e *PromScraperEngine) Stats() PromScraperStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.stats
}

// RegisterHTTPHandlers registers Prometheus scraper HTTP endpoints.
func (e *PromScraperEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/scraper/targets", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.ListTargets())
	})
	mux.HandleFunc("/api/v1/scraper/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.Stats())
	})
}
