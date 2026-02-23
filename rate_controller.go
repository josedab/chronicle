package chronicle

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// RateControllerConfig configures the ingestion rate controller.
type RateControllerConfig struct {
	Enabled        bool          `json:"enabled"`
	GlobalRateLimit int64        `json:"global_rate_limit"`
	PerMetricLimit  int64        `json:"per_metric_limit"`
	BurstSize       int64        `json:"burst_size"`
	WindowDuration  time.Duration `json:"window_duration"`
}

// DefaultRateControllerConfig returns sensible defaults.
func DefaultRateControllerConfig() RateControllerConfig {
	return RateControllerConfig{
		Enabled:         true,
		GlobalRateLimit: 100000,
		PerMetricLimit:  10000,
		BurstSize:       1000,
		WindowDuration:  time.Second,
	}
}

// RateBucket implements a token bucket for rate limiting.
type RateBucket struct {
	Tokens     float64   `json:"tokens"`
	MaxTokens  float64   `json:"max_tokens"`
	RefillRate float64   `json:"refill_rate"`
	LastRefill time.Time `json:"last_refill"`
}

// RateControllerStats tracks rate controller statistics.
type RateControllerStats struct {
	TotalAllowed     int64             `json:"total_allowed"`
	TotalThrottled   int64             `json:"total_throttled"`
	ThrottledMetrics map[string]int64  `json:"throttled_metrics"`
}

// RateControllerEngine manages ingestion rate limiting.
type RateControllerEngine struct {
	db     *DB
	config RateControllerConfig

	globalBucket *RateBucket
	metricBuckets map[string]*RateBucket
	stats         RateControllerStats
	running       bool
	stopCh        chan struct{}

	mu sync.RWMutex
}

// NewRateControllerEngine creates a new rate controller engine.
func NewRateControllerEngine(db *DB, cfg RateControllerConfig) *RateControllerEngine {
	return &RateControllerEngine{
		db:     db,
		config: cfg,
		globalBucket: &RateBucket{
			Tokens:     float64(cfg.BurstSize),
			MaxTokens:  float64(cfg.BurstSize),
			RefillRate: float64(cfg.GlobalRateLimit),
			LastRefill: time.Now(),
		},
		metricBuckets: make(map[string]*RateBucket),
		stats: RateControllerStats{
			ThrottledMetrics: make(map[string]int64),
		},
		stopCh: make(chan struct{}),
	}
}

// Start starts the rate controller engine.
func (e *RateControllerEngine) Start() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.running {
		return
	}
	e.running = true
}

// Stop stops the rate controller engine.
func (e *RateControllerEngine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	close(e.stopCh)
}

func (e *RateControllerEngine) refillBucket(b *RateBucket) {
	now := time.Now()
	elapsed := now.Sub(b.LastRefill).Seconds()
	b.Tokens += elapsed * b.RefillRate
	if b.Tokens > b.MaxTokens {
		b.Tokens = b.MaxTokens
	}
	b.LastRefill = now
}

func (e *RateControllerEngine) getMetricBucket(metric string) *RateBucket {
	bucket, exists := e.metricBuckets[metric]
	if !exists {
		bucket = &RateBucket{
			Tokens:     float64(e.config.BurstSize),
			MaxTokens:  float64(e.config.BurstSize),
			RefillRate: float64(e.config.PerMetricLimit),
			LastRefill: time.Now(),
		}
		e.metricBuckets[metric] = bucket
	}
	return bucket
}

// Allow checks if a write for the given metric is allowed.
func (e *RateControllerEngine) Allow(metric string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Check global bucket
	e.refillBucket(e.globalBucket)
	if e.globalBucket.Tokens < 1 {
		e.stats.TotalThrottled++
		e.stats.ThrottledMetrics[metric]++
		return false
	}

	// Check per-metric bucket
	metricBucket := e.getMetricBucket(metric)
	e.refillBucket(metricBucket)
	if metricBucket.Tokens < 1 {
		e.stats.TotalThrottled++
		e.stats.ThrottledMetrics[metric]++
		return false
	}

	e.globalBucket.Tokens--
	metricBucket.Tokens--
	e.stats.TotalAllowed++
	return true
}

// GetMetricRate returns the current token count for a metric bucket.
func (e *RateControllerEngine) GetMetricRate(metric string) float64 {
	e.mu.RLock()
	defer e.mu.RUnlock()

	bucket, exists := e.metricBuckets[metric]
	if !exists {
		return 0
	}
	return bucket.Tokens
}

// SetMetricLimit sets a custom rate limit for a specific metric.
func (e *RateControllerEngine) SetMetricLimit(metric string, limit int64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	bucket := e.getMetricBucket(metric)
	bucket.RefillRate = float64(limit)
	bucket.MaxTokens = float64(e.config.BurstSize)
}

// Stats returns aggregate rate controller statistics.
func (e *RateControllerEngine) Stats() RateControllerStats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	s := RateControllerStats{
		TotalAllowed:     e.stats.TotalAllowed,
		TotalThrottled:   e.stats.TotalThrottled,
		ThrottledMetrics: make(map[string]int64),
	}
	for k, v := range e.stats.ThrottledMetrics {
		s.ThrottledMetrics[k] = v
	}
	return s
}

// RegisterHTTPHandlers registers HTTP endpoints.
func (e *RateControllerEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/rate-controller/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.Stats())
	})
	mux.HandleFunc("/api/v1/rate-controller/check", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Metric string `json:"metric"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		allowed := e.Allow(req.Metric)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]bool{"allowed": allowed})
	})
	mux.HandleFunc("/api/v1/rate-controller/limit", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Metric string `json:"metric"`
			Limit  int64  `json:"limit"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		e.SetMetricLimit(req.Metric, req.Limit)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
}
