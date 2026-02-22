package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// QueryProfilerConfig configures the interactive query profiler.
type QueryProfilerConfig struct {
	Enabled           bool
	SlowQueryThreshold time.Duration
	MaxProfiles       int
	EnableTracing     bool
	RecordAllQueries  bool
}

// DefaultQueryProfilerConfig returns sensible defaults.
func DefaultQueryProfilerConfig() QueryProfilerConfig {
	return QueryProfilerConfig{
		Enabled:           true,
		SlowQueryThreshold: 100 * time.Millisecond,
		MaxProfiles:       1000,
		EnableTracing:     true,
		RecordAllQueries:  false,
	}
}

// QueryProfile represents a profiled query execution.
type QueryProfile struct {
	ID              string              `json:"id"`
	Query           string              `json:"query"`
	QueryType       string              `json:"query_type"` // sql, promql, cql
	StartedAt       time.Time           `json:"started_at"`
	Duration        time.Duration       `json:"duration"`
	IsSlow          bool                `json:"is_slow"`
	Stages          []ProfileStage      `json:"stages"`
	PartitionsScanned int               `json:"partitions_scanned"`
	IndexLookups    int                 `json:"index_lookups"`
	PointsScanned   int64               `json:"points_scanned"`
	PointsReturned  int64               `json:"points_returned"`
	BytesRead       int64               `json:"bytes_read"`
	BytesReturned   int64               `json:"bytes_returned"`
	MemoryUsedBytes int64               `json:"memory_used_bytes"`
	CacheHit        bool                `json:"cache_hit"`
	Recommendations []string            `json:"recommendations,omitempty"`
}

// ProfileStage represents a phase of query execution.
type ProfileStage struct {
	Name        string        `json:"name"`
	Duration    time.Duration `json:"duration"`
	Description string        `json:"description,omitempty"`
	Detail      string        `json:"detail,omitempty"`
}

// QueryExplainPlan represents a query explanation.
type QueryExplainPlan struct {
	Query           string            `json:"query"`
	Metric          string            `json:"metric"`
	EstimatedCost   float64           `json:"estimated_cost"`
	EstimatedRows   int64             `json:"estimated_rows"`
	PartitionPruned int               `json:"partitions_pruned"`
	PartitionTotal  int               `json:"partitions_total"`
	IndexUsed       bool              `json:"index_used"`
	Steps           []ExplainStep     `json:"steps"`
	Warnings        []string          `json:"warnings,omitempty"`
	Suggestions     []string          `json:"suggestions,omitempty"`
}

// ExplainStep represents a step in the query plan.
type ExplainStep struct {
	Operation   string  `json:"operation"`
	Description string  `json:"description"`
	Cost        float64 `json:"cost"`
}

// QueryProfilerStats holds profiler statistics.
type QueryProfilerStats struct {
	TotalProfiled    int64         `json:"total_profiled"`
	SlowQueries      int64         `json:"slow_queries"`
	AvgDuration      time.Duration `json:"avg_duration"`
	P95Duration      time.Duration `json:"p95_duration"`
	P99Duration      time.Duration `json:"p99_duration"`
	CacheHitRate     float64       `json:"cache_hit_rate"`
	TotalBytesRead   int64         `json:"total_bytes_read"`
}

// QueryProfilerEngine provides interactive query profiling and analysis.
type QueryProfilerEngine struct {
	db     *DB
	config QueryProfilerConfig

	mu       sync.RWMutex
	profiles []*QueryProfile
	running  bool
	stopCh   chan struct{}
	stats    QueryProfilerStats
	profSeq  int64
	cacheHits int64
	cacheMisses int64
}

// NewQueryProfilerEngine creates a new query profiler engine.
func NewQueryProfilerEngine(db *DB, cfg QueryProfilerConfig) *QueryProfilerEngine {
	return &QueryProfilerEngine{
		db:       db,
		config:   cfg,
		profiles: make([]*QueryProfile, 0),
		stopCh:   make(chan struct{}),
	}
}

// Start starts the profiler.
func (e *QueryProfilerEngine) Start() {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true
	e.mu.Unlock()
}

// Stop stops the profiler.
func (e *QueryProfilerEngine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	close(e.stopCh)
}

// Profile executes and profiles a query.
func (e *QueryProfilerEngine) Profile(queryStr string, queryType string) (*QueryProfile, error) {
	if queryStr == "" {
		return nil, fmt.Errorf("empty query")
	}

	start := time.Now()
	e.profSeq++

	profile := &QueryProfile{
		ID:        fmt.Sprintf("prof-%d", e.profSeq),
		Query:     queryStr,
		QueryType: queryType,
		StartedAt: start,
		Stages:    make([]ProfileStage, 0),
	}

	// Stage 1: Parse
	parseStart := time.Now()
	metric := ""
	switch queryType {
	case "promql":
		metric = extractMetricFromPromQL(queryStr)
	case "sql":
		metric = extractMetricFromSQL(queryStr)
	default:
		metric = queryStr
	}
	profile.Stages = append(profile.Stages, ProfileStage{
		Name:     "parse",
		Duration: time.Since(parseStart),
		Detail:   fmt.Sprintf("metric=%s", metric),
	})

	// Stage 2: Plan
	planStart := time.Now()
	q := Query{
		Metric: metric,
		Start:  0,
		End:    time.Now().UnixNano(),
	}
	profile.Stages = append(profile.Stages, ProfileStage{
		Name:     "plan",
		Duration: time.Since(planStart),
		Detail:   fmt.Sprintf("start=%d end=%d", q.Start, q.End),
	})

	// Stage 3: Execute
	execStart := time.Now()
	result, err := e.db.Execute(&q)
	execDuration := time.Since(execStart)
	profile.Stages = append(profile.Stages, ProfileStage{
		Name:     "execute",
		Duration: execDuration,
	})

	if err != nil {
		profile.Duration = time.Since(start)
		return profile, err
	}

	// Stage 4: Collect results
	collectStart := time.Now()
	if result != nil {
		profile.PointsReturned += int64(len(result.Points))
		profile.BytesReturned += int64(len(result.Points)) * 24 // estimate
	}
	profile.Stages = append(profile.Stages, ProfileStage{
		Name:     "collect",
		Duration: time.Since(collectStart),
		Detail:   fmt.Sprintf("series=%d points=%d", 1, profile.PointsReturned),
	})

	profile.Duration = time.Since(start)
	profile.IsSlow = profile.Duration > e.config.SlowQueryThreshold
	profile.PointsScanned = profile.PointsReturned // approximation
	profile.BytesRead = profile.PointsScanned * 24

	// Generate recommendations
	profile.Recommendations = e.generateRecommendations(profile)

	// Store profile
	e.mu.Lock()
	e.stats.TotalProfiled++
	if profile.IsSlow {
		e.stats.SlowQueries++
	}
	e.stats.TotalBytesRead += profile.BytesRead

	if e.config.RecordAllQueries || profile.IsSlow {
		e.profiles = append(e.profiles, profile)
		if len(e.profiles) > e.config.MaxProfiles {
			e.profiles = e.profiles[len(e.profiles)-e.config.MaxProfiles:]
		}
	}

	if e.stats.AvgDuration == 0 {
		e.stats.AvgDuration = profile.Duration
	} else {
		e.stats.AvgDuration = (e.stats.AvgDuration + profile.Duration) / 2
	}
	e.mu.Unlock()

	return profile, nil
}

// Explain produces an execution plan without running the query.
func (e *QueryProfilerEngine) Explain(queryStr string, queryType string) *QueryExplainPlan {
	metric := ""
	switch queryType {
	case "promql":
		metric = extractMetricFromPromQL(queryStr)
	case "sql":
		metric = extractMetricFromSQL(queryStr)
	default:
		metric = queryStr
	}

	plan := &QueryExplainPlan{
		Query:  queryStr,
		Metric: metric,
		Steps:  make([]ExplainStep, 0),
	}

	// Step 1: Index lookup
	plan.Steps = append(plan.Steps, ExplainStep{
		Operation:   "IndexScan",
		Description: fmt.Sprintf("Lookup metric '%s' in B-tree index", metric),
		Cost:        1.0,
	})

	// Step 2: Partition scan
	plan.Steps = append(plan.Steps, ExplainStep{
		Operation:   "PartitionScan",
		Description: "Scan matching partitions for time range",
		Cost:        10.0,
	})

	// Step 3: Filter
	plan.Steps = append(plan.Steps, ExplainStep{
		Operation:   "Filter",
		Description: "Apply tag filters and time range predicates",
		Cost:        5.0,
	})

	// Step 4: Aggregate (if applicable)
	plan.Steps = append(plan.Steps, ExplainStep{
		Operation:   "Aggregate",
		Description: "Apply aggregation function",
		Cost:        3.0,
	})

	plan.IndexUsed = true
	plan.EstimatedCost = 19.0
	plan.EstimatedRows = 100

	// Add suggestions
	if metric == "" {
		plan.Warnings = append(plan.Warnings, "No metric specified; full table scan required")
		plan.EstimatedCost = 1000.0
	}

	return plan
}

// ListProfiles returns stored profiles.
func (e *QueryProfilerEngine) ListProfiles(slowOnly bool, limit int) []*QueryProfile {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var filtered []*QueryProfile
	for _, p := range e.profiles {
		if slowOnly && !p.IsSlow {
			continue
		}
		filtered = append(filtered, p)
	}

	if limit > 0 && len(filtered) > limit {
		filtered = filtered[len(filtered)-limit:]
	}

	return filtered
}

// ClearProfiles removes all stored profiles.
func (e *QueryProfilerEngine) ClearProfiles() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.profiles = make([]*QueryProfile, 0)
}

// GetStats returns profiler statistics.
func (e *QueryProfilerEngine) GetStats() QueryProfilerStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	stats := e.stats
	total := e.cacheHits + e.cacheMisses
	if total > 0 {
		stats.CacheHitRate = float64(e.cacheHits) / float64(total)
	}
	return stats
}

func (e *QueryProfilerEngine) generateRecommendations(p *QueryProfile) []string {
	var recs []string

	if p.PartitionsScanned > 100 {
		recs = append(recs, "Consider narrowing the time range to reduce partitions scanned")
	}
	if p.PointsScanned > 0 && p.PointsReturned > 0 {
		selectivity := float64(p.PointsReturned) / float64(p.PointsScanned)
		if selectivity < 0.01 {
			recs = append(recs, "Low selectivity — add more specific tag filters")
		}
	}
	if p.Duration > 500*time.Millisecond {
		recs = append(recs, "Consider creating a recording rule for frequently-executed queries")
	}
	if p.BytesRead > 100*1024*1024 {
		recs = append(recs, "Large data scan — consider downsampling or materialized views")
	}

	return recs
}

// RegisterHTTPHandlers registers HTTP endpoints.
func (e *QueryProfilerEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/profiler/profile", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Query     string `json:"query"`
			QueryType string `json:"query_type"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		profile, err := e.Profile(req.Query, req.QueryType)
		if err != nil {
			// Return profile even on error (shows where it failed)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"profile": profile,
				"error":   err.Error(),
			})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(profile)
	})

	mux.HandleFunc("/api/v1/profiler/explain", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Query     string `json:"query"`
			QueryType string `json:"query_type"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		plan := e.Explain(req.Query, req.QueryType)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(plan)
	})

	mux.HandleFunc("/api/v1/profiler/slow-queries", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.ListProfiles(true, 50))
	})

	mux.HandleFunc("/api/v1/profiler/profiles", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.ListProfiles(false, 100))
	})

	mux.HandleFunc("/api/v1/profiler/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetStats())
	})
}
