package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"
)

// RetentionOptimizerConfig configures the retention policy optimizer.
type RetentionOptimizerConfig struct {
	Enabled        bool          `json:"enabled"`
	StorageBudgetMB int64        `json:"storage_budget_mb"`
	CheckInterval  time.Duration `json:"check_interval"`
	MinRetention   time.Duration `json:"min_retention"`
	MaxRetention   time.Duration `json:"max_retention"`
}

// DefaultRetentionOptimizerConfig returns sensible defaults.
func DefaultRetentionOptimizerConfig() RetentionOptimizerConfig {
	return RetentionOptimizerConfig{
		Enabled:        true,
		StorageBudgetMB: 10240,
		CheckInterval:  10 * time.Minute,
		MinRetention:   24 * time.Hour,
		MaxRetention:   90 * 24 * time.Hour,
	}
}

// RetOptRecommendation represents a retention policy recommendation.
type RetOptRecommendation struct {
	Metric               string        `json:"metric"`
	CurrentRetention     time.Duration `json:"current_retention"`
	RecommendedRetention time.Duration `json:"recommended_retention"`
	Reason               string        `json:"reason"`
	Priority             int           `json:"priority"` // 1 = highest
}

// MetricImportance tracks access patterns for a metric.
type MetricImportance struct {
	Metric          string  `json:"metric"`
	AccessFrequency int64   `json:"access_frequency"`
	QueryCount      int64   `json:"query_count"`
	Importance      float64 `json:"importance"` // 0.0 to 1.0
}

// RetentionOptimizerStats holds optimizer statistics.
type RetentionOptimizerStats struct {
	MetricsTracked       int   `json:"metrics_tracked"`
	TotalAccesses        int64 `json:"total_accesses"`
	RecommendationsCount int   `json:"recommendations_count"`
}

// RetentionOptimizerEngine analyzes metric access patterns to recommend retention policies.
type RetentionOptimizerEngine struct {
	db      *DB
	config  RetentionOptimizerConfig
	mu      sync.RWMutex
	metrics map[string]*MetricImportance
	stats   RetentionOptimizerStats
	stopCh  chan struct{}
	running bool
}

// NewRetentionOptimizerEngine creates a new RetentionOptimizerEngine.
func NewRetentionOptimizerEngine(db *DB, cfg RetentionOptimizerConfig) *RetentionOptimizerEngine {
	return &RetentionOptimizerEngine{
		db:      db,
		config:  cfg,
		metrics: make(map[string]*MetricImportance),
		stopCh:  make(chan struct{}),
	}
}

// Start begins the retention optimizer engine.
func (e *RetentionOptimizerEngine) Start() {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true
	e.mu.Unlock()
}

// Stop halts the retention optimizer engine.
func (e *RetentionOptimizerEngine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	close(e.stopCh)
}

// RecordAccess records a metric access event.
func (e *RetentionOptimizerEngine) RecordAccess(metric string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	m, ok := e.metrics[metric]
	if !ok {
		m = &MetricImportance{Metric: metric}
		e.metrics[metric] = m
	}
	m.AccessFrequency++
	m.QueryCount++
	e.stats.TotalAccesses++
	e.stats.MetricsTracked = len(e.metrics)
	e.recalcImportance()
}

// GetImportance returns the importance score for a metric.
func (e *RetentionOptimizerEngine) GetImportance(metric string) (MetricImportance, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	m, ok := e.metrics[metric]
	if !ok {
		return MetricImportance{}, false
	}
	return *m, true
}

// Analyze produces retention recommendations based on access patterns.
func (e *RetentionOptimizerEngine) Analyze() []RetOptRecommendation {
	e.mu.RLock()
	defer e.mu.RUnlock()
	var recs []RetOptRecommendation
	for _, m := range e.metrics {
		rec := RetOptRecommendation{
			Metric:           m.Metric,
			CurrentRetention: e.config.MaxRetention,
		}
		if m.Importance >= 0.7 {
			rec.RecommendedRetention = e.config.MaxRetention
			rec.Reason = "high importance: frequently accessed metric"
			rec.Priority = 3
		} else if m.Importance >= 0.3 {
			rec.RecommendedRetention = (e.config.MaxRetention + e.config.MinRetention) / 2
			rec.Reason = "medium importance: moderate access frequency"
			rec.Priority = 2
		} else {
			rec.RecommendedRetention = e.config.MinRetention
			rec.Reason = "low importance: rarely accessed metric"
			rec.Priority = 1
		}
		recs = append(recs, rec)
	}
	sort.Slice(recs, func(i, j int) bool {
		return recs[i].Priority < recs[j].Priority
	})
	e.stats.RecommendationsCount = len(recs)
	return recs
}

// GetStats returns optimizer statistics.
func (e *RetentionOptimizerEngine) GetStats() RetentionOptimizerStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.stats
}

func (e *RetentionOptimizerEngine) recalcImportance() {
	var maxFreq int64
	for _, m := range e.metrics {
		if m.AccessFrequency > maxFreq {
			maxFreq = m.AccessFrequency
		}
	}
	if maxFreq == 0 {
		return
	}
	for _, m := range e.metrics {
		m.Importance = float64(m.AccessFrequency) / float64(maxFreq)
	}
}

// RegisterHTTPHandlers registers retention optimizer HTTP endpoints.
func (e *RetentionOptimizerEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/retention-optimizer/analyze", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.Analyze())
	})
	mux.HandleFunc("/api/v1/retention-optimizer/importance", func(w http.ResponseWriter, r *http.Request) {
		metric := r.URL.Query().Get("metric")
		if metric == "" {
			http.Error(w, "metric required", http.StatusBadRequest)
			return
		}
		imp, ok := e.GetImportance(metric)
		if !ok {
			http.Error(w, "metric not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(imp)
	})
	mux.HandleFunc("/api/v1/retention-optimizer/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetStats())
	})
	mux.HandleFunc("/api/v1/retention-optimizer/record", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		metric := r.URL.Query().Get("metric")
		if metric == "" {
			http.Error(w, "metric required", http.StatusBadRequest)
			return
		}
		e.RecordAccess(metric)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "recorded"})
	})
}

// --- Query Frequency Heat Map ---

// HeatMapCell represents a single cell in the query frequency heat map.
type HeatMapCell struct {
	Metric    string  `json:"metric"`
	TimeStart int64   `json:"time_start"`
	TimeEnd   int64   `json:"time_end"`
	QueryFreq int64   `json:"query_frequency"`
	HeatScore float64 `json:"heat_score"` // 0.0 (cold) to 1.0 (hot)
}

// QueryHeatMap tracks query frequency per metric/time-range for tiering decisions.
type QueryHeatMap struct {
	cells      map[string]map[int64]*HeatMapCell // metric -> time_bucket -> cell
	bucketSize time.Duration
	mu         sync.RWMutex
}

// NewQueryHeatMap creates a new heat map with the specified bucket size.
func NewQueryHeatMap(bucketSize time.Duration) *QueryHeatMap {
	if bucketSize <= 0 {
		bucketSize = time.Hour
	}
	return &QueryHeatMap{
		cells:      make(map[string]map[int64]*HeatMapCell),
		bucketSize: bucketSize,
	}
}

// RecordQuery records a query access for a metric in a time range.
func (hm *QueryHeatMap) RecordQuery(metric string, startTime, endTime int64) {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	if hm.cells[metric] == nil {
		hm.cells[metric] = make(map[int64]*HeatMapCell)
	}

	bucketNs := hm.bucketSize.Nanoseconds()
	startBucket := (startTime / bucketNs) * bucketNs
	endBucket := (endTime / bucketNs) * bucketNs

	for bucket := startBucket; bucket <= endBucket; bucket += bucketNs {
		cell, ok := hm.cells[metric][bucket]
		if !ok {
			cell = &HeatMapCell{
				Metric:    metric,
				TimeStart: bucket,
				TimeEnd:   bucket + bucketNs,
			}
			hm.cells[metric][bucket] = cell
		}
		cell.QueryFreq++
	}
}

// ComputeHeatScores normalizes heat scores across all cells.
func (hm *QueryHeatMap) ComputeHeatScores() {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	var maxFreq int64
	for _, buckets := range hm.cells {
		for _, cell := range buckets {
			if cell.QueryFreq > maxFreq {
				maxFreq = cell.QueryFreq
			}
		}
	}

	if maxFreq == 0 {
		return
	}

	for _, buckets := range hm.cells {
		for _, cell := range buckets {
			cell.HeatScore = float64(cell.QueryFreq) / float64(maxFreq)
		}
	}
}

// GetHotCells returns cells with heat score above the threshold.
func (hm *QueryHeatMap) GetHotCells(threshold float64) []HeatMapCell {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	var hot []HeatMapCell
	for _, buckets := range hm.cells {
		for _, cell := range buckets {
			if cell.HeatScore >= threshold {
				hot = append(hot, *cell)
			}
		}
	}

	sort.Slice(hot, func(i, j int) bool {
		return hot[i].HeatScore > hot[j].HeatScore
	})
	return hot
}

// GetColdCells returns cells with heat score below the threshold.
func (hm *QueryHeatMap) GetColdCells(threshold float64) []HeatMapCell {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	var cold []HeatMapCell
	for _, buckets := range hm.cells {
		for _, cell := range buckets {
			if cell.HeatScore < threshold {
				cold = append(cold, *cell)
			}
		}
	}
	return cold
}

// --- Storage Cost Model ---

// TierCostConfig defines the cost per GB-month for a storage tier.
type TierCostConfig struct {
	TierName        string  `json:"tier_name"`
	CostPerGBMonth  float64 `json:"cost_per_gb_month"` // USD
	ReadCostPer1K   float64 `json:"read_cost_per_1k"`  // USD per 1000 reads
	WriteCostPer1K  float64 `json:"write_cost_per_1k"` // USD per 1000 writes
	TransitionCost  float64 `json:"transition_cost"`    // USD per GB to transition
	MinStorageDays  int     `json:"min_storage_days"`   // minimum before transition
}

// DefaultTierCosts returns cost configurations for standard tiers.
func DefaultTierCosts() []TierCostConfig {
	return []TierCostConfig{
		{TierName: "hot", CostPerGBMonth: 0.023, ReadCostPer1K: 0.0004, WriteCostPer1K: 0.005},
		{TierName: "warm", CostPerGBMonth: 0.0125, ReadCostPer1K: 0.001, WriteCostPer1K: 0.01, TransitionCost: 0.01, MinStorageDays: 30},
		{TierName: "cold", CostPerGBMonth: 0.004, ReadCostPer1K: 0.01, WriteCostPer1K: 0.05, TransitionCost: 0.02, MinStorageDays: 90},
		{TierName: "archive", CostPerGBMonth: 0.00099, ReadCostPer1K: 0.10, WriteCostPer1K: 0.0, TransitionCost: 0.05, MinStorageDays: 180},
	}
}

// StorageCostOptimizer solves for optimal data placement across tiers.
type StorageCostOptimizer struct {
	tiers []TierCostConfig
}

// NewStorageCostOptimizer creates a cost optimizer with the given tier costs.
func NewStorageCostOptimizer(tiers []TierCostConfig) *StorageCostOptimizer {
	return &StorageCostOptimizer{tiers: tiers}
}

// TieringRecommendation is the optimizer's output for a specific metric/time-range.
type TieringRecommendation struct {
	Metric      string  `json:"metric"`
	CurrentTier string  `json:"current_tier"`
	OptimalTier string  `json:"optimal_tier"`
	SizeGB      float64 `json:"size_gb"`
	MonthlyCost float64 `json:"monthly_cost_current"`
	OptimalCost float64 `json:"monthly_cost_optimal"`
	Savings     float64 `json:"savings_pct"`
}

// Optimize determines the optimal tier for data based on access patterns and cost.
func (o *StorageCostOptimizer) Optimize(metric string, sizeGB float64, readsPerMonth, writesPerMonth int64, ageDays int) TieringRecommendation {
	rec := TieringRecommendation{
		Metric: metric,
		SizeGB: sizeGB,
	}

	bestCost := float64(1<<63 - 1)
	var bestTier string

	for _, tier := range o.tiers {
		if ageDays < tier.MinStorageDays {
			continue
		}

		// Total monthly cost = storage + reads + writes
		storageCost := sizeGB * tier.CostPerGBMonth
		readCost := float64(readsPerMonth) / 1000.0 * tier.ReadCostPer1K
		writeCost := float64(writesPerMonth) / 1000.0 * tier.WriteCostPer1K
		totalCost := storageCost + readCost + writeCost

		if totalCost < bestCost {
			bestCost = totalCost
			bestTier = tier.TierName
		}
	}

	// Current cost (assuming hot tier)
	if len(o.tiers) > 0 {
		rec.CurrentTier = o.tiers[0].TierName
		rec.MonthlyCost = sizeGB * o.tiers[0].CostPerGBMonth
	}
	rec.OptimalTier = bestTier
	rec.OptimalCost = bestCost
	if rec.MonthlyCost > 0 {
		rec.Savings = (rec.MonthlyCost - rec.OptimalCost) / rec.MonthlyCost * 100
	}

	return rec
}

// --- Autonomous Tiering Agent ---

// LifecycleAgentConfig configures the autonomous lifecycle agent.
type LifecycleAgentConfig struct {
	Enabled     bool          `json:"enabled"`
	DryRun      bool          `json:"dry_run"` // preview changes without applying
	Interval    time.Duration `json:"interval"`
	HotThreshold  float64     `json:"hot_threshold"`  // heat score above = hot
	ColdThreshold float64     `json:"cold_threshold"` // heat score below = cold
	TargetSavings float64     `json:"target_savings"` // target cost reduction pct
}

// DefaultLifecycleAgentConfig returns defaults targeting ≥30% savings.
func DefaultLifecycleAgentConfig() LifecycleAgentConfig {
	return LifecycleAgentConfig{
		Enabled:       true,
		DryRun:        true,
		Interval:      time.Hour,
		HotThreshold:  0.7,
		ColdThreshold: 0.1,
		TargetSavings: 30.0,
	}
}

// LifecycleAction represents a tiering action to take.
type LifecycleAction struct {
	Metric    string    `json:"metric"`
	Action    string    `json:"action"` // "tier_down", "tier_up", "compress", "delete"
	FromTier  string    `json:"from_tier"`
	ToTier    string    `json:"to_tier"`
	SizeGB    float64   `json:"size_gb"`
	Savings   float64   `json:"estimated_savings"`
	Timestamp time.Time `json:"timestamp"`
	DryRun    bool      `json:"dry_run"`
}

// LifecycleAgentResult holds the output of an agent evaluation cycle.
type LifecycleAgentResult struct {
	CycleID          string            `json:"cycle_id"`
	Timestamp        time.Time         `json:"timestamp"`
	Actions          []LifecycleAction `json:"actions"`
	TotalSavings     float64           `json:"total_savings_pct"`
	MetricsEvaluated int               `json:"metrics_evaluated"`
	DryRun           bool              `json:"dry_run"`
}

// LifecycleAgent is an autonomous agent that predicts and applies optimal
// retention, compression, and tiering policies.
type LifecycleAgent struct {
	config    LifecycleAgentConfig
	heatMap   *QueryHeatMap
	optimizer *StorageCostOptimizer
	history   []LifecycleAgentResult
	mu        sync.RWMutex
	stopCh    chan struct{}
}

// NewLifecycleAgent creates a new autonomous lifecycle agent.
func NewLifecycleAgent(config LifecycleAgentConfig) *LifecycleAgent {
	return &LifecycleAgent{
		config:    config,
		heatMap:   NewQueryHeatMap(time.Hour),
		optimizer: NewStorageCostOptimizer(DefaultTierCosts()),
		stopCh:    make(chan struct{}),
	}
}

// Evaluate runs one cycle of the lifecycle agent and returns recommendations.
func (a *LifecycleAgent) Evaluate(metrics map[string]float64) *LifecycleAgentResult {
	a.heatMap.ComputeHeatScores()

	result := &LifecycleAgentResult{
		CycleID:          fmt.Sprintf("cycle-%d", time.Now().UnixNano()),
		Timestamp:        time.Now(),
		MetricsEvaluated: len(metrics),
		DryRun:           a.config.DryRun,
	}

	for metric, sizeGB := range metrics {
		hot := a.heatMap.GetHotCells(a.config.HotThreshold)
		cold := a.heatMap.GetColdCells(a.config.ColdThreshold)

		isHot := false
		for _, c := range hot {
			if c.Metric == metric {
				isHot = true
				break
			}
		}
		isCold := false
		for _, c := range cold {
			if c.Metric == metric {
				isCold = true
				break
			}
		}

		if isCold {
			rec := a.optimizer.Optimize(metric, sizeGB, 10, 0, 90)
			if rec.Savings > 0 {
				result.Actions = append(result.Actions, LifecycleAction{
					Metric:    metric,
					Action:    "tier_down",
					FromTier:  "hot",
					ToTier:    rec.OptimalTier,
					SizeGB:    sizeGB,
					Savings:   rec.Savings,
					Timestamp: time.Now(),
					DryRun:    a.config.DryRun,
				})
				result.TotalSavings += rec.Savings
			}
		} else if !isHot {
			rec := a.optimizer.Optimize(metric, sizeGB, 100, 10, 30)
			if rec.Savings > 10 {
				result.Actions = append(result.Actions, LifecycleAction{
					Metric:    metric,
					Action:    "tier_down",
					FromTier:  "hot",
					ToTier:    rec.OptimalTier,
					SizeGB:    sizeGB,
					Savings:   rec.Savings,
					Timestamp: time.Now(),
					DryRun:    a.config.DryRun,
				})
				result.TotalSavings += rec.Savings
			}
		}
	}

	if len(metrics) > 0 {
		result.TotalSavings /= float64(len(metrics))
	}

	a.mu.Lock()
	a.history = append(a.history, *result)
	a.mu.Unlock()

	return result
}

// History returns past evaluation results.
func (a *LifecycleAgent) History() []LifecycleAgentResult {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make([]LifecycleAgentResult, len(a.history))
	copy(out, a.history)
	return out
}

// RecordQuery records a query for heat map tracking.
func (a *LifecycleAgent) RecordQuery(metric string, startTime, endTime int64) {
	a.heatMap.RecordQuery(metric, startTime, endTime)
}

// Stop halts the lifecycle agent.
func (a *LifecycleAgent) Stop() {
	close(a.stopCh)
}
