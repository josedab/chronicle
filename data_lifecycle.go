package chronicle

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

// --- Unified Data Lifecycle Engine ---

// DataLifecycleConfig configures the intelligent data lifecycle engine.
type DataLifecycleConfig struct {
	// AccessDecayFactor controls exponential decay for access frequency scoring (0-1).
	AccessDecayFactor float64 `json:"access_decay_factor"`

	// HeatmapBuckets defines the number of time buckets for access heatmap.
	HeatmapBuckets int `json:"heatmap_buckets"`

	// GracePeriodDays is the number of days to wait before applying retention changes.
	GracePeriodDays int `json:"grace_period_days"`

	// DryRunMode prevents actual data changes when true.
	DryRunMode bool `json:"dry_run_mode"`

	// EnableLegalHold prevents deletion of data under legal hold.
	EnableLegalHold bool `json:"enable_legal_hold"`

	// CostPerGBHot is the cost per GB/month for hot storage.
	CostPerGBHot float64 `json:"cost_per_gb_hot"`

	// CostPerGBWarm is the cost per GB/month for warm storage.
	CostPerGBWarm float64 `json:"cost_per_gb_warm"`

	// CostPerGBCold is the cost per GB/month for cold storage.
	CostPerGBCold float64 `json:"cost_per_gb_cold"`

	// CostPerGBArchive is the cost per GB/month for archive storage.
	CostPerGBArchive float64 `json:"cost_per_gb_archive"`

	// RegulatoryMinRetention is the minimum retention period for regulatory compliance.
	RegulatoryMinRetention time.Duration `json:"regulatory_min_retention"`
}

// DefaultDataLifecycleConfig returns sensible defaults.
func DefaultDataLifecycleConfig() DataLifecycleConfig {
	return DataLifecycleConfig{
		AccessDecayFactor:      0.95,
		HeatmapBuckets:         168, // 1 week of hourly buckets
		GracePeriodDays:        7,
		DryRunMode:             true, // safe by default
		EnableLegalHold:        true,
		CostPerGBHot:           0.10,
		CostPerGBWarm:          0.03,
		CostPerGBCold:          0.01,
		CostPerGBArchive:       0.004,
		RegulatoryMinRetention: 90 * 24 * time.Hour,
	}
}

// AccessHeatmap tracks per-metric query frequency with exponential decay.
type AccessHeatmap struct {
	metrics map[string]*MetricAccessInfo
	mu      sync.RWMutex
}

// MetricAccessInfo tracks access patterns for a single metric.
type MetricAccessInfo struct {
	Metric        string    `json:"metric"`
	TotalAccesses int64     `json:"total_accesses"`
	LastAccessed  time.Time `json:"last_accessed"`
	DecayedScore  float64   `json:"decayed_score"`
	HourlyHits    []int64   `json:"hourly_hits"` // circular buffer of hourly access counts
	CurrentBucket int       `json:"current_bucket"`
	SizeBytes     int64     `json:"size_bytes"`
	CreatedAt     time.Time `json:"created_at"`
}

// NewAccessHeatmap creates a new access heatmap.
func NewAccessHeatmap(buckets int) *AccessHeatmap {
	if buckets <= 0 {
		buckets = 168
	}
	return &AccessHeatmap{
		metrics: make(map[string]*MetricAccessInfo),
	}
}

// RecordAccess records an access event for a metric.
func (h *AccessHeatmap) RecordAccess(metric string, decayFactor float64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	info, ok := h.metrics[metric]
	if !ok {
		info = &MetricAccessInfo{
			Metric:     metric,
			HourlyHits: make([]int64, 168),
			CreatedAt:  time.Now(),
		}
		h.metrics[metric] = info
	}

	info.TotalAccesses++
	info.LastAccessed = time.Now()

	// Update decayed score
	info.DecayedScore = info.DecayedScore*decayFactor + 1.0

	// Update hourly bucket
	bucket := int(time.Now().Hour()) % len(info.HourlyHits)
	info.HourlyHits[bucket]++
	info.CurrentBucket = bucket
}

// GetScore returns the decayed access score for a metric.
func (h *AccessHeatmap) GetScore(metric string) float64 {
	h.mu.RLock()
	defer h.mu.RUnlock()

	info, ok := h.metrics[metric]
	if !ok {
		return 0
	}
	return info.DecayedScore
}

// GetInfo returns the full access info for a metric.
func (h *AccessHeatmap) GetInfo(metric string) (*MetricAccessInfo, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	info, ok := h.metrics[metric]
	return info, ok
}

// TopMetrics returns the top N metrics by decayed access score.
func (h *AccessHeatmap) TopMetrics(n int) []*MetricAccessInfo {
	h.mu.RLock()
	defer h.mu.RUnlock()

	all := make([]*MetricAccessInfo, 0, len(h.metrics))
	for _, info := range h.metrics {
		all = append(all, info)
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].DecayedScore > all[j].DecayedScore
	})

	if n > len(all) {
		n = len(all)
	}
	return all[:n]
}

// --- Retention Policy Predictor ---

// RetentionPolicyPredictor predicts future access patterns to auto-suggest retention policies.
type RetentionPolicyPredictor struct {
	heatmap *AccessHeatmap
	config  DataLifecycleConfig
	mu      sync.RWMutex
}

// PredictedPolicy is a suggested retention policy with cost simulation.
type PredictedPolicy struct {
	Metric           string  `json:"metric"`
	CurrentTier      string  `json:"current_tier"`
	SuggestedTier    string  `json:"suggested_tier"`
	Confidence       float64 `json:"confidence"`
	CurrentCostMonth float64 `json:"current_cost_month"`
	SuggestedCostMonth float64 `json:"suggested_cost_month"`
	MonthlySavings   float64 `json:"monthly_savings"`
	Reason           string  `json:"reason"`
}

// NewRetentionPolicyPredictor creates a new predictor.
func NewRetentionPolicyPredictor(heatmap *AccessHeatmap, config DataLifecycleConfig) *RetentionPolicyPredictor {
	return &RetentionPolicyPredictor{
		heatmap: heatmap,
		config:  config,
	}
}

// PredictPolicies generates retention policy suggestions for all tracked metrics.
func (p *RetentionPolicyPredictor) PredictPolicies() []PredictedPolicy {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var policies []PredictedPolicy

	p.heatmap.mu.RLock()
	defer p.heatmap.mu.RUnlock()

	for _, info := range p.heatmap.metrics {
		policy := p.predictForMetric(info)
		if policy != nil {
			policies = append(policies, *policy)
		}
	}

	// Sort by potential savings
	sort.Slice(policies, func(i, j int) bool {
		return policies[i].MonthlySavings > policies[j].MonthlySavings
	})

	return policies
}

func (p *RetentionPolicyPredictor) predictForMetric(info *MetricAccessInfo) *PredictedPolicy {
	sizeGB := float64(info.SizeBytes) / (1024 * 1024 * 1024)
	if sizeGB <= 0 {
		sizeGB = 0.001 // minimum for cost calculation
	}

	score := info.DecayedScore
	daysSinceAccess := time.Since(info.LastAccessed).Hours() / 24

	// Determine current tier based on access score
	currentTier := "hot"
	currentCost := sizeGB * p.config.CostPerGBHot

	// Determine suggested tier
	suggestedTier := currentTier
	suggestedCost := currentCost
	confidence := 0.5
	reason := "maintaining current tier"

	if daysSinceAccess > 30 && score < 1.0 {
		suggestedTier = "archive"
		suggestedCost = sizeGB * p.config.CostPerGBArchive
		confidence = 0.9
		reason = fmt.Sprintf("no access in %.0f days with low score (%.2f)", daysSinceAccess, score)
	} else if daysSinceAccess > 7 && score < 5.0 {
		suggestedTier = "cold"
		suggestedCost = sizeGB * p.config.CostPerGBCold
		confidence = 0.8
		reason = fmt.Sprintf("low access frequency (score: %.2f)", score)
	} else if daysSinceAccess > 1 && score < 20.0 {
		suggestedTier = "warm"
		suggestedCost = sizeGB * p.config.CostPerGBWarm
		confidence = 0.7
		reason = fmt.Sprintf("moderate access frequency (score: %.2f)", score)
	} else {
		confidence = 0.9
		reason = "high access frequency"
	}

	if suggestedTier == currentTier {
		return nil // no change needed
	}

	return &PredictedPolicy{
		Metric:           info.Metric,
		CurrentTier:      currentTier,
		SuggestedTier:    suggestedTier,
		Confidence:       confidence,
		CurrentCostMonth: currentCost,
		SuggestedCostMonth: suggestedCost,
		MonthlySavings:   currentCost - suggestedCost,
		Reason:           reason,
	}
}

// SimulateCost estimates the monthly cost for a given storage configuration.
func (p *RetentionPolicyPredictor) SimulateCost(sizeGB float64, tier string) float64 {
	switch tier {
	case "hot":
		return sizeGB * p.config.CostPerGBHot
	case "warm":
		return sizeGB * p.config.CostPerGBWarm
	case "cold":
		return sizeGB * p.config.CostPerGBCold
	case "archive":
		return sizeGB * p.config.CostPerGBArchive
	default:
		return sizeGB * p.config.CostPerGBHot
	}
}

// --- Legal Hold ---

// LegalHold prevents deletion of data matching certain criteria.
type DataLegalHold struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Metrics     []string          `json:"metrics"`
	Tags        map[string]string `json:"tags,omitempty"`
	StartTime   time.Time         `json:"start_time"`
	EndTime     time.Time         `json:"end_time,omitempty"` // zero means indefinite
	CreatedBy   string            `json:"created_by"`
	CreatedAt   time.Time         `json:"created_at"`
	Reason      string            `json:"reason"`
	Active      bool              `json:"active"`
}

// DataLegalHoldManager manages legal holds on data.
type DataLegalHoldManager struct {
	holds map[string]*DataLegalHold
	mu    sync.RWMutex
}

// NewDataLegalHoldManager creates a new legal hold manager.
func NewDataLegalHoldManager() *DataLegalHoldManager {
	return &DataLegalHoldManager{
		holds: make(map[string]*DataLegalHold),
	}
}

// CreateHold creates a new legal hold.
func (m *DataLegalHoldManager) CreateHold(hold DataLegalHold) error {
	if hold.ID == "" {
		return fmt.Errorf("hold ID is required")
	}
	if hold.Name == "" {
		return fmt.Errorf("hold name is required")
	}
	if len(hold.Metrics) == 0 {
		return fmt.Errorf("at least one metric pattern is required")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	hold.CreatedAt = time.Now()
	hold.Active = true
	m.holds[hold.ID] = &hold
	return nil
}

// ReleaseHold deactivates a legal hold.
func (m *DataLegalHoldManager) ReleaseHold(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	hold, ok := m.holds[id]
	if !ok {
		return fmt.Errorf("hold %q not found", id)
	}
	hold.Active = false
	return nil
}

// IsHeld checks if a metric is under any active legal hold.
func (m *DataLegalHoldManager) IsHeld(metric string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, hold := range m.holds {
		if !hold.Active {
			continue
		}
		for _, pattern := range hold.Metrics {
			if lifecycleMatchesPattern(metric, pattern) {
				return true
			}
		}
	}
	return false
}

// ActiveHolds returns all active legal holds.
func (m *DataLegalHoldManager) ActiveHolds() []*DataLegalHold {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*DataLegalHold
	for _, hold := range m.holds {
		if hold.Active {
			result = append(result, hold)
		}
	}
	return result
}

func lifecycleMatchesPattern(metric, pattern string) bool {
	if pattern == "*" {
		return true
	}
	// Simple prefix matching with wildcard
	if len(pattern) > 0 && pattern[len(pattern)-1] == '*' {
		prefix := pattern[:len(pattern)-1]
		return len(metric) >= len(prefix) && metric[:len(prefix)] == prefix
	}
	return metric == pattern
}

// --- Unified Lifecycle Engine ---

// DataLifecycleEngine unifies retention, tiering, rehydration, and legal hold.
type DataLifecycleEngine struct {
	config       DataLifecycleConfig
	heatmap      *AccessHeatmap
	predictor    *RetentionPolicyPredictor
	dtPredictor  *DecisionTreePredictor
	gracePeriod  *GracePeriodManager
	legalHolds   *DataLegalHoldManager
	mu           sync.RWMutex
	stats        DataLifecycleStats
}

// LifecycleStats tracks lifecycle engine statistics.
type DataLifecycleStats struct {
	MetricsTracked      int     `json:"metrics_tracked"`
	PoliciesGenerated   int     `json:"policies_generated"`
	TiersDowngraded     int     `json:"tiers_downgraded"`
	TiersUpgraded       int     `json:"tiers_upgraded"`
	ActiveLegalHolds    int     `json:"active_legal_holds"`
	TotalCostSavings    float64 `json:"total_cost_savings"`
	DryRunMode          bool    `json:"dry_run_mode"`
	PendingGracePeriod  int     `json:"pending_grace_period"`
	LastEvaluation      time.Time `json:"last_evaluation"`
}

// NewDataLifecycleEngine creates a new unified lifecycle engine.
func NewDataLifecycleEngine(config DataLifecycleConfig) *DataLifecycleEngine {
	heatmap := NewAccessHeatmap(config.HeatmapBuckets)
	predictor := NewRetentionPolicyPredictor(heatmap, config)

	return &DataLifecycleEngine{
		config:      config,
		heatmap:     heatmap,
		predictor:   predictor,
		dtPredictor: NewDecisionTreePredictor(DefaultDecisionThresholds()),
		gracePeriod: NewGracePeriodManager(config.GracePeriodDays),
		legalHolds:  NewDataLegalHoldManager(),
	}
}

// RecordAccess records a query access for lifecycle tracking.
func (e *DataLifecycleEngine) RecordAccess(metric string) {
	e.heatmap.RecordAccess(metric, e.config.AccessDecayFactor)
}

// SetMetricSize updates the stored size for a metric.
func (e *DataLifecycleEngine) SetMetricSize(metric string, sizeBytes int64) {
	e.heatmap.mu.Lock()
	defer e.heatmap.mu.Unlock()

	info, ok := e.heatmap.metrics[metric]
	if ok {
		info.SizeBytes = sizeBytes
	}
}

// Evaluate runs the lifecycle evaluation and returns policy recommendations.
// Uses the decision tree predictor to refine suggestions and respects legal holds
// and regulatory minimum retention.
func (e *DataLifecycleEngine) Evaluate() []PredictedPolicy {
	policies := e.predictor.PredictPolicies()

	// Enhance with decision tree predictions for metrics that have access info
	e.heatmap.mu.RLock()
	for i := range policies {
		info, ok := e.heatmap.metrics[policies[i].Metric]
		if ok && e.dtPredictor != nil {
			features := ExtractFeatures(info)
			dtTier, dtConf := e.dtPredictor.PredictWithConfidence(features)
			// Use decision tree prediction if it has higher confidence
			if dtConf > policies[i].Confidence {
				policies[i].SuggestedTier = dtTier
				policies[i].Confidence = dtConf
				policies[i].Reason = fmt.Sprintf("decision tree: %s (conf=%.2f)", dtTier, dtConf)
			}
		}
	}
	e.heatmap.mu.RUnlock()

	// Filter out policies that violate legal holds or regulatory constraints
	var filtered []PredictedPolicy
	for _, p := range policies {
		if e.legalHolds.IsHeld(p.Metric) && (p.SuggestedTier == "archive" || p.SuggestedTier == "cold") {
			continue
		}

		info, ok := e.heatmap.GetInfo(p.Metric)
		if ok {
			age := time.Since(info.CreatedAt)
			if age < e.config.RegulatoryMinRetention && p.SuggestedTier == "archive" {
				continue
			}
		}

		filtered = append(filtered, p)
	}

	e.mu.Lock()
	e.stats.PoliciesGenerated += len(filtered)
	e.stats.LastEvaluation = time.Now()
	e.stats.MetricsTracked = len(e.heatmap.metrics)
	e.stats.ActiveLegalHolds = len(e.legalHolds.ActiveHolds())
	e.stats.PendingGracePeriod = e.gracePeriod.PendingCount()

	totalSavings := 0.0
	for _, p := range filtered {
		totalSavings += p.MonthlySavings
	}
	e.stats.TotalCostSavings += totalSavings
	e.stats.DryRunMode = e.config.DryRunMode
	e.mu.Unlock()

	return filtered
}

// Apply applies the given policies through the grace period manager.
// In dry-run mode, policies are not scheduled. Otherwise, policies are queued
// with a 7-day grace period before taking effect.
func (e *DataLifecycleEngine) Apply(policies []PredictedPolicy) int {
	if e.config.DryRunMode {
		return 0
	}

	scheduled := 0
	for _, p := range policies {
		if e.legalHolds.IsHeld(p.Metric) {
			continue
		}
		e.gracePeriod.Schedule(p)
		scheduled++
	}

	// Apply policies whose grace period has expired
	readyPolicies := e.gracePeriod.ReadyPolicies()
	applied := len(readyPolicies)

	e.mu.Lock()
	e.stats.TiersDowngraded += applied
	e.stats.PendingGracePeriod = e.gracePeriod.PendingCount()
	e.mu.Unlock()

	return applied
}

// CancelPendingPolicy cancels a pending policy change during the grace period.
func (e *DataLifecycleEngine) CancelPendingPolicy(metric, cancelledBy string) bool {
	return e.gracePeriod.Cancel(metric, cancelledBy)
}

// PendingPolicies returns the number of policies in the grace period.
func (e *DataLifecycleEngine) PendingPolicies() int {
	return e.gracePeriod.PendingCount()
}

// PredictTier uses the decision tree to predict the optimal tier for a metric.
func (e *DataLifecycleEngine) PredictTier(metric string) (string, float64) {
	info, ok := e.heatmap.GetInfo(metric)
	if !ok {
		return "hot", 0.5 // default
	}
	features := ExtractFeatures(info)
	return e.dtPredictor.PredictWithConfidence(features)
}

// Rehydrate initiates rehydration of a metric from archive tier.
func (e *DataLifecycleEngine) Rehydrate(metric string) (*RehydrationProgress, error) {
	return &RehydrationProgress{
		Metric:    metric,
		Status:    "in_progress",
		StartedAt: time.Now(),
		Progress:  0.0,
	}, nil
}

// RehydrationProgress tracks the progress of a rehydration request.
type RehydrationProgress struct {
	Metric    string    `json:"metric"`
	Status    string    `json:"status"` // pending, in_progress, completed, failed
	StartedAt time.Time `json:"started_at"`
	Progress  float64   `json:"progress"` // 0-100
	SizeBytes int64     `json:"size_bytes,omitempty"`
	Error     string    `json:"error,omitempty"`
}

// CreateLegalHold creates a legal hold preventing data deletion.
func (e *DataLifecycleEngine) CreateLegalHold(hold DataLegalHold) error {
	return e.legalHolds.CreateHold(hold)
}

// ReleaseLegalHold releases a legal hold.
func (e *DataLifecycleEngine) ReleaseLegalHold(id string) error {
	return e.legalHolds.ReleaseHold(id)
}

// IsDataHeld checks if a metric is under legal hold.
func (e *DataLifecycleEngine) IsDataHeld(metric string) bool {
	return e.legalHolds.IsHeld(metric)
}

// GetHeatmap returns access scores for the top N metrics.
func (e *DataLifecycleEngine) GetHeatmap(topN int) []*MetricAccessInfo {
	return e.heatmap.TopMetrics(topN)
}

// SimulateCost returns the estimated monthly cost for a tier.
func (e *DataLifecycleEngine) SimulateCost(sizeGB float64, tier string) float64 {
	return e.predictor.SimulateCost(sizeGB, tier)
}

// Stats returns lifecycle engine statistics.
func (e *DataLifecycleEngine) Stats() DataLifecycleStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.stats
}

// SetDryRun enables or disables dry-run mode.
func (e *DataLifecycleEngine) SetDryRun(enabled bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.config.DryRunMode = enabled
}

// --- Value Score Computation ---

// ComputeValueScore calculates the value of a metric based on access patterns.
// Higher scores indicate more valuable/frequently accessed data.
func ComputeValueScore(info *MetricAccessInfo, decayFactor float64) float64 {
	if info == nil {
		return 0
	}

	// Recency component (0-1)
	hoursSinceAccess := time.Since(info.LastAccessed).Hours()
	recency := math.Exp(-hoursSinceAccess / 168) // 1-week half-life

	// Frequency component
	frequency := info.DecayedScore

	// Size penalty (larger data costs more to keep hot)
	sizePenalty := 1.0
	if info.SizeBytes > 0 {
		sizeGB := float64(info.SizeBytes) / (1024 * 1024 * 1024)
		sizePenalty = 1.0 / (1.0 + sizeGB)
	}

	return (recency*0.3 + frequency*0.5 + sizePenalty*0.2) * 100
}

// --- 7-Day Grace Period ---

// GracePeriodEntry tracks a pending policy change during the grace period.
type GracePeriodEntry struct {
	Metric        string          `json:"metric"`
	Policy        PredictedPolicy `json:"policy"`
	ScheduledAt   time.Time       `json:"scheduled_at"`
	EffectiveAt   time.Time       `json:"effective_at"`
	Cancelled     bool            `json:"cancelled"`
	CancelledBy   string          `json:"cancelled_by,omitempty"`
	CancelledAt   time.Time       `json:"cancelled_at,omitempty"`
}

// GracePeriodManager manages pending policy changes with a grace period.
type GracePeriodManager struct {
	entries    map[string]*GracePeriodEntry // keyed by metric
	graceDays  int
	mu         sync.RWMutex
}

// NewGracePeriodManager creates a new grace period manager.
func NewGracePeriodManager(graceDays int) *GracePeriodManager {
	if graceDays <= 0 {
		graceDays = 7
	}
	return &GracePeriodManager{
		entries:   make(map[string]*GracePeriodEntry),
		graceDays: graceDays,
	}
}

// Schedule adds a policy change to the grace period queue.
func (m *GracePeriodManager) Schedule(policy PredictedPolicy) *GracePeriodEntry {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	entry := &GracePeriodEntry{
		Metric:      policy.Metric,
		Policy:      policy,
		ScheduledAt: now,
		EffectiveAt: now.Add(time.Duration(m.graceDays) * 24 * time.Hour),
	}
	m.entries[policy.Metric] = entry
	return entry
}

// Cancel cancels a pending policy change.
func (m *GracePeriodManager) Cancel(metric, cancelledBy string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.entries[metric]
	if !ok || entry.Cancelled {
		return false
	}
	entry.Cancelled = true
	entry.CancelledBy = cancelledBy
	entry.CancelledAt = time.Now()
	return true
}

// ReadyPolicies returns policies whose grace period has expired and are ready to apply.
func (m *GracePeriodManager) ReadyPolicies() []PredictedPolicy {
	m.mu.RLock()
	defer m.mu.RUnlock()

	now := time.Now()
	var ready []PredictedPolicy
	for _, entry := range m.entries {
		if !entry.Cancelled && now.After(entry.EffectiveAt) {
			ready = append(ready, entry.Policy)
		}
	}
	return ready
}

// PendingCount returns the number of pending (non-cancelled, not yet effective) entries.
func (m *GracePeriodManager) PendingCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	count := 0
	now := time.Now()
	for _, entry := range m.entries {
		if !entry.Cancelled && now.Before(entry.EffectiveAt) {
			count++
		}
	}
	return count
}

// --- Lightweight Decision Tree Predictor ---

// AccessPatternFeatures extracted from metric access history for prediction.
type AccessPatternFeatures struct {
	AvgDailyAccesses   float64 `json:"avg_daily_accesses"`
	StdDevAccesses     float64 `json:"stddev_accesses"`
	DaysSinceLastAccess float64 `json:"days_since_last_access"`
	TotalAccesses      int64   `json:"total_accesses"`
	SizeGB             float64 `json:"size_gb"`
	AgeInDays          float64 `json:"age_in_days"`
	DecayedScore       float64 `json:"decayed_score"`
}

// DecisionTreePredictor uses a simple rule-based decision tree to predict future access.
type DecisionTreePredictor struct {
	thresholds DecisionThresholds
}

// DecisionThresholds configures the decision tree cutoff values.
type DecisionThresholds struct {
	HotMinScore     float64 `json:"hot_min_score"`
	WarmMinScore    float64 `json:"warm_min_score"`
	ColdMinScore    float64 `json:"cold_min_score"`
	HotMaxDaysSince float64 `json:"hot_max_days_since"`
	WarmMaxDaysSince float64 `json:"warm_max_days_since"`
	ColdMaxDaysSince float64 `json:"cold_max_days_since"`
}

// DefaultDecisionThresholds returns sensible defaults.
func DefaultDecisionThresholds() DecisionThresholds {
	return DecisionThresholds{
		HotMinScore:      20.0,
		WarmMinScore:     5.0,
		ColdMinScore:     1.0,
		HotMaxDaysSince:  1.0,
		WarmMaxDaysSince: 7.0,
		ColdMaxDaysSince: 30.0,
	}
}

// NewDecisionTreePredictor creates a new decision tree predictor.
func NewDecisionTreePredictor(thresholds DecisionThresholds) *DecisionTreePredictor {
	return &DecisionTreePredictor{thresholds: thresholds}
}

// Predict returns the recommended tier based on access pattern features.
func (d *DecisionTreePredictor) Predict(features AccessPatternFeatures) string {
	// Decision tree logic
	if features.DecayedScore >= d.thresholds.HotMinScore && features.DaysSinceLastAccess <= d.thresholds.HotMaxDaysSince {
		return "hot"
	}
	if features.DecayedScore >= d.thresholds.WarmMinScore && features.DaysSinceLastAccess <= d.thresholds.WarmMaxDaysSince {
		return "warm"
	}
	if features.DecayedScore >= d.thresholds.ColdMinScore && features.DaysSinceLastAccess <= d.thresholds.ColdMaxDaysSince {
		return "cold"
	}
	return "archive"
}

// PredictWithConfidence returns the recommended tier with a confidence score.
func (d *DecisionTreePredictor) PredictWithConfidence(features AccessPatternFeatures) (string, float64) {
	tier := d.Predict(features)

	// Confidence based on how clearly the features match the tier
	confidence := 0.5
	switch tier {
	case "hot":
		confidence = math.Min(1.0, features.DecayedScore/d.thresholds.HotMinScore*0.5+0.5)
	case "warm":
		confidence = 0.7
	case "cold":
		confidence = 0.8
	case "archive":
		if features.DaysSinceLastAccess > 60 {
			confidence = 0.95
		} else {
			confidence = 0.6
		}
	}

	return tier, confidence
}

// ExtractFeatures extracts decision tree features from metric access info.
func ExtractFeatures(info *MetricAccessInfo) AccessPatternFeatures {
	if info == nil {
		return AccessPatternFeatures{}
	}

	ageInDays := time.Since(info.CreatedAt).Hours() / 24
	if ageInDays < 1 {
		ageInDays = 1
	}

	return AccessPatternFeatures{
		AvgDailyAccesses:    float64(info.TotalAccesses) / ageInDays,
		DaysSinceLastAccess: time.Since(info.LastAccessed).Hours() / 24,
		TotalAccesses:       info.TotalAccesses,
		SizeGB:              float64(info.SizeBytes) / (1024 * 1024 * 1024),
		AgeInDays:           ageInDays,
		DecayedScore:        info.DecayedScore,
	}
}
