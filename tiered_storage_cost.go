package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// Cost optimizer and cost reporting for tiered storage.

// ---------------------------------------------------------------------------
// CostOptimizer
// ---------------------------------------------------------------------------

// TierCostDetail holds cost detail for one tier.
type TierCostDetail struct {
	StorageCost    float64
	AccessCost     float64
	TotalCost      float64
	DataSizeBytes  int64
	PartitionCount int
}

// CostReport summarises current storage cost.
type CostReport struct {
	TotalMonthly     float64
	ByTier           map[StorageTierLevel]TierCostDetail
	DataDistribution map[StorageTierLevel]int64
}

// OptimizationRecommendation describes a possible cost optimisation action.
type OptimizationRecommendation struct {
	Type               string
	Description        string
	EstimatedSavings   float64
	PartitionsAffected int
	Priority           int
}

// DailyCost holds projected cost for a single day.
type DailyCost struct {
	Day  int
	Cost float64
}

// CostProjection holds a multi-day cost forecast.
type CostProjection struct {
	Days            int
	ProjectedCost   float64
	CostTrend       []DailyCost
	Recommendations []string
}

// CostOptimizerConfig configures cost-aware storage policies.
type CostOptimizerConfig struct {
	BudgetPerMonth       float64
	OptimizationInterval time.Duration
	PrefetchEnabled      bool
	PrefetchThreshold    float64
}

// DefaultCostOptimizerConfig returns sensible defaults for cost optimisation.
func DefaultCostOptimizerConfig() CostOptimizerConfig {
	return CostOptimizerConfig{
		BudgetPerMonth:       1000.0,
		OptimizationInterval: 1 * time.Hour,
		PrefetchEnabled:      true,
		PrefetchThreshold:    8.0,
	}
}

// CostOptimizer provides cost-aware storage policy recommendations.
type CostOptimizer struct {
	mu      sync.RWMutex
	tiers   []*StorageTierConfig
	tracker *AccessTracker
	config  CostOptimizerConfig
	tierMap map[StorageTierLevel]*StorageTierConfig
}

// NewCostOptimizer creates a new cost optimiser.
func NewCostOptimizer(tiers []*StorageTierConfig, tracker *AccessTracker, config CostOptimizerConfig) *CostOptimizer {
	tm := make(map[StorageTierLevel]*StorageTierConfig, len(tiers))
	for _, t := range tiers {
		tm[t.Level] = t
	}
	return &CostOptimizer{
		tiers:   tiers,
		tracker: tracker,
		config:  config,
		tierMap: tm,
	}
}

const bytesPerGB = 1 << 30

// CalculateCurrentCost computes the current monthly storage cost across all tiers.
func (o *CostOptimizer) CalculateCurrentCost(ctx context.Context) *CostReport {
	o.mu.RLock()
	defer o.mu.RUnlock()

	report := &CostReport{
		ByTier:           make(map[StorageTierLevel]TierCostDetail),
		DataDistribution: make(map[StorageTierLevel]int64),
	}

	for _, tier := range o.tiers {
		if tier.Backend == nil {
			continue
		}
		keys, err := tier.Backend.List(ctx, "")
		if err != nil {
			continue
		}
		var totalSize int64
		for _, key := range keys {
			data, err := tier.Backend.Read(ctx, key)
			if err == nil {
				totalSize += int64(len(data))
			}
		}

		storageCost := (float64(totalSize) / float64(bytesPerGB)) * tier.CostPerGBMonth
		detail := TierCostDetail{
			StorageCost:    storageCost,
			TotalCost:      storageCost,
			DataSizeBytes:  totalSize,
			PartitionCount: len(keys),
		}
		report.ByTier[tier.Level] = detail
		report.DataDistribution[tier.Level] = totalSize
		report.TotalMonthly += detail.TotalCost
	}
	return report
}

// RecommendOptimizations returns cost optimisation recommendations.
func (o *CostOptimizer) RecommendOptimizations(ctx context.Context) []*OptimizationRecommendation {
	o.mu.RLock()
	defer o.mu.RUnlock()

	var recs []*OptimizationRecommendation

	// Find hot partitions that are rarely accessed and could move down.
	if hotTier, ok := o.tierMap[TierHot]; ok && hotTier.Backend != nil {
		coldParts := o.tracker.GetColdPartitions(2.0)
		if len(coldParts) > 0 {
			savings := float64(len(coldParts)) * hotTier.CostPerGBMonth * 0.5
			recs = append(recs, &OptimizationRecommendation{
				Type:               "downgrade",
				Description:        "Move infrequently accessed partitions from hot to warm tier",
				EstimatedSavings:   savings,
				PartitionsAffected: len(coldParts),
				Priority:           1,
			})
		}
	}

	// Check if budget is exceeded.
	report := o.calculateCostInternal(ctx)
	if report.TotalMonthly > o.config.BudgetPerMonth {
		recs = append(recs, &OptimizationRecommendation{
			Type:               "budget_alert",
			Description:        fmt.Sprintf("Monthly cost $%.2f exceeds budget $%.2f", report.TotalMonthly, o.config.BudgetPerMonth),
			EstimatedSavings:   report.TotalMonthly - o.config.BudgetPerMonth,
			PartitionsAffected: 0,
			Priority:           0,
		})
	}

	return recs
}

func (o *CostOptimizer) calculateCostInternal(ctx context.Context) *CostReport {
	report := &CostReport{
		ByTier:           make(map[StorageTierLevel]TierCostDetail),
		DataDistribution: make(map[StorageTierLevel]int64),
	}
	for _, tier := range o.tiers {
		if tier.Backend == nil {
			continue
		}
		keys, err := tier.Backend.List(ctx, "")
		if err != nil {
			continue
		}
		var totalSize int64
		for _, key := range keys {
			data, err := tier.Backend.Read(ctx, key)
			if err == nil {
				totalSize += int64(len(data))
			}
		}
		storageCost := (float64(totalSize) / float64(bytesPerGB)) * tier.CostPerGBMonth
		detail := TierCostDetail{
			StorageCost:    storageCost,
			TotalCost:      storageCost,
			DataSizeBytes:  totalSize,
			PartitionCount: len(keys),
		}
		report.ByTier[tier.Level] = detail
		report.DataDistribution[tier.Level] = totalSize
		report.TotalMonthly += detail.TotalCost
	}
	return report
}

// ProjectCost projects storage cost over the given number of days.
func (o *CostOptimizer) ProjectCost(ctx context.Context, days int) *CostProjection {
	o.mu.RLock()
	defer o.mu.RUnlock()

	report := o.calculateCostInternal(ctx)
	dailyCost := report.TotalMonthly / 30.0

	proj := &CostProjection{
		Days:      days,
		CostTrend: make([]DailyCost, days),
	}
	var total float64
	for i := 0; i < days; i++ {
		cost := dailyCost * (1.0 + 0.001*float64(i)) // slight growth model
		proj.CostTrend[i] = DailyCost{Day: i + 1, Cost: cost}
		total += cost
	}
	proj.ProjectedCost = total

	if total > o.config.BudgetPerMonth*float64(days)/30.0 {
		proj.Recommendations = append(proj.Recommendations,
			"Projected cost exceeds budget; consider migrating data to colder tiers")
	}
	return proj
}

// ---------------------------------------------------------------------------
// AdaptiveTieredBackend
// ---------------------------------------------------------------------------

// AdaptiveTieredConfig configures the adaptive tiered storage backend.
type AdaptiveTieredConfig struct {
	Tiers         []*StorageTierConfig
	AccessTracker AccessTrackerConfig
	Migration     MigrationEngineConfig
	CostOptimizer CostOptimizerConfig
}

// DefaultAdaptiveTieredConfig returns sensible defaults for adaptive tiered storage.
func DefaultAdaptiveTieredConfig() AdaptiveTieredConfig {
	return AdaptiveTieredConfig{
		AccessTracker: DefaultAccessTrackerConfig(),
		Migration:     DefaultMigrationEngineConfig(),
		CostOptimizer: DefaultCostOptimizerConfig(),
	}
}

// TieredStorageStats holds runtime statistics for the adaptive tiered backend.
type TieredStorageStats struct {
	TotalPartitions     int
	PartitionsByTier    map[StorageTierLevel]int
	CostReport          *CostReport
	MigrationsPending   int
	MigrationsCompleted int
}

// AdaptiveTieredBackend implements StorageBackend with cost-aware adaptive tiering.
type AdaptiveTieredBackend struct {
	mu        sync.RWMutex
	tiers     []*StorageTierConfig
	tierMap   map[StorageTierLevel]*StorageTierConfig
	tracker   *AccessTracker
	migration *MigrationEngine
	cost      *CostOptimizer
}

// NewAdaptiveTieredBackend creates a new adaptive tiered storage backend.
func NewAdaptiveTieredBackend(config AdaptiveTieredConfig) (*AdaptiveTieredBackend, error) {
	if len(config.Tiers) == 0 {
		return nil, fmt.Errorf("at least one storage tier is required")
	}
	for _, t := range config.Tiers {
		if t.Backend == nil {
			return nil, fmt.Errorf("tier %s has nil backend", t.Level)
		}
	}

	tm := make(map[StorageTierLevel]*StorageTierConfig, len(config.Tiers))
	for _, t := range config.Tiers {
		tm[t.Level] = t
	}

	tracker := NewAccessTracker(config.AccessTracker)
	migration := NewMigrationEngine(config.Tiers, tracker, config.Migration)
	optimizer := NewCostOptimizer(config.Tiers, tracker, config.CostOptimizer)

	migration.Start()

	return &AdaptiveTieredBackend{
		tiers:     config.Tiers,
		tierMap:   tm,
		tracker:   tracker,
		migration: migration,
		cost:      optimizer,
	}, nil
}

// findKey locates the tier containing the given key.
func (b *AdaptiveTieredBackend) findKey(ctx context.Context, key string) *StorageTierConfig {
	for _, t := range b.tiers {
		exists, err := t.Backend.Exists(ctx, key)
		if err == nil && exists {
			return t
		}
	}
	return nil
}

// Read reads data from the appropriate tier and tracks the access.
func (b *AdaptiveTieredBackend) Read(ctx context.Context, key string) ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	tier := b.findKey(ctx, key)
	if tier == nil {
		return nil, fmt.Errorf("key %q not found in any tier", key)
	}

	data, err := tier.Backend.Read(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("tiered read failed: %w", err)
	}
	b.tracker.RecordRead(key)
	return data, nil
}

// Write writes data to the hottest available tier and tracks the access.
func (b *AdaptiveTieredBackend) Write(ctx context.Context, key string, data []byte) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Write to the first (hottest) tier.
	if len(b.tiers) == 0 {
		return fmt.Errorf("no tiers configured")
	}
	if err := b.tiers[0].Backend.Write(ctx, key, data); err != nil {
		return fmt.Errorf("tiered write failed: %w", err)
	}
	b.tracker.RecordWrite(key)
	return nil
}

// Delete removes the key from whichever tier holds it.
func (b *AdaptiveTieredBackend) Delete(ctx context.Context, key string) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	tier := b.findKey(ctx, key)
	if tier == nil {
		return fmt.Errorf("key %q not found in any tier", key)
	}
	if err := tier.Backend.Delete(ctx, key); err != nil {
		return fmt.Errorf("tiered delete failed: %w", err)
	}
	return nil
}

// List returns all keys matching the prefix across all tiers.
func (b *AdaptiveTieredBackend) List(ctx context.Context, prefix string) ([]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	seen := make(map[string]struct{})
	var all []string
	for _, t := range b.tiers {
		keys, err := t.Backend.List(ctx, prefix)
		if err != nil {
			return nil, fmt.Errorf("tiered list failed on %s tier: %w", t.Level, err)
		}
		for _, k := range keys {
			if _, dup := seen[k]; !dup {
				seen[k] = struct{}{}
				all = append(all, k)
			}
		}
	}
	return all, nil
}

// Exists checks whether the key exists in any tier.
func (b *AdaptiveTieredBackend) Exists(ctx context.Context, key string) (bool, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, t := range b.tiers {
		exists, err := t.Backend.Exists(ctx, key)
		if err != nil {
			return false, fmt.Errorf("tiered exists check failed on %s tier: %w", t.Level, err)
		}
		if exists {
			return true, nil
		}
	}
	return false, nil
}

// Close stops background processes and closes all tier backends.
func (b *AdaptiveTieredBackend) Close() error {
	b.migration.Stop()
	var firstErr error
	for _, t := range b.tiers {
		if err := t.Backend.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("tiered close failed on %s tier: %w", t.Level, err)
		}
	}
	return firstErr
}

// Stats returns runtime statistics for the adaptive tiered backend.
func (b *AdaptiveTieredBackend) Stats(ctx context.Context) *TieredStorageStats {
	b.mu.RLock()
	defer b.mu.RUnlock()

	stats := &TieredStorageStats{
		PartitionsByTier: make(map[StorageTierLevel]int),
	}
	for _, t := range b.tiers {
		keys, err := t.Backend.List(ctx, "")
		if err != nil {
			continue
		}
		stats.PartitionsByTier[t.Level] = len(keys)
		stats.TotalPartitions += len(keys)
	}
	stats.CostReport = b.cost.CalculateCurrentCost(ctx)
	history := b.migration.GetMigrationHistory()
	stats.MigrationsCompleted = len(history)
	pending := b.migration.PlanMigrations(ctx)
	stats.MigrationsPending = len(pending)
	return stats
}

// CostDashboard generates a comprehensive cost dashboard report.
type CostDashboard struct {
	CurrentCost      *CostReport                   `json:"current_cost"`
	Projections      []*CostProjection             `json:"projections"`
	Recommendations  []*OptimizationRecommendation `json:"recommendations"`
	TierDistribution map[string]int                `json:"tier_distribution"`
	MigrationSummary MigrationDashboardSummary     `json:"migration_summary"`
	GeneratedAt      time.Time                     `json:"generated_at"`
}

// MigrationDashboardSummary summarizes migration activity.
type MigrationDashboardSummary struct {
	Completed   int           `json:"completed"`
	Pending     int           `json:"pending"`
	TotalMoved  int64         `json:"total_bytes_moved"`
	AvgDuration time.Duration `json:"avg_duration"`
	FailureRate float64       `json:"failure_rate"`
}

// GenerateCostDashboard creates a full cost dashboard from the tiered backend.
func (b *AdaptiveTieredBackend) GenerateCostDashboard(ctx context.Context) *CostDashboard {
	dashboard := &CostDashboard{
		CurrentCost:      b.cost.CalculateCurrentCost(ctx),
		Recommendations:  b.cost.RecommendOptimizations(ctx),
		TierDistribution: make(map[string]int),
		GeneratedAt:      time.Now(),
	}

	// Generate 3, 6, 12 month projections
	for _, months := range []int{3, 6, 12} {
		proj := b.cost.ProjectCost(ctx, months*30)
		dashboard.Projections = append(dashboard.Projections, proj)
	}

	// Tier distribution
	for _, t := range b.tiers {
		keys, err := t.Backend.List(ctx, "")
		if err == nil {
			dashboard.TierDistribution[t.Level.String()] = len(keys)
		}
	}

	// Migration summary
	history := b.migration.GetMigrationHistory()
	dashboard.MigrationSummary.Completed = len(history)
	dashboard.MigrationSummary.Pending = len(b.migration.PlanMigrations(ctx))

	var totalMoved int64
	var totalDuration time.Duration
	failures := 0
	for _, r := range history {
		totalMoved += r.BytesMoved
		totalDuration += r.Duration
		if !r.Success {
			failures++
		}
	}
	dashboard.MigrationSummary.TotalMoved = totalMoved
	if len(history) > 0 {
		dashboard.MigrationSummary.AvgDuration = totalDuration / time.Duration(len(history))
		dashboard.MigrationSummary.FailureRate = float64(failures) / float64(len(history))
	}

	return dashboard
}

// reEncodeForColdStorage applies compression-optimized encoding for cold/archive tiers.
// Cold data is rarely read, so we optimize for storage size over access speed.
func reEncodeForColdStorage(data []byte) []byte {
	if len(data) == 0 {
		return data
	}

	// Apply simple run-length encoding for repeated byte sequences
	var encoded []byte
	i := 0
	for i < len(data) {
		b := data[i]
		count := 1
		for i+count < len(data) && data[i+count] == b && count < 255 {
			count++
		}
		if count >= 3 {
			// RLE escape: 0xFF, count, byte
			encoded = append(encoded, 0xFE, byte(count), b)
		} else {
			for j := 0; j < count; j++ {
				encoded = append(encoded, b)
			}
		}
		i += count
	}

	// Only use encoded version if it's actually smaller
	if len(encoded) < len(data) {
		return encoded
	}
	return data
}

// RegisterHTTPHandlers registers HTTP endpoints for the tiered storage dashboard.
func (b *AdaptiveTieredBackend) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/tiered/dashboard", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		dashboard := b.GenerateCostDashboard(r.Context())
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(dashboard)
	})
	mux.HandleFunc("/api/v1/tiered/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(b.Stats(r.Context()))
	})
	mux.HandleFunc("/api/v1/tiered/migrations", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"pending":   b.migration.PlanMigrations(r.Context()),
			"completed": b.migration.GetMigrationHistory(),
		})
	})
	mux.HandleFunc("/api/v1/tiered/cost", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(b.cost.CalculateCurrentCost(r.Context()))
	})
}
