package chronicle

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"
)

// StorageTierLevel represents the temperature of a storage tier.
type StorageTierLevel int

const (
	TierHot     StorageTierLevel = iota // Fastest, most expensive
	TierWarm                            // Moderate speed and cost
	TierCold                            // Slow, cheaper
	TierArchive                         // Slowest, cheapest
)

// String returns the human-readable name of the tier level.
func (l StorageTierLevel) String() string {
	switch l {
	case TierHot:
		return "hot"
	case TierWarm:
		return "warm"
	case TierCold:
		return "cold"
	case TierArchive:
		return "archive"
	default:
		return "unknown"
	}
}

// StorageTierConfig configures a single storage tier.
type StorageTierConfig struct {
	Level            StorageTierLevel
	Backend          StorageBackend
	CostPerGBMonth   float64
	MaxCapacityBytes int64
	ReadLatencySLA   time.Duration
}

// ---------------------------------------------------------------------------
// AccessTracker
// ---------------------------------------------------------------------------

// AccessTrackerConfig configures per-partition access pattern tracking.
type AccessTrackerConfig struct {
	TrackingWindowDuration time.Duration
	DecayFactor            float64
	HistogramBuckets       int
}

// DefaultAccessTrackerConfig returns sensible defaults for access tracking.
func DefaultAccessTrackerConfig() AccessTrackerConfig {
	return AccessTrackerConfig{
		TrackingWindowDuration: 24 * time.Hour,
		DecayFactor:            0.95,
		HistogramBuckets:       24,
	}
}

// AccessStats holds computed access statistics for a single partition.
type AccessStats struct {
	ReadCount       int64
	WriteCount      int64
	LastRead        time.Time
	LastWrite       time.Time
	AccessScore     float64
	HourlyHistogram []int
}

type accessRecord struct {
	readCount  int64
	writeCount int64
	lastRead   time.Time
	lastWrite  time.Time
	histogram  []int
}

// AccessTracker tracks per-partition access patterns.
type AccessTracker struct {
	mu      sync.RWMutex
	config  AccessTrackerConfig
	records map[string]*accessRecord
}

// NewAccessTracker creates a new access tracker.
func NewAccessTracker(config AccessTrackerConfig) *AccessTracker {
	if config.HistogramBuckets <= 0 {
		config.HistogramBuckets = 24
	}
	return &AccessTracker{
		config:  config,
		records: make(map[string]*accessRecord),
	}
}

func (t *AccessTracker) getOrCreate(partitionKey string) *accessRecord {
	rec, ok := t.records[partitionKey]
	if !ok {
		rec = &accessRecord{
			histogram: make([]int, t.config.HistogramBuckets),
		}
		t.records[partitionKey] = rec
	}
	return rec
}

// RecordRead records a read access for the given partition.
func (t *AccessTracker) RecordRead(partitionKey string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	rec := t.getOrCreate(partitionKey)
	rec.readCount++
	rec.lastRead = time.Now()
	bucket := time.Now().Hour() % t.config.HistogramBuckets
	rec.histogram[bucket]++
}

// RecordWrite records a write access for the given partition.
func (t *AccessTracker) RecordWrite(partitionKey string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	rec := t.getOrCreate(partitionKey)
	rec.writeCount++
	rec.lastWrite = time.Now()
	bucket := time.Now().Hour() % t.config.HistogramBuckets
	rec.histogram[bucket]++
}

// GetAccessScore computes a weighted recency score for the partition.
// Higher scores indicate more recent and frequent access.
func (t *AccessTracker) GetAccessScore(partitionKey string) float64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	rec, ok := t.records[partitionKey]
	if !ok {
		return 0
	}
	return t.computeScore(rec)
}

func (t *AccessTracker) computeScore(rec *accessRecord) float64 {
	now := time.Now()
	window := t.config.TrackingWindowDuration
	if window <= 0 {
		window = 24 * time.Hour
	}

	// Recency component based on last access
	lastAccess := rec.lastRead
	if rec.lastWrite.After(lastAccess) {
		lastAccess = rec.lastWrite
	}
	if lastAccess.IsZero() {
		return 0
	}
	age := now.Sub(lastAccess)
	recency := math.Exp(-float64(age) / float64(window))

	// Frequency component with decay
	frequency := float64(rec.readCount+rec.writeCount) * t.config.DecayFactor

	return recency * (1.0 + math.Log1p(frequency))
}

// GetAccessStats returns the full access statistics for a partition.
func (t *AccessTracker) GetAccessStats(partitionKey string) *AccessStats {
	t.mu.RLock()
	defer t.mu.RUnlock()
	rec, ok := t.records[partitionKey]
	if !ok {
		return nil
	}
	hist := make([]int, len(rec.histogram))
	copy(hist, rec.histogram)
	return &AccessStats{
		ReadCount:       rec.readCount,
		WriteCount:      rec.writeCount,
		LastRead:        rec.lastRead,
		LastWrite:       rec.lastWrite,
		AccessScore:     t.computeScore(rec),
		HourlyHistogram: hist,
	}
}

// GetColdPartitions returns partition keys with an access score below the threshold.
func (t *AccessTracker) GetColdPartitions(threshold float64) []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var cold []string
	for key, rec := range t.records {
		if t.computeScore(rec) < threshold {
			cold = append(cold, key)
		}
	}
	return cold
}

// ---------------------------------------------------------------------------
// MigrationEngine
// ---------------------------------------------------------------------------

// MigrationPlan describes a planned data migration between tiers.
type MigrationPlan struct {
	PartitionKey     string
	SourceTier       StorageTierLevel
	TargetTier       StorageTierLevel
	Reason           string
	EstimatedSavings float64
	DataSize         int64
}

// MigrationResult records the outcome of a completed migration.
type MigrationResult struct {
	Plan        *MigrationPlan
	Success     bool
	Duration    time.Duration
	BytesMoved  int64
	Error       string
	CompletedAt time.Time
}

// MigrationEngineConfig configures the background migration engine.
type MigrationEngineConfig struct {
	CheckInterval           time.Duration
	MigrationCooldown       time.Duration
	MaxConcurrentMigrations int
	DryRun                  bool
}

// DefaultMigrationEngineConfig returns sensible defaults for migration.
func DefaultMigrationEngineConfig() MigrationEngineConfig {
	return MigrationEngineConfig{
		CheckInterval:           5 * time.Minute,
		MigrationCooldown:       10 * time.Minute,
		MaxConcurrentMigrations: 2,
		DryRun:                  false,
	}
}

// MigrationEngine manages background tier-to-tier data migrations.
type MigrationEngine struct {
	mu      sync.RWMutex
	tiers   []*StorageTierConfig
	tracker *AccessTracker
	config  MigrationEngineConfig
	history []*MigrationResult
	running bool
	stopCh  chan struct{}
	tierMap map[StorageTierLevel]*StorageTierConfig
}

// NewMigrationEngine creates a new migration engine.
func NewMigrationEngine(tiers []*StorageTierConfig, tracker *AccessTracker, config MigrationEngineConfig) *MigrationEngine {
	tm := make(map[StorageTierLevel]*StorageTierConfig, len(tiers))
	for _, t := range tiers {
		tm[t.Level] = t
	}
	return &MigrationEngine{
		tiers:   tiers,
		tracker: tracker,
		config:  config,
		history: make([]*MigrationResult, 0),
		stopCh:  make(chan struct{}),
		tierMap: tm,
	}
}

// Start begins the background migration loop.
func (e *MigrationEngine) Start() {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true
	e.mu.Unlock()

	go e.loop()
}

// Stop halts the background migration loop.
func (e *MigrationEngine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	close(e.stopCh)
}

func (e *MigrationEngine) loop() {
	interval := e.config.CheckInterval
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			plans := e.PlanMigrations()
			sem := make(chan struct{}, e.config.MaxConcurrentMigrations)
			for _, p := range plans {
				sem <- struct{}{}
				go func(plan *MigrationPlan) {
					defer func() { <-sem }()
					result, _ := e.ExecuteMigration(plan)
					if result != nil {
						e.mu.Lock()
						e.history = append(e.history, result)
						e.mu.Unlock()
					}
				}(p)
			}
		}
	}
}

// PlanMigrations determines which partitions should be migrated between tiers.
func (e *MigrationEngine) PlanMigrations() []*MigrationPlan {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var plans []*MigrationPlan

	// Determine score thresholds for tier boundaries.
	// Hot → Warm when score < 5.0, Warm → Cold < 1.0, Cold → Archive < 0.1
	thresholds := map[StorageTierLevel]struct {
		score  float64
		target StorageTierLevel
	}{
		TierHot:  {score: 5.0, target: TierWarm},
		TierWarm: {score: 1.0, target: TierCold},
		TierCold: {score: 0.1, target: TierArchive},
	}

	for level, th := range thresholds {
		src, ok := e.tierMap[level]
		if !ok {
			continue
		}
		if _, ok := e.tierMap[th.target]; !ok {
			continue
		}
		if src.Backend == nil {
			continue
		}
		keys, err := src.Backend.List(context.Background(), "")
		if err != nil {
			continue
		}
		for _, key := range keys {
			score := e.tracker.GetAccessScore(key)
			if score < th.score {
				savings := src.CostPerGBMonth
				if dst, ok := e.tierMap[th.target]; ok {
					savings -= dst.CostPerGBMonth
				}
				plans = append(plans, &MigrationPlan{
					PartitionKey:     key,
					SourceTier:       level,
					TargetTier:       th.target,
					Reason:           fmt.Sprintf("access score %.2f below threshold %.2f", score, th.score),
					EstimatedSavings: savings,
				})
			}
		}
	}
	return plans
}

// ExecuteMigration performs a single partition migration between tiers.
func (e *MigrationEngine) ExecuteMigration(plan *MigrationPlan) (*MigrationResult, error) {
	start := time.Now()
	result := &MigrationResult{
		Plan: plan,
	}

	if e.config.DryRun {
		result.Success = true
		result.Duration = time.Since(start)
		result.CompletedAt = time.Now()
		return result, nil
	}

	src, ok := e.tierMap[plan.SourceTier]
	if !ok {
		result.Error = "source tier not found"
		result.CompletedAt = time.Now()
		return result, fmt.Errorf("source tier %s not found", plan.SourceTier)
	}
	dst, ok := e.tierMap[plan.TargetTier]
	if !ok {
		result.Error = "target tier not found"
		result.CompletedAt = time.Now()
		return result, fmt.Errorf("target tier %s not found", plan.TargetTier)
	}

	ctx := context.Background()

	data, err := src.Backend.Read(ctx, plan.PartitionKey)
	if err != nil {
		result.Error = fmt.Sprintf("read failed: %v", err)
		result.CompletedAt = time.Now()
		return result, fmt.Errorf("migration read failed: %w", err)
	}

	if err := dst.Backend.Write(ctx, plan.PartitionKey, data); err != nil {
		result.Error = fmt.Sprintf("write failed: %v", err)
		result.CompletedAt = time.Now()
		return result, fmt.Errorf("migration write failed: %w", err)
	}

	if err := src.Backend.Delete(ctx, plan.PartitionKey); err != nil {
		result.Error = fmt.Sprintf("delete from source failed: %v", err)
		result.CompletedAt = time.Now()
		return result, fmt.Errorf("migration source delete failed: %w", err)
	}

	result.Success = true
	result.BytesMoved = int64(len(data))
	result.Duration = time.Since(start)
	result.CompletedAt = time.Now()
	return result, nil
}

// GetMigrationHistory returns all completed migration results.
func (e *MigrationEngine) GetMigrationHistory() []*MigrationResult {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]*MigrationResult, len(e.history))
	copy(out, e.history)
	return out
}

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
func (o *CostOptimizer) CalculateCurrentCost() *CostReport {
	o.mu.RLock()
	defer o.mu.RUnlock()

	report := &CostReport{
		ByTier:           make(map[StorageTierLevel]TierCostDetail),
		DataDistribution: make(map[StorageTierLevel]int64),
	}
	ctx := context.Background()

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
func (o *CostOptimizer) RecommendOptimizations() []*OptimizationRecommendation {
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
	report := o.calculateCostInternal()
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

func (o *CostOptimizer) calculateCostInternal() *CostReport {
	report := &CostReport{
		ByTier:           make(map[StorageTierLevel]TierCostDetail),
		DataDistribution: make(map[StorageTierLevel]int64),
	}
	ctx := context.Background()
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
func (o *CostOptimizer) ProjectCost(days int) *CostProjection {
	o.mu.RLock()
	defer o.mu.RUnlock()

	report := o.calculateCostInternal()
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
func (b *AdaptiveTieredBackend) Stats() *TieredStorageStats {
	b.mu.RLock()
	defer b.mu.RUnlock()

	stats := &TieredStorageStats{
		PartitionsByTier: make(map[StorageTierLevel]int),
	}
	ctx := context.Background()
	for _, t := range b.tiers {
		keys, err := t.Backend.List(ctx, "")
		if err != nil {
			continue
		}
		stats.PartitionsByTier[t.Level] = len(keys)
		stats.TotalPartitions += len(keys)
	}
	stats.CostReport = b.cost.CalculateCurrentCost()
	history := b.migration.GetMigrationHistory()
	stats.MigrationsCompleted = len(history)
	pending := b.migration.PlanMigrations()
	stats.MigrationsPending = len(pending)
	return stats
}
