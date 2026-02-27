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
	EnableReEncoding        bool // re-encode data on tier transition for better compression
}

// DefaultMigrationEngineConfig returns sensible defaults for migration.
func DefaultMigrationEngineConfig() MigrationEngineConfig {
	return MigrationEngineConfig{
		CheckInterval:           5 * time.Minute,
		MigrationCooldown:       10 * time.Minute,
		MaxConcurrentMigrations: 2,
		DryRun:                  false,
		EnableReEncoding:        true,
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
	wg      sync.WaitGroup
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

	e.wg.Add(1)
	go e.loop()
}

// Stop halts the background migration loop.
func (e *MigrationEngine) Stop() {
	e.mu.Lock()
	if !e.running {
		e.mu.Unlock()
		return
	}
	e.running = false
	close(e.stopCh)
	e.mu.Unlock()
	e.wg.Wait()
}

func (e *MigrationEngine) loop() {
	defer e.wg.Done()
	// Derive a context that cancels when the engine stops.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-e.stopCh:
			cancel()
		case <-ctx.Done():
		}
	}()

	interval := e.config.CheckInterval
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			plans := e.PlanMigrations(ctx)
			sem := make(chan struct{}, e.config.MaxConcurrentMigrations)
			for _, p := range plans {
				sem <- struct{}{}
				go func(plan *MigrationPlan) {
					defer func() { <-sem }()
					result, _ := e.ExecuteMigration(ctx, plan)
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
func (e *MigrationEngine) PlanMigrations(ctx context.Context) []*MigrationPlan {
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
		keys, err := src.Backend.List(ctx, "")
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
func (e *MigrationEngine) ExecuteMigration(ctx context.Context, plan *MigrationPlan) (*MigrationResult, error) {
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

	data, err := src.Backend.Read(ctx, plan.PartitionKey)
	if err != nil {
		result.Error = fmt.Sprintf("read failed: %v", err)
		result.CompletedAt = time.Now()
		return result, fmt.Errorf("migration read failed: %w", err)
	}

	// Apply re-encoding on tier transition for better compression on cold tiers
	if e.config.EnableReEncoding && plan.TargetTier >= TierCold {
		data = reEncodeForColdStorage(data)
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
