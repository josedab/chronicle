package chronicle

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"
)

// LifecycleConfig configures automated data lifecycle management for tiered storage.
type LifecycleConfig struct {
	Enabled            bool          `json:"enabled"`
	EvaluationInterval time.Duration `json:"evaluation_interval"`
	HotToWarmAge       time.Duration `json:"hot_to_warm_age"`
	WarmToColdAge      time.Duration `json:"warm_to_cold_age"`
	ColdToArchiveAge   time.Duration `json:"cold_to_archive_age"`
	MaxHotSizeBytes    int64         `json:"max_hot_size_bytes"`
	MaxWarmSizeBytes   int64         `json:"max_warm_size_bytes"`
	CostBudgetMonthly  float64       `json:"cost_budget_monthly"`
	DryRun             bool          `json:"dry_run"`
}

// DefaultLifecycleConfig returns sensible defaults for lifecycle management.
func DefaultLifecycleConfig() LifecycleConfig {
	return LifecycleConfig{
		Enabled:            true,
		EvaluationInterval: 5 * time.Minute,
		HotToWarmAge:       1 * time.Hour,
		WarmToColdAge:      24 * time.Hour,
		ColdToArchiveAge:   7 * 24 * time.Hour,
		MaxHotSizeBytes:    10 * 1 << 30,  // 10 GB
		MaxWarmSizeBytes:   100 * 1 << 30, // 100 GB
		CostBudgetMonthly:  500.0,
		DryRun:             false,
	}
}

// LifecycleEvent records a single lifecycle transition for a partition.
type LifecycleEvent struct {
	ID           string           `json:"id"`
	Timestamp    time.Time        `json:"timestamp"`
	PartitionKey string           `json:"partition_key"`
	FromTier     StorageTierLevel `json:"from_tier"`
	ToTier       StorageTierLevel `json:"to_tier"`
	Reason       string           `json:"reason"`
	BytesMoved   int64            `json:"bytes_moved"`
	Duration     time.Duration    `json:"duration"`
	DryRun       bool             `json:"dry_run"`
}

// LifecycleStats holds aggregate statistics for lifecycle management.
type LifecycleStats struct {
	TotalEvaluations        int64                    `json:"total_evaluations"`
	TotalMigrations         int64                    `json:"total_migrations"`
	BytesMigrated           int64                    `json:"bytes_migrated"`
	EstimatedMonthlySavings float64                  `json:"estimated_monthly_savings"`
	LastEvaluation          time.Time                `json:"last_evaluation"`
	PartitionsByTier        map[StorageTierLevel]int `json:"partitions_by_tier"`
}

// MonthlyCostProjection holds projected costs for a single month.
type MonthlyCostProjection struct {
	Month       int     `json:"month"`
	HotCost     float64 `json:"hot_cost"`
	WarmCost    float64 `json:"warm_cost"`
	ColdCost    float64 `json:"cold_cost"`
	ArchiveCost float64 `json:"archive_cost"`
	TotalCost   float64 `json:"total_cost"`
}

// LifecycleManager provides automated data lifecycle management across storage tiers.
type LifecycleManager struct {
	config       LifecycleConfig
	backend      *AdaptiveTieredBackend
	retention    *SmartRetentionEngine
	policyEngine *TierPolicyEngine

	running bool
	cancel  context.CancelFunc
	mu      sync.RWMutex
	history []LifecycleEvent
	stats   LifecycleStats
}

// NewLifecycleManager creates a new lifecycle manager.
func NewLifecycleManager(
	config LifecycleConfig,
	backend *AdaptiveTieredBackend,
	retention *SmartRetentionEngine,
	policyEngine *TierPolicyEngine,
) *LifecycleManager {
	return &LifecycleManager{
		config:       config,
		backend:      backend,
		retention:    retention,
		policyEngine: policyEngine,
		history:      make([]LifecycleEvent, 0),
		stats: LifecycleStats{
			PartitionsByTier: make(map[StorageTierLevel]int),
		},
	}
}

// Start begins the background lifecycle evaluation loop.
func (lm *LifecycleManager) Start(ctx context.Context) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if lm.running {
		return fmt.Errorf("lifecycle manager is already running")
	}
	if lm.backend == nil {
		return fmt.Errorf("lifecycle manager requires an AdaptiveTieredBackend")
	}

	lm.running = true
	childCtx, cancel := context.WithCancel(ctx)
	lm.cancel = cancel

	go lm.loop(childCtx)
	return nil
}

// Stop stops the lifecycle manager.
func (lm *LifecycleManager) Stop() {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if !lm.running {
		return
	}
	lm.running = false
	if lm.cancel != nil {
		lm.cancel()
	}
}

func (lm *LifecycleManager) loop(ctx context.Context) {
	interval := lm.config.EvaluationInterval
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
			_, _ = lm.Evaluate()
		}
	}
}

// Evaluate runs one evaluation cycle: inspects all partitions, determines ideal
// tiers based on age and access frequency, and generates migration events.
func (lm *LifecycleManager) Evaluate() ([]LifecycleEvent, error) {
	lm.mu.Lock()
	lm.stats.TotalEvaluations++
	lm.stats.LastEvaluation = time.Now()
	lm.mu.Unlock()

	if lm.backend == nil {
		return nil, fmt.Errorf("lifecycle evaluate: backend is nil")
	}

	ctx := context.Background()
	var events []LifecycleEvent

	// Collect partition info from each tier.
	type partitionInfo struct {
		key         string
		tier        StorageTierLevel
		sizeBytes   int64
		accessScore float64
	}

	var partitions []partitionInfo
	tierCounts := make(map[StorageTierLevel]int)

	lm.backend.mu.RLock()
	tiers := lm.backend.tiers
	tracker := lm.backend.tracker
	lm.backend.mu.RUnlock()

	for _, t := range tiers {
		if t.Backend == nil {
			continue
		}
		keys, err := t.Backend.List(ctx, "")
		if err != nil {
			continue
		}
		tierCounts[t.Level] = len(keys)
		for _, key := range keys {
			var size int64
			data, err := t.Backend.Read(ctx, key)
			if err == nil {
				size = int64(len(data))
			}
			score := 0.0
			if tracker != nil {
				score = tracker.GetAccessScore(key)
			}
			partitions = append(partitions, partitionInfo{
				key:         key,
				tier:        t.Level,
				sizeBytes:   size,
				accessScore: score,
			})
		}
	}

	// Update partition counts in stats.
	lm.mu.Lock()
	lm.stats.PartitionsByTier = tierCounts
	lm.mu.Unlock()

	// Evaluate each partition and determine ideal tier.
	now := time.Now()
	for _, p := range partitions {
		// Estimate age from access score: lower score implies older/less accessed data.
		// Use access score as a proxy for recency.
		estimatedAge := lm.estimateAge(p.accessScore)
		idealTier := lm.determineTier(estimatedAge, p.accessScore)

		if idealTier == p.tier {
			continue
		}
		// Only migrate data to colder tiers (not promote).
		if idealTier < p.tier {
			continue
		}

		eventID := fmt.Sprintf("lc-%d-%s", now.UnixNano(), p.key)
		reason := fmt.Sprintf("access_score=%.2f age_est=%v: %s -> %s",
			p.accessScore, estimatedAge.Round(time.Second), p.tier, idealTier)

		start := time.Now()
		evt := LifecycleEvent{
			ID:           eventID,
			Timestamp:    now,
			PartitionKey: p.key,
			FromTier:     p.tier,
			ToTier:       idealTier,
			Reason:       reason,
			BytesMoved:   p.sizeBytes,
			DryRun:       lm.config.DryRun,
		}

		if !lm.config.DryRun {
			lm.backend.mu.RLock()
			srcTier := lm.backend.tierMap[p.tier]
			dstTier := lm.backend.tierMap[idealTier]
			lm.backend.mu.RUnlock()

			if srcTier != nil && dstTier != nil && srcTier.Backend != nil && dstTier.Backend != nil {
				data, err := srcTier.Backend.Read(ctx, p.key)
				if err != nil {
					continue
				}
				if err := dstTier.Backend.Write(ctx, p.key, data); err != nil {
					continue
				}
				if err := srcTier.Backend.Delete(ctx, p.key); err != nil {
					continue
				}
				evt.BytesMoved = int64(len(data))
			}
		}

		evt.Duration = time.Since(start)
		events = append(events, evt)
	}

	// Update history and stats.
	lm.mu.Lock()
	lm.history = append(lm.history, events...)
	lm.stats.TotalMigrations += int64(len(events))
	for _, evt := range events {
		lm.stats.BytesMigrated += evt.BytesMoved
	}
	lm.stats.EstimatedMonthlySavings = lm.estimateSavings()
	lm.mu.Unlock()

	return events, nil
}

// estimateAge approximates data age from the access score.
// A high score means recently/frequently accessed (young), low score means old.
func (lm *LifecycleManager) estimateAge(accessScore float64) time.Duration {
	if accessScore <= 0 {
		return lm.config.ColdToArchiveAge * 2
	}
	// Inverse exponential: age = -window * ln(score / maxScore)
	// Approximate with a simple mapping.
	hours := -24.0 * math.Log(math.Min(accessScore, 10.0)/10.0)
	if hours < 0 {
		hours = 0
	}
	return time.Duration(hours * float64(time.Hour))
}

// determineTier returns the ideal tier based on data age and access frequency.
func (lm *LifecycleManager) determineTier(age time.Duration, accessScore float64) StorageTierLevel {
	// High access score keeps data in hotter tiers regardless of age.
	if accessScore > 8.0 {
		return TierHot
	}

	if age <= lm.config.HotToWarmAge {
		return TierHot
	}
	if age <= lm.config.WarmToColdAge {
		return TierWarm
	}
	if age <= lm.config.ColdToArchiveAge {
		return TierCold
	}
	return TierArchive
}

// estimateSavings computes estimated monthly savings based on migration history.
func (lm *LifecycleManager) estimateSavings() float64 {
	// Cost ratios per tier (relative to hot).
	costRatio := map[StorageTierLevel]float64{
		TierHot:     1.0,
		TierWarm:    0.4,
		TierCold:    0.1,
		TierArchive: 0.02,
	}

	var savings float64
	for _, evt := range lm.history {
		fromCost, ok1 := costRatio[evt.FromTier]
		toCost, ok2 := costRatio[evt.ToTier]
		if ok1 && ok2 {
			gbMoved := float64(evt.BytesMoved) / float64(bytesPerGB)
			savings += gbMoved * (fromCost - toCost)
		}
	}
	return savings
}

// History returns all recorded lifecycle events.
func (lm *LifecycleManager) History() []LifecycleEvent {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	out := make([]LifecycleEvent, len(lm.history))
	copy(out, lm.history)
	return out
}

// Stats returns aggregate lifecycle statistics.
func (lm *LifecycleManager) Stats() LifecycleStats {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	s := lm.stats
	s.PartitionsByTier = make(map[StorageTierLevel]int, len(lm.stats.PartitionsByTier))
	for k, v := range lm.stats.PartitionsByTier {
		s.PartitionsByTier[k] = v
	}
	return s
}

// ProjectCost projects future monthly costs based on current tier distribution and growth.
func (lm *LifecycleManager) ProjectCost(months int) []MonthlyCostProjection {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	if months <= 0 {
		return nil
	}

	// Gather current tier sizes from backend.
	tierSizes := make(map[StorageTierLevel]int64)
	if lm.backend != nil {
		ctx := context.Background()
		lm.backend.mu.RLock()
		for _, t := range lm.backend.tiers {
			if t.Backend == nil {
				continue
			}
			keys, err := t.Backend.List(ctx, "")
			if err != nil {
				continue
			}
			var total int64
			for _, key := range keys {
				data, err := t.Backend.Read(ctx, key)
				if err == nil {
					total += int64(len(data))
				}
			}
			tierSizes[t.Level] = total
		}
		lm.backend.mu.RUnlock()
	}

	// Cost per GB/month per tier.
	costPerGB := map[StorageTierLevel]float64{
		TierHot:     23.0,
		TierWarm:    12.5,
		TierCold:    4.0,
		TierArchive: 1.0,
	}

	// Override with actual tier costs if available.
	if lm.backend != nil {
		lm.backend.mu.RLock()
		for _, t := range lm.backend.tiers {
			if t.CostPerGBMonth > 0 {
				costPerGB[t.Level] = t.CostPerGBMonth
			}
		}
		lm.backend.mu.RUnlock()
	}

	// Assume 5% monthly data growth and lifecycle migrations shift data colder.
	growthRate := 0.05
	projections := make([]MonthlyCostProjection, months)

	for m := 0; m < months; m++ {
		growth := math.Pow(1.0+growthRate, float64(m))

		hotGB := float64(tierSizes[TierHot]) / float64(bytesPerGB) * growth
		warmGB := float64(tierSizes[TierWarm]) / float64(bytesPerGB) * growth
		coldGB := float64(tierSizes[TierCold]) / float64(bytesPerGB) * growth
		archiveGB := float64(tierSizes[TierArchive]) / float64(bytesPerGB) * growth

		// Simulate lifecycle: each month some hot data ages into warm, etc.
		if m > 0 {
			shiftRate := 0.1 * float64(m)
			if shiftRate > 0.5 {
				shiftRate = 0.5
			}
			shifted := hotGB * shiftRate
			hotGB -= shifted
			warmGB += shifted * 0.6
			coldGB += shifted * 0.3
			archiveGB += shifted * 0.1
		}

		proj := MonthlyCostProjection{
			Month:       m + 1,
			HotCost:     hotGB * costPerGB[TierHot],
			WarmCost:    warmGB * costPerGB[TierWarm],
			ColdCost:    coldGB * costPerGB[TierCold],
			ArchiveCost: archiveGB * costPerGB[TierArchive],
		}
		proj.TotalCost = proj.HotCost + proj.WarmCost + proj.ColdCost + proj.ArchiveCost
		projections[m] = proj
	}

	return projections
}
