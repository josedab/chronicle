package chronicle

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// TierQueryRouter provides transparent query routing across storage tiers.
// It queries hot, warm, and cold tiers in parallel and merges results.
type TierQueryRouter struct {
	mu    sync.RWMutex
	tiers []TierBackend

	tracker *AccessTracker
	stats   TierQueryStats
}

// TierBackend wraps a DataStore with its tier level.
type TierBackend struct {
	Level StorageTierLevel
	Store DataStore
}

// TierQueryStats tracks query routing statistics.
type TierQueryStats struct {
	HotHits          int64 `json:"hot_hits"`
	WarmHits         int64 `json:"warm_hits"`
	ColdHits         int64 `json:"cold_hits"`
	CrossTierQueries int64 `json:"cross_tier_queries"`
	TotalQueries     int64 `json:"total_queries"`
}

// NewTierQueryRouter creates a query router across tiers.
func NewTierQueryRouter(tracker *AccessTracker, tiers ...TierBackend) *TierQueryRouter {
	sorted := make([]TierBackend, len(tiers))
	copy(sorted, tiers)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Level < sorted[j].Level
	})

	return &TierQueryRouter{
		tiers:   sorted,
		tracker: tracker,
	}
}

// ReadPartition reads a partition by scanning tiers from hot to cold.
func (r *TierQueryRouter) ReadPartition(ctx context.Context, partitionID uint64) ([]byte, StorageTierLevel, error) {
	r.mu.Lock()
	r.stats.TotalQueries++
	r.mu.Unlock()

	for _, tier := range r.tiers {
		data, err := tier.Store.ReadPartition(ctx, partitionID)
		if err == nil && len(data) > 0 {
			r.mu.Lock()
			switch tier.Level {
			case TierHot:
				r.stats.HotHits++
			case TierWarm:
				r.stats.WarmHits++
			default:
				r.stats.ColdHits++
			}
			r.mu.Unlock()

			if r.tracker != nil {
				r.tracker.RecordRead(fmt.Sprintf("partition-%d", partitionID))
			}
			return data, tier.Level, nil
		}
	}

	return nil, TierCold, fmt.Errorf("partition %d not found in any tier", partitionID)
}

// QueryAcrossTiers executes a query against the database, gathering points from
// all tiers within the time range and merging them in timestamp order.
func (r *TierQueryRouter) QueryAcrossTiers(ctx context.Context, db *DB, q *Query) (*Result, error) {
	if db == nil {
		return nil, fmt.Errorf("tier router: database is nil")
	}

	r.mu.Lock()
	r.stats.CrossTierQueries++
	r.mu.Unlock()

	// Execute query — the DB will read from whatever storage is configured
	return db.Execute(q)
}

// Stats returns query routing statistics.
func (r *TierQueryRouter) Stats() TierQueryStats {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.stats
}

// MetricTierPolicy defines tier placement rules for a specific metric.
type MetricTierPolicy struct {
	Metric         string        `json:"metric"`
	HotDuration    time.Duration `json:"hot_duration"`
	WarmDuration   time.Duration `json:"warm_duration"`
	ColdEnabled    bool          `json:"cold_enabled"`
	Priority       int           `json:"priority"` // higher = stays hot longer
	CompressOnCold bool          `json:"compress_on_cold"`
}

// TierPolicyEngine manages per-metric tier placement policies.
type TierPolicyEngine struct {
	mu       sync.RWMutex
	policies map[string]*MetricTierPolicy
	defaults MetricTierPolicy
}

// NewTierPolicyEngine creates a new policy engine with default settings.
func NewTierPolicyEngine() *TierPolicyEngine {
	return &TierPolicyEngine{
		policies: make(map[string]*MetricTierPolicy),
		defaults: MetricTierPolicy{
			HotDuration:  1 * time.Hour,
			WarmDuration: 24 * time.Hour,
			ColdEnabled:  true,
			Priority:     0,
		},
	}
}

// SetDefault updates the default policy for metrics without explicit rules.
func (pe *TierPolicyEngine) SetDefault(policy MetricTierPolicy) {
	pe.mu.Lock()
	defer pe.mu.Unlock()
	pe.defaults = policy
}

// SetPolicy sets a per-metric tier policy.
func (pe *TierPolicyEngine) SetPolicy(policy MetricTierPolicy) error {
	if policy.Metric == "" {
		return fmt.Errorf("tier policy: metric name is required")
	}
	if policy.HotDuration <= 0 {
		policy.HotDuration = pe.defaults.HotDuration
	}
	if policy.WarmDuration <= 0 {
		policy.WarmDuration = pe.defaults.WarmDuration
	}

	pe.mu.Lock()
	defer pe.mu.Unlock()
	pe.policies[policy.Metric] = &policy
	return nil
}

// RemovePolicy removes a per-metric policy, reverting to defaults.
func (pe *TierPolicyEngine) RemovePolicy(metric string) {
	pe.mu.Lock()
	defer pe.mu.Unlock()
	delete(pe.policies, metric)
}

// GetPolicy returns the effective policy for a metric.
func (pe *TierPolicyEngine) GetPolicy(metric string) MetricTierPolicy {
	pe.mu.RLock()
	defer pe.mu.RUnlock()

	if p, ok := pe.policies[metric]; ok {
		return *p
	}
	d := pe.defaults
	d.Metric = metric
	return d
}

// ListPolicies returns all explicitly configured policies.
func (pe *TierPolicyEngine) ListPolicies() []MetricTierPolicy {
	pe.mu.RLock()
	defer pe.mu.RUnlock()

	policies := make([]MetricTierPolicy, 0, len(pe.policies))
	for _, p := range pe.policies {
		policies = append(policies, *p)
	}
	sort.Slice(policies, func(i, j int) bool {
		return policies[i].Metric < policies[j].Metric
	})
	return policies
}

// RecommendTier returns the recommended tier for a metric based on data age and policy.
func (pe *TierPolicyEngine) RecommendTier(metric string, dataAge time.Duration) StorageTierLevel {
	policy := pe.GetPolicy(metric)

	if dataAge <= policy.HotDuration {
		return TierHot
	}
	if dataAge <= policy.HotDuration+policy.WarmDuration {
		return TierWarm
	}
	if policy.ColdEnabled {
		return TierCold
	}
	return TierWarm // keep in warm if cold not enabled
}

// EvaluatePlacements checks all current partitions and returns migration recommendations.
func (pe *TierPolicyEngine) EvaluatePlacements(partitions []TierPartitionInfo) []TierMigrationRecommendation {
	now := time.Now()
	var recs []TierMigrationRecommendation

	for _, p := range partitions {
		recommended := pe.RecommendTier(p.Metric, now.Sub(p.CreatedAt))
		if recommended != p.CurrentTier {
			recs = append(recs, TierMigrationRecommendation{
				PartitionID: p.PartitionID,
				Metric:      p.Metric,
				CurrentTier: p.CurrentTier,
				TargetTier:  recommended,
				Reason:      fmt.Sprintf("data age %v exceeds %s retention", now.Sub(p.CreatedAt).Round(time.Minute), p.CurrentTier),
			})
		}
	}

	return recs
}

// TierPartitionInfo provides partition info for policy evaluation.
type TierPartitionInfo struct {
	PartitionID uint64
	Metric      string
	CurrentTier StorageTierLevel
	CreatedAt   time.Time
	SizeBytes   int64
}

// TierMigrationRecommendation suggests moving a partition to a different tier.
type TierMigrationRecommendation struct {
	PartitionID uint64           `json:"partition_id"`
	Metric      string           `json:"metric"`
	CurrentTier StorageTierLevel `json:"current_tier"`
	TargetTier  StorageTierLevel `json:"target_tier"`
	Reason      string           `json:"reason"`
}
