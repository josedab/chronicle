package chronicle

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

// SmartRetentionConfig configures the smart retention engine.
type SmartRetentionConfig struct {
	Enabled              bool          `json:"enabled"`
	EvaluationInterval   time.Duration `json:"evaluation_interval"`
	MinRetention         time.Duration `json:"min_retention"`
	HotTierMaxAge        time.Duration `json:"hot_tier_max_age"`
	WarmTierMaxAge       time.Duration `json:"warm_tier_max_age"`
	WarmDownsampleWindow time.Duration `json:"warm_downsample_window"`
	ColdDownsampleWindow time.Duration `json:"cold_downsample_window"`
	AccessWeightDecay    float64       `json:"access_weight_decay"`
	HighValueThreshold   float64       `json:"high_value_threshold"`
	AutoMigrate          bool          `json:"auto_migrate"`
}

// DefaultSmartRetentionConfig returns sensible defaults.
func DefaultSmartRetentionConfig() SmartRetentionConfig {
	return SmartRetentionConfig{
		Enabled:              true,
		EvaluationInterval:   5 * time.Minute,
		MinRetention:         24 * time.Hour,
		HotTierMaxAge:        6 * time.Hour,
		WarmTierMaxAge:       7 * 24 * time.Hour,
		WarmDownsampleWindow: 5 * time.Minute,
		ColdDownsampleWindow: time.Hour,
		AccessWeightDecay:    0.95,
		HighValueThreshold:   0.7,
		AutoMigrate:          true,
	}
}

// SeriesAccessProfile tracks the access pattern for a single time-series.
type SeriesAccessProfile struct {
	Metric       string        `json:"metric"`
	AccessCount  int64         `json:"access_count"`
	LastAccessed time.Time     `json:"last_accessed"`
	CreatedAt    time.Time     `json:"created_at"`
	AvgInterval  time.Duration `json:"avg_interval"`
	ValueScore   float64       `json:"value_score"`
	CurrentTier  StorageTierLevel `json:"current_tier"`
}

// RetentionRecommendation is a recommendation for a series' retention policy.
type RetentionRecommendation struct {
	Metric         string           `json:"metric"`
	CurrentTier    StorageTierLevel `json:"current_tier"`
	RecommendedTier StorageTierLevel `json:"recommended_tier"`
	Reason         string           `json:"reason"`
	ValueScore     float64          `json:"value_score"`
	Confidence     float64          `json:"confidence"`
}

// SmartRetentionStats tracks overall smart retention metrics.
type SmartRetentionStats struct {
	TrackedSeries      int     `json:"tracked_series"`
	HotSeries          int     `json:"hot_series"`
	WarmSeries         int     `json:"warm_series"`
	ColdSeries         int     `json:"cold_series"`
	MigrationsExecuted int64   `json:"migrations_executed"`
	StorageSavingsEst  float64 `json:"storage_savings_est"`
	LastEvaluation     time.Time `json:"last_evaluation"`
}

// SmartRetentionEngine manages intelligent retention and tiering.
type SmartRetentionEngine struct {
	db      *DB
	config  SmartRetentionConfig

	profiles   map[string]*SeriesAccessProfile
	migrations int64
	lastEval   time.Time

	mu sync.RWMutex
}

// NewSmartRetentionEngine creates a new smart retention engine.
func NewSmartRetentionEngine(db *DB, cfg SmartRetentionConfig) *SmartRetentionEngine {
	return &SmartRetentionEngine{
		db:       db,
		config:   cfg,
		profiles: make(map[string]*SeriesAccessProfile),
	}
}

// RecordAccess records an access event for a metric.
func (sre *SmartRetentionEngine) RecordAccess(metric string) {
	sre.mu.Lock()
	defer sre.mu.Unlock()

	profile, exists := sre.profiles[metric]
	if !exists {
		profile = &SeriesAccessProfile{
			Metric:      metric,
			CreatedAt:   time.Now(),
			CurrentTier: TierHot,
		}
		sre.profiles[metric] = profile
	}

	now := time.Now()
	if profile.AccessCount > 0 && !profile.LastAccessed.IsZero() {
		interval := now.Sub(profile.LastAccessed)
		if profile.AvgInterval == 0 {
			profile.AvgInterval = interval
		} else {
			// Exponential moving average
			profile.AvgInterval = time.Duration(
				float64(profile.AvgInterval)*sre.config.AccessWeightDecay +
					float64(interval)*(1-sre.config.AccessWeightDecay))
		}
	}

	profile.AccessCount++
	profile.LastAccessed = now
	profile.ValueScore = sre.computeValueScore(profile)
}

func (sre *SmartRetentionEngine) computeValueScore(profile *SeriesAccessProfile) float64 {
	now := time.Now()

	// Recency factor (0-1): higher for recently accessed series
	recency := 0.0
	if !profile.LastAccessed.IsZero() {
		hoursSinceAccess := now.Sub(profile.LastAccessed).Hours()
		recency = math.Exp(-hoursSinceAccess / 24) // Decay over 24 hours
	}

	// Frequency factor (0-1): higher for frequently accessed series
	frequency := 0.0
	if profile.AccessCount > 0 {
		frequency = math.Min(1.0, float64(profile.AccessCount)/100.0)
	}

	// Regularity factor (0-1): higher for regularly accessed series
	regularity := 0.0
	if profile.AvgInterval > 0 && profile.AvgInterval < 24*time.Hour {
		regularity = 1.0 - math.Min(1.0, profile.AvgInterval.Hours()/24)
	}

	// Weighted combination
	return 0.4*recency + 0.35*frequency + 0.25*regularity
}

// Evaluate evaluates all tracked series and returns recommendations.
func (sre *SmartRetentionEngine) Evaluate() []RetentionRecommendation {
	sre.mu.Lock()
	defer sre.mu.Unlock()

	sre.lastEval = time.Now()
	var recommendations []RetentionRecommendation

	for _, profile := range sre.profiles {
		profile.ValueScore = sre.computeValueScore(profile)
		rec := sre.recommendTier(profile)
		if rec.RecommendedTier != rec.CurrentTier {
			recommendations = append(recommendations, rec)
		}
	}

	// Sort by confidence descending
	sort.Slice(recommendations, func(i, j int) bool {
		return recommendations[i].Confidence > recommendations[j].Confidence
	})

	return recommendations
}

func (sre *SmartRetentionEngine) recommendTier(profile *SeriesAccessProfile) RetentionRecommendation {
	rec := RetentionRecommendation{
		Metric:      profile.Metric,
		CurrentTier: profile.CurrentTier,
		ValueScore:  profile.ValueScore,
	}

	age := time.Since(profile.CreatedAt)
	timeSinceAccess := time.Since(profile.LastAccessed)

	switch {
	case profile.ValueScore >= sre.config.HighValueThreshold:
		rec.RecommendedTier = TierHot
		rec.Reason = "high value score - keep at full resolution"
		rec.Confidence = profile.ValueScore
	case timeSinceAccess > sre.config.WarmTierMaxAge || (age > sre.config.WarmTierMaxAge && profile.ValueScore < 0.3):
		rec.RecommendedTier = TierCold
		rec.Reason = fmt.Sprintf("low access (last: %s ago), downsample to %s windows",
			timeSinceAccess.Round(time.Minute), sre.config.ColdDownsampleWindow)
		rec.Confidence = 1.0 - profile.ValueScore
	case timeSinceAccess > sre.config.HotTierMaxAge || (age > sre.config.HotTierMaxAge && profile.ValueScore < 0.5):
		rec.RecommendedTier = TierWarm
		rec.Reason = fmt.Sprintf("moderate access, downsample to %s windows",
			sre.config.WarmDownsampleWindow)
		rec.Confidence = 0.5 + (1.0-profile.ValueScore)*0.5
	default:
		rec.RecommendedTier = TierHot
		rec.Reason = "recently active, keep at full resolution"
		rec.Confidence = profile.ValueScore
	}

	return rec
}

// ApplyRecommendations applies tier migration recommendations.
func (sre *SmartRetentionEngine) ApplyRecommendations(recs []RetentionRecommendation) int {
	sre.mu.Lock()
	defer sre.mu.Unlock()

	applied := 0
	for _, rec := range recs {
		profile, exists := sre.profiles[rec.Metric]
		if !exists {
			continue
		}
		if profile.CurrentTier != rec.RecommendedTier {
			profile.CurrentTier = rec.RecommendedTier
			applied++
			sre.migrations++
		}
	}
	return applied
}

// GetProfile returns the access profile for a metric.
func (sre *SmartRetentionEngine) GetProfile(metric string) (SeriesAccessProfile, bool) {
	sre.mu.RLock()
	defer sre.mu.RUnlock()
	profile, exists := sre.profiles[metric]
	if !exists {
		return SeriesAccessProfile{}, false
	}
	return *profile, true
}

// ListProfiles returns all tracked access profiles.
func (sre *SmartRetentionEngine) ListProfiles() []SeriesAccessProfile {
	sre.mu.RLock()
	defer sre.mu.RUnlock()
	profiles := make([]SeriesAccessProfile, 0, len(sre.profiles))
	for _, p := range sre.profiles {
		profiles = append(profiles, *p)
	}
	return profiles
}

// Stats returns aggregate statistics for smart retention.
func (sre *SmartRetentionEngine) Stats() SmartRetentionStats {
	sre.mu.RLock()
	defer sre.mu.RUnlock()

	stats := SmartRetentionStats{
		TrackedSeries:      len(sre.profiles),
		MigrationsExecuted: sre.migrations,
		LastEvaluation:     sre.lastEval,
	}

	for _, p := range sre.profiles {
		switch p.CurrentTier {
		case TierHot:
			stats.HotSeries++
		case TierWarm:
			stats.WarmSeries++
		case TierCold:
			stats.ColdSeries++
		}
	}

	// Estimate storage savings (warm = 60% reduction, cold = 85% reduction)
	if stats.TrackedSeries > 0 {
		warmPct := float64(stats.WarmSeries) / float64(stats.TrackedSeries)
		coldPct := float64(stats.ColdSeries) / float64(stats.TrackedSeries)
		stats.StorageSavingsEst = warmPct*0.6 + coldPct*0.85
	}

	return stats
}
