package chronicle

import (
	"math"
	"sort"
	"sync"
	"time"
)

// AccessPatternPredictor uses time-decay models to predict future access patterns.
type AccessPatternPredictor struct {
	mu          sync.RWMutex
	decayRate   float64 // exponential decay rate (higher = faster decay)
	history     map[string][]accessEvent
	maxHistory  int
}

type accessEvent struct {
	timestamp time.Time
	isRead    bool
}

// NewAccessPatternPredictor creates a new predictor with the given decay rate.
func NewAccessPatternPredictor(decayRate float64, maxHistory int) *AccessPatternPredictor {
	if decayRate <= 0 {
		decayRate = 0.1 // default: moderate decay
	}
	if maxHistory <= 0 {
		maxHistory = 1000
	}
	return &AccessPatternPredictor{
		decayRate:  decayRate,
		history:    make(map[string][]accessEvent),
		maxHistory: maxHistory,
	}
}

// RecordAccess records an access event for prediction.
func (p *AccessPatternPredictor) RecordAccess(partitionKey string, isRead bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	events := p.history[partitionKey]
	events = append(events, accessEvent{timestamp: time.Now(), isRead: isRead})
	if len(events) > p.maxHistory {
		events = events[len(events)-p.maxHistory:]
	}
	p.history[partitionKey] = events
}

// PredictAccessScore computes a time-decay weighted access score.
// Uses exponential decay: score = Σ e^(-λ * age_hours)
func (p *AccessPatternPredictor) PredictAccessScore(partitionKey string) float64 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	events, ok := p.history[partitionKey]
	if !ok || len(events) == 0 {
		return 0
	}

	now := time.Now()
	var score float64
	for _, e := range events {
		ageHours := now.Sub(e.timestamp).Hours()
		weight := math.Exp(-p.decayRate * ageHours)
		if e.isRead {
			score += weight // reads weighted 1x
		} else {
			score += weight * 0.5 // writes weighted 0.5x
		}
	}
	return score
}

// PredictNextAccess estimates hours until next access based on historical intervals.
func (p *AccessPatternPredictor) PredictNextAccess(partitionKey string) float64 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	events := p.history[partitionKey]
	if len(events) < 2 {
		return math.Inf(1) // no prediction possible
	}

	// Compute average interval with time-decay weighting
	var weightedSum, weightSum float64
	for i := 1; i < len(events); i++ {
		interval := events[i].timestamp.Sub(events[i-1].timestamp).Hours()
		recency := float64(i) / float64(len(events))
		weight := math.Pow(recency, 2) // more recent intervals weighted higher
		weightedSum += interval * weight
		weightSum += weight
	}

	if weightSum == 0 {
		return math.Inf(1)
	}
	return weightedSum / weightSum
}

// RecommendTier suggests the optimal tier based on predicted access patterns.
func (p *AccessPatternPredictor) RecommendTier(partitionKey string) StorageTierLevel {
	score := p.PredictAccessScore(partitionKey)
	nextAccess := p.PredictNextAccess(partitionKey)

	switch {
	case score > 10.0 || nextAccess < 1.0:
		return TierHot
	case score > 2.0 || nextAccess < 24.0:
		return TierWarm
	case score > 0.5 || nextAccess < 168.0: // 1 week
		return TierCold
	default:
		return TierArchive
	}
}

// --- Storage Cost Optimizer ---

// TieredStorageCostOptimizer analyzes storage costs and recommends optimizations.
type TieredStorageCostOptimizer struct {
	mu        sync.RWMutex
	tiers     []TierCostProfile
	predictor *AccessPatternPredictor
}

// TierCostProfile defines cost characteristics for a storage tier.
type TierCostProfile struct {
	Tier              StorageTierLevel
	CostPerGBMonth    float64 // $/GB/month for storage
	ReadCostPerGB     float64 // $/GB for reads
	WriteCostPerGB    float64 // $/GB for writes
	MinRetentionDays  int     // minimum retention before downtiering
	TransitionCostGB  float64 // $/GB for tier transition
}

// CostOptimizationReport provides cost analysis and recommendations.
type CostOptimizationReport struct {
	CurrentMonthlyCost  float64                `json:"current_monthly_cost"`
	OptimizedMonthlyCost float64               `json:"optimized_monthly_cost"`
	EstimatedSavings    float64                `json:"estimated_savings"`
	SavingsPercent      float64                `json:"savings_percent"`
	Recommendations     []CostRecommendation   `json:"recommendations"`
	TierDistribution    map[string]TierUsage   `json:"tier_distribution"`
	GeneratedAt         time.Time              `json:"generated_at"`
}

// CostRecommendation is a single cost optimization action.
type CostRecommendation struct {
	PartitionKey   string           `json:"partition_key"`
	CurrentTier    StorageTierLevel `json:"current_tier"`
	RecommendedTier StorageTierLevel `json:"recommended_tier"`
	MonthlySavings float64          `json:"monthly_savings"`
	Reason         string           `json:"reason"`
	DataSizeGB     float64          `json:"data_size_gb"`
}

// TierUsage tracks usage for a specific tier.
type TierUsage struct {
	StorageGB    float64 `json:"storage_gb"`
	MonthlyCost  float64 `json:"monthly_cost"`
	PartitionCount int   `json:"partition_count"`
}

// NewTieredStorageCostOptimizer creates a new cost optimizer.
func NewTieredStorageCostOptimizer(tiers []TierCostProfile, predictor *AccessPatternPredictor) *TieredStorageCostOptimizer {
	return &TieredStorageCostOptimizer{
		tiers:     tiers,
		predictor: predictor,
	}
}

// DefaultTierCostProfiles returns cost profiles matching major cloud providers.
func DefaultTierCostProfiles() []TierCostProfile {
	return []TierCostProfile{
		{Tier: TierHot, CostPerGBMonth: 0.023, ReadCostPerGB: 0.0004, WriteCostPerGB: 0.005},
		{Tier: TierWarm, CostPerGBMonth: 0.0125, ReadCostPerGB: 0.001, WriteCostPerGB: 0.01, MinRetentionDays: 30},
		{Tier: TierCold, CostPerGBMonth: 0.004, ReadCostPerGB: 0.01, WriteCostPerGB: 0.02, MinRetentionDays: 90, TransitionCostGB: 0.01},
		{Tier: TierArchive, CostPerGBMonth: 0.00099, ReadCostPerGB: 0.02, WriteCostPerGB: 0.05, MinRetentionDays: 180, TransitionCostGB: 0.02},
	}
}

// Analyze generates a cost optimization report for the given partition data.
func (co *TieredStorageCostOptimizer) Analyze(partitions []PartitionInfo) *CostOptimizationReport {
	co.mu.RLock()
	defer co.mu.RUnlock()

	report := &CostOptimizationReport{
		TierDistribution: make(map[string]TierUsage),
		GeneratedAt:      time.Now(),
	}

	tierCosts := make(map[StorageTierLevel]*TierCostProfile)
	for i := range co.tiers {
		tierCosts[co.tiers[i].Tier] = &co.tiers[i]
	}

	for _, p := range partitions {
		sizeGB := float64(p.SizeBytes) / (1024 * 1024 * 1024)
		currentProfile := tierCosts[p.CurrentTier]
		if currentProfile == nil {
			continue
		}

		currentCost := sizeGB * currentProfile.CostPerGBMonth
		report.CurrentMonthlyCost += currentCost

		// Track tier distribution
		tierName := p.CurrentTier.String()
		usage := report.TierDistribution[tierName]
		usage.StorageGB += sizeGB
		usage.MonthlyCost += currentCost
		usage.PartitionCount++
		report.TierDistribution[tierName] = usage

		// Compute optimal tier
		recommendedTier := co.predictor.RecommendTier(p.Key)
		recommendedProfile := tierCosts[recommendedTier]
		if recommendedProfile == nil {
			continue
		}

		optimizedCost := sizeGB * recommendedProfile.CostPerGBMonth
		report.OptimizedMonthlyCost += optimizedCost

		if recommendedTier != p.CurrentTier && currentCost-optimizedCost > 0.01 {
			report.Recommendations = append(report.Recommendations, CostRecommendation{
				PartitionKey:    p.Key,
				CurrentTier:     p.CurrentTier,
				RecommendedTier: recommendedTier,
				MonthlySavings:  currentCost - optimizedCost,
				DataSizeGB:      sizeGB,
				Reason:          co.predictor.RecommendTier(p.Key).String() + " tier optimal for access pattern",
			})
		}
	}

	report.EstimatedSavings = report.CurrentMonthlyCost - report.OptimizedMonthlyCost
	if report.CurrentMonthlyCost > 0 {
		report.SavingsPercent = math.Round(report.EstimatedSavings/report.CurrentMonthlyCost*10000) / 100
	}

	// Sort recommendations by savings (highest first)
	sort.Slice(report.Recommendations, func(i, j int) bool {
		return report.Recommendations[i].MonthlySavings > report.Recommendations[j].MonthlySavings
	})

	return report
}

// PartitionInfo provides metadata about a storage partition for cost analysis.
type PartitionInfo struct {
	Key         string
	CurrentTier StorageTierLevel
	SizeBytes   int64
	CreatedAt   time.Time
}

// --- Multi-Cloud Lifecycle Rules ---

// LifecycleRule defines an automatic data transition rule.
type LifecycleRule struct {
	Name            string           `json:"name"`
	Provider        string           `json:"provider"` // "aws", "gcs", "azure"
	SourceTier      StorageTierLevel `json:"source_tier"`
	DestTier        StorageTierLevel `json:"dest_tier"`
	AgeDays         int              `json:"age_days"`
	MinAccessScore  float64          `json:"min_access_score"`
	Enabled         bool             `json:"enabled"`
}

// LifecycleRuleEngine evaluates and applies lifecycle rules.
type LifecycleRuleEngine struct {
	mu    sync.RWMutex
	rules []LifecycleRule
}

// NewLifecycleRuleEngine creates a new lifecycle rule engine.
func NewLifecycleRuleEngine() *LifecycleRuleEngine {
	return &LifecycleRuleEngine{}
}

// AddRule adds a lifecycle rule.
func (e *LifecycleRuleEngine) AddRule(rule LifecycleRule) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rules = append(e.rules, rule)
}

// EvaluatePartition checks which rules apply to a given partition.
func (e *LifecycleRuleEngine) EvaluatePartition(partition PartitionInfo, accessScore float64) []LifecycleRule {
	e.mu.RLock()
	defer e.mu.RUnlock()

	ageInDays := int(time.Since(partition.CreatedAt).Hours() / 24)
	var applicable []LifecycleRule

	for _, rule := range e.rules {
		if !rule.Enabled {
			continue
		}
		if rule.SourceTier != partition.CurrentTier {
			continue
		}
		if ageInDays < rule.AgeDays {
			continue
		}
		if rule.MinAccessScore > 0 && accessScore >= rule.MinAccessScore {
			continue
		}
		applicable = append(applicable, rule)
	}

	return applicable
}

// DefaultAWSLifecycleRules returns standard AWS S3 lifecycle rules.
func DefaultAWSLifecycleRules() []LifecycleRule {
	return []LifecycleRule{
		{Name: "hot-to-ia", Provider: "aws", SourceTier: TierHot, DestTier: TierWarm, AgeDays: 30, Enabled: true},
		{Name: "ia-to-glacier", Provider: "aws", SourceTier: TierWarm, DestTier: TierCold, AgeDays: 90, Enabled: true},
		{Name: "glacier-to-deep", Provider: "aws", SourceTier: TierCold, DestTier: TierArchive, AgeDays: 180, Enabled: true},
	}
}

// DefaultGCSLifecycleRules returns standard GCS lifecycle rules.
func DefaultGCSLifecycleRules() []LifecycleRule {
	return []LifecycleRule{
		{Name: "standard-to-nearline", Provider: "gcs", SourceTier: TierHot, DestTier: TierWarm, AgeDays: 30, Enabled: true},
		{Name: "nearline-to-coldline", Provider: "gcs", SourceTier: TierWarm, DestTier: TierCold, AgeDays: 90, Enabled: true},
		{Name: "coldline-to-archive", Provider: "gcs", SourceTier: TierCold, DestTier: TierArchive, AgeDays: 365, Enabled: true},
	}
}

// DefaultAzureLifecycleRules returns standard Azure Blob lifecycle rules.
func DefaultAzureLifecycleRules() []LifecycleRule {
	return []LifecycleRule{
		{Name: "hot-to-cool", Provider: "azure", SourceTier: TierHot, DestTier: TierWarm, AgeDays: 30, Enabled: true},
		{Name: "cool-to-cold", Provider: "azure", SourceTier: TierWarm, DestTier: TierCold, AgeDays: 90, Enabled: true},
		{Name: "cold-to-archive", Provider: "azure", SourceTier: TierCold, DestTier: TierArchive, AgeDays: 180, Enabled: true},
	}
}
