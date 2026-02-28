package chronicle

import (
	"math"
	"testing"
	"time"
)

func TestAccessPatternPredictor_Score(t *testing.T) {
	p := NewAccessPatternPredictor(0.1, 100)

	// Record recent accesses
	for i := 0; i < 10; i++ {
		p.RecordAccess("hot-partition", true)
	}

	score := p.PredictAccessScore("hot-partition")
	if score <= 0 {
		t.Fatalf("expected positive score, got %f", score)
	}

	// Cold partition should have score 0
	coldScore := p.PredictAccessScore("cold-partition")
	if coldScore != 0 {
		t.Fatalf("expected 0 for cold, got %f", coldScore)
	}
}

func TestAccessPatternPredictor_RecommendTier(t *testing.T) {
	p := NewAccessPatternPredictor(0.01, 1000)

	// Hot data: lots of recent reads
	for i := 0; i < 50; i++ {
		p.RecordAccess("hot-data", true)
	}

	tier := p.RecommendTier("hot-data")
	if tier != TierHot {
		t.Fatalf("expected hot tier, got %s", tier)
	}

	// Cold data: no accesses
	tier = p.RecommendTier("no-access-data")
	if tier != TierArchive {
		t.Fatalf("expected archive tier for no-access data, got %s", tier)
	}
}

func TestAccessPatternPredictor_PredictNextAccess(t *testing.T) {
	p := NewAccessPatternPredictor(0.1, 100)

	// No history
	next := p.PredictNextAccess("unknown")
	if !math.IsInf(next, 1) {
		t.Fatalf("expected +Inf for unknown, got %f", next)
	}

	// Record events
	p.RecordAccess("regular", true)
	time.Sleep(time.Millisecond)
	p.RecordAccess("regular", true)

	next = p.PredictNextAccess("regular")
	if math.IsInf(next, 1) {
		t.Fatal("expected finite prediction")
	}
}

func TestTieredStorageCostOptimizer_Analyze(t *testing.T) {
	predictor := NewAccessPatternPredictor(0.1, 100)

	// Hot partition with many accesses
	for i := 0; i < 20; i++ {
		predictor.RecordAccess("partition-hot", true)
	}
	// Cold partition with no accesses — will recommend archive

	optimizer := NewTieredStorageCostOptimizer(DefaultTierCostProfiles(), predictor)

	partitions := []PartitionInfo{
		{Key: "partition-hot", CurrentTier: TierHot, SizeBytes: 10 * 1024 * 1024 * 1024},   // 10GB
		{Key: "partition-cold", CurrentTier: TierHot, SizeBytes: 100 * 1024 * 1024 * 1024},  // 100GB
	}

	report := optimizer.Analyze(partitions)
	if report.CurrentMonthlyCost <= 0 {
		t.Fatal("expected positive current cost")
	}
	if report.EstimatedSavings < 0 {
		t.Fatal("expected non-negative savings")
	}
	if len(report.Recommendations) == 0 {
		t.Fatal("expected at least one recommendation for cold data")
	}
}

func TestLifecycleRuleEngine_Evaluate(t *testing.T) {
	engine := NewLifecycleRuleEngine()
	for _, rule := range DefaultAWSLifecycleRules() {
		engine.AddRule(rule)
	}

	// Partition aged 60 days in hot tier
	partition := PartitionInfo{
		Key:         "old-data",
		CurrentTier: TierHot,
		SizeBytes:   1024 * 1024 * 1024,
		CreatedAt:   time.Now().Add(-60 * 24 * time.Hour),
	}

	rules := engine.EvaluatePartition(partition, 0.5)
	if len(rules) == 0 {
		t.Fatal("expected rule to apply for 60-day-old hot data")
	}
	if rules[0].DestTier != TierWarm {
		t.Fatalf("expected warm dest, got %s", rules[0].DestTier)
	}

	// Fresh partition should not trigger rules
	fresh := PartitionInfo{
		Key:         "fresh-data",
		CurrentTier: TierHot,
		SizeBytes:   1024 * 1024,
		CreatedAt:   time.Now().Add(-1 * time.Hour),
	}
	rules = engine.EvaluatePartition(fresh, 10.0)
	if len(rules) != 0 {
		t.Fatalf("expected no rules for fresh data, got %d", len(rules))
	}
}

func TestDefaultLifecycleRules(t *testing.T) {
	aws := DefaultAWSLifecycleRules()
	if len(aws) != 3 {
		t.Fatalf("expected 3 AWS rules, got %d", len(aws))
	}
	gcs := DefaultGCSLifecycleRules()
	if len(gcs) != 3 {
		t.Fatalf("expected 3 GCS rules, got %d", len(gcs))
	}
	azure := DefaultAzureLifecycleRules()
	if len(azure) != 3 {
		t.Fatalf("expected 3 Azure rules, got %d", len(azure))
	}
}
