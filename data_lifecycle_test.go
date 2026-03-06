package chronicle

import (
	"testing"
	"time"
)

func TestAccessHeatmap(t *testing.T) {
	hm := NewAccessHeatmap(168)

	hm.RecordAccess("cpu_usage", 0.95)
	hm.RecordAccess("cpu_usage", 0.95)
	hm.RecordAccess("memory_used", 0.95)

	score := hm.GetScore("cpu_usage")
	if score <= 0 {
		t.Fatalf("expected positive score for cpu_usage, got %f", score)
	}

	info, ok := hm.GetInfo("cpu_usage")
	if !ok {
		t.Fatal("expected info for cpu_usage")
	}
	if info.TotalAccesses != 2 {
		t.Fatalf("expected 2 accesses, got %d", info.TotalAccesses)
	}

	top := hm.TopMetrics(10)
	if len(top) != 2 {
		t.Fatalf("expected 2 metrics, got %d", len(top))
	}
	// cpu_usage should be first (higher score)
	if top[0].Metric != "cpu_usage" {
		t.Fatalf("expected cpu_usage first, got %s", top[0].Metric)
	}
}

func TestRetentionPolicyPredictor(t *testing.T) {
	hm := NewAccessHeatmap(168)
	config := DefaultDataLifecycleConfig()
	predictor := NewRetentionPolicyPredictor(hm, config)

	// Add metric with low access
	hm.mu.Lock()
	hm.metrics["old_metric"] = &MetricAccessInfo{
		Metric:       "old_metric",
		TotalAccesses: 1,
		LastAccessed: time.Now().Add(-40 * 24 * time.Hour), // 40 days ago
		DecayedScore: 0.1,
		HourlyHits:   make([]int64, 168),
		SizeBytes:    1024 * 1024 * 100, // 100 MB
		CreatedAt:    time.Now().Add(-180 * 24 * time.Hour),
	}
	hm.mu.Unlock()

	policies := predictor.PredictPolicies()
	if len(policies) != 1 {
		t.Fatalf("expected 1 policy, got %d", len(policies))
	}
	if policies[0].SuggestedTier == "hot" {
		t.Fatal("expected tier change for old metric")
	}
	if policies[0].MonthlySavings <= 0 {
		t.Fatal("expected positive savings")
	}
}

func TestCostSimulation(t *testing.T) {
	config := DefaultDataLifecycleConfig()
	hm := NewAccessHeatmap(168)
	predictor := NewRetentionPolicyPredictor(hm, config)

	costHot := predictor.SimulateCost(10, "hot")
	costCold := predictor.SimulateCost(10, "cold")
	costArchive := predictor.SimulateCost(10, "archive")

	if costHot <= costCold {
		t.Fatal("hot should cost more than cold")
	}
	if costCold <= costArchive {
		t.Fatal("cold should cost more than archive")
	}
}

func TestDataLegalHoldManager(t *testing.T) {
	mgr := NewDataLegalHoldManager()

	err := mgr.CreateHold(DataLegalHold{
		ID:       "hold-1",
		Name:     "Audit Hold",
		Metrics:  []string{"audit_*"},
		Reason:   "Regulatory audit",
		CreatedBy: "admin",
	})
	if err != nil {
		t.Fatalf("create hold: %v", err)
	}

	// Check if metric is held
	if !mgr.IsHeld("audit_log") {
		t.Fatal("expected audit_log to be held")
	}
	if mgr.IsHeld("cpu_usage") {
		t.Fatal("cpu_usage should not be held")
	}

	// Release hold
	if err := mgr.ReleaseHold("hold-1"); err != nil {
		t.Fatalf("release hold: %v", err)
	}
	if mgr.IsHeld("audit_log") {
		t.Fatal("audit_log should not be held after release")
	}
}

func TestDataLegalHoldManager_Validation(t *testing.T) {
	mgr := NewDataLegalHoldManager()

	if err := mgr.CreateHold(DataLegalHold{ID: "", Name: "test"}); err == nil {
		t.Fatal("expected error for empty ID")
	}
	if err := mgr.CreateHold(DataLegalHold{ID: "1", Name: ""}); err == nil {
		t.Fatal("expected error for empty name")
	}
	if err := mgr.CreateHold(DataLegalHold{ID: "1", Name: "test", Metrics: nil}); err == nil {
		t.Fatal("expected error for empty metrics")
	}
}

func TestDataLifecycleEngine(t *testing.T) {
	config := DefaultDataLifecycleConfig()
	engine := NewDataLifecycleEngine(config)

	// Record accesses
	engine.RecordAccess("cpu_usage")
	engine.RecordAccess("cpu_usage")
	engine.RecordAccess("memory_used")

	// Check heatmap
	top := engine.GetHeatmap(10)
	if len(top) != 2 {
		t.Fatalf("expected 2 metrics in heatmap, got %d", len(top))
	}

	// Create legal hold
	err := engine.CreateLegalHold(DataLegalHold{
		ID: "hold-1", Name: "Test", Metrics: []string{"audit_*"}, CreatedBy: "admin",
	})
	if err != nil {
		t.Fatalf("create hold: %v", err)
	}
	if !engine.IsDataHeld("audit_log") {
		t.Fatal("expected audit_log to be held")
	}

	// Evaluate to populate stats
	engine.Evaluate()

	// Check stats
	stats := engine.Stats()
	if stats.DryRunMode != true {
		t.Fatal("expected dry run mode")
	}
}

func TestDataLifecycleEngine_DryRun(t *testing.T) {
	config := DefaultDataLifecycleConfig()
	config.DryRunMode = true
	engine := NewDataLifecycleEngine(config)

	policies := []PredictedPolicy{
		{Metric: "test", SuggestedTier: "cold", MonthlySavings: 5.0},
	}

	applied := engine.Apply(policies)
	if applied != 0 {
		t.Fatal("dry run should not apply any policies")
	}
}

func TestDataLifecycleEngine_Rehydrate(t *testing.T) {
	engine := NewDataLifecycleEngine(DefaultDataLifecycleConfig())

	progress, err := engine.Rehydrate("old_metric")
	if err != nil {
		t.Fatalf("rehydrate: %v", err)
	}
	if progress.Status != "in_progress" {
		t.Fatalf("expected 'in_progress', got %s", progress.Status)
	}
}

func TestDataLifecycleEngine_GracePeriodIntegration(t *testing.T) {
	config := DefaultDataLifecycleConfig()
	config.DryRunMode = false // not dry run
	engine := NewDataLifecycleEngine(config)

	policies := []PredictedPolicy{
		{Metric: "old_data", SuggestedTier: "archive", MonthlySavings: 10.0},
		{Metric: "med_data", SuggestedTier: "cold", MonthlySavings: 5.0},
	}

	// Apply schedules into grace period, but none are ready yet
	applied := engine.Apply(policies)
	if applied != 0 {
		t.Errorf("expected 0 applied (grace period), got %d", applied)
	}
	if engine.PendingPolicies() != 2 {
		t.Errorf("expected 2 pending, got %d", engine.PendingPolicies())
	}

	// Cancel one
	if !engine.CancelPendingPolicy("old_data", "admin") {
		t.Error("expected successful cancel")
	}
	if engine.PendingPolicies() != 1 {
		t.Errorf("expected 1 pending after cancel, got %d", engine.PendingPolicies())
	}
}

func TestDataLifecycleEngine_DecisionTreeIntegration(t *testing.T) {
	config := DefaultDataLifecycleConfig()
	engine := NewDataLifecycleEngine(config)

	// Record access to populate heatmap
	for i := 0; i < 100; i++ {
		engine.RecordAccess("hot_metric")
	}
	engine.RecordAccess("cold_metric")

	// Predict tier using decision tree
	tier, conf := engine.PredictTier("hot_metric")
	// With frequent access, should be hot or warm (both acceptable for active metric)
	if tier != "hot" && tier != "warm" {
		t.Errorf("expected 'hot' or 'warm' for frequently accessed metric, got %q", tier)
	}
	if conf < 0.5 {
		t.Errorf("expected confidence >= 0.5, got %f", conf)
	}

	// Unknown metric defaults to hot
	tier, _ = engine.PredictTier("nonexistent")
	if tier != "hot" {
		t.Errorf("expected 'hot' default, got %q", tier)
	}
}

func TestDataLifecycleEngine_StatsIncludeGracePeriod(t *testing.T) {
	config := DefaultDataLifecycleConfig()
	config.DryRunMode = false
	engine := NewDataLifecycleEngine(config)

	engine.Apply([]PredictedPolicy{{Metric: "test", SuggestedTier: "cold"}})
	engine.Evaluate() // trigger stats update

	stats := engine.Stats()
	if stats.PendingGracePeriod != 1 {
		t.Errorf("expected 1 pending in stats, got %d", stats.PendingGracePeriod)
	}
}

func TestComputeValueScore(t *testing.T) {
	info := &MetricAccessInfo{
		Metric:       "cpu_usage",
		TotalAccesses: 100,
		LastAccessed: time.Now(),
		DecayedScore: 50.0,
		SizeBytes:    1024 * 1024,
	}

	score := ComputeValueScore(info, 0.95)
	if score <= 0 {
		t.Fatalf("expected positive value score, got %f", score)
	}

	// Nil should return 0
	if ComputeValueScore(nil, 0.95) != 0 {
		t.Fatal("expected 0 for nil info")
	}
}

func TestMatchesPattern(t *testing.T) {
	tests := []struct {
		metric, pattern string
		expected        bool
	}{
		{"cpu_usage", "cpu_usage", true},
		{"cpu_usage", "cpu_*", true},
		{"memory_used", "cpu_*", false},
		{"anything", "*", true},
		{"audit_log", "audit_*", true},
	}

	for _, tt := range tests {
		got := lifecycleMatchesPattern(tt.metric, tt.pattern)
		if got != tt.expected {
			t.Errorf("lifecycleMatchesPattern(%q, %q) = %v, want %v", tt.metric, tt.pattern, got, tt.expected)
		}
	}
}

func TestGracePeriodManager(t *testing.T) {
	mgr := NewGracePeriodManager(7)

	policy := PredictedPolicy{
		Metric: "old_data", SuggestedTier: "archive", MonthlySavings: 10.0,
	}

	entry := mgr.Schedule(policy)
	if entry.Cancelled {
		t.Fatal("new entry should not be cancelled")
	}
	if mgr.PendingCount() != 1 {
		t.Fatalf("expected 1 pending, got %d", mgr.PendingCount())
	}

	ready := mgr.ReadyPolicies()
	if len(ready) != 0 {
		t.Fatal("expected 0 ready policies during grace period")
	}

	if !mgr.Cancel("old_data", "admin") {
		t.Fatal("expected successful cancel")
	}
	if mgr.PendingCount() != 0 {
		t.Fatal("expected 0 pending after cancel")
	}
}

func TestDecisionTreePredictor(t *testing.T) {
	predictor := NewDecisionTreePredictor(DefaultDecisionThresholds())

	tests := []struct {
		name     string
		features AccessPatternFeatures
		expected string
	}{
		{"hot", AccessPatternFeatures{DecayedScore: 50, DaysSinceLastAccess: 0.5}, "hot"},
		{"warm", AccessPatternFeatures{DecayedScore: 10, DaysSinceLastAccess: 3}, "warm"},
		{"cold", AccessPatternFeatures{DecayedScore: 2, DaysSinceLastAccess: 15}, "cold"},
		{"archive", AccessPatternFeatures{DecayedScore: 0.1, DaysSinceLastAccess: 90}, "archive"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tier := predictor.Predict(tt.features)
			if tier != tt.expected {
				t.Errorf("predicted %q, want %q", tier, tt.expected)
			}
		})
	}
}

func TestDecisionTreePredictor_WithConfidence(t *testing.T) {
	predictor := NewDecisionTreePredictor(DefaultDecisionThresholds())
	tier, conf := predictor.PredictWithConfidence(AccessPatternFeatures{
		DecayedScore: 100, DaysSinceLastAccess: 0.1,
	})
	if tier != "hot" {
		t.Errorf("expected hot, got %s", tier)
	}
	if conf < 0.5 || conf > 1.0 {
		t.Errorf("invalid confidence: %f", conf)
	}
}

func TestExtractFeatures(t *testing.T) {
	info := &MetricAccessInfo{
		Metric:        "cpu",
		TotalAccesses: 1000,
		LastAccessed:  time.Now(),
		DecayedScore:  50,
		SizeBytes:     1024 * 1024 * 100,
		CreatedAt:     time.Now().Add(-30 * 24 * time.Hour),
	}
	features := ExtractFeatures(info)
	if features.AgeInDays < 29 || features.AgeInDays > 31 {
		t.Errorf("expected ~30 days age, got %f", features.AgeInDays)
	}
	if features.AvgDailyAccesses <= 0 {
		t.Error("expected positive daily accesses")
	}
	nilFeatures := ExtractFeatures(nil)
	if nilFeatures.DecayedScore != 0 {
		t.Error("expected 0 for nil info")
	}
}

