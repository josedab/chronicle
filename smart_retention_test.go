package chronicle

import (
	"testing"
	"time"
)

func TestSmartRetentionConfig(t *testing.T) {
	cfg := DefaultSmartRetentionConfig()
	if !cfg.Enabled {
		t.Error("expected enabled")
	}
	if cfg.HotTierMaxAge != 6*time.Hour {
		t.Errorf("expected 6h hot tier, got %v", cfg.HotTierMaxAge)
	}
	if cfg.HighValueThreshold != 0.7 {
		t.Errorf("expected 0.7 threshold, got %f", cfg.HighValueThreshold)
	}
}

func TestSmartRetentionRecordAccess(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	sre := NewSmartRetentionEngine(db, DefaultSmartRetentionConfig())

	sre.RecordAccess("cpu_usage")
	sre.RecordAccess("cpu_usage")
	sre.RecordAccess("cpu_usage")

	profile, exists := sre.GetProfile("cpu_usage")
	if !exists {
		t.Fatal("expected profile to exist")
	}
	if profile.AccessCount != 3 {
		t.Errorf("expected 3 accesses, got %d", profile.AccessCount)
	}
	if profile.Metric != "cpu_usage" {
		t.Errorf("expected metric cpu_usage, got %s", profile.Metric)
	}
	if profile.CurrentTier != TierHot {
		t.Errorf("expected hot tier, got %v", profile.CurrentTier)
	}
}

func TestSmartRetentionValueScore(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	sre := NewSmartRetentionEngine(db, DefaultSmartRetentionConfig())

	// High-frequency access should have higher value score
	for i := 0; i < 50; i++ {
		sre.RecordAccess("hot_metric")
	}
	sre.RecordAccess("cold_metric")

	hotProfile, _ := sre.GetProfile("hot_metric")
	coldProfile, _ := sre.GetProfile("cold_metric")

	if hotProfile.ValueScore <= coldProfile.ValueScore {
		t.Errorf("hot metric score (%.4f) should be > cold metric score (%.4f)",
			hotProfile.ValueScore, coldProfile.ValueScore)
	}
}

func TestSmartRetentionEvaluate(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	sre := NewSmartRetentionEngine(db, DefaultSmartRetentionConfig())

	for i := 0; i < 20; i++ {
		sre.RecordAccess("active_metric")
	}
	sre.RecordAccess("stale_metric")

	recs := sre.Evaluate()
	// Both are hot (recently created) so there may not be migrations
	if recs == nil {
		recs = []RetentionRecommendation{}
	}

	stats := sre.Stats()
	if stats.TrackedSeries != 2 {
		t.Errorf("expected 2 tracked series, got %d", stats.TrackedSeries)
	}
}

func TestSmartRetentionApplyRecommendations(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	sre := NewSmartRetentionEngine(db, DefaultSmartRetentionConfig())
	sre.RecordAccess("test_metric")

	recs := []RetentionRecommendation{
		{
			Metric:          "test_metric",
			CurrentTier:     TierHot,
			RecommendedTier: TierWarm,
			Reason:          "test migration",
			Confidence:      0.9,
		},
	}

	applied := sre.ApplyRecommendations(recs)
	if applied != 1 {
		t.Errorf("expected 1 applied, got %d", applied)
	}

	profile, _ := sre.GetProfile("test_metric")
	if profile.CurrentTier != TierWarm {
		t.Errorf("expected warm tier after migration, got %v", profile.CurrentTier)
	}

	stats := sre.Stats()
	if stats.MigrationsExecuted != 1 {
		t.Errorf("expected 1 migration, got %d", stats.MigrationsExecuted)
	}
	if stats.WarmSeries != 1 {
		t.Errorf("expected 1 warm series, got %d", stats.WarmSeries)
	}
}

func TestSmartRetentionListProfiles(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	sre := NewSmartRetentionEngine(db, DefaultSmartRetentionConfig())
	sre.RecordAccess("metric_a")
	sre.RecordAccess("metric_b")
	sre.RecordAccess("metric_c")

	profiles := sre.ListProfiles()
	if len(profiles) != 3 {
		t.Errorf("expected 3 profiles, got %d", len(profiles))
	}
}

func TestSmartRetentionStats(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	sre := NewSmartRetentionEngine(db, DefaultSmartRetentionConfig())
	stats := sre.Stats()
	if stats.TrackedSeries != 0 {
		t.Errorf("expected 0 tracked series, got %d", stats.TrackedSeries)
	}
	if stats.StorageSavingsEst != 0 {
		t.Errorf("expected 0 savings, got %f", stats.StorageSavingsEst)
	}
}
