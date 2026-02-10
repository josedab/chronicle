package chronicle

import (
	"testing"
	"time"
)

func TestTierPolicyEngine_DefaultPolicy(t *testing.T) {
	pe := NewTierPolicyEngine()
	policy := pe.GetPolicy("cpu")

	if policy.HotDuration != 1*time.Hour {
		t.Errorf("hot duration = %v, want 1h", policy.HotDuration)
	}
	if policy.WarmDuration != 24*time.Hour {
		t.Errorf("warm duration = %v, want 24h", policy.WarmDuration)
	}
	if !policy.ColdEnabled {
		t.Error("cold should be enabled by default")
	}
}

func TestTierPolicyEngine_SetPolicy(t *testing.T) {
	pe := NewTierPolicyEngine()
	err := pe.SetPolicy(MetricTierPolicy{
		Metric:       "critical_metric",
		HotDuration:  4 * time.Hour,
		WarmDuration: 72 * time.Hour,
		ColdEnabled:  false,
		Priority:     10,
	})
	if err != nil {
		t.Fatal(err)
	}

	policy := pe.GetPolicy("critical_metric")
	if policy.HotDuration != 4*time.Hour {
		t.Errorf("hot = %v, want 4h", policy.HotDuration)
	}
	if policy.ColdEnabled {
		t.Error("cold should be disabled")
	}
}

func TestTierPolicyEngine_SetPolicyValidation(t *testing.T) {
	pe := NewTierPolicyEngine()
	err := pe.SetPolicy(MetricTierPolicy{})
	if err == nil {
		t.Error("expected error for empty metric")
	}
}

func TestTierPolicyEngine_RemovePolicy(t *testing.T) {
	pe := NewTierPolicyEngine()
	pe.SetPolicy(MetricTierPolicy{Metric: "x", HotDuration: 10 * time.Hour})
	pe.RemovePolicy("x")

	policy := pe.GetPolicy("x")
	if policy.HotDuration != 1*time.Hour {
		t.Error("should revert to default after removal")
	}
}

func TestTierPolicyEngine_ListPolicies(t *testing.T) {
	pe := NewTierPolicyEngine()
	pe.SetPolicy(MetricTierPolicy{Metric: "b", HotDuration: time.Hour})
	pe.SetPolicy(MetricTierPolicy{Metric: "a", HotDuration: time.Hour})

	policies := pe.ListPolicies()
	if len(policies) != 2 {
		t.Fatalf("policies = %d, want 2", len(policies))
	}
	if policies[0].Metric != "a" {
		t.Error("expected sorted by metric name")
	}
}

func TestTierPolicyEngine_RecommendTier(t *testing.T) {
	pe := NewTierPolicyEngine()
	pe.SetPolicy(MetricTierPolicy{
		Metric:       "cpu",
		HotDuration:  1 * time.Hour,
		WarmDuration: 24 * time.Hour,
		ColdEnabled:  true,
	})

	tests := []struct {
		age      time.Duration
		expected StorageTierLevel
	}{
		{30 * time.Minute, TierHot},
		{2 * time.Hour, TierWarm},
		{48 * time.Hour, TierCold},
	}

	for _, tt := range tests {
		tier := pe.RecommendTier("cpu", tt.age)
		if tier != tt.expected {
			t.Errorf("age=%v: got %s, want %s", tt.age, tier, tt.expected)
		}
	}
}

func TestTierPolicyEngine_RecommendTierNoCold(t *testing.T) {
	pe := NewTierPolicyEngine()
	pe.SetPolicy(MetricTierPolicy{
		Metric:       "temp",
		HotDuration:  time.Hour,
		WarmDuration: time.Hour,
		ColdEnabled:  false,
	})

	tier := pe.RecommendTier("temp", 100*time.Hour)
	if tier != TierWarm {
		t.Errorf("got %s, want warm (cold disabled)", tier)
	}
}

func TestTierPolicyEngine_EvaluatePlacements(t *testing.T) {
	pe := NewTierPolicyEngine()
	pe.SetPolicy(MetricTierPolicy{
		Metric:       "cpu",
		HotDuration:  1 * time.Hour,
		WarmDuration: 24 * time.Hour,
		ColdEnabled:  true,
	})

	partitions := []TierPartitionInfo{
		{PartitionID: 1, Metric: "cpu", CurrentTier: TierHot, CreatedAt: time.Now().Add(-2 * time.Hour)},
		{PartitionID: 2, Metric: "cpu", CurrentTier: TierHot, CreatedAt: time.Now().Add(-30 * time.Minute)},
		{PartitionID: 3, Metric: "cpu", CurrentTier: TierWarm, CreatedAt: time.Now().Add(-48 * time.Hour)},
	}

	recs := pe.EvaluatePlacements(partitions)

	// Partition 1 should be demoted (2h old, hot retention is 1h)
	// Partition 2 should stay (30m old)
	// Partition 3 should be demoted to cold (48h old, warm retention is 24h)
	if len(recs) < 2 {
		t.Errorf("recs = %d, want >= 2", len(recs))
	}

	hasPartition1 := false
	hasPartition3 := false
	for _, r := range recs {
		if r.PartitionID == 1 && r.TargetTier == TierWarm {
			hasPartition1 = true
		}
		if r.PartitionID == 3 && r.TargetTier == TierCold {
			hasPartition3 = true
		}
	}
	if !hasPartition1 {
		t.Error("expected partition 1 to be recommended for warm")
	}
	if !hasPartition3 {
		t.Error("expected partition 3 to be recommended for cold")
	}
}

func TestTierQueryRouter_Stats(t *testing.T) {
	router := NewTierQueryRouter(nil)
	stats := router.Stats()
	if stats.TotalQueries != 0 {
		t.Error("expected 0 queries initially")
	}
}

func TestTierPolicyEngine_SetDefault(t *testing.T) {
	pe := NewTierPolicyEngine()
	pe.SetDefault(MetricTierPolicy{
		HotDuration:  2 * time.Hour,
		WarmDuration: 48 * time.Hour,
	})

	policy := pe.GetPolicy("any_metric")
	if policy.HotDuration != 2*time.Hour {
		t.Errorf("hot = %v, want 2h", policy.HotDuration)
	}
}

func TestStorageTierLevel_String(t *testing.T) {
	if TierHot.String() != "hot" {
		t.Error("unexpected string for hot")
	}
	if TierCold.String() != "cold" {
		t.Error("unexpected string for cold")
	}
}
