package chronicle

import (
	"context"
	"testing"
	"time"
)

func TestAccessTracker_RecordAndScore(t *testing.T) {
	cfg := DefaultAccessTrackerConfig()
	tracker := NewAccessTracker(cfg)

	tracker.RecordRead("partition-1")
	tracker.RecordRead("partition-1")
	tracker.RecordWrite("partition-1")

	score := tracker.GetAccessScore("partition-1")
	if score <= 0 {
		t.Errorf("expected positive access score, got %f", score)
	}

	stats := tracker.GetAccessStats("partition-1")
	if stats == nil {
		t.Fatal("expected non-nil stats")
	}
	if stats.ReadCount != 2 {
		t.Errorf("expected ReadCount=2, got %d", stats.ReadCount)
	}
	if stats.WriteCount != 1 {
		t.Errorf("expected WriteCount=1, got %d", stats.WriteCount)
	}

	t.Run("UnknownPartition", func(t *testing.T) {
		score := tracker.GetAccessScore("nonexistent")
		if score != 0 {
			t.Errorf("expected score=0 for unknown partition, got %f", score)
		}
		if tracker.GetAccessStats("nonexistent") != nil {
			t.Errorf("expected nil stats for unknown partition")
		}
	})
}

func TestAccessTracker_ColdPartitions(t *testing.T) {
	cfg := DefaultAccessTrackerConfig()
	tracker := NewAccessTracker(cfg)

	tracker.RecordRead("hot-partition")
	for i := 0; i < 100; i++ {
		tracker.RecordRead("hot-partition")
	}
	tracker.RecordRead("cold-partition")

	// Use a high threshold so that at least cold-partition appears.
	cold := tracker.GetColdPartitions(1000.0)
	if len(cold) == 0 {
		t.Errorf("expected at least one cold partition")
	}
}

func TestMigrationEngine_PlanMigrations(t *testing.T) {
	hotBackend := NewMemoryBackend()
	warmBackend := NewMemoryBackend()

	ctx := context.Background()
	_ = hotBackend.Write(ctx, "key1", []byte("data1"))
	_ = hotBackend.Write(ctx, "key2", []byte("data2"))

	tiers := []*StorageTierConfig{
		{Level: TierHot, Backend: hotBackend, CostPerGBMonth: 10.0},
		{Level: TierWarm, Backend: warmBackend, CostPerGBMonth: 2.0},
	}

	tracker := NewAccessTracker(DefaultAccessTrackerConfig())
	// Don't record any access so keys appear cold.

	cfg := DefaultMigrationEngineConfig()
	cfg.DryRun = true
	engine := NewMigrationEngine(tiers, tracker, cfg)

	plans := engine.PlanMigrations()
	if len(plans) == 0 {
		t.Errorf("expected migration plans for cold data")
	}
	for _, p := range plans {
		if p.SourceTier != TierHot {
			t.Errorf("expected source tier Hot, got %v", p.SourceTier)
		}
	}
}

func TestCostOptimizer_CalculateCost(t *testing.T) {
	backend := NewMemoryBackend()
	ctx := context.Background()
	_ = backend.Write(ctx, "data1", make([]byte, 1024))

	tiers := []*StorageTierConfig{
		{Level: TierHot, Backend: backend, CostPerGBMonth: 10.0},
	}

	tracker := NewAccessTracker(DefaultAccessTrackerConfig())
	optimizer := NewCostOptimizer(tiers, tracker, DefaultCostOptimizerConfig())

	report := optimizer.CalculateCurrentCost()
	if report == nil {
		t.Fatal("expected non-nil report")
	}
	if len(report.ByTier) == 0 {
		t.Errorf("expected tier cost details")
	}

	detail, ok := report.ByTier[TierHot]
	if !ok {
		t.Fatal("expected hot tier in report")
	}
	if detail.DataSizeBytes != 1024 {
		t.Errorf("expected 1024 bytes, got %d", detail.DataSizeBytes)
	}
}

func TestCostOptimizer_Recommendations(t *testing.T) {
	hotBackend := NewMemoryBackend()
	ctx := context.Background()
	_ = hotBackend.Write(ctx, "key1", make([]byte, 1024))

	tiers := []*StorageTierConfig{
		{Level: TierHot, Backend: hotBackend, CostPerGBMonth: 10.0},
	}

	tracker := NewAccessTracker(DefaultAccessTrackerConfig())
	// Record access so the partition appears in the tracker with a low score.
	tracker.RecordRead("key1")

	cfg := DefaultCostOptimizerConfig()
	cfg.BudgetPerMonth = 0.0 // zero budget so any cost triggers budget_alert
	optimizer := NewCostOptimizer(tiers, tracker, cfg)

	recs := optimizer.RecommendOptimizations()
	// Either downgrade rec (cold partitions) or budget alert should appear.
	foundRec := false
	for _, r := range recs {
		if r.Type == "downgrade" || r.Type == "budget_alert" {
			foundRec = true
		}
	}
	if !foundRec {
		t.Errorf("expected at least one recommendation of type 'downgrade' or 'budget_alert'")
	}
}

func TestAdaptiveTieredBackend_ReadWrite(t *testing.T) {
	hotBackend := NewMemoryBackend()
	warmBackend := NewMemoryBackend()

	cfg := DefaultAdaptiveTieredConfig()
	cfg.Tiers = []*StorageTierConfig{
		{Level: TierHot, Backend: hotBackend, CostPerGBMonth: 10.0, ReadLatencySLA: time.Millisecond},
		{Level: TierWarm, Backend: warmBackend, CostPerGBMonth: 2.0, ReadLatencySLA: 10 * time.Millisecond},
	}

	backend, err := NewAdaptiveTieredBackend(cfg)
	if err != nil {
		t.Fatalf("NewAdaptiveTieredBackend: %v", err)
	}
	defer backend.Close()

	ctx := context.Background()

	// Write
	if err := backend.Write(ctx, "testkey", []byte("testvalue")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Read
	data, err := backend.Read(ctx, "testkey")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(data) != "testvalue" {
		t.Errorf("expected 'testvalue', got %q", string(data))
	}

	// Exists
	exists, err := backend.Exists(ctx, "testkey")
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if !exists {
		t.Errorf("expected key to exist")
	}

	// List
	keys, err := backend.List(ctx, "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(keys) != 1 {
		t.Errorf("expected 1 key, got %d", len(keys))
	}

	// Delete
	if err := backend.Delete(ctx, "testkey"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	exists, _ = backend.Exists(ctx, "testkey")
	if exists {
		t.Errorf("expected key to be deleted")
	}
}
