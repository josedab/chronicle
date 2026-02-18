package chronicle

import (
	"testing"
	"time"
)

func TestUsageMeteringEngine_RecordWrite(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cp := NewSaaSControlPlane(db, DefaultSaaSControlPlaneConfig())
	cp.ProvisionTenant("t1", "Test Tenant")

	engine := NewUsageMeteringEngine(cp, DefaultUsageMeteringConfig())

	engine.RecordWrite("t1", 100)
	engine.RecordWrite("t1", 50)
	engine.RecordQuery("t1")
	engine.RecordQuery("t1")
	engine.RecordQuery("t1")

	summary := engine.GetTenantUsageSummary("t1", 24)
	if summary.TotalWrites != 150 {
		t.Errorf("expected 150 writes, got %d", summary.TotalWrites)
	}
	if summary.TotalQueries != 3 {
		t.Errorf("expected 3 queries, got %d", summary.TotalQueries)
	}
}

func TestUsageMeteringEngine_UpdateStorage(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cp := NewSaaSControlPlane(db, DefaultSaaSControlPlaneConfig())
	cp.ProvisionTenant("t1", "Test")

	engine := NewUsageMeteringEngine(cp, DefaultUsageMeteringConfig())

	engine.UpdateStorage("t1", 1024*1024, 50)

	summary := engine.GetTenantUsageSummary("t1", 24)
	if summary.TenantID != "t1" {
		t.Error("expected tenant t1")
	}
}

func TestUsageMeteringEngine_FlushPeriod(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cp := NewSaaSControlPlane(db, DefaultSaaSControlPlaneConfig())
	cp.ProvisionTenant("t1", "Test")

	engine := NewUsageMeteringEngine(cp, DefaultUsageMeteringConfig())

	engine.RecordWrite("t1", 100)
	engine.FlushPeriod()

	// After flush, current should be reset
	summary := engine.GetTenantUsageSummary("t1", 24)
	if summary.TotalWrites != 100 {
		t.Errorf("expected 100 total writes after flush, got %d", summary.TotalWrites)
	}
}

func TestUsageMeteringEngine_GetAllUsage(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cp := NewSaaSControlPlane(db, DefaultSaaSControlPlaneConfig())
	cp.ProvisionTenant("t1", "Tenant 1")
	cp.ProvisionTenant("t2", "Tenant 2")

	engine := NewUsageMeteringEngine(cp, DefaultUsageMeteringConfig())
	engine.RecordWrite("t1", 100)
	engine.RecordWrite("t2", 200)

	summaries := engine.GetAllUsage(24)
	if len(summaries) != 2 {
		t.Errorf("expected 2 summaries, got %d", len(summaries))
	}
}

func TestUsageMeteringEngine_PruneOldRecords(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cp := NewSaaSControlPlane(db, DefaultSaaSControlPlaneConfig())
	engine := NewUsageMeteringEngine(cp, UsageMeteringConfig{
		Enabled:       true,
		RetentionDays: 1,
	})

	// Add an old record directly
	engine.records["t1"] = []TenantUsageRecord{
		{
			TenantID:    "t1",
			PeriodStart: time.Now().AddDate(0, 0, -2),
			PeriodEnd:   time.Now().AddDate(0, 0, -2).Add(time.Hour),
			Writes:      100,
		},
	}

	engine.pruneOldRecords()

	if len(engine.records["t1"]) != 0 {
		t.Error("old records should be pruned")
	}
}
