package chronicle

import (
	"testing"
	"time"
)

func TestDefaultTenantGovernanceConfig(t *testing.T) {
	cfg := DefaultTenantGovernanceConfig()

	if !cfg.Enabled {
		t.Error("expected Enabled to be true")
	}
	if cfg.DefaultCPUBudget != 4000 {
		t.Errorf("DefaultCPUBudget: got %d, want 4000", cfg.DefaultCPUBudget)
	}
	if cfg.DefaultMemoryBudgetMB != 2048 {
		t.Errorf("DefaultMemoryBudgetMB: got %d, want 2048", cfg.DefaultMemoryBudgetMB)
	}
	if cfg.DefaultStorageBudgetGB != 100.0 {
		t.Errorf("DefaultStorageBudgetGB: got %f, want 100.0", cfg.DefaultStorageBudgetGB)
	}
	if cfg.DefaultQPSLimit != 1000 {
		t.Errorf("DefaultQPSLimit: got %f, want 1000", cfg.DefaultQPSLimit)
	}
	if cfg.DefaultQueryCostBudget != 100000 {
		t.Errorf("DefaultQueryCostBudget: got %f, want 100000", cfg.DefaultQueryCostBudget)
	}
	if cfg.MeteringInterval != 30*time.Second {
		t.Errorf("MeteringInterval: got %v, want 30s", cfg.MeteringInterval)
	}
	if cfg.BillingCycleHours != 720 {
		t.Errorf("BillingCycleHours: got %d, want 720", cfg.BillingCycleHours)
	}
	if cfg.EnableChargeback {
		t.Error("expected EnableChargeback to be false")
	}
	if cfg.CostPerCPUHour != 0.05 {
		t.Errorf("CostPerCPUHour: got %f, want 0.05", cfg.CostPerCPUHour)
	}
	if cfg.CostPerGBHour != 0.01 {
		t.Errorf("CostPerGBHour: got %f, want 0.01", cfg.CostPerGBHour)
	}
	if cfg.CostPerQueryUnit != 0.001 {
		t.Errorf("CostPerQueryUnit: got %f, want 0.001", cfg.CostPerQueryUnit)
	}
	if cfg.ThrottleGracePeriod != 10*time.Second {
		t.Errorf("ThrottleGracePeriod: got %v, want 10s", cfg.ThrottleGracePeriod)
	}
}

func TestTenantThrottler_Admit(t *testing.T) {
	throttler := NewTenantThrottler(5 * time.Second)

	budget := ResourceBudget{
		TenantID:        "t1",
		QPSLimit:        10,
		WritesPerSecond: 10,
		QueryCostBudget: 36000,
		Priority:        5,
	}
	throttler.ensureTenant("t1", budget)

	// Admit within budget
	admitted, reason := throttler.Admit("t1", "qps", 1)
	if !admitted {
		t.Errorf("expected admit within budget, got denied: %s", reason)
	}

	// Unknown tenant should be denied
	admitted, reason = throttler.Admit("unknown", "qps", 1)
	if admitted {
		t.Error("expected deny for unknown tenant")
	}
	if reason == "" {
		t.Error("expected non-empty reason for unknown tenant")
	}

	// Unknown resource type should be denied
	admitted, reason = throttler.Admit("t1", "disk", 1)
	if admitted {
		t.Error("expected deny for unknown resource type")
	}
	if reason == "" {
		t.Error("expected non-empty reason for unknown resource type")
	}

	// Exhaust the QPS bucket (capacity = QPSLimit*2 = 20)
	for i := 0; i < 25; i++ {
		throttler.Admit("t1", "qps", 1)
	}

	// Write admission should still work (separate bucket)
	admitted, _ = throttler.Admit("t1", "write", 1)
	if !admitted {
		t.Error("expected write admit to succeed on separate bucket")
	}
}

func TestTenantThrottler_GracePeriod(t *testing.T) {
	gracePeriod := 100 * time.Millisecond
	throttler := NewTenantThrottler(gracePeriod)

	budget := ResourceBudget{
		TenantID:        "t1",
		QPSLimit:        1,
		WritesPerSecond: 1,
		QueryCostBudget: 3600,
		Priority:        5,
	}
	throttler.ensureTenant("t1", budget)

	// Exhaust tokens (capacity = 1*2 = 2)
	throttler.Admit("t1", "qps", 1)
	throttler.Admit("t1", "qps", 1)

	// Next admit should start grace period
	admitted, reason := throttler.Admit("t1", "qps", 1)
	if !admitted {
		t.Errorf("expected grace period to allow admission, got denied: %s", reason)
	}

	// During grace period, still admitted
	admitted, _ = throttler.Admit("t1", "qps", 1)
	if !admitted {
		t.Error("expected admission during grace period")
	}

	// Wait for grace period to expire
	time.Sleep(gracePeriod + 20*time.Millisecond)

	// After grace period, should be denied
	admitted, reason = throttler.Admit("t1", "qps", 1)
	if admitted {
		t.Error("expected deny after grace period expired")
	}
	if reason == "" {
		t.Error("expected non-empty reason after grace expiry")
	}
}

func TestQueryCostAccounter(t *testing.T) {
	accounter := NewQueryCostAccounter(720)

	// Initially no usage
	cost, count := accounter.GetUsage("t1")
	if cost != 0 || count != 0 {
		t.Errorf("initial usage: got cost=%f count=%d, want 0/0", cost, count)
	}

	// Account some queries
	accounter.AccountQuery("t1", 10.5)
	accounter.AccountQuery("t1", 20.0)

	cost, count = accounter.GetUsage("t1")
	if cost != 30.5 {
		t.Errorf("query cost: got %f, want 30.5", cost)
	}
	if count != 2 {
		t.Errorf("query count: got %d, want 2", count)
	}

	// Account writes
	accounter.AccountWrite("t1", 100, 6400)

	full := accounter.GetFullUsage("t1")
	if full.writeCount != 100 {
		t.Errorf("write count: got %d, want 100", full.writeCount)
	}
	if full.writeBytes != 6400 {
		t.Errorf("write bytes: got %d, want 6400", full.writeBytes)
	}

	// Account reads
	accounter.AccountRead("t1", 1024)
	full = accounter.GetFullUsage("t1")
	if full.readBytes != 1024 {
		t.Errorf("read bytes: got %d, want 1024", full.readBytes)
	}

	// Reset billing cycle
	accounter.ResetBillingCycle("t1")
	cost, count = accounter.GetUsage("t1")
	if cost != 0 || count != 0 {
		t.Errorf("after reset: got cost=%f count=%d, want 0/0", cost, count)
	}

	// GetFullUsage for unknown tenant returns zero state
	unknown := accounter.GetFullUsage("nonexistent")
	if unknown.totalQueryCost != 0 || unknown.queryCount != 0 {
		t.Error("expected zero state for unknown tenant")
	}
}

func TestNewTenantGovernanceEngine(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultTenantGovernanceConfig()
	engine := NewTenantGovernanceEngine(db, cfg)

	if engine == nil {
		t.Fatal("expected non-nil engine")
	}

	status := engine.Status()
	if status.Running {
		t.Error("expected engine not running initially")
	}
	if status.TenantCount != 0 {
		t.Errorf("expected 0 tenants, got %d", status.TenantCount)
	}
}

func TestTenantGovernanceEngine_StartStop(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultTenantGovernanceConfig()
	cfg.MeteringInterval = 50 * time.Millisecond
	engine := NewTenantGovernanceEngine(db, cfg)

	if err := engine.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	status := engine.Status()
	if !status.Running {
		t.Error("expected engine running after Start")
	}

	// Start again should be a no-op
	if err := engine.Start(); err != nil {
		t.Fatalf("second start: %v", err)
	}

	if err := engine.Stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}

	status = engine.Status()
	if status.Running {
		t.Error("expected engine not running after Stop")
	}

	// Stop again should be a no-op
	if err := engine.Stop(); err != nil {
		t.Fatalf("second stop: %v", err)
	}
}

func TestTenantGovernanceEngine_BudgetCRUD(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultTenantGovernanceConfig()
	engine := NewTenantGovernanceEngine(db, cfg)

	budget := ResourceBudget{
		CPUMillicores:   2000,
		MemoryMB:        1024,
		StorageGB:       50.0,
		QPSLimit:        500,
		QueryCostBudget: 50000,
		WritesPerSecond: 200,
		Priority:        5,
	}

	// Create
	if err := engine.CreateBudget("t1", budget); err != nil {
		t.Fatalf("create budget: %v", err)
	}

	// Duplicate create should error
	if err := engine.CreateBudget("t1", budget); err == nil {
		t.Error("expected error on duplicate budget creation")
	}

	// Get
	got, err := engine.GetBudget("t1")
	if err != nil {
		t.Fatalf("get budget: %v", err)
	}
	if got.TenantID != "t1" {
		t.Errorf("tenant ID: got %q, want %q", got.TenantID, "t1")
	}
	if got.CPUMillicores != 2000 {
		t.Errorf("CPU: got %d, want 2000", got.CPUMillicores)
	}
	if got.Priority != 5 {
		t.Errorf("priority: got %d, want 5", got.Priority)
	}

	// Get non-existent
	_, err = engine.GetBudget("nonexistent")
	if err == nil {
		t.Error("expected error for non-existent tenant")
	}

	// Update
	budget.CPUMillicores = 4000
	budget.Priority = 8
	if err := engine.UpdateBudget("t1", budget); err != nil {
		t.Fatalf("update budget: %v", err)
	}

	got, _ = engine.GetBudget("t1")
	if got.CPUMillicores != 4000 {
		t.Errorf("updated CPU: got %d, want 4000", got.CPUMillicores)
	}
	if got.Priority != 8 {
		t.Errorf("updated priority: got %d, want 8", got.Priority)
	}

	// Update non-existent should error
	if err := engine.UpdateBudget("nonexistent", budget); err == nil {
		t.Error("expected error updating non-existent tenant")
	}

	// Priority clamping
	budget.Priority = 0
	if err := engine.CreateBudget("t-low", budget); err != nil {
		t.Fatalf("create low priority: %v", err)
	}
	got, _ = engine.GetBudget("t-low")
	if got.Priority != 1 {
		t.Errorf("clamped low priority: got %d, want 1", got.Priority)
	}

	budget.Priority = 99
	if err := engine.CreateBudget("t-high", budget); err != nil {
		t.Fatalf("create high priority: %v", err)
	}
	got, _ = engine.GetBudget("t-high")
	if got.Priority != 10 {
		t.Errorf("clamped high priority: got %d, want 10", got.Priority)
	}

	// Status should reflect tenant count
	status := engine.Status()
	if status.TenantCount != 3 {
		t.Errorf("tenant count: got %d, want 3", status.TenantCount)
	}
}

func TestTenantGovernanceEngine_AdmitQuery(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultTenantGovernanceConfig()
	engine := NewTenantGovernanceEngine(db, cfg)

	// No budget configured - should deny
	admitted, reason := engine.AdmitQuery("t1", 1.0)
	if admitted {
		t.Error("expected deny when no budget configured")
	}
	if reason == "" {
		t.Error("expected non-empty reason")
	}

	budget := ResourceBudget{
		QPSLimit:        100,
		QueryCostBudget: 100,
		WritesPerSecond: 100,
		Priority:        5,
	}
	engine.CreateBudget("t1", budget)

	// Normal query should be admitted
	admitted, _ = engine.AdmitQuery("t1", 1.0)
	if !admitted {
		t.Error("expected query to be admitted")
	}

	// Query that would exceed cost budget should be denied
	admitted, reason = engine.AdmitQuery("t1", 200.0)
	if admitted {
		t.Error("expected deny for query exceeding cost budget")
	}
	if reason == "" {
		t.Error("expected non-empty reason for cost budget exceeded")
	}
}

func TestTenantGovernanceEngine_AdmitWrite(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultTenantGovernanceConfig()
	engine := NewTenantGovernanceEngine(db, cfg)

	// No budget configured - should deny
	admitted, reason := engine.AdmitWrite("t1", 10)
	if admitted {
		t.Error("expected deny when no budget configured")
	}
	if reason == "" {
		t.Error("expected non-empty reason")
	}

	budget := ResourceBudget{
		QPSLimit:        100,
		QueryCostBudget: 50000,
		WritesPerSecond: 100,
		Priority:        5,
	}
	engine.CreateBudget("t1", budget)

	// Normal write should be admitted
	admitted, _ = engine.AdmitWrite("t1", 5)
	if !admitted {
		t.Error("expected write to be admitted")
	}
}

func TestTenantGovernanceEngine_GenerateChargeback(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultTenantGovernanceConfig()
	cfg.EnableChargeback = true
	engine := NewTenantGovernanceEngine(db, cfg)

	budget := ResourceBudget{
		QPSLimit:        1000,
		QueryCostBudget: 100000,
		WritesPerSecond: 1000,
		Priority:        5,
	}
	engine.CreateBudget("t1", budget)

	now := time.Now()
	start := now.Add(-1 * time.Hour)

	// Chargeback report with no metering records
	report, err := engine.GenerateChargebackReport("t1", start, now)
	if err != nil {
		t.Fatalf("generate chargeback: %v", err)
	}
	if report.TenantID != "t1" {
		t.Errorf("tenant ID: got %q, want %q", report.TenantID, "t1")
	}
	if report.Currency != "USD" {
		t.Errorf("currency: got %q, want %q", report.Currency, "USD")
	}
	if len(report.LineItems) != 5 {
		t.Errorf("line items: got %d, want 5", len(report.LineItems))
	}
	if report.TotalCost != 0 {
		t.Errorf("total cost with no data: got %f, want 0", report.TotalCost)
	}

	// Showback report should use "SHOWBACK" currency
	showback, err := engine.GenerateShowbackReport("t1", start, now)
	if err != nil {
		t.Fatalf("generate showback: %v", err)
	}
	if showback.Currency != "SHOWBACK" {
		t.Errorf("showback currency: got %q, want %q", showback.Currency, "SHOWBACK")
	}

	// Verify line item descriptions
	descriptions := make(map[string]bool)
	for _, li := range report.LineItems {
		descriptions[li.Description] = true
	}
	expected := []string{"CPU Usage", "Memory Usage", "Storage Usage", "Query Cost", "Write Operations"}
	for _, desc := range expected {
		if !descriptions[desc] {
			t.Errorf("missing line item: %s", desc)
		}
	}
}

func TestTenantGovernanceEngine_GetUsage(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultTenantGovernanceConfig()
	engine := NewTenantGovernanceEngine(db, cfg)

	// No budget - should error
	_, err := engine.GetUsage("t1")
	if err == nil {
		t.Error("expected error for tenant with no budget")
	}

	budget := ResourceBudget{
		QPSLimit:        100,
		QueryCostBudget: 50000,
		WritesPerSecond: 100,
		Priority:        5,
	}
	engine.CreateBudget("t1", budget)

	// After budget creation, usage should be initialized
	usage, err := engine.GetUsage("t1")
	if err != nil {
		t.Fatalf("get usage: %v", err)
	}
	if usage.TenantID != "t1" {
		t.Errorf("tenant ID: got %q, want %q", usage.TenantID, "t1")
	}
	if usage.CPUUsedMillicores != 0 {
		t.Errorf("initial CPU usage: got %d, want 0", usage.CPUUsedMillicores)
	}

	// Update usage and verify
	engine.UpdateUsage("t1", GovernanceResourceUsage{
		CPUUsedMillicores: 1500,
		MemoryUsedMB:      512,
		StorageUsedGB:     25.0,
		CurrentQPS:        50,
	})

	usage, _ = engine.GetUsage("t1")
	if usage.CPUUsedMillicores != 1500 {
		t.Errorf("updated CPU: got %d, want 1500", usage.CPUUsedMillicores)
	}
	if usage.MemoryUsedMB != 512 {
		t.Errorf("updated memory: got %d, want 512", usage.MemoryUsedMB)
	}
	if usage.StorageUsedGB != 25.0 {
		t.Errorf("updated storage: got %f, want 25.0", usage.StorageUsedGB)
	}
}

func TestTenantGovernanceEngine_MeteringRecords(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultTenantGovernanceConfig()
	cfg.MeteringInterval = 50 * time.Millisecond
	engine := NewTenantGovernanceEngine(db, cfg)

	budget := ResourceBudget{
		QPSLimit:        1000,
		QueryCostBudget: 100000,
		WritesPerSecond: 1000,
		Priority:        5,
	}
	engine.CreateBudget("t1", budget)

	// Update usage so metering has data to collect
	engine.UpdateUsage("t1", GovernanceResourceUsage{
		CPUUsedMillicores: 2000,
		MemoryUsedMB:      1024,
		StorageUsedGB:     50.0,
	})

	// Account some operations
	engine.costAccounter.AccountQuery("t1", 10.0)
	engine.costAccounter.AccountWrite("t1", 5, 320)

	// Start engine to trigger metering collection
	engine.Start()

	// Wait for at least one metering cycle
	time.Sleep(150 * time.Millisecond)

	engine.Stop()

	now := time.Now()
	start := now.Add(-1 * time.Hour)
	records := engine.GetMeteringRecords("t1", start, now)

	if len(records) == 0 {
		t.Fatal("expected at least one metering record")
	}

	rec := records[0]
	if rec.TenantID != "t1" {
		t.Errorf("record tenant ID: got %q, want %q", rec.TenantID, "t1")
	}
	if rec.CPUSeconds <= 0 {
		t.Error("expected positive CPU seconds from metering")
	}

	// Records outside time range should be empty
	future := now.Add(1 * time.Hour)
	empty := engine.GetMeteringRecords("t1", future, future.Add(time.Hour))
	if len(empty) != 0 {
		t.Errorf("expected 0 records for future range, got %d", len(empty))
	}

	// Non-existent tenant should return empty
	empty = engine.GetMeteringRecords("nonexistent", start, now)
	if len(empty) != 0 {
		t.Errorf("expected 0 records for nonexistent tenant, got %d", len(empty))
	}
}
