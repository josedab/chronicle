package chronicle

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestTenantIsolationCreateAndList(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultTenantIsolationConfig()
	engine := NewTenantIsolationEngine(db, cfg)

	tenant := IsolationTenant{
		ID:     "tenant-1",
		Name:   "Test Tenant",
		Active: true,
	}

	if err := engine.CreateTenant(tenant); err != nil {
		t.Fatalf("create tenant: %v", err)
	}

	tenants := engine.ListTenants()
	if len(tenants) != 1 {
		t.Errorf("tenant count: got %d, want 1", len(tenants))
	}

	got, err := engine.GetTenant("tenant-1")
	if err != nil {
		t.Fatalf("get tenant: %v", err)
	}
	if got.Name != "Test Tenant" {
		t.Errorf("tenant name: got %q, want %q", got.Name, "Test Tenant")
	}
	if got.MemoryBudgetMB != cfg.DefaultMemoryBudgetMB {
		t.Errorf("default memory budget: got %d, want %d", got.MemoryBudgetMB, cfg.DefaultMemoryBudgetMB)
	}
}

func TestTenantIsolationDelete(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultTenantIsolationConfig()
	engine := NewTenantIsolationEngine(db, cfg)

	engine.CreateTenant(IsolationTenant{ID: "del-1", Name: "Delete Me"})

	if err := engine.DeleteTenant("del-1"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	if _, err := engine.GetTenant("del-1"); err == nil {
		t.Error("expected error for deleted tenant")
	}

	if err := engine.DeleteTenant("nonexistent"); err == nil {
		t.Error("expected error for nonexistent tenant")
	}
}

func TestTenantIsolationDuplicate(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultTenantIsolationConfig()
	engine := NewTenantIsolationEngine(db, cfg)

	engine.CreateTenant(IsolationTenant{ID: "dup-1", Name: "Original"})
	if err := engine.CreateTenant(IsolationTenant{ID: "dup-1", Name: "Duplicate"}); err == nil {
		t.Error("expected error for duplicate tenant")
	}
}

func TestTenantIsolationMaxTenants(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultTenantIsolationConfig()
	cfg.MaxTenants = 2
	engine := NewTenantIsolationEngine(db, cfg)

	for i := 0; i < 2; i++ {
		engine.CreateTenant(IsolationTenant{
			ID:   fmt.Sprintf("tenant-%d", i),
			Name: fmt.Sprintf("Tenant %d", i),
		})
	}

	err := engine.CreateTenant(IsolationTenant{ID: "tenant-extra", Name: "Extra"})
	if err == nil {
		t.Error("expected error when exceeding max tenants")
	}
}

func TestTenantIsolationCheckQuotaAllows(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultTenantIsolationConfig()
	engine := NewTenantIsolationEngine(db, cfg)

	engine.CreateTenant(IsolationTenant{
		ID:             "quota-ok",
		Name:           "Quota OK",
		MemoryBudgetMB: 256,
		QueryQuota:     1000,
		StorageLimitMB: 10240,
		Active:         true,
	})

	ok, violation := engine.CheckQuota("quota-ok", "memory")
	if !ok {
		t.Errorf("memory check should pass, got violation: %v", violation)
	}

	ok, violation = engine.CheckQuota("quota-ok", "query")
	if !ok {
		t.Errorf("query check should pass, got violation: %v", violation)
	}
}

func TestTenantIsolationCheckQuotaRejects(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultTenantIsolationConfig()
	engine := NewTenantIsolationEngine(db, cfg)

	engine.CreateTenant(IsolationTenant{
		ID:              "quota-exceed",
		Name:            "Over Quota",
		MemoryBudgetMB:  100,
		QueryQuota:      10,
		StorageLimitMB:  500,
		CurrentMemoryMB: 100,
		QueriesUsed:     10,
		StorageUsedMB:   500,
		Active:          true,
	})

	ok, violation := engine.CheckQuota("quota-exceed", "memory")
	if ok {
		t.Error("memory check should fail")
	}
	if violation == nil || violation.Type != "memory" {
		t.Error("should return memory violation")
	}

	ok, violation = engine.CheckQuota("quota-exceed", "query")
	if ok {
		t.Error("query check should fail")
	}
	if violation == nil || violation.Type != "query" {
		t.Error("should return query violation")
	}

	ok, violation = engine.CheckQuota("quota-exceed", "storage")
	if ok {
		t.Error("storage check should fail")
	}
	if violation == nil || violation.Type != "storage" {
		t.Error("should return storage violation")
	}
}

func TestTenantIsolationRecordUsage(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultTenantIsolationConfig()
	engine := NewTenantIsolationEngine(db, cfg)

	engine.CreateTenant(IsolationTenant{
		ID:             "usage-1",
		Name:           "Usage Tenant",
		MemoryBudgetMB: 100,
		QueryQuota:     5,
		Active:         true,
	})

	if err := engine.RecordUsage("usage-1", "memory", 50); err != nil {
		t.Fatalf("record memory: %v", err)
	}
	if err := engine.RecordUsage("usage-1", "query", 3); err != nil {
		t.Fatalf("record query: %v", err)
	}

	got, _ := engine.GetTenant("usage-1")
	if got.CurrentMemoryMB != 50 {
		t.Errorf("memory usage: got %d, want 50", got.CurrentMemoryMB)
	}
	if got.QueriesUsed != 3 {
		t.Errorf("queries used: got %d, want 3", got.QueriesUsed)
	}
}

func TestTenantIsolationResetQuotas(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultTenantIsolationConfig()
	engine := NewTenantIsolationEngine(db, cfg)

	engine.CreateTenant(IsolationTenant{
		ID:         "reset-1",
		Name:       "Reset Tenant",
		QueryQuota: 100,
		Active:     true,
	})
	engine.RecordUsage("reset-1", "query", 50)

	engine.ResetQuotas()

	got, _ := engine.GetTenant("reset-1")
	if got.QueriesUsed != 0 {
		t.Errorf("queries after reset: got %d, want 0", got.QueriesUsed)
	}
}

func TestTenantIsolationStats(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultTenantIsolationConfig()
	engine := NewTenantIsolationEngine(db, cfg)

	engine.CreateTenant(IsolationTenant{ID: "s1", Name: "Active", Active: true})
	engine.CreateTenant(IsolationTenant{ID: "s2", Name: "Inactive", Active: false})

	stats := engine.Stats()
	if stats.TotalTenants != 2 {
		t.Errorf("total tenants: got %d, want 2", stats.TotalTenants)
	}
	if stats.ActiveTenants != 1 {
		t.Errorf("active tenants: got %d, want 1", stats.ActiveTenants)
	}
}

func TestTenantIsolationStartStop(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultTenantIsolationConfig()
	engine := NewTenantIsolationEngine(db, cfg)

	if err := engine.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := engine.Start(); err != nil {
		t.Fatalf("second start: %v", err)
	}
	if err := engine.Stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if err := engine.Stop(); err != nil {
		t.Fatalf("second stop: %v", err)
	}
}

func TestTenantIsolationHTTPHandlers(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultTenantIsolationConfig()
	engine := NewTenantIsolationEngine(db, cfg)

	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	req := httptest.NewRequest("GET", "/api/v1/isolation/stats", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("stats status: got %d, want %d", w.Code, http.StatusOK)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type: got %q, want %q", ct, "application/json")
	}
}

func TestTenantIsolationDefaultConfig(t *testing.T) {
	cfg := DefaultTenantIsolationConfig()
	if cfg.MaxTenants != 100 {
		t.Errorf("max tenants: got %d, want 100", cfg.MaxTenants)
	}
	if cfg.DefaultMemoryBudgetMB != 256 {
		t.Errorf("default memory budget: got %d, want 256", cfg.DefaultMemoryBudgetMB)
	}
	if cfg.DefaultQueryQuota != 1000 {
		t.Errorf("default query quota: got %d, want 1000", cfg.DefaultQueryQuota)
	}
	if cfg.DefaultStorageLimitMB != 10240 {
		t.Errorf("default storage limit: got %d, want 10240", cfg.DefaultStorageLimitMB)
	}
}
