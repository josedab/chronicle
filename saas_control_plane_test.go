package chronicle

import (
	"testing"
)

func TestSaaSControlPlaneProvision(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cp := NewSaaSControlPlane(db, DefaultSaaSControlPlaneConfig())

	tenant, err := cp.ProvisionTenant("t1", "Acme Corp")
	if err != nil {
		t.Fatalf("ProvisionTenant failed: %v", err)
	}
	if tenant.ID != "t1" {
		t.Errorf("expected ID t1, got %s", tenant.ID)
	}
	if tenant.Status != TenantActive {
		t.Errorf("expected active, got %s", tenant.Status)
	}
	if tenant.APIKey == "" {
		t.Error("expected non-empty API key")
	}
}

func TestSaaSControlPlaneDuplicate(t *testing.T) {
	cp := NewSaaSControlPlane(nil, DefaultSaaSControlPlaneConfig())
	cp.ProvisionTenant("dup", "Dup")

	_, err := cp.ProvisionTenant("dup", "Dup2")
	if err == nil {
		t.Error("expected error for duplicate tenant")
	}
}

func TestSaaSControlPlaneListTenants(t *testing.T) {
	cp := NewSaaSControlPlane(nil, DefaultSaaSControlPlaneConfig())
	cp.ProvisionTenant("a", "A")
	cp.ProvisionTenant("b", "B")

	tenants := cp.ListTenants()
	if len(tenants) != 2 {
		t.Errorf("expected 2 tenants, got %d", len(tenants))
	}
}

func TestSaaSControlPlaneSuspendActivate(t *testing.T) {
	cp := NewSaaSControlPlane(nil, DefaultSaaSControlPlaneConfig())
	cp.ProvisionTenant("s1", "Suspend")

	cp.SuspendTenant("s1")
	tenant, _ := cp.GetTenant("s1")
	if tenant.Status != TenantSuspended {
		t.Errorf("expected suspended, got %s", tenant.Status)
	}

	cp.ActivateTenant("s1")
	tenant, _ = cp.GetTenant("s1")
	if tenant.Status != TenantActive {
		t.Errorf("expected active, got %s", tenant.Status)
	}
}

func TestSaaSControlPlaneDelete(t *testing.T) {
	cp := NewSaaSControlPlane(nil, DefaultSaaSControlPlaneConfig())
	tenant, _ := cp.ProvisionTenant("del", "Delete")

	cp.DeleteTenant("del")
	_, found := cp.GetTenant("del")
	if found {
		t.Error("expected tenant to be deleted")
	}

	// API key should also be removed
	_, err := cp.ValidateAPIKey(tenant.APIKey)
	if err == nil {
		t.Error("expected API key validation to fail")
	}
}

func TestSaaSControlPlaneAPIKey(t *testing.T) {
	cp := NewSaaSControlPlane(nil, DefaultSaaSControlPlaneConfig())
	tenant, _ := cp.ProvisionTenant("key1", "Key Test")

	tenantID, err := cp.ValidateAPIKey(tenant.APIKey)
	if err != nil {
		t.Fatalf("ValidateAPIKey failed: %v", err)
	}
	if tenantID != "key1" {
		t.Errorf("expected tenant ID key1, got %s", tenantID)
	}

	_, err = cp.ValidateAPIKey("invalid-key")
	if err == nil {
		t.Error("expected error for invalid key")
	}
}

func TestSaaSControlPlaneQuota(t *testing.T) {
	cp := NewSaaSControlPlane(nil, DefaultSaaSControlPlaneConfig())
	cp.ProvisionTenant("q1", "Quota")

	if err := cp.CheckQuota("q1"); err != nil {
		t.Fatalf("CheckQuota failed: %v", err)
	}

	newQuota := TenantQuota{MaxStorageBytes: 100}
	cp.UpdateQuota("q1", newQuota)

	tenant, _ := cp.GetTenant("q1")
	if tenant.Quota.MaxStorageBytes != 100 {
		t.Errorf("expected quota 100, got %d", tenant.Quota.MaxStorageBytes)
	}
}

func TestSaaSControlPlaneUsage(t *testing.T) {
	cp := NewSaaSControlPlane(nil, DefaultSaaSControlPlaneConfig())
	cp.ProvisionTenant("u1", "Usage")

	cp.RecordWrite("u1", 100)
	cp.RecordQuery("u1")

	tenant, _ := cp.GetTenant("u1")
	if tenant.Usage.WritesTotal != 100 {
		t.Errorf("expected 100 writes, got %d", tenant.Usage.WritesTotal)
	}
	if tenant.Usage.QueriesTotal != 1 {
		t.Errorf("expected 1 query, got %d", tenant.Usage.QueriesTotal)
	}
}

func TestSaaSControlPlaneSuspendedWrite(t *testing.T) {
	cp := NewSaaSControlPlane(nil, DefaultSaaSControlPlaneConfig())
	cp.ProvisionTenant("sw", "Suspended Write")
	cp.SuspendTenant("sw")

	err := cp.RecordWrite("sw", 1)
	if err == nil {
		t.Error("expected error writing to suspended tenant")
	}
}

func TestSaaSControlPlaneUsageSummary(t *testing.T) {
	cp := NewSaaSControlPlane(nil, DefaultSaaSControlPlaneConfig())
	cp.ProvisionTenant("sum1", "Sum1")
	cp.ProvisionTenant("sum2", "Sum2")

	summary := cp.GetUsageSummary()
	if summary["total_tenants"].(int) != 2 {
		t.Errorf("expected 2 tenants in summary")
	}
	if summary["active_tenants"].(int) != 2 {
		t.Errorf("expected 2 active tenants")
	}
}

func TestSaaSControlPlaneMaxTenants(t *testing.T) {
	config := DefaultSaaSControlPlaneConfig()
	config.MaxTenants = 1
	cp := NewSaaSControlPlane(nil, config)

	cp.ProvisionTenant("first", "First")
	_, err := cp.ProvisionTenant("second", "Second")
	if err == nil {
		t.Error("expected max tenants error")
	}
}
