package chronicle

import (
	"os"
	"testing"
	"time"
)

func TestTenantManager_GetTenant(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	tm := NewTenantManager(db, TenantConfig{
		Enabled:       true,
		DefaultTenant: "default",
	})

	// Get default tenant
	tenant, err := tm.GetTenant("")
	if err != nil {
		t.Fatalf("GetTenant failed: %v", err)
	}
	if tenant.Name() != "default" {
		t.Errorf("expected default tenant, got %s", tenant.Name())
	}

	// Get named tenant
	tenant, err = tm.GetTenant("acme")
	if err != nil {
		t.Fatalf("GetTenant failed: %v", err)
	}
	if tenant.Name() != "acme" {
		t.Errorf("expected acme tenant, got %s", tenant.Name())
	}
}

func TestTenantManager_AllowedTenants(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	tm := NewTenantManager(db, TenantConfig{
		Enabled:        true,
		AllowedTenants: []string{"acme", "globex"},
	})

	// Allowed tenant
	_, err := tm.GetTenant("acme")
	if err != nil {
		t.Errorf("expected acme to be allowed: %v", err)
	}

	// Not allowed tenant
	_, err = tm.GetTenant("unknown")
	if err == nil {
		t.Error("expected error for unknown tenant")
	}

	// Add new tenant
	tm.AddAllowedTenant("newco")
	_, err = tm.GetTenant("newco")
	if err != nil {
		t.Errorf("expected newco to be allowed after adding: %v", err)
	}

	// Remove tenant
	tm.RemoveAllowedTenant("acme")
	_, err = tm.GetTenant("acme")
	if err == nil {
		t.Error("expected error after removing acme")
	}
}

func TestTenant_WriteAndQuery(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	tm := NewTenantManager(db, TenantConfig{Enabled: true})

	// Write as tenant A
	tenantA, _ := tm.GetTenant("tenant-a")
	now := time.Now().UnixNano()

	// Use WriteBatch which flushes directly
	err := tenantA.WriteBatch([]Point{{
		Metric:    "cpu",
		Value:     50.0,
		Timestamp: now,
		Tags:      map[string]string{"host": "host1"},
	}})
	if err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	// Write as tenant B
	tenantB, _ := tm.GetTenant("tenant-b")
	err = tenantB.WriteBatch([]Point{{
		Metric:    "cpu",
		Value:     75.0,
		Timestamp: now,
		Tags:      map[string]string{"host": "host1"},
	}})
	if err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	// Query as tenant A - should only see tenant A's data
	// Note: no time range means search all partitions
	result, err := tenantA.Execute(&Query{
		Metric: "cpu",
	})
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if len(result.Points) != 1 {
		t.Errorf("expected 1 point for tenant A, got %d", len(result.Points))
	}
	if len(result.Points) > 0 && result.Points[0].Value != 50.0 {
		t.Errorf("expected value 50.0, got %v", result.Points[0].Value)
	}

	// Query as tenant B - should only see tenant B's data
	result, err = tenantB.Execute(&Query{
		Metric: "cpu",
	})
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if len(result.Points) != 1 {
		t.Errorf("expected 1 point for tenant B, got %d", len(result.Points))
	}
	if len(result.Points) > 0 && result.Points[0].Value != 75.0 {
		t.Errorf("expected value 75.0, got %v", result.Points[0].Value)
	}
}

func TestTenant_WriteBatch(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	tm := NewTenantManager(db, TenantConfig{Enabled: true})
	tenant, _ := tm.GetTenant("batch-tenant")

	now := time.Now().UnixNano()
	points := []Point{
		{Metric: "mem", Value: 100, Timestamp: now},
		{Metric: "mem", Value: 200, Timestamp: now + 1000},
		{Metric: "mem", Value: 300, Timestamp: now + 2000},
	}

	if err := tenant.WriteBatch(points); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	result, err := tenant.Execute(&Query{
		Metric: "mem",
	})
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if len(result.Points) != 3 {
		t.Errorf("expected 3 points, got %d", len(result.Points))
	}
}

func TestTenantFromTags(t *testing.T) {
	tags := map[string]string{
		"host":       "server1",
		TenantTagKey: "acme",
	}

	tenant := TenantFromTags(tags)
	if tenant != "acme" {
		t.Errorf("expected acme, got %s", tenant)
	}

	stripped := StripTenantTag(tags)
	if _, ok := stripped[TenantTagKey]; ok {
		t.Error("expected tenant tag to be stripped")
	}
	if stripped["host"] != "server1" {
		t.Error("expected host tag to remain")
	}
}

func TestTenantMetricName(t *testing.T) {
	name := TenantMetricName("acme", "cpu.usage")
	if name != "acme/cpu.usage" {
		t.Errorf("unexpected name: %s", name)
	}

	tenant, metric := ParseTenantMetric(name)
	if tenant != "acme" {
		t.Errorf("expected tenant acme, got %s", tenant)
	}
	if metric != "cpu.usage" {
		t.Errorf("expected metric cpu.usage, got %s", metric)
	}

	// No separator
	_, metric = ParseTenantMetric("simple")
	if metric != "simple" {
		t.Errorf("expected simple, got %s", metric)
	}
}

func TestTenantManager_ListTenants(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	tm := NewTenantManager(db, TenantConfig{
		Enabled:        true,
		AllowedTenants: []string{"a", "b", "c"},
	})

	tenants := tm.ListTenants()
	if len(tenants) != 3 {
		t.Errorf("expected 3 tenants, got %d", len(tenants))
	}
}

func openTestDB(t *testing.T) (*DB, func()) {
	t.Helper()
	path := "test_tenant_" + time.Now().Format("20060102150405.000000000") + ".db"
	db, err := Open(path, Config{
		BufferSize:        100,
		PartitionDuration: time.Hour,
	})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	return db, func() {
		_ = db.Close()
		_ = os.Remove(path)
		_ = os.Remove(path + ".wal")
	}
}
