package chronicle

import (
	"testing"
	"time"
)

func TestSSOManager_ConfigureAndSession(t *testing.T) {
	m := NewSSOManager()

	config := SSOConfig{
		Provider: SSOProviderOIDC,
		Issuer:   "https://accounts.example.com",
		ClientID: "client-123",
	}
	if err := m.ConfigureSSO("tenant-1", config); err != nil {
		t.Fatalf("configure: %v", err)
	}

	c, ok := m.GetSSOConfig("tenant-1")
	if !ok {
		t.Fatal("expected config")
	}
	if c.Provider != SSOProviderOIDC {
		t.Fatalf("expected OIDC, got %s", c.Provider)
	}

	session, err := m.CreateSession("tenant-1", "user-1", "user@example.com", "User", []string{"admin"}, SSOProviderOIDC)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}

	validated, err := m.ValidateSession(session.SessionID)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if validated.Email != "user@example.com" {
		t.Fatalf("email mismatch")
	}

	m.RevokeSession(session.SessionID)
	if _, err := m.ValidateSession(session.SessionID); err == nil {
		t.Fatal("expected error after revoke")
	}
}

func TestRBACManager_Permissions(t *testing.T) {
	rm := NewRBACManager()

	if err := rm.BindUser("t1", "u1", "admin"); err != nil {
		t.Fatalf("bind: %v", err)
	}
	if err := rm.BindUser("t1", "u2", "viewer"); err != nil {
		t.Fatalf("bind: %v", err)
	}

	if !rm.HasPermission("t1", "u1", "write") {
		t.Fatal("admin should have write")
	}
	if !rm.HasPermission("t1", "u1", "admin") {
		t.Fatal("admin should have admin")
	}
	if rm.HasPermission("t1", "u2", "write") {
		t.Fatal("viewer should not have write")
	}
	if !rm.HasPermission("t1", "u2", "read") {
		t.Fatal("viewer should have read")
	}

	roles := rm.GetUserRoles("t1", "u1")
	if len(roles) != 1 || roles[0] != "admin" {
		t.Fatalf("expected [admin], got %v", roles)
	}

	// Unknown role
	if err := rm.BindUser("t1", "u3", "nonexistent"); err == nil {
		t.Fatal("expected error for unknown role")
	}
}

func TestBillingManager_InvoiceCalculation(t *testing.T) {
	bm := NewBillingManager()

	account, err := bm.CreateAccount("t1", "starter")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if account.Status != "active" {
		t.Fatalf("expected active, got %s", account.Status)
	}

	// Record usage within quota
	bm.RecordUsage("t1", 500_000, 50_000)
	invoice, err := bm.CalculateInvoice("t1")
	if err != nil {
		t.Fatalf("invoice: %v", err)
	}
	if invoice != 29 { // base price only
		t.Fatalf("expected $29, got $%.2f", invoice)
	}

	// Record overage
	bm.RecordUsage("t1", 1_000_000, 200_000)
	invoice, err = bm.CalculateInvoice("t1")
	if err != nil {
		t.Fatalf("invoice: %v", err)
	}
	if invoice <= 29 {
		t.Fatalf("expected more than $29 with overage, got $%.2f", invoice)
	}
}

func TestBillingManager_Plans(t *testing.T) {
	bm := NewBillingManager()
	plans := bm.ListPlans()
	if len(plans) < 4 {
		t.Fatalf("expected at least 4 plans, got %d", len(plans))
	}
}

func TestAPIKeyManager_Lifecycle(t *testing.T) {
	m := NewAPIKeyManager()

	key, err := m.CreateKey("t1", "my-key", []string{"read", "write"}, 30*24*time.Hour)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if key.Name != "my-key" {
		t.Fatalf("expected 'my-key', got %q", key.Name)
	}

	// Validate
	validated, err := m.ValidateKey(key.Key)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if validated.TenantID != "t1" {
		t.Fatalf("expected t1, got %s", validated.TenantID)
	}

	// List (keys should be masked)
	keys := m.ListKeys("t1")
	if len(keys) != 1 {
		t.Fatalf("expected 1 key, got %d", len(keys))
	}
	if len(keys[0].Key) > 12 {
		t.Fatal("key should be masked")
	}

	// Revoke
	if err := m.RevokeKey(key.Prefix, "t1"); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if _, err := m.ValidateKey(key.Key); err == nil {
		t.Fatal("expected error after revoke")
	}
}

func TestOnboardingState(t *testing.T) {
	state := NewOnboardingState("t1")

	if state.Progress() != 0 {
		t.Fatalf("expected 0%% progress, got %.0f%%", state.Progress())
	}

	state.CompleteStep("create-account")
	progress := state.Progress()
	if progress < 16 || progress > 17 {
		t.Fatalf("expected ~16.7%% progress, got %.1f%%", progress)
	}

	// Complete all
	for _, s := range state.Steps {
		state.CompleteStep(s.ID)
	}
	if !state.Complete {
		t.Fatal("expected complete")
	}
	if state.Progress() != 100 {
		t.Fatalf("expected 100%%, got %.0f%%", state.Progress())
	}
}
