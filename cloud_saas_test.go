package chronicle

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNewCloudSaaSEngine(t *testing.T) {
	config := DefaultCloudSaaSConfig()
	engine := NewCloudSaaSEngine(nil, config)

	if engine == nil {
		t.Fatal("expected non-nil engine")
	}
	if engine.config.Region != "us-east-1" {
		t.Errorf("expected region us-east-1, got %s", engine.config.Region)
	}
	if engine.config.Tier != TierFree {
		t.Errorf("expected tier free, got %s", engine.config.Tier)
	}
	if engine.config.MaxInstances != 100 {
		t.Errorf("expected max instances 100, got %d", engine.config.MaxInstances)
	}
}

func TestCloudSaaSCreateOrganization(t *testing.T) {
	engine := NewCloudSaaSEngine(nil, DefaultCloudSaaSConfig())

	org, err := engine.CreateOrganization("Acme Corp", "admin@acme.com", TierPro)
	if err != nil {
		t.Fatalf("create org failed: %v", err)
	}
	if org.Name != "Acme Corp" {
		t.Errorf("expected name Acme Corp, got %s", org.Name)
	}
	if org.OwnerEmail != "admin@acme.com" {
		t.Errorf("expected email admin@acme.com, got %s", org.OwnerEmail)
	}
	if org.Tier != TierPro {
		t.Errorf("expected tier pro, got %s", org.Tier)
	}
	if org.BillingInfo.Plan != "pro" {
		t.Errorf("expected billing plan pro, got %s", org.BillingInfo.Plan)
	}

	// Retrieve
	got, err := engine.GetOrganization(org.ID)
	if err != nil {
		t.Fatalf("get org failed: %v", err)
	}
	if got.ID != org.ID {
		t.Errorf("expected ID %s, got %s", org.ID, got.ID)
	}

	// List
	orgs := engine.ListOrganizations()
	if len(orgs) != 1 {
		t.Errorf("expected 1 org, got %d", len(orgs))
	}

	// Errors
	_, err = engine.CreateOrganization("", "a@b.com", TierFree)
	if err == nil {
		t.Error("expected error for empty name")
	}
	_, err = engine.CreateOrganization("X", "", TierFree)
	if err == nil {
		t.Error("expected error for empty email")
	}

	// Not found
	_, err = engine.GetOrganization("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent org")
	}
}

func TestCloudSaaSDeployInstance(t *testing.T) {
	engine := NewCloudSaaSEngine(nil, DefaultCloudSaaSConfig())

	org, _ := engine.CreateOrganization("TestOrg", "test@test.com", TierPro)

	spec := DeploymentSpec{
		Name:   "prod-db",
		Region: "eu-west-1",
		Tier:   TierPro,
		Config: map[string]string{"retention": "90d"},
	}

	inst, err := engine.DeployInstance(org.ID, spec)
	if err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	if inst.Name != "prod-db" {
		t.Errorf("expected name prod-db, got %s", inst.Name)
	}
	if inst.Region != "eu-west-1" {
		t.Errorf("expected region eu-west-1, got %s", inst.Region)
	}
	if inst.State != InstanceRunning {
		t.Errorf("expected running, got %s", inst.State)
	}

	// Get instance
	got, err := engine.GetInstance(inst.ID)
	if err != nil {
		t.Fatalf("get instance failed: %v", err)
	}
	if got.ID != inst.ID {
		t.Errorf("expected ID %s, got %s", inst.ID, got.ID)
	}

	// List instances
	instances := engine.ListInstances(org.ID)
	if len(instances) != 1 {
		t.Errorf("expected 1 instance, got %d", len(instances))
	}

	// Errors
	_, err = engine.DeployInstance("nonexistent", spec)
	if err == nil {
		t.Error("expected error for nonexistent org")
	}
	_, err = engine.DeployInstance(org.ID, DeploymentSpec{})
	if err == nil {
		t.Error("expected error for empty spec name")
	}

	// Not found
	_, err = engine.GetInstance("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent instance")
	}

	// Nil list for nonexistent org
	nilInstances := engine.ListInstances("nonexistent")
	if nilInstances != nil {
		t.Errorf("expected nil, got %v", nilInstances)
	}
}

func TestCloudSaaSInstanceLifecycle(t *testing.T) {
	engine := NewCloudSaaSEngine(nil, DefaultCloudSaaSConfig())

	org, _ := engine.CreateOrganization("LifecycleOrg", "lc@test.com", TierEnterprise)
	inst, _ := engine.DeployInstance(org.ID, DeploymentSpec{
		Name: "lifecycle-db",
		Tier: TierEnterprise,
	})

	if inst.State != InstanceRunning {
		t.Fatalf("expected running, got %s", inst.State)
	}

	// Stop
	if err := engine.StopInstance(inst.ID); err != nil {
		t.Fatalf("stop failed: %v", err)
	}
	got, _ := engine.GetInstance(inst.ID)
	if got.State != InstanceStopped {
		t.Errorf("expected stopped, got %s", got.State)
	}

	// Stop again should fail
	if err := engine.StopInstance(inst.ID); err == nil {
		t.Error("expected error stopping non-running instance")
	}

	// Stop nonexistent
	if err := engine.StopInstance("nonexistent"); err == nil {
		t.Error("expected error for nonexistent instance")
	}

	// Deploy another to terminate
	inst2, _ := engine.DeployInstance(org.ID, DeploymentSpec{
		Name: "terminate-db",
		Tier: TierEnterprise,
	})

	if err := engine.TerminateInstance(inst2.ID); err != nil {
		t.Fatalf("terminate failed: %v", err)
	}
	_, err := engine.GetInstance(inst2.ID)
	if err == nil {
		t.Error("expected error after termination")
	}

	// Terminate nonexistent
	if err := engine.TerminateInstance("nonexistent"); err == nil {
		t.Error("expected error for nonexistent instance")
	}
}

func TestCloudSaaSAPIKeys(t *testing.T) {
	engine := NewCloudSaaSEngine(nil, DefaultCloudSaaSConfig())

	org, _ := engine.CreateOrganization("KeyOrg", "keys@test.com", TierPro)

	key, err := engine.GenerateAPIKey(org.ID, "prod-key", []string{"read", "write"})
	if err != nil {
		t.Fatalf("generate key failed: %v", err)
	}
	if key.Name != "prod-key" {
		t.Errorf("expected name prod-key, got %s", key.Name)
	}
	if len(key.Permissions) != 2 {
		t.Errorf("expected 2 permissions, got %d", len(key.Permissions))
	}
	if key.KeyHash == "" {
		t.Error("expected non-empty key hash")
	}

	// Revoke
	if err := engine.RevokeAPIKey(key.ID); err != nil {
		t.Fatalf("revoke failed: %v", err)
	}

	// Revoke again should fail
	if err := engine.RevokeAPIKey(key.ID); err == nil {
		t.Error("expected error revoking nonexistent key")
	}

	// Errors
	_, err = engine.GenerateAPIKey("nonexistent", "k", []string{"read"})
	if err == nil {
		t.Error("expected error for nonexistent org")
	}
	_, err = engine.GenerateAPIKey(org.ID, "", []string{"read"})
	if err == nil {
		t.Error("expected error for empty key name")
	}
}

func TestCloudSaaSUsageTracking(t *testing.T) {
	engine := NewCloudSaaSEngine(nil, DefaultCloudSaaSConfig())

	org, _ := engine.CreateOrganization("UsageOrg", "usage@test.com", TierPro)
	inst, _ := engine.DeployInstance(org.ID, DeploymentSpec{Name: "usage-db", Tier: TierPro})

	record := UsageRecord{
		OrgID:        org.ID,
		InstanceID:   inst.ID,
		StorageBytes: 1024 * 1024,
		Queries:      100,
		Writes:       500,
		Reads:        200,
	}

	if err := engine.RecordUsage(record); err != nil {
		t.Fatalf("record usage failed: %v", err)
	}

	// Record second usage point
	record2 := UsageRecord{
		OrgID:        org.ID,
		InstanceID:   inst.ID,
		StorageBytes: 2048 * 1024,
		Queries:      50,
		Writes:       100,
		Reads:        75,
	}
	engine.RecordUsage(record2)

	summary := engine.GetUsageSummary(org.ID)
	if summary.TotalQueries != 150 {
		t.Errorf("expected 150 queries, got %d", summary.TotalQueries)
	}
	if summary.TotalWrites != 600 {
		t.Errorf("expected 600 writes, got %d", summary.TotalWrites)
	}
	if summary.RecordCount != 2 {
		t.Errorf("expected 2 records, got %d", summary.RecordCount)
	}

	// Errors
	err := engine.RecordUsage(UsageRecord{})
	if err == nil {
		t.Error("expected error for empty org ID")
	}
	err = engine.RecordUsage(UsageRecord{OrgID: "nonexistent"})
	if err == nil {
		t.Error("expected error for nonexistent org")
	}
}

func TestCloudSaaSBilling(t *testing.T) {
	engine := NewCloudSaaSEngine(nil, DefaultCloudSaaSConfig())

	// Free tier
	freeOrg, _ := engine.CreateOrganization("FreeOrg", "free@test.com", TierFree)
	freeBilling := engine.GetBilling(freeOrg.ID)
	if freeBilling.Plan != "free" {
		t.Errorf("expected free plan, got %s", freeBilling.Plan)
	}
	if freeBilling.MonthlyCost != 0 {
		t.Errorf("expected 0 cost, got %f", freeBilling.MonthlyCost)
	}

	// Pro tier
	proOrg, _ := engine.CreateOrganization("ProOrg", "pro@test.com", TierPro)
	proBilling := engine.GetBilling(proOrg.ID)
	if proBilling.Plan != "pro" {
		t.Errorf("expected pro plan, got %s", proBilling.Plan)
	}
	if proBilling.MonthlyCost != 99.0 {
		t.Errorf("expected 99.0 cost, got %f", proBilling.MonthlyCost)
	}

	// Enterprise tier
	entOrg, _ := engine.CreateOrganization("EntOrg", "ent@test.com", TierEnterprise)
	entBilling := engine.GetBilling(entOrg.ID)
	if entBilling.Plan != "enterprise" {
		t.Errorf("expected enterprise plan, got %s", entBilling.Plan)
	}
	if entBilling.MonthlyCost != 499.0 {
		t.Errorf("expected 499.0 cost, got %f", entBilling.MonthlyCost)
	}

	// Nonexistent org
	if billing := engine.GetBilling("nonexistent"); billing != nil {
		t.Error("expected nil billing for nonexistent org")
	}
}

func TestCloudSaaSFederatedQuery(t *testing.T) {
	engine := NewCloudSaaSEngine(nil, DefaultCloudSaaSConfig())

	org, _ := engine.CreateOrganization("FedOrg", "fed@test.com", TierEnterprise)
	engine.DeployInstance(org.ID, DeploymentSpec{Name: "fed-db-1", Tier: TierEnterprise})
	engine.DeployInstance(org.ID, DeploymentSpec{Name: "fed-db-2", Tier: TierEnterprise})

	result, err := engine.FederatedQuery(org.ID, "SELECT * FROM metrics")
	if err != nil {
		t.Fatalf("federated query failed: %v", err)
	}

	resultMap, ok := result.(map[string]interface{})
	if !ok {
		t.Fatal("expected map result")
	}
	if resultMap["status"] != "completed" {
		t.Errorf("expected completed status, got %v", resultMap["status"])
	}
	instances, ok := resultMap["instances"].([]string)
	if !ok {
		t.Fatal("expected instances list")
	}
	if len(instances) != 2 {
		t.Errorf("expected 2 instances, got %d", len(instances))
	}

	// Errors
	_, err = engine.FederatedQuery(org.ID, "")
	if err == nil {
		t.Error("expected error for empty query")
	}
	_, err = engine.FederatedQuery("nonexistent", "SELECT 1")
	if err == nil {
		t.Error("expected error for nonexistent org")
	}

	// No running instances
	emptyOrg, _ := engine.CreateOrganization("EmptyOrg", "empty@test.com", TierFree)
	_, err = engine.FederatedQuery(emptyOrg.ID, "SELECT 1")
	if err == nil {
		t.Error("expected error for org with no instances")
	}
}

func TestCloudSaaSStats(t *testing.T) {
	engine := NewCloudSaaSEngine(nil, DefaultCloudSaaSConfig())

	org1, _ := engine.CreateOrganization("Org1", "o1@test.com", TierPro)
	org2, _ := engine.CreateOrganization("Org2", "o2@test.com", TierEnterprise)

	engine.DeployInstance(org1.ID, DeploymentSpec{Name: "db-1", Tier: TierPro})
	engine.DeployInstance(org1.ID, DeploymentSpec{Name: "db-2", Tier: TierPro})
	inst3, _ := engine.DeployInstance(org2.ID, DeploymentSpec{Name: "db-3", Tier: TierEnterprise})

	engine.RecordUsage(UsageRecord{OrgID: org1.ID, Queries: 100})
	engine.RecordUsage(UsageRecord{OrgID: org2.ID, Queries: 200})

	// Stop one instance
	engine.StopInstance(inst3.ID)

	stats := engine.Stats()
	if stats.TotalOrgs != 2 {
		t.Errorf("expected 2 orgs, got %d", stats.TotalOrgs)
	}
	if stats.TotalInstances != 3 {
		t.Errorf("expected 3 instances, got %d", stats.TotalInstances)
	}
	if stats.ActiveInstances != 2 {
		t.Errorf("expected 2 active instances, got %d", stats.ActiveInstances)
	}
	if stats.TotalQueries != 300 {
		t.Errorf("expected 300 queries, got %d", stats.TotalQueries)
	}
	if stats.MonthlyRevenueEstimate != 598.0 { // 99 + 499
		t.Errorf("expected 598.0 revenue, got %f", stats.MonthlyRevenueEstimate)
	}
}

func TestCloudSaaSHTTPHandlers(t *testing.T) {
	engine := NewCloudSaaSEngine(nil, DefaultCloudSaaSConfig())
	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	// POST create org
	body := `{"name":"HTTPOrg","email":"http@test.com","tier":"pro"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/cloud/orgs", strings.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}

	var org CloudOrganization
	json.NewDecoder(w.Body).Decode(&org)
	if org.Name != "HTTPOrg" {
		t.Errorf("expected HTTPOrg, got %s", org.Name)
	}

	// GET list orgs
	req = httptest.NewRequest(http.MethodGet, "/api/v1/cloud/orgs", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	// GET stats
	req = httptest.NewRequest(http.MethodGet, "/api/v1/cloud/stats", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var stats CloudSaaSStats
	json.NewDecoder(w.Body).Decode(&stats)
	if stats.TotalOrgs != 1 {
		t.Errorf("expected 1 org in stats, got %d", stats.TotalOrgs)
	}

	// GET usage without org_id
	req = httptest.NewRequest(http.MethodGet, "/api/v1/cloud/usage", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}

	// GET instances without org_id
	req = httptest.NewRequest(http.MethodGet, "/api/v1/cloud/instances", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}

	// Method not allowed
	req = httptest.NewRequest(http.MethodDelete, "/api/v1/cloud/orgs", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
}
