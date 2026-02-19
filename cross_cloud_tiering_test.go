package chronicle

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestDefaultCrossCloudTieringConfig(t *testing.T) {
	cfg := DefaultCrossCloudTieringConfig()
	if !cfg.Enabled {
		t.Fatal("expected enabled by default")
	}
	if cfg.EvaluationInterval != 5*time.Minute {
		t.Fatalf("expected 5m evaluation interval, got %v", cfg.EvaluationInterval)
	}
	if cfg.MaxConcurrentMigrations != 4 {
		t.Fatalf("expected 4 max concurrent migrations, got %d", cfg.MaxConcurrentMigrations)
	}
	if !cfg.CostOptimizationEnabled {
		t.Fatal("expected cost optimization enabled")
	}
	if !cfg.EgressAware {
		t.Fatal("expected egress aware")
	}
	if cfg.DryRunMode {
		t.Fatal("expected dry run mode off by default")
	}
	if cfg.DefaultPolicy != "cost-optimized" {
		t.Fatalf("expected default policy 'cost-optimized', got %q", cfg.DefaultPolicy)
	}
}

func newTestCrossCloudEngine(t *testing.T) *CrossCloudTieringEngine {
	t.Helper()
	cfg := DefaultCrossCloudTieringConfig()
	cfg.DryRunMode = true
	return NewCrossCloudTieringEngine(nil, cfg)
}

func TestCrossCloudAddRemoveEndpoints(t *testing.T) {
	engine := newTestCrossCloudEngine(t)

	ep := CloudEndpoint{
		ID:         "aws-us-east-1",
		Provider:   CloudAWS,
		Region:     "us-east-1",
		Bucket:     "chronicle-hot",
		CostPerGB:  0.023,
		EgressCost: 0.09,
		Available:  true,
	}

	if err := engine.AddEndpoint(ep); err != nil {
		t.Fatalf("AddEndpoint: %v", err)
	}

	// Duplicate should fail
	if err := engine.AddEndpoint(ep); err == nil {
		t.Fatal("expected error on duplicate endpoint")
	}

	// Empty ID should fail
	if err := engine.AddEndpoint(CloudEndpoint{}); err == nil {
		t.Fatal("expected error on empty ID")
	}

	endpoints := engine.ListEndpoints()
	if len(endpoints) != 1 {
		t.Fatalf("expected 1 endpoint, got %d", len(endpoints))
	}
	if endpoints[0].ID != "aws-us-east-1" {
		t.Fatalf("expected aws-us-east-1, got %s", endpoints[0].ID)
	}

	// Remove
	if err := engine.RemoveEndpoint("aws-us-east-1"); err != nil {
		t.Fatalf("RemoveEndpoint: %v", err)
	}
	if err := engine.RemoveEndpoint("nonexistent"); err == nil {
		t.Fatal("expected error removing nonexistent endpoint")
	}
	if len(engine.ListEndpoints()) != 0 {
		t.Fatal("expected 0 endpoints after removal")
	}
}

func TestCrossCloudPolicyCRUD(t *testing.T) {
	engine := newTestCrossCloudEngine(t)

	policy := TieringPolicy{
		ID:      "age-based",
		Name:    "Age-based tiering",
		Enabled: true,
		Rules: []TieringRule{
			{
				Condition:   TieringCondition{Type: "age", Operator: "gt", Duration: 24 * time.Hour},
				Action:      TieringAction{Type: "migrate"},
				TargetCloud: "gcp-cold",
				TargetTier:  "cold",
			},
		},
	}

	if err := engine.CreatePolicy(policy); err != nil {
		t.Fatalf("CreatePolicy: %v", err)
	}
	if err := engine.CreatePolicy(policy); err == nil {
		t.Fatal("expected error on duplicate policy")
	}
	if err := engine.CreatePolicy(TieringPolicy{}); err == nil {
		t.Fatal("expected error on empty ID")
	}

	policies := engine.ListPolicies()
	if len(policies) != 1 {
		t.Fatalf("expected 1 policy, got %d", len(policies))
	}

	// Update
	policy.Name = "Updated age-based"
	if err := engine.UpdatePolicy(policy); err != nil {
		t.Fatalf("UpdatePolicy: %v", err)
	}
	if err := engine.UpdatePolicy(TieringPolicy{ID: "nonexistent"}); err == nil {
		t.Fatal("expected error updating nonexistent policy")
	}
	if err := engine.UpdatePolicy(TieringPolicy{}); err == nil {
		t.Fatal("expected error on empty ID update")
	}

	// Delete
	if err := engine.DeletePolicy("age-based"); err != nil {
		t.Fatalf("DeletePolicy: %v", err)
	}
	if err := engine.DeletePolicy("nonexistent"); err == nil {
		t.Fatal("expected error deleting nonexistent policy")
	}
	if len(engine.ListPolicies()) != 0 {
		t.Fatal("expected 0 policies after deletion")
	}
}

func TestCrossCloudDataPlacement(t *testing.T) {
	engine := newTestCrossCloudEngine(t)

	engine.AddEndpoint(CloudEndpoint{
		ID:        "aws-1",
		Provider:  CloudAWS,
		CostPerGB: 0.023,
		Available: true,
	})

	engine.TrackPlacement("cpu.usage", "part-001", "hot", "aws-1", 1<<30)
	engine.TrackPlacement("mem.usage", "part-002", "warm", "aws-1", 2<<30)

	stats := engine.Stats()
	if stats.TotalPlacements != 2 {
		t.Fatalf("expected 2 placements, got %d", stats.TotalPlacements)
	}

	// Update existing placement
	engine.TrackPlacement("cpu.usage", "part-001", "cold", "aws-1", 1<<30)
	stats = engine.Stats()
	if stats.TotalPlacements != 2 {
		t.Fatalf("expected still 2 placements after update, got %d", stats.TotalPlacements)
	}
}

func TestCrossCloudRecordAccess(t *testing.T) {
	engine := newTestCrossCloudEngine(t)

	engine.AddEndpoint(CloudEndpoint{ID: "aws-1", CostPerGB: 0.023, Available: true})
	engine.TrackPlacement("cpu.usage", "part-001", "hot", "aws-1", 1<<30)

	engine.RecordAccess("cpu.usage")
	engine.RecordAccess("cpu.usage")
	engine.RecordAccess("cpu.usage")

	engine.mu.RLock()
	p := engine.placements["part-001"]
	engine.mu.RUnlock()
	if p.AccessCount != 3 {
		t.Fatalf("expected 3 accesses, got %d", p.AccessCount)
	}
}

func TestCrossCloudEvaluatePolicies(t *testing.T) {
	engine := newTestCrossCloudEngine(t)

	engine.AddEndpoint(CloudEndpoint{ID: "aws-1", CostPerGB: 0.023, EgressCost: 0.09, Available: true})
	engine.AddEndpoint(CloudEndpoint{ID: "gcp-cold", CostPerGB: 0.004, Available: true})

	// Track placement with old creation time
	engine.TrackPlacement("old.metric", "part-old", "hot", "aws-1", 1<<30)
	engine.mu.Lock()
	engine.placements["part-old"].CreatedAt = time.Now().Add(-48 * time.Hour)
	engine.mu.Unlock()

	engine.CreatePolicy(TieringPolicy{
		ID:      "age-policy",
		Name:    "Move old data to cold",
		Enabled: true,
		Rules: []TieringRule{
			{
				Condition:   TieringCondition{Type: "age", Operator: "gt", Duration: 24 * time.Hour},
				Action:      TieringAction{Type: "migrate"},
				TargetCloud: "gcp-cold",
				TargetTier:  "cold",
			},
		},
	})

	jobs, err := engine.EvaluatePolicies()
	if err != nil {
		t.Fatalf("EvaluatePolicies: %v", err)
	}
	if len(jobs) == 0 {
		t.Fatal("expected at least one migration job")
	}
	if jobs[0].TargetCloud != "gcp-cold" {
		t.Fatalf("expected target gcp-cold, got %s", jobs[0].TargetCloud)
	}
	if jobs[0].State != "pending" {
		t.Fatalf("expected pending state, got %s", jobs[0].State)
	}
}

func TestCrossCloudEvaluateAccessFrequency(t *testing.T) {
	engine := newTestCrossCloudEngine(t)
	engine.AddEndpoint(CloudEndpoint{ID: "aws-1", CostPerGB: 0.023, Available: true})
	engine.AddEndpoint(CloudEndpoint{ID: "cold-store", CostPerGB: 0.004, Available: true})

	engine.TrackPlacement("low.access", "part-low", "hot", "aws-1", 1<<20)
	// Access count stays at 0

	engine.CreatePolicy(TieringPolicy{
		ID:      "freq-policy",
		Enabled: true,
		Rules: []TieringRule{
			{
				Condition:   TieringCondition{Type: "access_frequency", Operator: "lt", Value: "5"},
				Action:      TieringAction{Type: "migrate"},
				TargetCloud: "cold-store",
				TargetTier:  "cold",
			},
		},
	})

	jobs, err := engine.EvaluatePolicies()
	if err != nil {
		t.Fatalf("EvaluatePolicies: %v", err)
	}
	if len(jobs) == 0 {
		t.Fatal("expected migration job for low-access data")
	}
}

func TestCrossCloudEvaluateSizeCondition(t *testing.T) {
	engine := newTestCrossCloudEngine(t)
	engine.AddEndpoint(CloudEndpoint{ID: "local", CostPerGB: 0.01, Available: true})
	engine.AddEndpoint(CloudEndpoint{ID: "archive", CostPerGB: 0.001, Available: true})

	engine.TrackPlacement("big.metric", "part-big", "hot", "local", 10<<30) // 10 GB

	engine.CreatePolicy(TieringPolicy{
		ID:      "size-policy",
		Enabled: true,
		Rules: []TieringRule{
			{
				Condition:   TieringCondition{Type: "size", Operator: "gt", Value: "1073741824"}, // 1 GB
				Action:      TieringAction{Type: "migrate"},
				TargetCloud: "archive",
				TargetTier:  "archive",
			},
		},
	})

	jobs, err := engine.EvaluatePolicies()
	if err != nil {
		t.Fatalf("EvaluatePolicies: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job for large data, got %d", len(jobs))
	}
}

func TestCrossCloudEvaluateMetricPattern(t *testing.T) {
	engine := newTestCrossCloudEngine(t)
	engine.AddEndpoint(CloudEndpoint{ID: "aws-1", CostPerGB: 0.02, Available: true})
	engine.AddEndpoint(CloudEndpoint{ID: "r2", CostPerGB: 0.005, Available: true})

	engine.TrackPlacement("system.cpu.idle", "part-cpu", "hot", "aws-1", 1<<20)
	engine.TrackPlacement("app.requests", "part-app", "hot", "aws-1", 1<<20)

	engine.CreatePolicy(TieringPolicy{
		ID:      "pattern-policy",
		Enabled: true,
		Rules: []TieringRule{
			{
				Condition:   TieringCondition{Type: "metric_pattern", Operator: "matches", Value: "system."},
				Action:      TieringAction{Type: "migrate"},
				TargetCloud: "r2",
				TargetTier:  "warm",
			},
		},
	})

	jobs, err := engine.EvaluatePolicies()
	if err != nil {
		t.Fatalf("EvaluatePolicies: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job (only system.* match), got %d", len(jobs))
	}
}

func TestCrossCloudDisabledPolicySkipped(t *testing.T) {
	engine := newTestCrossCloudEngine(t)
	engine.AddEndpoint(CloudEndpoint{ID: "aws-1", CostPerGB: 0.02, Available: true})
	engine.TrackPlacement("metric", "part-1", "hot", "aws-1", 1<<20)

	engine.CreatePolicy(TieringPolicy{
		ID:      "disabled-policy",
		Enabled: false,
		Rules: []TieringRule{
			{
				Condition:   TieringCondition{Type: "access_frequency", Operator: "lt", Value: "100"},
				Action:      TieringAction{Type: "migrate"},
				TargetCloud: "aws-1",
				TargetTier:  "cold",
			},
		},
	})

	jobs, _ := engine.EvaluatePolicies()
	if len(jobs) != 0 {
		t.Fatalf("expected 0 jobs for disabled policy, got %d", len(jobs))
	}
}

func TestCrossCloudMigrationLifecycle(t *testing.T) {
	engine := newTestCrossCloudEngine(t)

	engine.AddEndpoint(CloudEndpoint{ID: "aws-1", CostPerGB: 0.023, Available: true})
	engine.AddEndpoint(CloudEndpoint{ID: "gcp-cold", CostPerGB: 0.004, Available: true})
	engine.TrackPlacement("cpu.usage", "part-001", "hot", "aws-1", 1<<30)

	job := &MigrationJob{
		ID:          "test-job-1",
		SourceCloud: "aws-1",
		TargetCloud: "gcp-cold",
		Partitions:  []string{"part-001"},
		State:       "pending",
		BytesTotal:  1 << 30,
		DryRun:      true,
	}
	engine.mu.Lock()
	engine.migrations[job.ID] = job
	engine.mu.Unlock()

	if err := engine.Migrate(job); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if job.State != "completed" {
		t.Fatalf("expected completed, got %s", job.State)
	}
	if job.Progress != 100.0 {
		t.Fatalf("expected 100%% progress, got %.1f", job.Progress)
	}

	// Verify stats
	stats := engine.Stats()
	if stats.CompletedMigrations != 1 {
		t.Fatalf("expected 1 completed migration, got %d", stats.CompletedMigrations)
	}
	if stats.BytesMigrated == 0 {
		t.Fatal("expected bytes migrated > 0")
	}
}

func TestCrossCloudMigrationNonDryRun(t *testing.T) {
	cfg := DefaultCrossCloudTieringConfig()
	cfg.DryRunMode = false
	engine := NewCrossCloudTieringEngine(nil, cfg)

	engine.AddEndpoint(CloudEndpoint{ID: "src", CostPerGB: 0.02, Available: true})
	engine.AddEndpoint(CloudEndpoint{ID: "dst", CostPerGB: 0.005, Available: true})
	engine.TrackPlacement("metric.a", "p1", "hot", "src", 500)

	job := &MigrationJob{
		ID:          "real-job",
		SourceCloud: "src",
		TargetCloud: "dst",
		Partitions:  []string{"p1"},
		State:       "pending",
		BytesTotal:  500,
		DryRun:      false,
	}
	engine.mu.Lock()
	engine.migrations[job.ID] = job
	engine.mu.Unlock()

	if err := engine.Migrate(job); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if job.State != "completed" {
		t.Fatalf("expected completed, got %s", job.State)
	}

	// Verify placement was updated
	engine.mu.RLock()
	p := engine.placements["p1"]
	engine.mu.RUnlock()
	if p.CloudID != "dst" {
		t.Fatalf("expected placement moved to dst, got %s", p.CloudID)
	}
}

func TestCrossCloudListGetMigration(t *testing.T) {
	engine := newTestCrossCloudEngine(t)

	job := &MigrationJob{ID: "j1", State: "pending"}
	engine.mu.Lock()
	engine.migrations["j1"] = job
	engine.mu.Unlock()

	list := engine.ListMigrations()
	if len(list) != 1 {
		t.Fatalf("expected 1 migration, got %d", len(list))
	}

	got := engine.GetMigration("j1")
	if got == nil || got.ID != "j1" {
		t.Fatal("GetMigration returned unexpected result")
	}
	if engine.GetMigration("nonexistent") != nil {
		t.Fatal("expected nil for nonexistent migration")
	}
}

func TestCrossCloudSimulatePolicy(t *testing.T) {
	engine := newTestCrossCloudEngine(t)

	engine.AddEndpoint(CloudEndpoint{ID: "aws-1", CostPerGB: 0.023, Available: true})
	engine.AddEndpoint(CloudEndpoint{ID: "gcp-cold", CostPerGB: 0.004, Available: true})

	engine.TrackPlacement("old.metric", "part-old", "hot", "aws-1", 1<<30)
	engine.mu.Lock()
	engine.placements["part-old"].CreatedAt = time.Now().Add(-72 * time.Hour)
	engine.placements["part-old"].Cost = 0.023
	engine.mu.Unlock()

	engine.CreatePolicy(TieringPolicy{
		ID:      "sim-policy",
		Enabled: true,
		Rules: []TieringRule{
			{
				Condition:   TieringCondition{Type: "age", Operator: "gt", Duration: 24 * time.Hour},
				Action:      TieringAction{Type: "migrate"},
				TargetCloud: "gcp-cold",
				TargetTier:  "cold",
			},
		},
	})

	sim, err := engine.SimulatePolicy("sim-policy")
	if err != nil {
		t.Fatalf("SimulatePolicy: %v", err)
	}
	if sim.MigrationCount != 1 {
		t.Fatalf("expected 1 migration in simulation, got %d", sim.MigrationCount)
	}
	if sim.AffectedData != 1<<30 {
		t.Fatalf("expected 1 GB affected, got %d", sim.AffectedData)
	}
	if sim.ProjectedSaving <= 0 {
		t.Fatalf("expected positive saving, got %f", sim.ProjectedSaving)
	}

	// Nonexistent policy
	if _, err := engine.SimulatePolicy("nope"); err == nil {
		t.Fatal("expected error for nonexistent policy")
	}
}

func TestCrossCloudCostReport(t *testing.T) {
	engine := newTestCrossCloudEngine(t)

	engine.AddEndpoint(CloudEndpoint{ID: "aws-1", CostPerGB: 0.023, Available: true})
	engine.TrackPlacement("cpu.usage", "part-001", "hot", "aws-1", 1<<30)

	report, err := engine.GenerateCostReport("monthly")
	if err != nil {
		t.Fatalf("GenerateCostReport: %v", err)
	}
	if report.Period != "monthly" {
		t.Fatalf("expected monthly period, got %s", report.Period)
	}
	if report.TotalCost <= 0 {
		t.Fatal("expected positive total cost")
	}
	if len(report.CostByCloud) == 0 {
		t.Fatal("expected cost breakdown by cloud")
	}
	if len(report.CostByTier) == 0 {
		t.Fatal("expected cost breakdown by tier")
	}

	// Daily scaling
	dailyReport, err := engine.GenerateCostReport("daily")
	if err != nil {
		t.Fatalf("GenerateCostReport daily: %v", err)
	}
	if dailyReport.TotalCost >= report.TotalCost {
		t.Fatal("expected daily cost < monthly cost")
	}

	// Weekly
	weeklyReport, err := engine.GenerateCostReport("weekly")
	if err != nil {
		t.Fatalf("GenerateCostReport weekly: %v", err)
	}
	if weeklyReport.TotalCost >= report.TotalCost {
		t.Fatal("expected weekly cost < monthly cost")
	}

	// Invalid period
	if _, err := engine.GenerateCostReport("yearly"); err == nil {
		t.Fatal("expected error for unsupported period")
	}
}

func TestCrossCloudCostReportRecommendations(t *testing.T) {
	engine := newTestCrossCloudEngine(t)

	engine.AddEndpoint(CloudEndpoint{ID: "aws-1", CostPerGB: 0.023, Available: true})
	// Low access placement in hot tier
	engine.TrackPlacement("idle.metric", "part-idle", "hot", "aws-1", 1<<30)

	report, err := engine.GenerateCostReport("monthly")
	if err != nil {
		t.Fatalf("GenerateCostReport: %v", err)
	}
	if len(report.Recommendations) == 0 {
		t.Fatal("expected at least one recommendation for low-access hot data")
	}
	if report.Recommendations[0].FromTier != "hot" || report.Recommendations[0].ToTier != "warm" {
		t.Fatal("expected hot->warm recommendation")
	}
}

func TestCrossCloudStats(t *testing.T) {
	engine := newTestCrossCloudEngine(t)

	engine.AddEndpoint(CloudEndpoint{ID: "ep1", Available: true})
	engine.CreatePolicy(TieringPolicy{ID: "p1", Enabled: true})
	engine.TrackPlacement("m1", "part1", "hot", "ep1", 100)

	stats := engine.Stats()
	if stats.TotalEndpoints != 1 {
		t.Fatalf("expected 1 endpoint, got %d", stats.TotalEndpoints)
	}
	if stats.TotalPolicies != 1 {
		t.Fatalf("expected 1 policy, got %d", stats.TotalPolicies)
	}
	if stats.TotalPlacements != 1 {
		t.Fatalf("expected 1 placement, got %d", stats.TotalPlacements)
	}
	if stats.ActiveMigrations != 0 {
		t.Fatalf("expected 0 active migrations, got %d", stats.ActiveMigrations)
	}
}

func TestCrossCloudStartStop(t *testing.T) {
	cfg := DefaultCrossCloudTieringConfig()
	cfg.EvaluationInterval = 100 * time.Millisecond
	engine := NewCrossCloudTieringEngine(nil, cfg)

	engine.Start()
	// Calling Start again should be a no-op
	engine.Start()

	time.Sleep(50 * time.Millisecond)

	engine.Stop()
	// Calling Stop again should be a no-op
	engine.Stop()
}

func TestCrossCloudProviderString(t *testing.T) {
	tests := []struct {
		p    CrossCloudTierProvider
		want string
	}{
		{CloudLocal, "local"},
		{CloudAWS, "aws"},
		{CloudGCP, "gcp"},
		{CloudAzure, "azure"},
		{CloudCloudflareR2, "cloudflare-r2"},
		{CloudMinIO, "minio"},
		{CrossCloudTierProvider(99), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.p.String(); got != tt.want {
			t.Errorf("CrossCloudTierProvider(%d).String() = %q, want %q", tt.p, got, tt.want)
		}
	}
}

// ---------------------------------------------------------------------------
// HTTP handler tests
// ---------------------------------------------------------------------------

func TestCrossCloudHTTPEndpoints(t *testing.T) {
	engine := newTestCrossCloudEngine(t)
	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	// GET /api/v1/tiering/endpoints - empty
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/api/v1/tiering/endpoints", nil)
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	// POST /api/v1/tiering/endpoints
	body := `{"id":"ep1","provider":1,"region":"us-east-1","bucket":"b","cost_per_gb":0.023,"available":true}`
	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodPost, "/api/v1/tiering/endpoints", strings.NewReader(body))
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}

	// GET endpoints again
	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodGet, "/api/v1/tiering/endpoints", nil)
	mux.ServeHTTP(w, r)
	var eps []CloudEndpoint
	json.NewDecoder(w.Body).Decode(&eps)
	if len(eps) != 1 {
		t.Fatalf("expected 1 endpoint, got %d", len(eps))
	}

	// POST duplicate
	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodPost, "/api/v1/tiering/endpoints", strings.NewReader(body))
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409 for duplicate, got %d", w.Code)
	}
}

func TestCrossCloudHTTPPolicies(t *testing.T) {
	engine := newTestCrossCloudEngine(t)
	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	body := `{"id":"p1","name":"test","enabled":true,"rules":[{"condition":{"type":"age","operator":"gt","duration":86400000000000},"action":{"type":"migrate"},"target_cloud":"cold","target_tier":"cold"}]}`
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/api/v1/tiering/policies", strings.NewReader(body))
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}

	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodGet, "/api/v1/tiering/policies", nil)
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestCrossCloudHTTPEvaluate(t *testing.T) {
	engine := newTestCrossCloudEngine(t)
	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/api/v1/tiering/evaluate", nil)
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	// Wrong method
	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodGet, "/api/v1/tiering/evaluate", nil)
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestCrossCloudHTTPSimulate(t *testing.T) {
	engine := newTestCrossCloudEngine(t)
	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	engine.CreatePolicy(TieringPolicy{ID: "sp1", Enabled: true})

	body := `{"policy_id":"sp1"}`
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/api/v1/tiering/simulate", strings.NewReader(body))
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// Nonexistent policy
	body = `{"policy_id":"nope"}`
	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodPost, "/api/v1/tiering/simulate", strings.NewReader(body))
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestCrossCloudHTTPMigrations(t *testing.T) {
	engine := newTestCrossCloudEngine(t)
	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/api/v1/tiering/migrations", nil)
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestCrossCloudHTTPCostReport(t *testing.T) {
	engine := newTestCrossCloudEngine(t)
	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/api/v1/tiering/cost-report?period=monthly", nil)
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	// Invalid period
	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodGet, "/api/v1/tiering/cost-report?period=yearly", nil)
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid period, got %d", w.Code)
	}

	// Default period (no query param)
	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodGet, "/api/v1/tiering/cost-report", nil)
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for default period, got %d", w.Code)
	}
}

func TestCrossCloudHTTPStats(t *testing.T) {
	engine := newTestCrossCloudEngine(t)
	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/api/v1/tiering/stats", nil)
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var stats CrossCloudTieringStats
	if err := json.NewDecoder(w.Body).Decode(&stats); err != nil {
		t.Fatalf("decode stats: %v", err)
	}
}
