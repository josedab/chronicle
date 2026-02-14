package chronicle

import (
	"testing"
	"time"
)

func TestDefaultCompliancePacksConfig(t *testing.T) {
	cfg := DefaultCompliancePacksConfig()
	if !cfg.Enabled {
		t.Error("expected Enabled to be true")
	}
	if !cfg.AuditLogEnabled {
		t.Error("expected AuditLogEnabled to be true")
	}
	if cfg.AuditLogPath == "" {
		t.Error("expected AuditLogPath to be set")
	}
	if cfg.AutoRemediate {
		t.Error("expected AutoRemediate to be false")
	}
}

func TestNewCompliancePacksEngine(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultCompliancePacksConfig()
	engine := NewCompliancePacksEngine(db, cfg)
	if engine == nil {
		t.Fatal("expected non-nil engine")
	}
	if len(engine.packs) != 5 {
		t.Errorf("expected 5 packs, got %d", len(engine.packs))
	}
}

func TestCompliancePacksInitAllStandards(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	engine := NewCompliancePacksEngine(db, DefaultCompliancePacksConfig())

	standards := []ComplianceStandard{
		ComplianceHIPAA,
		ComplianceSOC2,
		ComplianceGDPR,
		CompliancePCIDSS,
		ComplianceISO27001,
	}

	for _, std := range standards {
		pack := engine.GetPack(std)
		if pack == nil {
			t.Errorf("expected pack for %s", std)
			continue
		}
		if pack.Name == "" {
			t.Errorf("expected non-empty name for %s", std)
		}
		if pack.Version == "" {
			t.Errorf("expected non-empty version for %s", std)
		}
		if len(pack.Requirements) < 8 {
			t.Errorf("expected at least 8 requirements for %s, got %d", std, len(pack.Requirements))
		}
		if pack.Status != PackDisabled {
			t.Errorf("expected pack %s to be disabled initially", std)
		}
	}
}

func TestCompliancePacksEnableDisable(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	engine := NewCompliancePacksEngine(db, DefaultCompliancePacksConfig())

	// Enable HIPAA
	if err := engine.EnablePack(ComplianceHIPAA); err != nil {
		t.Fatalf("unexpected error enabling HIPAA: %v", err)
	}
	pack := engine.GetPack(ComplianceHIPAA)
	if !pack.Enabled {
		t.Error("expected HIPAA to be enabled")
	}
	if pack.Status != PackEnabled {
		t.Error("expected PackEnabled status")
	}
	if pack.EnabledAt.IsZero() {
		t.Error("expected EnabledAt to be set")
	}

	// Disable HIPAA
	if err := engine.DisablePack(ComplianceHIPAA); err != nil {
		t.Fatalf("unexpected error disabling HIPAA: %v", err)
	}
	pack = engine.GetPack(ComplianceHIPAA)
	if pack.Enabled {
		t.Error("expected HIPAA to be disabled")
	}
	if pack.Status != PackDisabled {
		t.Error("expected PackDisabled status")
	}

	// Enable unknown standard
	if err := engine.EnablePack(ComplianceStandard("UNKNOWN")); err == nil {
		t.Error("expected error for unknown standard")
	}

	// Disable unknown standard
	if err := engine.DisablePack(ComplianceStandard("UNKNOWN")); err == nil {
		t.Error("expected error for unknown standard")
	}
}

func TestCompliancePacksValidate(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	engine := NewCompliancePacksEngine(db, DefaultCompliancePacksConfig())
	_ = engine.EnablePack(ComplianceHIPAA)

	report, err := engine.ValidatePack(ComplianceHIPAA)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if report == nil {
		t.Fatal("expected non-nil report")
	}
	if report.Standard != string(ComplianceHIPAA) {
		t.Errorf("expected standard HIPAA, got %s", report.Standard)
	}
	if report.Score < 0 || report.Score > 100 {
		t.Errorf("expected score between 0-100, got %f", report.Score)
	}
	if report.OverallStatus == "" {
		t.Error("expected non-empty overall status")
	}
	if len(report.Requirements) == 0 {
		t.Error("expected non-empty requirements")
	}
	if report.GeneratedAt.IsZero() {
		t.Error("expected GeneratedAt to be set")
	}

	// Validate unknown standard
	_, err = engine.ValidatePack(ComplianceStandard("UNKNOWN"))
	if err == nil {
		t.Error("expected error for unknown standard")
	}
}

func TestCompliancePacksAuditLogging(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	engine := NewCompliancePacksEngine(db, DefaultCompliancePacksConfig())

	entry := AuditLogEntry{
		Action:   "query",
		User:     "test-user",
		Resource: "metrics.cpu",
		Details:  "queried CPU metrics",
		SourceIP: "192.168.1.1",
		Success:  true,
		Standard: string(ComplianceHIPAA),
	}
	engine.LogAudit(entry)

	entries := engine.GetAuditLog(time.Time{}, 100)
	if len(entries) < 1 {
		t.Fatal("expected at least 1 audit log entry")
	}

	found := false
	for _, e := range entries {
		if e.User == "test-user" && e.Action == "query" {
			found = true
			if e.ID == "" {
				t.Error("expected auto-generated ID")
			}
			if e.Timestamp.IsZero() {
				t.Error("expected auto-generated timestamp")
			}
			break
		}
	}
	if !found {
		t.Error("expected to find the logged audit entry")
	}

	// Test filtering by time
	future := time.Now().Add(1 * time.Hour)
	entries = engine.GetAuditLog(future, 100)
	if len(entries) != 0 {
		t.Errorf("expected 0 entries for future filter, got %d", len(entries))
	}
}

func TestCompliancePacksDataClassification(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	engine := NewCompliancePacksEngine(db, DefaultCompliancePacksConfig())

	dc := DataClassification{
		MetricName:     "user.health_records",
		Classification: "restricted",
		ContainsPII:    true,
		PIITypes:       []string{"name", "ssn", "medical_records"},
		HandlingRules:  []string{"encrypt_at_rest", "audit_access", "no_export"},
		ClassifiedBy:   "admin",
	}

	if err := engine.ClassifyMetric(dc); err != nil {
		t.Fatalf("unexpected error classifying metric: %v", err)
	}

	// Get classification
	got := engine.GetClassification("user.health_records")
	if got == nil {
		t.Fatal("expected non-nil classification")
	}
	if got.Classification != "restricted" {
		t.Errorf("expected restricted, got %s", got.Classification)
	}
	if !got.ContainsPII {
		t.Error("expected ContainsPII to be true")
	}
	if got.ClassifiedAt.IsZero() {
		t.Error("expected ClassifiedAt to be auto-set")
	}

	// Get non-existent
	if engine.GetClassification("nonexistent") != nil {
		t.Error("expected nil for nonexistent metric")
	}

	// List classifications
	all := engine.ListClassifications()
	if len(all) != 1 {
		t.Errorf("expected 1 classification, got %d", len(all))
	}

	// Empty metric name
	err := engine.ClassifyMetric(DataClassification{})
	if err == nil {
		t.Error("expected error for empty metric name")
	}
}

func TestCompliancePacksGenerateReport(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	engine := NewCompliancePacksEngine(db, DefaultCompliancePacksConfig())
	_ = engine.EnablePack(ComplianceGDPR)

	report, err := engine.GenerateReport(ComplianceGDPR)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if report.Standard != string(ComplianceGDPR) {
		t.Errorf("expected GDPR standard, got %s", report.Standard)
	}
	if len(report.Recommendations) == 0 {
		t.Error("expected recommendations")
	}
	if report.NextReviewDate.IsZero() {
		t.Error("expected next review date")
	}
}

func TestCompliancePacksRequirementChecking(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	engine := NewCompliancePacksEngine(db, DefaultCompliancePacksConfig())

	// With default config (Enabled=true, AuditLogEnabled=true), most requirements pass
	req := ComplianceRequirement{Category: "encryption"}
	if !engine.checkRequirement(req) {
		t.Error("expected encryption requirement to be met with enabled config")
	}

	req = ComplianceRequirement{Category: "audit"}
	if !engine.checkRequirement(req) {
		t.Error("expected audit requirement to be met with audit logging enabled")
	}

	req = ComplianceRequirement{Category: "unknown_category"}
	if engine.checkRequirement(req) {
		t.Error("expected unknown category requirement to not be met")
	}

	// With disabled config
	disabledCfg := CompliancePacksConfig{Enabled: false, AuditLogEnabled: false}
	engine2 := NewCompliancePacksEngine(db, disabledCfg)
	req = ComplianceRequirement{Category: "encryption"}
	if engine2.checkRequirement(req) {
		t.Error("expected encryption requirement to not be met with disabled config")
	}
}

func TestCompliancePacksListPacks(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	engine := NewCompliancePacksEngine(db, DefaultCompliancePacksConfig())
	packs := engine.ListPacks()
	if len(packs) != 5 {
		t.Errorf("expected 5 packs, got %d", len(packs))
	}
}

func TestCompliancePacksStats(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	engine := NewCompliancePacksEngine(db, DefaultCompliancePacksConfig())

	// Initial stats
	stats := engine.Stats()
	if stats.EnabledPacks != 0 {
		t.Errorf("expected 0 enabled packs, got %d", stats.EnabledPacks)
	}
	if stats.TotalRequirements == 0 {
		t.Error("expected non-zero total requirements")
	}

	// Enable and validate a pack
	_ = engine.EnablePack(ComplianceHIPAA)
	_, _ = engine.ValidatePack(ComplianceHIPAA)

	stats = engine.Stats()
	if stats.EnabledPacks != 1 {
		t.Errorf("expected 1 enabled pack, got %d", stats.EnabledPacks)
	}
	if stats.AuditLogEntries == 0 {
		t.Error("expected non-zero audit log entries")
	}
	if stats.LastValidation.IsZero() {
		t.Error("expected LastValidation to be set")
	}

	// Add classification
	_ = engine.ClassifyMetric(DataClassification{
		MetricName:     "test.metric",
		Classification: "internal",
	})
	stats = engine.Stats()
	if stats.ClassifiedMetrics != 1 {
		t.Errorf("expected 1 classified metric, got %d", stats.ClassifiedMetrics)
	}
}

func TestCompliancePacksAuditLogOnEnableDisable(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	engine := NewCompliancePacksEngine(db, DefaultCompliancePacksConfig())

	_ = engine.EnablePack(ComplianceSOC2)
	_ = engine.DisablePack(ComplianceSOC2)

	entries := engine.GetAuditLog(time.Time{}, 100)
	if len(entries) < 2 {
		t.Errorf("expected at least 2 audit entries, got %d", len(entries))
	}
}
