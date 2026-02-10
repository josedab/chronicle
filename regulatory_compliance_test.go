package chronicle

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNewRegulatoryComplianceEngine(t *testing.T) {
	cfg := DefaultRegulatoryComplianceConfig()
	engine := NewRegulatoryComplianceEngine(nil, cfg)

	if engine == nil {
		t.Fatal("expected non-nil engine")
	}
	if len(engine.config.EnabledFrameworks) != 3 {
		t.Errorf("expected 3 enabled frameworks, got %d", len(engine.config.EnabledFrameworks))
	}

	// Verify controls were initialized for enabled frameworks
	for _, fw := range cfg.EnabledFrameworks {
		controls := engine.GetControls(fw)
		if len(controls) == 0 {
			t.Errorf("expected controls for framework %s", fw)
		}
	}
}

func TestRegulatoryComplianceAuditLog(t *testing.T) {
	engine := NewRegulatoryComplianceEngine(nil, DefaultRegulatoryComplianceConfig())

	err := engine.LogAudit(RegulatoryAuditEntry{
		Actor:     "admin",
		Action:    "read",
		Resource:  "metrics/cpu",
		IPAddress: "10.0.0.1",
		SessionID: "sess-1",
		Details:   map[string]string{"reason": "monitoring"},
	})
	if err != nil {
		t.Fatalf("LogAudit failed: %v", err)
	}

	err = engine.LogAudit(RegulatoryAuditEntry{
		Actor:    "user1",
		Action:   "write",
		Resource: "metrics/memory",
	})
	if err != nil {
		t.Fatalf("LogAudit second entry failed: %v", err)
	}

	entries := engine.GetAuditLog(time.Time{}, 0)
	if len(entries) != 2 {
		t.Errorf("expected 2 audit entries, got %d", len(entries))
	}

	// Verify hash chain
	if entries[0].Hash == "" {
		t.Error("expected non-empty hash on first entry")
	}
	if entries[1].Hash == "" {
		t.Error("expected non-empty hash on second entry")
	}
	if entries[0].Hash == entries[1].Hash {
		t.Error("expected different hashes for different entries")
	}

	// Test missing actor
	err = engine.LogAudit(RegulatoryAuditEntry{Action: "read", Resource: "x"})
	if err == nil {
		t.Error("expected error for missing actor")
	}

	// Test missing action
	err = engine.LogAudit(RegulatoryAuditEntry{Actor: "admin", Resource: "x"})
	if err == nil {
		t.Error("expected error for missing action")
	}

	// Test limit
	limited := engine.GetAuditLog(time.Time{}, 1)
	if len(limited) != 1 {
		t.Errorf("expected 1 entry with limit, got %d", len(limited))
	}

	// Test since filter
	future := time.Now().Add(time.Hour)
	filtered := engine.GetAuditLog(future, 0)
	if len(filtered) != 0 {
		t.Errorf("expected 0 entries from future, got %d", len(filtered))
	}
}

func TestRegulatoryComplianceAuditIntegrity(t *testing.T) {
	engine := NewRegulatoryComplianceEngine(nil, DefaultRegulatoryComplianceConfig())

	// Empty log should be valid
	valid, err := engine.VerifyAuditIntegrity()
	if err != nil || !valid {
		t.Fatal("expected empty audit log to be valid")
	}

	// Add entries
	for i := 0; i < 5; i++ {
		engine.LogAudit(RegulatoryAuditEntry{
			Actor:    "admin",
			Action:   "read",
			Resource: "test",
		})
	}

	valid, err = engine.VerifyAuditIntegrity()
	if err != nil {
		t.Fatalf("VerifyAuditIntegrity error: %v", err)
	}
	if !valid {
		t.Error("expected audit log to be valid")
	}

	// Tamper with the log
	engine.mu.Lock()
	if len(engine.auditLog) > 2 {
		engine.auditLog[2].Hash = "tampered"
	}
	engine.mu.Unlock()

	valid, err = engine.VerifyAuditIntegrity()
	if valid {
		t.Error("expected tampered audit log to be invalid")
	}
	if err == nil {
		t.Error("expected error for tampered audit log")
	}
}

func TestRegulatoryComplianceDataLineage(t *testing.T) {
	engine := NewRegulatoryComplianceEngine(nil, DefaultRegulatoryComplianceConfig())

	err := engine.RecordLineage(DataLineageRecord{
		DataID:        "dataset-1",
		Operation:     DataOpCreate,
		Actor:         "etl-pipeline",
		Source:        "raw-ingest",
		Destination:   "processed-store",
		Justification: "Initial data ingest",
	})
	if err != nil {
		t.Fatalf("RecordLineage failed: %v", err)
	}

	err = engine.RecordLineage(DataLineageRecord{
		DataID:    "dataset-1",
		Operation: DataOpUpdate,
		Actor:     "transform-job",
		Source:    "processed-store",
	})
	if err != nil {
		t.Fatalf("RecordLineage second entry failed: %v", err)
	}

	// Record for different data
	engine.RecordLineage(DataLineageRecord{
		DataID:    "dataset-2",
		Operation: DataOpRead,
		Actor:     "analyst",
	})

	records := engine.GetLineage("dataset-1")
	if len(records) != 2 {
		t.Errorf("expected 2 lineage records for dataset-1, got %d", len(records))
	}

	records2 := engine.GetLineage("dataset-2")
	if len(records2) != 1 {
		t.Errorf("expected 1 lineage record for dataset-2, got %d", len(records2))
	}

	// Test missing data_id
	err = engine.RecordLineage(DataLineageRecord{Operation: DataOpCreate, Actor: "test"})
	if err == nil {
		t.Error("expected error for missing data_id")
	}

	// Test non-existent data
	records3 := engine.GetLineage("nonexistent")
	if len(records3) != 0 {
		t.Errorf("expected 0 lineage records for nonexistent, got %d", len(records3))
	}
}

func TestRegulatoryComplianceAssessment(t *testing.T) {
	cfg := DefaultRegulatoryComplianceConfig()
	engine := NewRegulatoryComplianceEngine(nil, cfg)

	// Add some audit data to influence assessment
	engine.LogAudit(RegulatoryAuditEntry{
		Actor:  "admin",
		Action: "config",
	})
	engine.RecordLineage(DataLineageRecord{
		DataID:    "data-1",
		Operation: DataOpCreate,
		Actor:     "system",
	})
	engine.AddRetentionPolicy(DataRetentionPolicy{
		Framework:    FrameworkGDPR,
		DataType:     "user_data",
		MinRetention: 30 * 24 * time.Hour,
		MaxRetention: 365 * 24 * time.Hour,
	})

	report, err := engine.AssessCompliance(FrameworkHIPAA)
	if err != nil {
		t.Fatalf("AssessCompliance failed: %v", err)
	}
	if report.Framework != FrameworkHIPAA {
		t.Errorf("expected HIPAA framework, got %s", report.Framework)
	}
	if report.ControlsAssessed == 0 {
		t.Error("expected controls to be assessed")
	}
	if report.Score < 0 || report.Score > 100 {
		t.Errorf("expected score between 0-100, got %.1f", report.Score)
	}
	if report.Summary == "" {
		t.Error("expected non-empty summary")
	}

	// Assess non-enabled framework
	_, err = engine.AssessCompliance(ComplianceFramework("UNKNOWN"))
	if err == nil {
		t.Error("expected error for unknown framework")
	}
}

func TestRegulatoryComplianceViolations(t *testing.T) {
	cfg := DefaultRegulatoryComplianceConfig()
	cfg.EncryptionRequired = false
	cfg.ImmutableAuditTrail = false
	engine := NewRegulatoryComplianceEngine(nil, cfg)

	// Assess without audit data to trigger violations
	report, err := engine.AssessCompliance(FrameworkHIPAA)
	if err != nil {
		t.Fatalf("AssessCompliance failed: %v", err)
	}

	violations := engine.GetViolations(FrameworkHIPAA)
	if len(violations) == 0 && len(report.Violations) == 0 {
		t.Error("expected violations when no controls are met")
	}

	// Verify violations are tracked
	if len(report.Violations) > 0 {
		v := report.Violations[0]
		if v.Framework != FrameworkHIPAA {
			t.Errorf("expected HIPAA framework on violation, got %s", v.Framework)
		}
		if v.Status != ViolationOpen {
			t.Errorf("expected open status, got %s", v.Status)
		}
	}
}

func TestRegulatoryComplianceRemediation(t *testing.T) {
	cfg := DefaultRegulatoryComplianceConfig()
	cfg.EncryptionRequired = false
	cfg.ImmutableAuditTrail = false
	engine := NewRegulatoryComplianceEngine(nil, cfg)

	// Generate violations
	engine.AssessCompliance(FrameworkHIPAA)

	violations := engine.GetViolations(FrameworkHIPAA)
	if len(violations) == 0 {
		t.Skip("no violations to remediate")
	}

	vid := violations[0].ID
	err := engine.RemediateViolation(vid)
	if err != nil {
		t.Fatalf("RemediateViolation failed: %v", err)
	}

	// Verify status changed
	found := false
	for _, v := range engine.GetViolations(FrameworkHIPAA) {
		if v.ID == vid {
			found = true
			if v.Status != ViolationRemediated {
				t.Errorf("expected remediated status, got %s", v.Status)
			}
			if !v.AutoRemediated {
				t.Error("expected auto_remediated to be true")
			}
		}
	}
	if !found {
		t.Error("expected to find remediated violation")
	}

	// Remediate non-existent
	err = engine.RemediateViolation("nonexistent")
	if err == nil {
		t.Error("expected error for non-existent violation")
	}
}

func TestRegulatoryComplianceExemption(t *testing.T) {
	cfg := DefaultRegulatoryComplianceConfig()
	cfg.EncryptionRequired = false
	cfg.ImmutableAuditTrail = false
	engine := NewRegulatoryComplianceEngine(nil, cfg)

	engine.AssessCompliance(FrameworkHIPAA)

	violations := engine.GetViolations(FrameworkHIPAA)
	if len(violations) == 0 {
		t.Skip("no violations to exempt")
	}

	vid := violations[0].ID

	// Missing justification
	err := engine.ExemptViolation(vid, "")
	if err == nil {
		t.Error("expected error for missing justification")
	}

	// Valid exemption
	err = engine.ExemptViolation(vid, "Compensating control in place")
	if err != nil {
		t.Fatalf("ExemptViolation failed: %v", err)
	}

	// Verify status
	for _, v := range engine.GetViolations(FrameworkHIPAA) {
		if v.ID == vid {
			if v.Status != ViolationExempted {
				t.Errorf("expected exempted status, got %s", v.Status)
			}
			if v.Justification != "Compensating control in place" {
				t.Errorf("unexpected justification: %s", v.Justification)
			}
		}
	}

	// Non-existent violation
	err = engine.ExemptViolation("nonexistent", "reason")
	if err == nil {
		t.Error("expected error for non-existent violation")
	}
}

func TestRegulatoryComplianceReport(t *testing.T) {
	cfg := DefaultRegulatoryComplianceConfig()
	engine := NewRegulatoryComplianceEngine(nil, cfg)

	engine.LogAudit(RegulatoryAuditEntry{Actor: "admin", Action: "read"})

	report, err := engine.GenerateReport(FrameworkSOC2)
	if err != nil {
		t.Fatalf("GenerateReport failed: %v", err)
	}
	if report.Framework != FrameworkSOC2 {
		t.Errorf("expected SOC2 framework, got %s", report.Framework)
	}
	if report.ID == "" {
		t.Error("expected non-empty report ID")
	}
	if report.GeneratedAt.IsZero() {
		t.Error("expected non-zero generated_at")
	}
	if report.PeriodStart.IsZero() || report.PeriodEnd.IsZero() {
		t.Error("expected non-zero period start/end")
	}
	if report.ControlsAssessed == 0 {
		t.Error("expected assessed controls")
	}

	// Non-enabled framework
	_, err = engine.GenerateReport(ComplianceFramework("UNKNOWN"))
	if err == nil {
		t.Error("expected error for unknown framework")
	}
}

func TestRegulatoryComplianceRetention(t *testing.T) {
	engine := NewRegulatoryComplianceEngine(nil, DefaultRegulatoryComplianceConfig())

	err := engine.AddRetentionPolicy(DataRetentionPolicy{
		Framework:      FrameworkHIPAA,
		DataType:       "patient_records",
		MinRetention:   6 * 365 * 24 * time.Hour,
		MaxRetention:   10 * 365 * 24 * time.Hour,
		DeletionMethod: "secure_erase",
		Justification:  "HIPAA requires 6 year minimum retention",
	})
	if err != nil {
		t.Fatalf("AddRetentionPolicy failed: %v", err)
	}

	err = engine.AddRetentionPolicy(DataRetentionPolicy{
		Framework:    FrameworkGDPR,
		DataType:     "user_data",
		MinRetention: 30 * 24 * time.Hour,
		MaxRetention: 365 * 24 * time.Hour,
	})
	if err != nil {
		t.Fatalf("AddRetentionPolicy second policy failed: %v", err)
	}

	// Missing data type
	err = engine.AddRetentionPolicy(DataRetentionPolicy{MinRetention: time.Hour})
	if err == nil {
		t.Error("expected error for missing data_type")
	}

	// Zero min retention
	err = engine.AddRetentionPolicy(DataRetentionPolicy{DataType: "test"})
	if err == nil {
		t.Error("expected error for zero min_retention")
	}

	count, err := engine.EnforceRetention()
	if err != nil {
		t.Fatalf("EnforceRetention failed: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 enforcement actions, got %d", count)
	}
}

func TestRegulatoryComplianceAccessReview(t *testing.T) {
	engine := NewRegulatoryComplianceEngine(nil, DefaultRegulatoryComplianceConfig())

	review, err := engine.CreateAccessReview("alice")
	if err != nil {
		t.Fatalf("CreateAccessReview failed: %v", err)
	}
	if review.User != "alice" {
		t.Errorf("expected user alice, got %s", review.User)
	}
	if review.ID == "" {
		t.Error("expected non-empty review ID")
	}
	if len(review.Permissions) == 0 {
		t.Error("expected default permissions")
	}

	_, err = engine.CreateAccessReview("bob")
	if err != nil {
		t.Fatalf("CreateAccessReview second review failed: %v", err)
	}

	reviews := engine.ListAccessReviews()
	if len(reviews) != 2 {
		t.Errorf("expected 2 reviews, got %d", len(reviews))
	}

	// Empty user
	_, err = engine.CreateAccessReview("")
	if err == nil {
		t.Error("expected error for empty user")
	}
}

func TestRegulatoryComplianceStats(t *testing.T) {
	cfg := DefaultRegulatoryComplianceConfig()
	engine := NewRegulatoryComplianceEngine(nil, cfg)

	// Initial stats
	stats := engine.Stats()
	if stats.FrameworksEnabled != 3 {
		t.Errorf("expected 3 frameworks, got %d", stats.FrameworksEnabled)
	}
	if stats.TotalControls == 0 {
		t.Error("expected non-zero total controls")
	}
	if stats.AuditEntries != 0 {
		t.Errorf("expected 0 audit entries initially, got %d", stats.AuditEntries)
	}

	// Add data
	engine.LogAudit(RegulatoryAuditEntry{Actor: "admin", Action: "read"})
	engine.RecordLineage(DataLineageRecord{DataID: "d1", Operation: DataOpCreate, Actor: "sys"})
	engine.AddRetentionPolicy(DataRetentionPolicy{
		Framework:    FrameworkGDPR,
		DataType:     "logs",
		MinRetention: time.Hour,
	})
	engine.AssessCompliance(FrameworkHIPAA)

	stats = engine.Stats()
	if stats.AuditEntries != 1 {
		t.Errorf("expected 1 audit entry, got %d", stats.AuditEntries)
	}
	if stats.LineageRecords != 1 {
		t.Errorf("expected 1 lineage record, got %d", stats.LineageRecords)
	}
	if stats.ReportsGenerated != 1 {
		t.Errorf("expected 1 report, got %d", stats.ReportsGenerated)
	}
	if stats.LastAssessment.IsZero() {
		t.Error("expected non-zero last assessment time")
	}
}

func TestRegulatoryComplianceHTTPHandlers(t *testing.T) {
	cfg := DefaultRegulatoryComplianceConfig()
	engine := NewRegulatoryComplianceEngine(nil, cfg)

	engine.LogAudit(RegulatoryAuditEntry{Actor: "admin", Action: "test"})

	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	tests := []struct {
		name   string
		path   string
		status int
	}{
		{"audit", "/api/v1/regulatory/audit", http.StatusOK},
		{"violations", "/api/v1/regulatory/violations?framework=HIPAA", http.StatusOK},
		{"assess", "/api/v1/regulatory/assess?framework=HIPAA", http.StatusOK},
		{"reports", "/api/v1/regulatory/reports?framework=SOC2", http.StatusOK},
		{"controls", "/api/v1/regulatory/controls?framework=GDPR", http.StatusOK},
		{"stats", "/api/v1/regulatory/stats", http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)

			if w.Code != tt.status {
				t.Errorf("expected status %d, got %d", tt.status, w.Code)
			}
			if w.Header().Get("Content-Type") != "application/json" {
				t.Errorf("expected application/json content type, got %s", w.Header().Get("Content-Type"))
			}

			// Verify valid JSON
			var result any
			if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
				t.Errorf("invalid JSON response: %v", err)
			}
		})
	}

	// Test assess with unknown framework
	req := httptest.NewRequest(http.MethodGet, "/api/v1/regulatory/assess?framework=UNKNOWN", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for unknown framework, got %d", w.Code)
	}
}
