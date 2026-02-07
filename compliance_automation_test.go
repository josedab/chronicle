package chronicle

import (
	"testing"
	"time"
)

func TestComplianceAutomationPIIDetection(t *testing.T) {
	ca := NewComplianceAutomation(nil, DefaultComplianceAutomationConfig())

	tags := map[string]string{
		"user_email": "test@example.com",
		"hostname":   "web-01",
		"ip":         "192.168.1.1",
	}

	results := ca.DetectPII(tags)
	if len(results) < 2 {
		t.Errorf("expected at least 2 PII detections (email + IP), got %d", len(results))
	}

	foundEmail := false
	foundIP := false
	for _, r := range results {
		if r.PIIType == "email" {
			foundEmail = true
		}
		if r.PIIType == "ip_address" {
			foundIP = true
		}
	}
	if !foundEmail {
		t.Error("expected email PII detection")
	}
	if !foundIP {
		t.Error("expected IP address PII detection")
	}
}

func TestComplianceAutomationPIIDisabled(t *testing.T) {
	cfg := DefaultComplianceAutomationConfig()
	cfg.PIIDetectionEnabled = false
	ca := NewComplianceAutomation(nil, cfg)

	results := ca.DetectPII(map[string]string{"email": "test@example.com"})
	if len(results) != 0 {
		t.Errorf("expected no detections when disabled, got %d", len(results))
	}
}

func TestComplianceAutomationRetentionPolicy(t *testing.T) {
	ca := NewComplianceAutomation(nil, DefaultComplianceAutomationConfig())

	policy := RetentionPolicy{
		ID:            "gdpr-30d",
		Name:          "GDPR 30-day retention",
		MetricPattern: "user.*",
		MaxAge:        30 * 24 * time.Hour,
		Standard:      ComplianceGDPR,
		DeleteAction:  "delete",
	}

	if err := ca.AddRetentionPolicy(policy); err != nil {
		t.Fatalf("AddRetentionPolicy failed: %v", err)
	}

	got := ca.GetRetentionPolicy("gdpr-30d")
	if got == nil {
		t.Fatal("expected policy to exist")
	}
	if got.MaxAge != 30*24*time.Hour {
		t.Errorf("unexpected max age: %v", got.MaxAge)
	}

	policies := ca.ListRetentionPolicies()
	if len(policies) != 1 {
		t.Errorf("expected 1 policy, got %d", len(policies))
	}
}

func TestComplianceAutomationRetentionValidation(t *testing.T) {
	ca := NewComplianceAutomation(nil, DefaultComplianceAutomationConfig())

	err := ca.AddRetentionPolicy(RetentionPolicy{MetricPattern: "*", MaxAge: time.Hour})
	if err == nil {
		t.Fatal("expected error for missing ID")
	}

	err = ca.AddRetentionPolicy(RetentionPolicy{ID: "p1", MetricPattern: "*"})
	if err == nil {
		t.Fatal("expected error for zero max age")
	}
}

func TestComplianceAutomationConsent(t *testing.T) {
	ca := NewComplianceAutomation(nil, DefaultComplianceAutomationConfig())

	ca.RecordConsent(ConsentRecord{
		SubjectID: "user-123",
		Purpose:   "analytics",
		Granted:   true,
		Source:    "web_form",
	})

	if !ca.HasConsent("user-123", "analytics") {
		t.Error("expected consent to be granted")
	}
	if ca.HasConsent("user-123", "marketing") {
		t.Error("expected no consent for marketing")
	}
}

func TestComplianceAutomationConsentExpiry(t *testing.T) {
	ca := NewComplianceAutomation(nil, DefaultComplianceAutomationConfig())

	ca.RecordConsent(ConsentRecord{
		SubjectID: "user-123",
		Purpose:   "analytics",
		Granted:   true,
		ExpiresAt: time.Now().Add(-time.Hour), // already expired
	})

	if ca.HasConsent("user-123", "analytics") {
		t.Error("expected expired consent to be invalid")
	}
}

func TestComplianceAutomationReport(t *testing.T) {
	ca := NewComplianceAutomation(nil, DefaultComplianceAutomationConfig())

	ca.AddRetentionPolicy(RetentionPolicy{ID: "p1", MaxAge: time.Hour * 24})
	ca.RecordConsent(ConsentRecord{SubjectID: "u1", Purpose: "analytics", Granted: true})
	ca.Engine().AuditTrail().Record("admin", "read", "metrics", "test", true)

	report := ca.GenerateComplianceReport(ComplianceGDPR)
	if report.Standard != ComplianceGDPR {
		t.Errorf("expected GDPR standard, got %s", report.Standard)
	}
	if report.Score <= 0 {
		t.Error("expected positive score")
	}
	if len(report.Categories) == 0 {
		t.Error("expected categories in report")
	}
}

func TestComplianceAutomationEnforceRetention(t *testing.T) {
	ca := NewComplianceAutomation(nil, DefaultComplianceAutomationConfig())

	ca.AddRetentionPolicy(RetentionPolicy{ID: "p1", MetricPattern: "*", MaxAge: time.Hour * 24})

	enforced := ca.EnforceRetention()
	if len(enforced) != 1 {
		t.Errorf("expected 1 enforced policy, got %d", len(enforced))
	}

	policy := ca.GetRetentionPolicy("p1")
	if policy.EnforcedCount != 1 {
		t.Errorf("expected enforced count 1, got %d", policy.EnforcedCount)
	}
}

func TestComplianceAutomationStats(t *testing.T) {
	ca := NewComplianceAutomation(nil, DefaultComplianceAutomationConfig())

	ca.DetectPII(map[string]string{"email": "test@example.com"})
	ca.AddRetentionPolicy(RetentionPolicy{ID: "p1", MaxAge: time.Hour})
	ca.RecordConsent(ConsentRecord{SubjectID: "u1", Purpose: "p", Granted: true})

	stats := ca.Stats()
	if stats.PIIScans != 1 {
		t.Errorf("expected 1 PII scan, got %d", stats.PIIScans)
	}
	if stats.RetentionPolicies != 1 {
		t.Errorf("expected 1 retention policy, got %d", stats.RetentionPolicies)
	}
	if stats.ConsentRecords != 1 {
		t.Errorf("expected 1 consent record, got %d", stats.ConsentRecords)
	}
}
