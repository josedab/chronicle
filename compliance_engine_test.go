package chronicle

import (
	"crypto/rand"
	"fmt"
	"testing"
	"time"
)

func TestComplianceEngine_Standards(t *testing.T) {
	ce := NewComplianceEngine(ComplianceGDPR, ComplianceHIPAA)
	if !ce.IsEnabled(ComplianceGDPR) {
		t.Error("GDPR should be enabled")
	}
	if !ce.IsEnabled(ComplianceHIPAA) {
		t.Error("HIPAA should be enabled")
	}
	if ce.IsEnabled(ComplianceSOC2) {
		t.Error("SOC2 should not be enabled")
	}

	ce.EnableStandard(ComplianceSOC2)
	if !ce.IsEnabled(ComplianceSOC2) {
		t.Error("SOC2 should now be enabled")
	}

	standards := ce.Standards()
	if len(standards) != 3 {
		t.Errorf("standards = %d, want 3", len(standards))
	}
}

func TestFieldEncryptor_RoundTrip(t *testing.T) {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}

	fe, err := NewFieldEncryptor(key, "key-1")
	if err != nil {
		t.Fatal(err)
	}

	if fe.KeyID() != "key-1" {
		t.Errorf("keyID = %q", fe.KeyID())
	}

	plaintext := "sensitive-user-data"
	encrypted, err := fe.Encrypt(plaintext)
	if err != nil {
		t.Fatal(err)
	}

	if encrypted == plaintext {
		t.Error("encrypted should differ from plaintext")
	}

	decrypted, err := fe.Decrypt(encrypted)
	if err != nil {
		t.Fatal(err)
	}

	if decrypted != plaintext {
		t.Errorf("decrypted = %q, want %q", decrypted, plaintext)
	}
}

func TestFieldEncryptor_BadKeySize(t *testing.T) {
	_, err := NewFieldEncryptor([]byte("short"), "k1")
	if err == nil {
		t.Error("expected error for short key")
	}
}

func TestFieldEncryptor_SensitiveFields(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)
	fe, _ := NewFieldEncryptor(key, "k1")

	fe.AddSensitiveField("user_id")
	fe.AddSensitiveField("email")

	if !fe.IsSensitive("user_id") {
		t.Error("user_id should be sensitive")
	}
	if fe.IsSensitive("hostname") {
		t.Error("hostname should not be sensitive")
	}

	fields := fe.SensitiveFields()
	if len(fields) != 2 {
		t.Errorf("fields = %d, want 2", len(fields))
	}
}

func TestFieldEncryptor_DecryptBadHex(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)
	fe, _ := NewFieldEncryptor(key, "k1")

	_, err := fe.Decrypt("not-hex!!!")
	if err == nil {
		t.Error("expected error for bad hex")
	}
}

func TestFieldEncryptor_DecryptTooShort(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)
	fe, _ := NewFieldEncryptor(key, "k1")

	_, err := fe.Decrypt("aabb")
	if err == nil {
		t.Error("expected error for too-short ciphertext")
	}
}

func TestComplianceAuditTrail_Record(t *testing.T) {
	at := NewComplianceAuditTrail(100)

	at.Record("admin", "read", "cpu.usage", "queried data", true)
	at.Record("admin", "delete", "user.data", "erasure request", true)
	at.Record("system", "write", "logs", "wrote logs", true)

	if at.Count() != 3 {
		t.Errorf("count = %d, want 3", at.Count())
	}

	// Query by actor
	results := at.Query("admin", "", time.Time{})
	if len(results) != 2 {
		t.Errorf("admin results = %d, want 2", len(results))
	}

	// Query by action
	results = at.Query("", "delete", time.Time{})
	if len(results) != 1 {
		t.Errorf("delete results = %d, want 1", len(results))
	}
}

func TestComplianceAuditTrail_MaxSize(t *testing.T) {
	at := NewComplianceAuditTrail(5)

	for i := 0; i < 10; i++ {
		at.Record("user", "read", fmt.Sprintf("resource-%d", i), "", true)
	}

	if at.Count() != 5 {
		t.Errorf("count = %d, want 5", at.Count())
	}
}

func TestDSAREngine_SubmitAndComplete(t *testing.T) {
	engine := NewDSAREngine()

	req := engine.SubmitLookup("user-123", "user_id")
	if req.Type != DSARLookup {
		t.Errorf("type = %q", req.Type)
	}
	if req.Status != "pending" {
		t.Errorf("status = %q", req.Status)
	}

	pending := engine.PendingRequests()
	if len(pending) != 1 {
		t.Errorf("pending = %d", len(pending))
	}

	err := engine.CompleteRequest(req.ID, 42)
	if err != nil {
		t.Fatal(err)
	}

	pending = engine.PendingRequests()
	if len(pending) != 0 {
		t.Errorf("pending after complete = %d", len(pending))
	}

	completed := engine.GetRequest(req.ID)
	if completed.Status != "completed" {
		t.Errorf("status = %q", completed.Status)
	}
	if completed.ResultCount != 42 {
		t.Errorf("result count = %d", completed.ResultCount)
	}
}

func TestDSAREngine_SubmitExportAndErasure(t *testing.T) {
	engine := NewDSAREngine()

	export := engine.SubmitExport("user-456", "email")
	if export.Type != DSARExport {
		t.Error("wrong type")
	}

	erasure := engine.SubmitErasure("user-456", "email")
	if erasure.Type != DSARErasure {
		t.Error("wrong type")
	}

	all := engine.AllRequests()
	if len(all) != 2 {
		t.Errorf("all = %d", len(all))
	}
}

func TestDSAREngine_FailRequest(t *testing.T) {
	engine := NewDSAREngine()
	req := engine.SubmitLookup("user-1", "id")

	err := engine.FailRequest(req.ID, fmt.Errorf("not found"))
	if err != nil {
		t.Fatal(err)
	}

	failed := engine.GetRequest(req.ID)
	if failed.Status != "failed" {
		t.Errorf("status = %q", failed.Status)
	}
	if failed.Error != "not found" {
		t.Errorf("error = %q", failed.Error)
	}
}

func TestDSAREngine_NotFound(t *testing.T) {
	engine := NewDSAREngine()

	if engine.GetRequest("nonexistent") != nil {
		t.Error("expected nil")
	}

	err := engine.CompleteRequest("nonexistent", 0)
	if err == nil {
		t.Error("expected error")
	}

	err = engine.FailRequest("nonexistent", fmt.Errorf("x"))
	if err == nil {
		t.Error("expected error")
	}
}

func TestComplianceEngine_Residency(t *testing.T) {
	ce := NewComplianceEngine(ComplianceGDPR)
	ce.AddResidencyRule(DataResidencyRule{
		MetricPattern:  "user.data",
		AllowedRegions: []string{"eu-west-1", "eu-central-1"},
		Standard:       ComplianceGDPR,
	})

	// Compliant
	v := ce.CheckResidency("user.data", "eu-west-1")
	if v != nil {
		t.Error("should be compliant for eu-west-1")
	}

	// Non-compliant
	v = ce.CheckResidency("user.data", "us-east-1")
	if v == nil {
		t.Fatal("should violate for us-east-1")
	}
	if v.Region != "us-east-1" {
		t.Errorf("region = %q", v.Region)
	}

	// Unmatched metric
	v = ce.CheckResidency("cpu.usage", "us-east-1")
	if v != nil {
		t.Error("unmatched metric should pass")
	}
}

func TestComplianceEngine_Report(t *testing.T) {
	ce := NewComplianceEngine(ComplianceGDPR)

	report := ce.GenerateReport(ComplianceGDPR)
	if report.Standard != ComplianceGDPR {
		t.Errorf("standard = %q", report.Standard)
	}
	if len(report.Checks) != 4 {
		t.Errorf("checks = %d, want 4", len(report.Checks))
	}
	if report.PassRate < 0 || report.PassRate > 1 {
		t.Errorf("pass rate = %f", report.PassRate)
	}
}

func TestComplianceEngine_ReportWithEncryption(t *testing.T) {
	ce := NewComplianceEngine(ComplianceHIPAA)

	key := make([]byte, 32)
	rand.Read(key)
	fe, _ := NewFieldEncryptor(key, "k1")
	ce.SetFieldEncryptor(fe)
	ce.AddResidencyRule(DataResidencyRule{MetricPattern: "*", AllowedRegions: []string{"us-east-1"}})

	report := ce.GenerateReport(ComplianceHIPAA)
	// All 4 checks should pass now
	if report.PassRate != 1.0 {
		t.Errorf("pass rate = %f, want 1.0", report.PassRate)
	}
}
