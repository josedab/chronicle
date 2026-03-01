package chronicle

import (
	"testing"
	"time"
)

func newTestConfidentialEngine(t *testing.T) (*ConfidentialEngine, *DB) {
	t.Helper()
	db := setupTestDB(t)
	config := DefaultConfidentialConfig()
	config.TEEType = TEETypeSoftware
	config.AttestationEnabled = true
	config.AttestationInterval = time.Hour
	config.VerifyBeforeDecrypt = true
	engine, err := NewConfidentialEngine(db, config)
	if err != nil {
		t.Fatalf("Failed to create confidential engine: %v", err)
	}
	// Start to enable attestation so UnsealData works.
	if err := engine.Start(); err != nil {
		t.Fatalf("Failed to start confidential engine: %v", err)
	}
	t.Cleanup(func() { engine.Stop() })
	return engine, db
}

func TestSealDataUnsealDataRoundtrip(t *testing.T) {
	engine, _ := newTestConfidentialEngine(t)

	original := []byte("sensitive time-series data: cpu_usage=99.5")
	sealed, err := engine.SealData(original)
	if err != nil {
		t.Fatalf("SealData failed: %v", err)
	}

	if len(sealed) == 0 {
		t.Fatal("Sealed data is empty")
	}

	unsealed, err := engine.UnsealData(sealed)
	if err != nil {
		t.Fatalf("UnsealData failed: %v", err)
	}

	if string(unsealed) != string(original) {
		t.Errorf("Roundtrip mismatch: got %q, want %q", string(unsealed), string(original))
	}
}

func TestSealDataEmptyInput(t *testing.T) {
	engine, _ := newTestConfidentialEngine(t)

	sealed, err := engine.SealData([]byte{})
	if err != nil {
		t.Fatalf("SealData with empty input failed: %v", err)
	}

	unsealed, err := engine.UnsealData(sealed)
	if err != nil {
		t.Fatalf("UnsealData with empty input failed: %v", err)
	}

	if len(unsealed) != 0 {
		t.Errorf("Expected empty unsealed data, got %d bytes", len(unsealed))
	}
}

func TestSealDataLargePayload(t *testing.T) {
	engine, _ := newTestConfidentialEngine(t)

	// 1MB payload
	original := make([]byte, 1024*1024)
	for i := range original {
		original[i] = byte(i % 256)
	}

	sealed, err := engine.SealData(original)
	if err != nil {
		t.Fatalf("SealData large payload failed: %v", err)
	}

	unsealed, err := engine.UnsealData(sealed)
	if err != nil {
		t.Fatalf("UnsealData large payload failed: %v", err)
	}

	if len(unsealed) != len(original) {
		t.Errorf("Length mismatch: got %d, want %d", len(unsealed), len(original))
	}
}

func TestUnsealDataRequiresAttestation(t *testing.T) {
	db := setupTestDB(t)
	config := DefaultConfidentialConfig()
	config.TEEType = TEETypeSoftware
	config.AttestationEnabled = false
	config.VerifyBeforeDecrypt = true
	engine, err := NewConfidentialEngine(db, config)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Stop()

	sealed, err := engine.SealData([]byte("data"))
	if err != nil {
		t.Fatalf("SealData failed: %v", err)
	}

	// Without attestation, unseal should fail when VerifyBeforeDecrypt is true.
	_, err = engine.UnsealData(sealed)
	if err == nil {
		t.Error("Expected error when unsealing without attestation")
	}
}

func TestUnsealDataWorksAfterAttestation(t *testing.T) {
	engine, _ := newTestConfidentialEngine(t)

	sealed, err := engine.SealData([]byte("protected"))
	if err != nil {
		t.Fatalf("SealData failed: %v", err)
	}

	unsealed, err := engine.UnsealData(sealed)
	if err != nil {
		t.Fatalf("UnsealData after attestation failed: %v", err)
	}

	if string(unsealed) != "protected" {
		t.Errorf("Expected 'protected', got %q", string(unsealed))
	}
}

func TestGetAttestationBeforeStart(t *testing.T) {
	db := setupTestDB(t)
	config := DefaultConfidentialConfig()
	config.AttestationEnabled = false
	engine, err := NewConfidentialEngine(db, config)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Stop()

	_, err = engine.GetAttestation()
	if err == nil {
		t.Error("Expected error getting attestation before start")
	}
}

func TestGetAttestationAfterStart(t *testing.T) {
	engine, _ := newTestConfidentialEngine(t)

	attestation, err := engine.GetAttestation()
	if err != nil {
		t.Fatalf("GetAttestation failed: %v", err)
	}
	if attestation == nil {
		t.Error("Expected non-nil attestation")
	}
}

func TestVerifyRemoteAttestationValid(t *testing.T) {
	engine, _ := newTestConfidentialEngine(t)

	// Get our own attestation and verify it.
	attestation, err := engine.GetAttestation()
	if err != nil {
		t.Fatalf("GetAttestation failed: %v", err)
	}

	err = engine.VerifyRemoteAttestation(attestation)
	if err != nil {
		t.Errorf("VerifyRemoteAttestation failed for valid attestation: %v", err)
	}
}

func TestVerifyRemoteAttestationInvalid(t *testing.T) {
	engine, _ := newTestConfidentialEngine(t)

	// Fabricate invalid attestation.
	invalid := &Attestation{
		TEEType:     TEETypeSGX,
		Measurement: []byte("tampered"),
		Signature:   []byte("invalid"),
		Timestamp:   time.Now().Add(-48 * time.Hour), // expired
	}

	err := engine.VerifyRemoteAttestation(invalid)
	if err == nil {
		t.Error("Expected error for invalid attestation")
	}
}

func TestIsOperationAllowed(t *testing.T) {
	engine, _ := newTestConfidentialEngine(t)

	// Default allows Read, Write, Query, Aggregate.
	if !engine.isOperationAllowed(OpRead) {
		t.Error("OpRead should be allowed")
	}
	if !engine.isOperationAllowed(OpWrite) {
		t.Error("OpWrite should be allowed")
	}
	if !engine.isOperationAllowed(OpQuery) {
		t.Error("OpQuery should be allowed")
	}
	if !engine.isOperationAllowed(OpAggregate) {
		t.Error("OpAggregate should be allowed")
	}
	if engine.isOperationAllowed(OpExport) {
		t.Error("OpExport should not be allowed by default")
	}
	if engine.isOperationAllowed(OpDecrypt) {
		t.Error("OpDecrypt should not be allowed by default")
	}
}

func TestSecureWriteDisallowedOperation(t *testing.T) {
	db := setupTestDB(t)
	config := DefaultConfidentialConfig()
	config.AllowedOperations = []ConfidentialOp{OpRead, OpQuery}
	engine, err := NewConfidentialEngine(db, config)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Stop()

	err = engine.SecureWrite("cpu", nil, map[string]any{"usage": 50.0}, time.Now())
	if err == nil {
		t.Error("Expected error when write operation is not allowed")
	}
}

func TestSecureQueryDisallowedOperation(t *testing.T) {
	db := setupTestDB(t)
	config := DefaultConfidentialConfig()
	config.AllowedOperations = []ConfidentialOp{OpRead, OpWrite}
	engine, err := NewConfidentialEngine(db, config)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Stop()

	_, err = engine.SecureQuery("SELECT * FROM cpu")
	if err == nil {
		t.Error("Expected error when query operation is not allowed")
	}
}

func TestIsHardwareTEESoftware(t *testing.T) {
	engine, _ := newTestConfidentialEngine(t)

	if engine.IsHardwareTEE() {
		t.Error("Software TEE should not report as hardware TEE")
	}
}

func TestGetMeasurement(t *testing.T) {
	engine, _ := newTestConfidentialEngine(t)

	measurement, err := engine.GetMeasurement()
	if err != nil {
		t.Fatalf("GetMeasurement failed: %v", err)
	}
	if len(measurement) == 0 {
		t.Error("Expected non-empty measurement")
	}
}

func TestConfidentialStats(t *testing.T) {
	engine, _ := newTestConfidentialEngine(t)

	stats := engine.Stats()
	if stats.TEEType != "software" {
		t.Errorf("Expected software TEE type, got %s", stats.TEEType)
	}
	if stats.HardwareTEE {
		t.Error("Expected no hardware TEE")
	}
	if !stats.HasAttestation {
		t.Error("Expected attestation after Start()")
	}
}

func TestConfidentialEngineStartStop(t *testing.T) {
	engine, _ := newTestConfidentialEngine(t)

	if err := engine.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if err := engine.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
}
