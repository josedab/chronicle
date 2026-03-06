package chronicle

import (
	"bytes"
	"testing"
	"time"
)

func newEdgeTestConfidentialEngine(t *testing.T) *ConfidentialEngine {
	t.Helper()
	config := DefaultConfidentialConfig()
	config.TEEType = TEETypeSoftware
	config.AttestationEnabled = true
	config.AttestationInterval = time.Hour
	config.VerifyBeforeDecrypt = false // Disable for basic tests
	engine, err := NewConfidentialEngine(nil, config)
	if err != nil {
		t.Fatalf("Failed to create confidential engine: %v", err)
	}
	return engine
}

func TestSealUnsealRoundtrip(t *testing.T) {
	engine := newEdgeTestConfidentialEngine(t)
	defer engine.Stop()

	testData := []byte("sensitive time-series data")
	sealed, err := engine.SealData(testData)
	if err != nil {
		t.Fatalf("SealData failed: %v", err)
	}

	if bytes.Equal(sealed, testData) {
		t.Error("Sealed data should differ from original")
	}

	unsealed, err := engine.UnsealData(sealed)
	if err != nil {
		t.Fatalf("UnsealData failed: %v", err)
	}

	if !bytes.Equal(unsealed, testData) {
		t.Errorf("Unsealed data mismatch: got %q, want %q", unsealed, testData)
	}
}

func TestSealUnsealEmptyData(t *testing.T) {
	engine := newEdgeTestConfidentialEngine(t)
	defer engine.Stop()

	sealed, err := engine.SealData([]byte{})
	if err != nil {
		t.Fatalf("SealData empty: %v", err)
	}

	unsealed, err := engine.UnsealData(sealed)
	if err != nil {
		t.Fatalf("UnsealData empty: %v", err)
	}

	if len(unsealed) != 0 {
		t.Errorf("Expected empty data, got %d bytes", len(unsealed))
	}
}

func TestSealUnsealLargeData(t *testing.T) {
	engine := newEdgeTestConfidentialEngine(t)
	defer engine.Stop()

	largeData := make([]byte, 64*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	sealed, err := engine.SealData(largeData)
	if err != nil {
		t.Fatalf("SealData large: %v", err)
	}

	unsealed, err := engine.UnsealData(sealed)
	if err != nil {
		t.Fatalf("UnsealData large: %v", err)
	}

	if !bytes.Equal(unsealed, largeData) {
		t.Error("Large data roundtrip mismatch")
	}
}

func TestIsOperationAllowedRestricted(t *testing.T) {
	config := DefaultConfidentialConfig()
	config.AllowedOperations = []ConfidentialOp{OpRead, OpWrite, OpQuery}
	config.VerifyBeforeDecrypt = false
	engine, err := NewConfidentialEngine(nil, config)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Stop()

	if !engine.isOperationAllowed(OpRead) {
		t.Error("OpRead should be allowed")
	}
	if !engine.isOperationAllowed(OpWrite) {
		t.Error("OpWrite should be allowed")
	}
	if !engine.isOperationAllowed(OpQuery) {
		t.Error("OpQuery should be allowed")
	}
	if engine.isOperationAllowed(OpExport) {
		t.Error("OpExport should NOT be allowed")
	}
	if engine.isOperationAllowed(OpDecrypt) {
		t.Error("OpDecrypt should NOT be allowed")
	}
	if engine.isOperationAllowed(OpAggregate) {
		t.Error("OpAggregate should NOT be allowed")
	}
}

func TestIsOperationAllowedEmptyList(t *testing.T) {
	config := DefaultConfidentialConfig()
	config.AllowedOperations = []ConfidentialOp{}
	config.VerifyBeforeDecrypt = false
	engine, err := NewConfidentialEngine(nil, config)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Stop()

	if engine.isOperationAllowed(OpRead) {
		t.Error("No operations should be allowed with empty list")
	}
}

func TestGetAttestationBeforeStartEdge(t *testing.T) {
	engine := newEdgeTestConfidentialEngine(t)
	defer engine.Stop()

	// Before Start(), there's no attestation
	_, err := engine.GetAttestation()
	if err == nil {
		t.Error("Expected error when getting attestation before Start")
	}
}

func TestGetAttestationAfterStartEdge(t *testing.T) {
	config := DefaultConfidentialConfig()
	config.AttestationEnabled = true
	config.AttestationInterval = time.Hour
	config.VerifyBeforeDecrypt = false
	engine, err := NewConfidentialEngine(nil, config)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Stop()

	if err := engine.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	attestation, err := engine.GetAttestation()
	if err != nil {
		t.Fatalf("GetAttestation after Start failed: %v", err)
	}
	if attestation == nil {
		t.Error("Attestation should not be nil after Start")
	}
}

func TestVerifyRemoteAttestation(t *testing.T) {
	engine := newEdgeTestConfidentialEngine(t)
	defer engine.Stop()

	if err := engine.Start(); err != nil {
		t.Fatal(err)
	}

	attestation, err := engine.GetAttestation()
	if err != nil {
		t.Fatal(err)
	}

	// Verify our own attestation
	err = engine.VerifyRemoteAttestation(attestation)
	if err != nil {
		t.Errorf("VerifyRemoteAttestation failed: %v", err)
	}
}

func TestIsHardwareTEE_Software(t *testing.T) {
	engine := newEdgeTestConfidentialEngine(t)
	defer engine.Stop()

	if engine.IsHardwareTEE() {
		t.Error("Software TEE should not report as hardware TEE")
	}
}

func TestGetMeasurementEdge(t *testing.T) {
	engine := newEdgeTestConfidentialEngine(t)
	defer engine.Stop()

	measurement, err := engine.GetMeasurement()
	if err != nil {
		t.Fatalf("GetMeasurement failed: %v", err)
	}
	if len(measurement) == 0 {
		t.Error("Measurement should not be empty")
	}
}

func TestUnsealDataWithVerifyBeforeDecrypt(t *testing.T) {
	config := DefaultConfidentialConfig()
	config.VerifyBeforeDecrypt = true
	config.AttestationEnabled = true
	config.AttestationInterval = time.Hour
	engine, err := NewConfidentialEngine(nil, config)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Stop()

	// Seal some data
	testData := []byte("protected data")
	sealed, err := engine.SealData(testData)
	if err != nil {
		t.Fatal(err)
	}

	// Without Start(), there's no attestation, so UnsealData should fail
	_, err = engine.UnsealData(sealed)
	if err == nil {
		t.Error("UnsealData should fail when VerifyBeforeDecrypt is true and no attestation exists")
	}

	// After Start(), attestation is available
	if err := engine.Start(); err != nil {
		t.Fatal(err)
	}

	unsealed, err := engine.UnsealData(sealed)
	if err != nil {
		t.Fatalf("UnsealData should succeed after Start: %v", err)
	}
	if !bytes.Equal(unsealed, testData) {
		t.Error("Unsealed data mismatch")
	}
}

func TestConfidentialEngineStats(t *testing.T) {
	config := DefaultConfidentialConfig()
	config.AttestationEnabled = true
	config.VerifyBeforeDecrypt = false
	engine, err := NewConfidentialEngine(nil, config)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Stop()

	stats := engine.Stats()
	if stats.TEEType != "software" {
		t.Errorf("Expected software TEE type, got %s", stats.TEEType)
	}
	if stats.HardwareTEE {
		t.Error("Should not be hardware TEE")
	}
	if stats.HasAttestation {
		t.Error("Should not have attestation before Start")
	}

	engine.Start()
	stats = engine.Stats()
	if !stats.HasAttestation {
		t.Error("Should have attestation after Start")
	}
}

func TestConfidentialStartStop(t *testing.T) {
	config := DefaultConfidentialConfig()
	config.AttestationEnabled = true
	config.AttestationInterval = 50 * time.Millisecond
	config.VerifyBeforeDecrypt = false
	engine, err := NewConfidentialEngine(nil, config)
	if err != nil {
		t.Fatal(err)
	}

	if err := engine.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Let attestation loop run
	time.Sleep(100 * time.Millisecond)

	if err := engine.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestConfidentialStartWithoutAttestation(t *testing.T) {
	config := DefaultConfidentialConfig()
	config.AttestationEnabled = false
	config.VerifyBeforeDecrypt = false
	engine, err := NewConfidentialEngine(nil, config)
	if err != nil {
		t.Fatal(err)
	}

	if err := engine.Start(); err != nil {
		t.Fatalf("Start without attestation: %v", err)
	}
	if err := engine.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}
