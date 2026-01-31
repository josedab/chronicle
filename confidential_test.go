package chronicle

import (
	"testing"
	"time"
)

func TestConfidentialComputingConfig(t *testing.T) {
	config := DefaultConfidentialConfig()

	if !config.Enabled {
		t.Error("Confidential computing should be enabled by default")
	}
}

func TestTEETypes(t *testing.T) {
	types := []TEEType{
		TEETypeSoftware,
		TEETypeSGX,
		TEETypeTrustZone,
		TEETypeSEV,
		TEETypeNitro,
	}

	for _, tee := range types {
		if tee.String() == "" {
			t.Errorf("TEE type %v should have string representation", tee)
		}
	}
}

func TestTEETypeString(t *testing.T) {
	tests := []struct {
		tee      TEEType
		expected string
	}{
		{TEETypeSoftware, "software"},
		{TEETypeSGX, "sgx"},
		{TEETypeTrustZone, "trustzone"},
		{TEETypeSEV, "sev"},
		{TEETypeNitro, "nitro"},
	}

	for _, tc := range tests {
		if tc.tee.String() != tc.expected {
			t.Errorf("Expected %s, got %s", tc.expected, tc.tee.String())
		}
	}
}

func TestAttestation(t *testing.T) {
	attestation := Attestation{
		TEEType:     TEETypeSGX,
		Measurement: []byte("mock-measurement"),
		Signature:   []byte("mock-signature"),
		UserData:    []byte("mock-user-data"),
		Timestamp:   time.Now(),
	}

	if attestation.TEEType != TEETypeSGX {
		t.Errorf("Expected SGX, got %v", attestation.TEEType)
	}
	if len(attestation.Measurement) == 0 {
		t.Error("Measurement should not be empty")
	}
	if attestation.Timestamp.IsZero() {
		t.Error("Timestamp should be set")
	}
}

func TestConfidentialEngine(t *testing.T) {
	config := DefaultConfidentialConfig()
	config.TEEType = TEETypeSoftware
	engine, err := NewConfidentialEngine(nil, config)

	if err != nil {
		t.Fatalf("Failed to create ConfidentialEngine: %v", err)
	}
	if engine == nil {
		t.Fatal("Engine should not be nil")
	}
}

func TestSoftwareTee(t *testing.T) {
	tee := NewSoftwareTee()

	if tee == nil {
		t.Fatal("Failed to create SoftwareTee")
	}

	// Test availability
	if !tee.IsAvailable() {
		t.Error("Software TEE should always be available")
	}
}

func TestSoftwareTeeAttestation(t *testing.T) {
	tee := NewSoftwareTee()

	// Initialize
	err := tee.Initialize()
	if err != nil {
		t.Fatalf("Failed to initialize: %v", err)
	}

	// Generate attestation
	attestation, err := tee.GetAttestation()
	if err != nil {
		t.Fatalf("Failed to generate attestation: %v", err)
	}

	if attestation.Measurement == nil {
		t.Error("Measurement should not be nil")
	}
}

func TestSoftwareTeeSealing(t *testing.T) {
	tee := NewSoftwareTee()

	err := tee.Initialize()
	if err != nil {
		t.Fatalf("Failed to initialize: %v", err)
	}

	// Test sealing
	plaintext := []byte("sensitive data")
	sealed, err := tee.SealData(plaintext)
	if err != nil {
		t.Fatalf("Failed to seal: %v", err)
	}

	// Test unsealing
	unsealed, err := tee.UnsealData(sealed)
	if err != nil {
		t.Fatalf("Failed to unseal: %v", err)
	}

	if string(unsealed) != string(plaintext) {
		t.Error("Unsealed data doesn't match original")
	}
}

func TestVerifyAttestation(t *testing.T) {
	tee := NewSoftwareTee()
	_ = tee.Initialize()

	attestation, err := tee.GetAttestation()
	if err != nil {
		t.Fatalf("Failed to generate attestation: %v", err)
	}

	// Verify
	err = tee.VerifyAttestation(attestation)
	if err != nil {
		t.Errorf("Attestation verification failed: %v", err)
	}
}

func TestConfidentialDBWrapper(t *testing.T) {
	config := DefaultConfidentialConfig()

	if !config.Enabled {
		t.Error("Config should be enabled")
	}
}
