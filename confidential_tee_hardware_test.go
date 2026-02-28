package chronicle

import (
	"testing"
)

func TestSGXHardwareTee_Initialize(t *testing.T) {
	tee := NewSGXHardwareTee(DefaultEnclaveConfig())
	if err := tee.Initialize(); err != nil {
		t.Fatalf("initialize failed: %v", err)
	}
	// On CI, hardware is not available — should fall back to software
	m, err := tee.GetMeasurement()
	if err != nil {
		t.Fatalf("get measurement: %v", err)
	}
	if len(m) == 0 {
		t.Fatal("expected non-empty measurement")
	}
}

func TestSGXHardwareTee_Attestation(t *testing.T) {
	tee := NewSGXHardwareTee(DefaultEnclaveConfig())
	if err := tee.Initialize(); err != nil {
		t.Fatalf("initialize failed: %v", err)
	}

	att, err := tee.GetAttestation()
	if err != nil {
		t.Fatalf("get attestation: %v", err)
	}
	if att.TEEType != TEETypeSGX {
		t.Fatalf("expected SGX type, got %v", att.TEEType)
	}
	if len(att.Signature) == 0 {
		t.Fatal("expected non-empty signature")
	}
	if att.PlatformData == nil {
		t.Fatal("expected platform data")
	}
}

func TestSGXHardwareTee_SealUnseal(t *testing.T) {
	tee := NewSGXHardwareTee(DefaultEnclaveConfig())
	if err := tee.Initialize(); err != nil {
		t.Fatalf("initialize: %v", err)
	}

	plaintext := []byte("secret time-series data")
	sealed, err := tee.SealData(plaintext)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if len(sealed) <= len(plaintext) {
		t.Fatal("sealed data should be larger")
	}

	unsealed, err := tee.UnsealData(sealed)
	if err != nil {
		t.Fatalf("unseal: %v", err)
	}
	if string(unsealed) != string(plaintext) {
		t.Fatalf("data mismatch: got %q", unsealed)
	}
}

func TestSGXHardwareTee_SealingKey(t *testing.T) {
	tee := NewSGXHardwareTee(DefaultEnclaveConfig())
	if err := tee.Initialize(); err != nil {
		t.Fatalf("initialize: %v", err)
	}

	key1, err := tee.GetSealingKey(SealingPolicyMRENclave)
	if err != nil {
		t.Fatalf("get key: %v", err)
	}
	key2, err := tee.GetSealingKey(SealingPolicyMRSigner)
	if err != nil {
		t.Fatalf("get key: %v", err)
	}
	if string(key1) == string(key2) {
		t.Fatal("different policies should produce different keys")
	}
}

func TestSGXHardwareTee_Close(t *testing.T) {
	tee := NewSGXHardwareTee(DefaultEnclaveConfig())
	if err := tee.Initialize(); err != nil {
		t.Fatalf("initialize: %v", err)
	}
	if err := tee.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestSEVHardwareTee_Initialize(t *testing.T) {
	tee := NewSEVHardwareTee(DefaultEnclaveConfig())
	if err := tee.Initialize(); err != nil {
		t.Fatalf("initialize: %v", err)
	}
	m, err := tee.GetMeasurement()
	if err != nil {
		t.Fatalf("get measurement: %v", err)
	}
	if len(m) != 48 {
		t.Fatalf("expected 48-byte measurement, got %d", len(m))
	}
}

func TestSEVHardwareTee_Attestation(t *testing.T) {
	tee := NewSEVHardwareTee(DefaultEnclaveConfig())
	if err := tee.Initialize(); err != nil {
		t.Fatalf("initialize: %v", err)
	}

	att, err := tee.GetAttestation()
	if err != nil {
		t.Fatalf("get attestation: %v", err)
	}
	if att.TEEType != TEETypeSEV {
		t.Fatalf("expected SEV type, got %v", att.TEEType)
	}

	// Verify the attestation we just generated
	if err := tee.VerifyAttestation(att); err != nil {
		t.Fatalf("verify own attestation: %v", err)
	}
}

func TestSEVHardwareTee_SealUnseal(t *testing.T) {
	tee := NewSEVHardwareTee(DefaultEnclaveConfig())
	if err := tee.Initialize(); err != nil {
		t.Fatalf("initialize: %v", err)
	}

	plaintext := []byte("amd-sev-confidential-data")
	sealed, err := tee.SealData(plaintext)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	unsealed, err := tee.UnsealData(sealed)
	if err != nil {
		t.Fatalf("unseal: %v", err)
	}
	if string(unsealed) != string(plaintext) {
		t.Fatal("data mismatch")
	}
}

func TestRemoteAttestationVerifier(t *testing.T) {
	tee := NewSGXHardwareTee(DefaultEnclaveConfig())
	if err := tee.Initialize(); err != nil {
		t.Fatalf("initialize: %v", err)
	}

	att, err := tee.GetAttestation()
	if err != nil {
		t.Fatalf("get attestation: %v", err)
	}

	// Verifier with empty trust list accepts anything
	v := NewRemoteAttestationVerifier(nil)
	if err := v.Verify(att); err != nil {
		t.Fatalf("verify with empty trust: %v", err)
	}

	// Nil attestation should fail
	if err := v.Verify(nil); err == nil {
		t.Fatal("expected error for nil attestation")
	}
}

func TestAttestationType_String(t *testing.T) {
	tests := []struct {
		at   AttestationType
		want string
	}{
		{AttestationNone, "none"},
		{AttestationEPID, "EPID"},
		{AttestationDCAP, "DCAP"},
		{AttestationSEVSNP, "SEV-SNP"},
		{AttestationNitroNSM, "Nitro-NSM"},
	}
	for _, tt := range tests {
		if got := tt.at.String(); got != tt.want {
			t.Errorf("AttestationType(%d).String() = %q, want %q", tt.at, got, tt.want)
		}
	}
}
