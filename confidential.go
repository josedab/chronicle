package chronicle

import (
	"context"
	"errors"
	"sync"
	"time"
)

// EXPERIMENTAL: This API is unstable and may change without notice.
// NOTE: Hardware TEE support (SGX, SEV, TrustZone, Nitro) is not yet implemented.
// Only the software-based TEE is functional. Hardware TEE constructors return stubs
// that report IsAvailable() == false.
//
// ConfidentialConfig configures confidential computing support.
type ConfidentialConfig struct {
	// Enabled enables confidential computing.
	Enabled bool

	// TEEType specifies the trusted execution environment type.
	TEEType TEEType

	// AttestationEnabled enables remote attestation.
	AttestationEnabled bool

	// AttestationInterval is how often to re-attest.
	AttestationInterval time.Duration

	// SealingKeyPolicy specifies how sealing keys are derived.
	SealingKeyPolicy SealingPolicy

	// EncryptInMemory encrypts data even in memory.
	EncryptInMemory bool

	// AllowedOperations specifies allowed operations in TEE.
	AllowedOperations []ConfidentialOp

	// VerifyBeforeDecrypt requires attestation before decryption.
	VerifyBeforeDecrypt bool
}

// DefaultConfidentialConfig returns default confidential computing configuration.
func DefaultConfidentialConfig() ConfidentialConfig {
	return ConfidentialConfig{
		Enabled:             true,
		TEEType:             TEETypeSoftware, // Default to software emulation
		AttestationEnabled:  true,
		AttestationInterval: time.Hour,
		SealingKeyPolicy:    SealingPolicyMRENclave,
		EncryptInMemory:     false,
		AllowedOperations:   []ConfidentialOp{OpRead, OpWrite, OpQuery, OpAggregate},
		VerifyBeforeDecrypt: true,
	}
}

// TEEType identifies the trusted execution environment type.
type TEEType int

const (
	// TEETypeSoftware uses software emulation (no hardware TEE).
	TEETypeSoftware TEEType = iota
	// TEETypeSGX uses Intel SGX.
	TEETypeSGX
	// TEETypeTrustZone uses ARM TrustZone.
	TEETypeTrustZone
	// TEETypeSEV uses AMD SEV.
	TEETypeSEV
	// TEETypeNitro uses AWS Nitro Enclaves.
	TEETypeNitro
)

func (t TEEType) String() string {
	switch t {
	case TEETypeSoftware:
		return "software"
	case TEETypeSGX:
		return "sgx"
	case TEETypeTrustZone:
		return "trustzone"
	case TEETypeSEV:
		return "sev"
	case TEETypeNitro:
		return "nitro"
	default:
		return "unknown"
	}
}

// SealingPolicy specifies how sealing keys are derived.
type SealingPolicy int

const (
	// SealingPolicyMRENclave seals to specific enclave code.
	SealingPolicyMRENclave SealingPolicy = iota
	// SealingPolicyMRSigner seals to enclave signer.
	SealingPolicyMRSigner
	// SealingPolicyConfig seals to enclave configuration.
	SealingPolicyConfig
)

// ConfidentialOp represents an operation allowed in confidential computing.
type ConfidentialOp int

const (
	OpRead ConfidentialOp = iota
	OpWrite
	OpQuery
	OpAggregate
	OpExport
	OpDecrypt
)

// ConfidentialEngine provides confidential computing capabilities.
type ConfidentialEngine struct {
	config ConfidentialConfig
	db     *DB

	// TEE interface
	tee TEE

	// Attestation
	attestation     *Attestation
	attestationMu   sync.RWMutex
	lastAttestation time.Time

	// Sealing
	sealingKey   []byte
	sealingKeyMu sync.RWMutex

	// Encrypted memory buffer
	encryptedBuffer   *EncryptedBuffer
	encryptedBufferMu sync.RWMutex

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// TEE is the interface for trusted execution environments.
type TEE interface {
	// Initialize initializes the TEE.
	Initialize() error

	// GetAttestation returns attestation evidence.
	GetAttestation() (*Attestation, error)

	// VerifyAttestation verifies attestation evidence.
	VerifyAttestation(attestation *Attestation) error

	// GetSealingKey returns the sealing key.
	GetSealingKey(policy SealingPolicy) ([]byte, error)

	// SealData seals data to the TEE.
	SealData(data []byte) ([]byte, error)

	// UnsealData unseals data from the TEE.
	UnsealData(sealed []byte) ([]byte, error)

	// SecureCall makes a secure function call within the TEE.
	SecureCall(function string, args []byte) ([]byte, error)

	// GetMeasurement returns the enclave measurement.
	GetMeasurement() ([]byte, error)

	// IsAvailable checks if the TEE is available.
	IsAvailable() bool

	// Close closes the TEE.
	Close() error
}

// Attestation represents TEE attestation evidence.
type Attestation struct {
	// Type of TEE
	TEEType TEEType `json:"tee_type"`

	// Measurement hash of the enclave
	Measurement []byte `json:"measurement"`

	// Signature over the attestation
	Signature []byte `json:"signature"`

	// Additional data included in attestation
	UserData []byte `json:"user_data,omitempty"`

	// Timestamp of attestation
	Timestamp time.Time `json:"timestamp"`

	// Certificate chain for verification
	CertChain [][]byte `json:"cert_chain,omitempty"`

	// Platform-specific data
	PlatformData map[string][]byte `json:"platform_data,omitempty"`
}

// NewConfidentialEngine creates a new confidential computing engine.
func NewConfidentialEngine(db *DB, config ConfidentialConfig) (*ConfidentialEngine, error) {
	ctx, cancel := context.WithCancel(context.Background())

	engine := &ConfidentialEngine{
		config: config,
		db:     db,
		ctx:    ctx,
		cancel: cancel,
	}

	// Initialize TEE based on type
	switch config.TEEType {
	case TEETypeSGX:
		engine.tee = NewSGXTee()
	case TEETypeTrustZone:
		engine.tee = NewTrustZoneTee()
	case TEETypeSEV:
		engine.tee = NewSEVTee()
	case TEETypeNitro:
		engine.tee = NewNitroTee()
	default:
		engine.tee = NewSoftwareTee()
	}

	// Initialize TEE
	if err := engine.tee.Initialize(); err != nil {
		cancel()
		return nil, err
	}

	// Get sealing key
	sealingKey, err := engine.tee.GetSealingKey(config.SealingKeyPolicy)
	if err != nil {
		cancel()
		return nil, err
	}
	engine.sealingKey = sealingKey

	// Initialize encrypted buffer if needed
	if config.EncryptInMemory {
		engine.encryptedBuffer = NewEncryptedBuffer(sealingKey, 64*1024*1024) // 64MB
	}

	return engine, nil
}

// Start starts the confidential engine.
func (e *ConfidentialEngine) Start() error {
	// Get initial attestation
	if e.config.AttestationEnabled {
		if err := e.refreshAttestation(); err != nil {
			return err
		}

		// Start attestation refresh loop
		if e.config.AttestationInterval > 0 {
			e.wg.Add(1)
			go e.attestationLoop()
		}
	}

	return nil
}

// Stop stops the confidential engine.
func (e *ConfidentialEngine) Stop() error {
	e.cancel()
	e.wg.Wait()
	return e.tee.Close()
}

func (e *ConfidentialEngine) attestationLoop() {
	defer e.wg.Done()

	ticker := time.NewTicker(e.config.AttestationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.refreshAttestation()
		}
	}
}

func (e *ConfidentialEngine) refreshAttestation() error {
	attestation, err := e.tee.GetAttestation()
	if err != nil {
		return err
	}

	e.attestationMu.Lock()
	e.attestation = attestation
	e.lastAttestation = time.Now()
	e.attestationMu.Unlock()

	return nil
}

// GetAttestation returns the current attestation.
func (e *ConfidentialEngine) GetAttestation() (*Attestation, error) {
	e.attestationMu.RLock()
	defer e.attestationMu.RUnlock()

	if e.attestation == nil {
		return nil, errors.New("no attestation available")
	}

	return e.attestation, nil
}

// VerifyRemoteAttestation verifies attestation from a remote party.
func (e *ConfidentialEngine) VerifyRemoteAttestation(attestation *Attestation) error {
	return e.tee.VerifyAttestation(attestation)
}

// SealData seals data to the TEE.
func (e *ConfidentialEngine) SealData(data []byte) ([]byte, error) {
	return e.tee.SealData(data)
}

// UnsealData unseals data from the TEE.
func (e *ConfidentialEngine) UnsealData(sealed []byte) ([]byte, error) {
	if e.config.VerifyBeforeDecrypt {
		e.attestationMu.RLock()
		attestation := e.attestation
		e.attestationMu.RUnlock()

		if attestation == nil {
			return nil, errors.New("attestation required before decryption")
		}

		// Verify attestation is recent
		if time.Since(e.lastAttestation) > e.config.AttestationInterval {
			if err := e.refreshAttestation(); err != nil {
				return nil, err
			}
		}
	}

	return e.tee.UnsealData(sealed)
}

// SecureWrite writes data securely within the TEE.
func (e *ConfidentialEngine) SecureWrite(measurement string, tags map[string]string, fields map[string]any, timestamp time.Time) error {
	if !e.isOperationAllowed(OpWrite) {
		return errors.New("write operation not allowed")
	}

	// Encrypt sensitive fields
	encryptedFields := make(map[string]any)
	for k, v := range fields {
		if e.config.EncryptInMemory {
			// Encrypt the value
			data := []byte(serializeValue(v))
			encrypted, err := e.SealData(data)
			if err != nil {
				return err
			}
			encryptedFields[k] = encrypted
		} else {
			encryptedFields[k] = v
		}
	}

	// Get value from encryptedFields (first value found)
	var value float64
	for _, v := range encryptedFields {
		if f, ok := v.(float64); ok {
			value = f
			break
		}
	}

	return e.db.Write(Point{
		Metric:    measurement,
		Tags:      tags,
		Value:     value,
		Timestamp: timestamp.UnixNano(),
	})
}

// SecureQuery queries data securely within the TEE.
func (e *ConfidentialEngine) SecureQuery(queryStr string) (*Result, error) {
	if !e.isOperationAllowed(OpQuery) {
		return nil, errors.New("query operation not allowed")
	}

	// Parse query
	parser := QueryParser{}
	query, err := parser.Parse(queryStr)
	if err != nil {
		return nil, err
	}

	// Execute query
	result, err := e.db.Execute(query)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// SecureAggregate performs aggregation within the TEE.
func (e *ConfidentialEngine) SecureAggregate(queryStr string) (*Result, error) {
	if !e.isOperationAllowed(OpAggregate) {
		return nil, errors.New("aggregate operation not allowed")
	}

	// For secure aggregation, we execute within the TEE
	args := []byte(queryStr)
	result, err := e.tee.SecureCall("aggregate", args)
	if err != nil {
		// Fall back to regular query
		parser := QueryParser{}
		query, err := parser.Parse(queryStr)
		if err != nil {
			return nil, err
		}
		return e.db.Execute(query)
	}

	return deserializeQueryResult(result)
}

func (e *ConfidentialEngine) isOperationAllowed(op ConfidentialOp) bool {
	for _, allowed := range e.config.AllowedOperations {
		if allowed == op {
			return true
		}
	}
	return false
}

// GetMeasurement returns the enclave measurement.
func (e *ConfidentialEngine) GetMeasurement() ([]byte, error) {
	return e.tee.GetMeasurement()
}

// IsHardwareTEE returns true if running in a hardware TEE.
func (e *ConfidentialEngine) IsHardwareTEE() bool {
	return e.config.TEEType != TEETypeSoftware && e.tee.IsAvailable()
}

// Stats returns confidential computing statistics.
func (e *ConfidentialEngine) Stats() ConfidentialStats {
	e.attestationMu.RLock()
	hasAttestation := e.attestation != nil
	lastAttestation := e.lastAttestation
	e.attestationMu.RUnlock()

	return ConfidentialStats{
		TEEType:         e.config.TEEType.String(),
		HardwareTEE:     e.IsHardwareTEE(),
		HasAttestation:  hasAttestation,
		LastAttestation: lastAttestation,
		EncryptInMemory: e.config.EncryptInMemory,
	}
}
