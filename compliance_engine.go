package chronicle

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"time"
)

// ComplianceStandard identifies a compliance framework.
type ComplianceStandard string

const (
	ComplianceGDPR   ComplianceStandard = "GDPR"
	ComplianceHIPAA  ComplianceStandard = "HIPAA"
	ComplianceSOC2   ComplianceStandard = "SOC2"
	CompliancePCIDSS ComplianceStandard = "PCI-DSS"
	ComplianceCCPA   ComplianceStandard = "CCPA"
)

// ComplianceEngine manages compliance controls for stored data.
type ComplianceEngine struct {
	mu              sync.RWMutex
	standards       map[ComplianceStandard]bool
	fieldEncryption *FieldEncryptor
	auditTrail      *ComplianceAuditTrail
	dsarEngine      *DSAREngine
	residencyRules  []DataResidencyRule
}

// NewComplianceEngine creates a new compliance engine.
func NewComplianceEngine(standards ...ComplianceStandard) *ComplianceEngine {
	ce := &ComplianceEngine{
		standards:  make(map[ComplianceStandard]bool),
		auditTrail: NewComplianceAuditTrail(10000),
		dsarEngine: NewDSAREngine(),
	}
	for _, s := range standards {
		ce.standards[s] = true
	}
	return ce
}

// EnableStandard enables a compliance standard.
func (ce *ComplianceEngine) EnableStandard(std ComplianceStandard) {
	ce.mu.Lock()
	defer ce.mu.Unlock()
	ce.standards[std] = true
}

// IsEnabled checks if a standard is enabled.
func (ce *ComplianceEngine) IsEnabled(std ComplianceStandard) bool {
	ce.mu.RLock()
	defer ce.mu.RUnlock()
	return ce.standards[std]
}

// Standards returns enabled standards.
func (ce *ComplianceEngine) Standards() []ComplianceStandard {
	ce.mu.RLock()
	defer ce.mu.RUnlock()
	result := make([]ComplianceStandard, 0, len(ce.standards))
	for s := range ce.standards {
		result = append(result, s)
	}
	return result
}

// SetFieldEncryptor sets the field-level encryption handler.
func (ce *ComplianceEngine) SetFieldEncryptor(fe *FieldEncryptor) {
	ce.mu.Lock()
	defer ce.mu.Unlock()
	ce.fieldEncryption = fe
}

// AuditTrail returns the audit trail.
func (ce *ComplianceEngine) AuditTrail() *ComplianceAuditTrail {
	return ce.auditTrail
}

// DSAREngine returns the DSAR engine.
func (ce *ComplianceEngine) DSAREngine() *DSAREngine {
	return ce.dsarEngine
}

// AddResidencyRule adds a data residency rule.
func (ce *ComplianceEngine) AddResidencyRule(rule DataResidencyRule) {
	ce.mu.Lock()
	defer ce.mu.Unlock()
	ce.residencyRules = append(ce.residencyRules, rule)
}

// CheckResidency validates that a metric's data region is compliant.
func (ce *ComplianceEngine) CheckResidency(metric, region string) *ResidencyViolation {
	ce.mu.RLock()
	defer ce.mu.RUnlock()

	for _, rule := range ce.residencyRules {
		if rule.MetricPattern == "" || rule.MetricPattern == "*" || rule.MetricPattern == metric {
			allowed := false
			for _, r := range rule.AllowedRegions {
				if r == region {
					allowed = true
					break
				}
			}
			if !allowed {
				return &ResidencyViolation{
					Metric:         metric,
					Region:         region,
					Rule:           rule,
					AllowedRegions: rule.AllowedRegions,
				}
			}
		}
	}
	return nil
}

// GenerateReport generates a compliance report for a standard.
func (ce *ComplianceEngine) GenerateReport(std ComplianceStandard) ComplianceReport {
	ce.mu.RLock()
	defer ce.mu.RUnlock()

	report := ComplianceReport{
		Standard:    std,
		GeneratedAt: time.Now(),
		Checks:      make([]ComplianceCheck, 0),
	}

	report.Checks = append(report.Checks, ComplianceCheck{
		Name:   "audit_trail_enabled",
		Status: ce.auditTrail != nil,
		Detail: "Audit trail is configured",
	})

	report.Checks = append(report.Checks, ComplianceCheck{
		Name:   "field_encryption_available",
		Status: ce.fieldEncryption != nil,
		Detail: "Field-level encryption is configured",
	})

	report.Checks = append(report.Checks, ComplianceCheck{
		Name:   "dsar_engine_available",
		Status: ce.dsarEngine != nil,
		Detail: "DSAR engine is available for data subject requests",
	})

	report.Checks = append(report.Checks, ComplianceCheck{
		Name:   "data_residency_rules",
		Status: len(ce.residencyRules) > 0,
		Detail: fmt.Sprintf("%d residency rules configured", len(ce.residencyRules)),
	})

	passing := 0
	for _, c := range report.Checks {
		if c.Status {
			passing++
		}
	}
	report.PassRate = float64(passing) / float64(len(report.Checks))
	return report
}

// ComplianceReport is a compliance status report.
type ComplianceReport struct {
	Standard    ComplianceStandard `json:"standard"`
	GeneratedAt time.Time          `json:"generated_at"`
	Checks      []ComplianceCheck  `json:"checks"`
	PassRate    float64            `json:"pass_rate"`
}

// ComplianceCheck is a single compliance check result.
type ComplianceCheck struct {
	Name   string `json:"name"`
	Status bool   `json:"status"`
	Detail string `json:"detail"`
}

// DataResidencyRule defines where data may be stored.
type DataResidencyRule struct {
	MetricPattern  string             `json:"metric_pattern"`
	AllowedRegions []string           `json:"allowed_regions"`
	Standard       ComplianceStandard `json:"standard"`
}

// ResidencyViolation describes a data residency violation.
type ResidencyViolation struct {
	Metric         string            `json:"metric"`
	Region         string            `json:"region"`
	Rule           DataResidencyRule `json:"rule"`
	AllowedRegions []string          `json:"allowed_regions"`
}

// FieldEncryptor provides field-level AES-GCM encryption.
type FieldEncryptor struct {
	mu     sync.RWMutex
	aead   cipher.AEAD
	keyID  string
	fields map[string]bool // fields requiring encryption
}

// NewFieldEncryptor creates a field encryptor with a 32-byte AES key.
func NewFieldEncryptor(key []byte, keyID string) (*FieldEncryptor, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("compliance: AES key must be 32 bytes, got %d", len(key))
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("compliance: %w", err)
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("compliance: %w", err)
	}

	return &FieldEncryptor{
		aead:   aead,
		keyID:  keyID,
		fields: make(map[string]bool),
	}, nil
}

// AddSensitiveField marks a tag key as requiring encryption.
func (fe *FieldEncryptor) AddSensitiveField(field string) {
	fe.mu.Lock()
	defer fe.mu.Unlock()
	fe.fields[field] = true
}

// IsSensitive checks if a field requires encryption.
func (fe *FieldEncryptor) IsSensitive(field string) bool {
	fe.mu.RLock()
	defer fe.mu.RUnlock()
	return fe.fields[field]
}

// Encrypt encrypts a plaintext value.
func (fe *FieldEncryptor) Encrypt(plaintext string) (string, error) {
	nonce := make([]byte, fe.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("compliance: nonce generation failed: %w", err)
	}

	ciphertext := fe.aead.Seal(nonce, nonce, []byte(plaintext), nil)
	return hex.EncodeToString(ciphertext), nil
}

// Decrypt decrypts a hex-encoded ciphertext.
func (fe *FieldEncryptor) Decrypt(ciphertextHex string) (string, error) {
	ciphertext, err := hex.DecodeString(ciphertextHex)
	if err != nil {
		return "", fmt.Errorf("compliance: invalid hex: %w", err)
	}

	nonceSize := fe.aead.NonceSize()
	if len(ciphertext) < nonceSize {
		return "", fmt.Errorf("compliance: ciphertext too short")
	}

	nonce, ciphertextBytes := ciphertext[:nonceSize], ciphertext[nonceSize:]
	plaintext, err := fe.aead.Open(nil, nonce, ciphertextBytes, nil)
	if err != nil {
		return "", fmt.Errorf("compliance: decryption failed: %w", err)
	}

	return string(plaintext), nil
}

// KeyID returns the encryption key identifier.
func (fe *FieldEncryptor) KeyID() string {
	return fe.keyID
}

// SensitiveFields returns the list of sensitive fields.
func (fe *FieldEncryptor) SensitiveFields() []string {
	fe.mu.RLock()
	defer fe.mu.RUnlock()
	result := make([]string, 0, len(fe.fields))
	for f := range fe.fields {
		result = append(result, f)
	}
	return result
}

// ComplianceAuditTrail records all data access and modifications.
type ComplianceAuditTrail struct {
	mu      sync.RWMutex
	entries []ComplianceAuditRecord
	maxSize int
}

// ComplianceAuditRecord is a single audit record.
type ComplianceAuditRecord struct {
	ID        string    `json:"id"`
	Timestamp time.Time `json:"timestamp"`
	Actor     string    `json:"actor"`
	Action    string    `json:"action"` // read, write, delete, export, encrypt
	Resource  string    `json:"resource"`
	Detail    string    `json:"detail"`
	Success   bool      `json:"success"`
	IP        string    `json:"ip,omitempty"`
}

// NewComplianceAuditTrail creates an audit trail with a max record count.
func NewComplianceAuditTrail(maxSize int) *ComplianceAuditTrail {
	if maxSize <= 0 {
		maxSize = 10000
	}
	return &ComplianceAuditTrail{
		entries: make([]ComplianceAuditRecord, 0),
		maxSize: maxSize,
	}
}

// Record adds an audit record.
func (at *ComplianceAuditTrail) Record(actor, action, resource, detail string, success bool) {
	at.mu.Lock()
	defer at.mu.Unlock()

	entry := ComplianceAuditRecord{
		ID:        fmt.Sprintf("audit-%d", time.Now().UnixNano()),
		Timestamp: time.Now(),
		Actor:     actor,
		Action:    action,
		Resource:  resource,
		Detail:    detail,
		Success:   success,
	}

	at.entries = append(at.entries, entry)
	if len(at.entries) > at.maxSize {
		at.entries = at.entries[len(at.entries)-at.maxSize:]
	}
}

// Query returns audit records matching criteria.
func (at *ComplianceAuditTrail) Query(actor, action string, since time.Time) []ComplianceAuditRecord {
	at.mu.RLock()
	defer at.mu.RUnlock()

	var results []ComplianceAuditRecord
	for _, e := range at.entries {
		if actor != "" && e.Actor != actor {
			continue
		}
		if action != "" && e.Action != action {
			continue
		}
		if !since.IsZero() && e.Timestamp.Before(since) {
			continue
		}
		results = append(results, e)
	}
	return results
}

// Count returns the total number of audit records.
func (at *ComplianceAuditTrail) Count() int {
	at.mu.RLock()
	defer at.mu.RUnlock()
	return len(at.entries)
}

// DSARRequestType is the type of data subject access request.
type DSARRequestType string

const (
	DSARLookup  DSARRequestType = "LOOKUP"
	DSARExport  DSARRequestType = "EXPORT"
	DSARErasure DSARRequestType = "ERASURE"
)

// DSARRequest represents a data subject access request.
type DSARRequest struct {
	ID          string          `json:"id"`
	Type        DSARRequestType `json:"type"`
	SubjectID   string          `json:"subject_id"`
	SubjectKey  string          `json:"subject_key"` // tag key identifying the subject
	RequestedAt time.Time       `json:"requested_at"`
	CompletedAt *time.Time      `json:"completed_at,omitempty"`
	Status      string          `json:"status"` // pending, processing, completed, failed
	ResultCount int             `json:"result_count"`
	Error       string          `json:"error,omitempty"`
}

// DSAREngine processes data subject access requests.
type DSAREngine struct {
	mu       sync.RWMutex
	requests []DSARRequest
	counter  int
}

// NewDSAREngine creates a new DSAR engine.
func NewDSAREngine() *DSAREngine {
	return &DSAREngine{
		requests: make([]DSARRequest, 0),
	}
}

// SubmitLookup submits a data subject lookup request.
func (d *DSAREngine) SubmitLookup(subjectID, subjectKey string) *DSARRequest {
	return d.submit(DSARLookup, subjectID, subjectKey)
}

// SubmitExport submits a data subject export request.
func (d *DSAREngine) SubmitExport(subjectID, subjectKey string) *DSARRequest {
	return d.submit(DSARExport, subjectID, subjectKey)
}

// SubmitErasure submits a data subject erasure request.
func (d *DSAREngine) SubmitErasure(subjectID, subjectKey string) *DSARRequest {
	return d.submit(DSARErasure, subjectID, subjectKey)
}

func (d *DSAREngine) submit(reqType DSARRequestType, subjectID, subjectKey string) *DSARRequest {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.counter++
	req := DSARRequest{
		ID:          fmt.Sprintf("dsar-%d", d.counter),
		Type:        reqType,
		SubjectID:   subjectID,
		SubjectKey:  subjectKey,
		RequestedAt: time.Now(),
		Status:      "pending",
	}
	d.requests = append(d.requests, req)
	return &d.requests[len(d.requests)-1]
}

// GetRequest retrieves a DSAR request by ID.
func (d *DSAREngine) GetRequest(id string) *DSARRequest {
	d.mu.RLock()
	defer d.mu.RUnlock()

	for i := range d.requests {
		if d.requests[i].ID == id {
			return &d.requests[i]
		}
	}
	return nil
}

// CompleteRequest marks a request as completed.
func (d *DSAREngine) CompleteRequest(id string, resultCount int) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	for i := range d.requests {
		if d.requests[i].ID == id {
			now := time.Now()
			d.requests[i].Status = "completed"
			d.requests[i].CompletedAt = &now
			d.requests[i].ResultCount = resultCount
			return nil
		}
	}
	return fmt.Errorf("dsar: request %q not found", id)
}

// FailRequest marks a request as failed.
func (d *DSAREngine) FailRequest(id string, err error) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	for i := range d.requests {
		if d.requests[i].ID == id {
			d.requests[i].Status = "failed"
			d.requests[i].Error = err.Error()
			return nil
		}
	}
	return fmt.Errorf("dsar: request %q not found", id)
}

// PendingRequests returns all pending DSAR requests.
func (d *DSAREngine) PendingRequests() []DSARRequest {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var pending []DSARRequest
	for _, r := range d.requests {
		if r.Status == "pending" {
			pending = append(pending, r)
		}
	}
	return pending
}

// AllRequests returns all DSAR requests.
func (d *DSAREngine) AllRequests() []DSARRequest {
	d.mu.RLock()
	defer d.mu.RUnlock()
	result := make([]DSARRequest, len(d.requests))
	copy(result, d.requests)
	return result
}
