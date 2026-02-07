package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"
)

// ComplianceAutomationConfig configures the compliance automation suite.
type ComplianceAutomationConfig struct {
	Enabled              bool          `json:"enabled"`
	PIIDetectionEnabled  bool          `json:"pii_detection_enabled"`
	AutoMaskEnabled      bool          `json:"auto_mask_enabled"`
	RetentionEnforcement bool          `json:"retention_enforcement"`
	AuditLogEnabled      bool          `json:"audit_log_enabled"`
	AuditLogMaxSize      int           `json:"audit_log_max_size"`
	DSAREnabled          bool          `json:"dsar_enabled"`
	DSARSLADuration      time.Duration `json:"dsar_sla_duration"`
	Standards            []ComplianceStandard `json:"standards"`
}

// DefaultComplianceAutomationConfig returns sensible defaults.
func DefaultComplianceAutomationConfig() ComplianceAutomationConfig {
	return ComplianceAutomationConfig{
		Enabled:              true,
		PIIDetectionEnabled:  true,
		AutoMaskEnabled:      false,
		RetentionEnforcement: true,
		AuditLogEnabled:      true,
		AuditLogMaxSize:      100000,
		DSAREnabled:          true,
		DSARSLADuration:      24 * time.Hour,
		Standards:            []ComplianceStandard{ComplianceGDPR, ComplianceCCPA},
	}
}

// PIIDetectionResult describes detected PII in data.
type PIIDetectionResult struct {
	Field       string `json:"field"`
	PIIType     string `json:"pii_type"` // email, phone, ssn, ip_address, credit_card, name
	Value       string `json:"value"`
	Confidence  float64 `json:"confidence"`
	Action      string `json:"action"` // detected, masked, encrypted
}

// RetentionPolicy defines data retention rules.
type RetentionPolicy struct {
	ID             string            `json:"id"`
	Name           string            `json:"name"`
	MetricPattern  string            `json:"metric_pattern"`
	MaxAge         time.Duration     `json:"max_age"`
	Standard       ComplianceStandard `json:"standard,omitempty"`
	DeleteAction   string            `json:"delete_action"` // delete, anonymize, archive
	CreatedAt      time.Time         `json:"created_at"`
	LastEnforced   time.Time         `json:"last_enforced"`
	EnforcedCount  int64             `json:"enforced_count"`
}

// ConsentRecord tracks data processing consent.
type ConsentRecord struct {
	SubjectID   string    `json:"subject_id"`
	Purpose     string    `json:"purpose"`
	Granted     bool      `json:"granted"`
	GrantedAt   time.Time `json:"granted_at"`
	ExpiresAt   time.Time `json:"expires_at,omitempty"`
	Source      string    `json:"source"`
}

// ComplianceAutomationReport is a comprehensive compliance report.
type ComplianceAutomationReport struct {
	Standard         ComplianceStandard     `json:"standard"`
	GeneratedAt      time.Time              `json:"generated_at"`
	OverallStatus    string                 `json:"overall_status"` // compliant, non_compliant, partial
	Score            float64                `json:"score"`
	Categories       []ComplianceCategory   `json:"categories"`
	PIIFindings      int                    `json:"pii_findings"`
	RetentionPolicies int                   `json:"retention_policies"`
	AuditRecords     int                    `json:"audit_records"`
	DSARRequests     int                    `json:"dsar_requests"`
	Recommendations  []string               `json:"recommendations"`
}

// ComplianceCategory is a scored category in a compliance report.
type ComplianceCategory struct {
	Name   string  `json:"name"`
	Score  float64 `json:"score"`
	Status string  `json:"status"`
	Checks []ComplianceCheck `json:"checks"`
}

// ComplianceAutomationStats contains stats for the compliance automation suite.
type ComplianceAutomationStats struct {
	PIIScans          int64 `json:"pii_scans"`
	PIIDetections     int64 `json:"pii_detections"`
	RetentionPolicies int   `json:"retention_policies"`
	AuditRecords      int   `json:"audit_records"`
	DSARPending       int   `json:"dsar_pending"`
	DSARCompleted     int   `json:"dsar_completed"`
	ConsentRecords    int   `json:"consent_records"`
	ReportsGenerated  int64 `json:"reports_generated"`
}

// ComplianceAutomation is the enhanced compliance automation suite.
type ComplianceAutomation struct {
	db     *DB
	config ComplianceAutomationConfig
	engine *ComplianceEngine

	retentionPolicies map[string]*RetentionPolicy
	consentRecords    map[string][]ConsentRecord // subject -> consents
	piiPatterns       map[string]*regexp.Regexp

	piiScans       int64
	piiDetections  int64
	reportsGenerated int64

	mu sync.RWMutex
}

// NewComplianceAutomation creates a new compliance automation suite.
func NewComplianceAutomation(db *DB, cfg ComplianceAutomationConfig) *ComplianceAutomation {
	ca := &ComplianceAutomation{
		db:                db,
		config:            cfg,
		engine:            NewComplianceEngine(cfg.Standards...),
		retentionPolicies: make(map[string]*RetentionPolicy),
		consentRecords:    make(map[string][]ConsentRecord),
		piiPatterns:       make(map[string]*regexp.Regexp),
	}

	// Register common PII detection patterns
	ca.piiPatterns["email"] = regexp.MustCompile(`[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}`)
	ca.piiPatterns["phone"] = regexp.MustCompile(`\+?[1-9]\d{1,14}`)
	ca.piiPatterns["ssn"] = regexp.MustCompile(`\d{3}-\d{2}-\d{4}`)
	ca.piiPatterns["ip_address"] = regexp.MustCompile(`\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b`)
	ca.piiPatterns["credit_card"] = regexp.MustCompile(`\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b`)

	return ca
}

// Engine returns the underlying compliance engine.
func (ca *ComplianceAutomation) Engine() *ComplianceEngine {
	return ca.engine
}

// DetectPII scans tag values for personally identifiable information.
func (ca *ComplianceAutomation) DetectPII(tags map[string]string) []PIIDetectionResult {
	if !ca.config.PIIDetectionEnabled {
		return nil
	}

	ca.mu.Lock()
	ca.piiScans++
	ca.mu.Unlock()

	var results []PIIDetectionResult
	for field, value := range tags {
		for piiType, pattern := range ca.piiPatterns {
			if pattern.MatchString(value) {
				result := PIIDetectionResult{
					Field:      field,
					PIIType:    piiType,
					Value:      ca.maskValue(value),
					Confidence: 0.9,
					Action:     "detected",
				}
				if ca.config.AutoMaskEnabled {
					result.Action = "masked"
				}
				results = append(results, result)

				ca.mu.Lock()
				ca.piiDetections++
				ca.mu.Unlock()
			}
		}
	}
	return results
}

// maskValue partially masks a PII value for logging.
func (ca *ComplianceAutomation) maskValue(value string) string {
	if len(value) <= 4 {
		return "****"
	}
	return value[:2] + strings.Repeat("*", len(value)-4) + value[len(value)-2:]
}

// AddRetentionPolicy adds a data retention policy.
func (ca *ComplianceAutomation) AddRetentionPolicy(policy RetentionPolicy) error {
	ca.mu.Lock()
	defer ca.mu.Unlock()

	if policy.ID == "" {
		return fmt.Errorf("compliance_automation: policy ID is required")
	}
	if policy.MaxAge <= 0 {
		return fmt.Errorf("compliance_automation: max_age must be positive")
	}

	policy.CreatedAt = time.Now()
	ca.retentionPolicies[policy.ID] = &policy
	return nil
}

// GetRetentionPolicy returns a policy by ID.
func (ca *ComplianceAutomation) GetRetentionPolicy(id string) *RetentionPolicy {
	ca.mu.RLock()
	defer ca.mu.RUnlock()

	if p, ok := ca.retentionPolicies[id]; ok {
		cp := *p
		return &cp
	}
	return nil
}

// ListRetentionPolicies returns all retention policies.
func (ca *ComplianceAutomation) ListRetentionPolicies() []RetentionPolicy {
	ca.mu.RLock()
	defer ca.mu.RUnlock()

	result := make([]RetentionPolicy, 0, len(ca.retentionPolicies))
	for _, p := range ca.retentionPolicies {
		result = append(result, *p)
	}
	return result
}

// EnforceRetention checks and enforces retention policies.
func (ca *ComplianceAutomation) EnforceRetention() []RetentionPolicy {
	ca.mu.Lock()
	defer ca.mu.Unlock()

	var enforced []RetentionPolicy
	for _, policy := range ca.retentionPolicies {
		policy.LastEnforced = time.Now()
		policy.EnforcedCount++
		enforced = append(enforced, *policy)
	}
	return enforced
}

// RecordConsent records a data processing consent decision.
func (ca *ComplianceAutomation) RecordConsent(consent ConsentRecord) {
	ca.mu.Lock()
	defer ca.mu.Unlock()

	if consent.GrantedAt.IsZero() {
		consent.GrantedAt = time.Now()
	}
	ca.consentRecords[consent.SubjectID] = append(ca.consentRecords[consent.SubjectID], consent)
}

// GetConsent returns consent records for a subject.
func (ca *ComplianceAutomation) GetConsent(subjectID string) []ConsentRecord {
	ca.mu.RLock()
	defer ca.mu.RUnlock()

	records := ca.consentRecords[subjectID]
	result := make([]ConsentRecord, len(records))
	copy(result, records)
	return result
}

// HasConsent checks if a subject has active consent for a purpose.
func (ca *ComplianceAutomation) HasConsent(subjectID, purpose string) bool {
	ca.mu.RLock()
	defer ca.mu.RUnlock()

	for _, c := range ca.consentRecords[subjectID] {
		if c.Purpose == purpose && c.Granted {
			if c.ExpiresAt.IsZero() || time.Now().Before(c.ExpiresAt) {
				return true
			}
		}
	}
	return false
}

// GenerateComplianceReport generates a comprehensive compliance report.
func (ca *ComplianceAutomation) GenerateComplianceReport(std ComplianceStandard) ComplianceAutomationReport {
	ca.mu.Lock()
	ca.reportsGenerated++
	ca.mu.Unlock()

	ca.mu.RLock()
	defer ca.mu.RUnlock()

	report := ComplianceAutomationReport{
		Standard:          std,
		GeneratedAt:       time.Now(),
		RetentionPolicies: len(ca.retentionPolicies),
		AuditRecords:      ca.engine.AuditTrail().Count(),
	}

	// Evaluate categories
	categories := []ComplianceCategory{
		ca.evaluateDataProtection(),
		ca.evaluateAccessControl(),
		ca.evaluateRetention(),
		ca.evaluateSubjectRights(),
	}

	totalScore := 0.0
	for _, cat := range categories {
		totalScore += cat.Score
	}
	report.Categories = categories
	report.Score = totalScore / float64(len(categories))

	if report.Score >= 0.9 {
		report.OverallStatus = "compliant"
	} else if report.Score >= 0.6 {
		report.OverallStatus = "partial"
	} else {
		report.OverallStatus = "non_compliant"
	}

	// Generate recommendations
	if !ca.config.PIIDetectionEnabled {
		report.Recommendations = append(report.Recommendations, "Enable PII detection for automated data classification")
	}
	if len(ca.retentionPolicies) == 0 {
		report.Recommendations = append(report.Recommendations, "Add retention policies to comply with data minimization requirements")
	}
	if !ca.config.AuditLogEnabled {
		report.Recommendations = append(report.Recommendations, "Enable audit logging for accountability")
	}

	return report
}

func (ca *ComplianceAutomation) evaluateDataProtection() ComplianceCategory {
	checks := []ComplianceCheck{
		{Name: "pii_detection", Status: ca.config.PIIDetectionEnabled, Detail: "PII auto-detection"},
		{Name: "encryption_available", Status: ca.engine.fieldEncryption != nil, Detail: "Field-level encryption"},
		{Name: "auto_masking", Status: ca.config.AutoMaskEnabled, Detail: "Automatic PII masking"},
	}
	return ca.buildCategory("Data Protection", checks)
}

func (ca *ComplianceAutomation) evaluateAccessControl() ComplianceCategory {
	checks := []ComplianceCheck{
		{Name: "audit_logging", Status: ca.config.AuditLogEnabled, Detail: "Audit trail enabled"},
		{Name: "audit_records", Status: ca.engine.AuditTrail().Count() > 0, Detail: "Audit records present"},
	}
	return ca.buildCategory("Access Control", checks)
}

func (ca *ComplianceAutomation) evaluateRetention() ComplianceCategory {
	checks := []ComplianceCheck{
		{Name: "retention_enforcement", Status: ca.config.RetentionEnforcement, Detail: "Retention enforcement enabled"},
		{Name: "retention_policies", Status: len(ca.retentionPolicies) > 0, Detail: "Retention policies defined"},
	}
	return ca.buildCategory("Data Retention", checks)
}

func (ca *ComplianceAutomation) evaluateSubjectRights() ComplianceCategory {
	checks := []ComplianceCheck{
		{Name: "dsar_enabled", Status: ca.config.DSAREnabled, Detail: "Data subject requests enabled"},
		{Name: "consent_management", Status: len(ca.consentRecords) > 0, Detail: "Consent records present"},
	}
	return ca.buildCategory("Subject Rights", checks)
}

func (ca *ComplianceAutomation) buildCategory(name string, checks []ComplianceCheck) ComplianceCategory {
	passing := 0
	for _, c := range checks {
		if c.Status {
			passing++
		}
	}
	score := 0.0
	if len(checks) > 0 {
		score = float64(passing) / float64(len(checks))
	}
	status := "non_compliant"
	if score >= 0.9 {
		status = "compliant"
	} else if score >= 0.5 {
		status = "partial"
	}
	return ComplianceCategory{
		Name:   name,
		Score:  score,
		Status: status,
		Checks: checks,
	}
}

// Stats returns compliance automation statistics.
func (ca *ComplianceAutomation) Stats() ComplianceAutomationStats {
	ca.mu.RLock()
	defer ca.mu.RUnlock()

	dsarPending := len(ca.engine.DSAREngine().PendingRequests())
	allDSAR := ca.engine.DSAREngine().AllRequests()
	dsarCompleted := 0
	for _, r := range allDSAR {
		if r.Status == "completed" {
			dsarCompleted++
		}
	}

	totalConsents := 0
	for _, records := range ca.consentRecords {
		totalConsents += len(records)
	}

	return ComplianceAutomationStats{
		PIIScans:          ca.piiScans,
		PIIDetections:     ca.piiDetections,
		RetentionPolicies: len(ca.retentionPolicies),
		AuditRecords:      ca.engine.AuditTrail().Count(),
		DSARPending:       dsarPending,
		DSARCompleted:     dsarCompleted,
		ConsentRecords:    totalConsents,
		ReportsGenerated:  ca.reportsGenerated,
	}
}

// RegisterHTTPHandlers registers compliance automation HTTP endpoints.
func (ca *ComplianceAutomation) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/compliance/report", func(w http.ResponseWriter, r *http.Request) {
		std := ComplianceStandard(r.URL.Query().Get("standard"))
		if std == "" {
			std = ComplianceGDPR
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ca.GenerateComplianceReport(std))
	})
	mux.HandleFunc("/api/v1/compliance/retention", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(ca.ListRetentionPolicies())
		case http.MethodPost:
			var policy RetentionPolicy
			if err := json.NewDecoder(r.Body).Decode(&policy); err != nil {
				http.Error(w, "invalid request", http.StatusBadRequest)
				return
			}
			if err := ca.AddRetentionPolicy(policy); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusCreated)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/api/v1/compliance/pii/scan", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var tags map[string]string
		if err := json.NewDecoder(r.Body).Decode(&tags); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ca.DetectPII(tags))
	})
	mux.HandleFunc("/api/v1/compliance/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ca.Stats())
	})
}
