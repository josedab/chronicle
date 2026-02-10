package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// ComplianceISO27001 extends the existing ComplianceStandard constants.
const ComplianceISO27001 ComplianceStandard = "ISO27001"

// CompliancePacksConfig configures the compliance packs engine.
type CompliancePacksConfig struct {
	Enabled         bool   `json:"enabled"`
	AuditLogEnabled bool   `json:"audit_log_enabled"`
	AuditLogPath    string `json:"audit_log_path"`
	AutoRemediate   bool   `json:"auto_remediate"`
}

// DefaultCompliancePacksConfig returns sensible defaults for compliance packs.
func DefaultCompliancePacksConfig() CompliancePacksConfig {
	return CompliancePacksConfig{
		Enabled:         true,
		AuditLogEnabled: true,
		AuditLogPath:    "/var/log/chronicle/compliance_audit.log",
		AutoRemediate:   false,
	}
}

// CompliancePackStatus represents the status of a compliance pack.
type CompliancePackStatus int

const (
	PackDisabled CompliancePackStatus = iota
	PackEnabled
	PackCompliant
	PackNonCompliant
	PackPartial
)

// CompliancePack defines a pre-configured compliance bundle.
type CompliancePack struct {
	Standard      ComplianceStandard      `json:"standard"`
	Name          string                  `json:"name"`
	Description   string                  `json:"description"`
	Version       string                  `json:"version"`
	Requirements  []ComplianceRequirement `json:"requirements"`
	Settings      ComplianceSettings      `json:"settings"`
	Enabled       bool                    `json:"enabled"`
	EnabledAt     time.Time               `json:"enabled_at"`
	LastValidated time.Time               `json:"last_validated"`
	Status        CompliancePackStatus    `json:"status"`
}

// ComplianceRequirement is a single compliance requirement within a pack.
type ComplianceRequirement struct {
	ID                string `json:"id"`
	Name              string `json:"name"`
	Description       string `json:"description"`
	Category          string `json:"category"`
	Severity          string `json:"severity"`
	Met               bool   `json:"met"`
	Evidence          string `json:"evidence"`
	RemediationAction string `json:"remediation_action"`
}

// ComplianceSettings defines the technical settings for a compliance pack.
type ComplianceSettings struct {
	EncryptionAtRest    bool           `json:"encryption_at_rest"`
	EncryptionAlgorithm string         `json:"encryption_algorithm"`
	MinKeyLength        int            `json:"min_key_length"`
	AccessLogging       bool           `json:"access_logging"`
	QueryAuditLog       bool           `json:"query_audit_log"`
	DataRetentionDays   int            `json:"data_retention_days"`
	ImmutableAuditLog   bool           `json:"immutable_audit_log"`
	PIIDetection        bool           `json:"pii_detection"`
	DataMasking         bool           `json:"data_masking"`
	RightToDelete       bool           `json:"right_to_delete"`
	ConsentTracking     bool           `json:"consent_tracking"`
	BreachNotification  bool           `json:"breach_notification"`
	BackupEncryption    bool           `json:"backup_encryption"`
	BackupFrequency     time.Duration  `json:"backup_frequency"`
	SessionTimeout      time.Duration  `json:"session_timeout"`
	MaxFailedLogins     int            `json:"max_failed_logins"`
	PasswordPolicy      PasswordPolicy `json:"password_policy"`
	NetworkEncryption   bool           `json:"network_encryption"`
}

// PasswordPolicy defines password requirements.
type PasswordPolicy struct {
	MinLength      int           `json:"min_length"`
	RequireUpper   bool          `json:"require_upper"`
	RequireLower   bool          `json:"require_lower"`
	RequireDigit   bool          `json:"require_digit"`
	RequireSpecial bool          `json:"require_special"`
	MaxAge         time.Duration `json:"max_age"`
	HistoryCount   int           `json:"history_count"`
}

// AuditLogEntry tracks all compliance-relevant operations.
type AuditLogEntry struct {
	ID        string    `json:"id"`
	Timestamp time.Time `json:"timestamp"`
	Action    string    `json:"action"`
	User      string    `json:"user"`
	Resource  string    `json:"resource"`
	Details   string    `json:"details"`
	SourceIP  string    `json:"source_ip"`
	Success   bool      `json:"success"`
	Standard  string    `json:"standard"`
}

// CompliancePackReport is a validation report for a compliance pack.
type CompliancePackReport struct {
	Standard        string              `json:"standard"`
	GeneratedAt     time.Time           `json:"generated_at"`
	OverallStatus   string              `json:"overall_status"`
	Score           float64             `json:"score"`
	Requirements    []RequirementStatus `json:"requirements"`
	Findings        []ComplianceFinding `json:"findings"`
	Recommendations []string            `json:"recommendations"`
	NextReviewDate  time.Time           `json:"next_review_date"`
}

// RequirementStatus describes the status of a single requirement.
type RequirementStatus struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Status   string `json:"status"`
	Evidence string `json:"evidence"`
	Notes    string `json:"notes"`
}

// ComplianceFinding describes a compliance issue found during validation.
type ComplianceFinding struct {
	Severity    string    `json:"severity"`
	Category    string    `json:"category"`
	Description string    `json:"description"`
	Remediation string    `json:"remediation"`
	Deadline    time.Time `json:"deadline"`
}

// DataClassification tracks PII/sensitive data classification for a metric.
type DataClassification struct {
	MetricName     string    `json:"metric_name"`
	Classification string    `json:"classification"`
	ContainsPII    bool      `json:"contains_pii"`
	PIITypes       []string  `json:"pii_types"`
	HandlingRules  []string  `json:"handling_rules"`
	ClassifiedAt   time.Time `json:"classified_at"`
	ClassifiedBy   string    `json:"classified_by"`
}

// CompliancePacksStats holds aggregate statistics for the compliance packs engine.
type CompliancePacksStats struct {
	EnabledPacks      int       `json:"enabled_packs"`
	CompliantPacks    int       `json:"compliant_packs"`
	TotalRequirements int       `json:"total_requirements"`
	MetRequirements   int       `json:"met_requirements"`
	AuditLogEntries   int64     `json:"audit_log_entries"`
	ClassifiedMetrics int       `json:"classified_metrics"`
	LastValidation    time.Time `json:"last_validation"`
	FindingsCount     int       `json:"findings_count"`
}

// CompliancePacksEngine manages pre-configured compliance bundles.
//
// 🧪 EXPERIMENTAL: This API may change or be removed without notice.
// See api_stability.go for stability classifications.
type CompliancePacksEngine struct {
	db              *DB
	config          CompliancePacksConfig
	mu              sync.RWMutex
	packs           map[ComplianceStandard]*CompliancePack
	auditLog        []AuditLogEntry
	classifications map[string]*DataClassification
	stats           CompliancePacksStats
}

// NewCompliancePacksEngine creates a new compliance packs engine.
func NewCompliancePacksEngine(db *DB, cfg CompliancePacksConfig) *CompliancePacksEngine {
	e := &CompliancePacksEngine{
		db:              db,
		config:          cfg,
		packs:           make(map[ComplianceStandard]*CompliancePack),
		auditLog:        make([]AuditLogEntry, 0),
		classifications: make(map[string]*DataClassification),
	}
	e.initPacks()
	return e
}

func (e *CompliancePacksEngine) initPacks() {
	e.packs[ComplianceHIPAA] = e.createHIPAAPack()
	e.packs[ComplianceSOC2] = e.createSOC2Pack()
	e.packs[ComplianceGDPR] = e.createGDPRPack()
	e.packs[CompliancePCIDSS] = e.createPCIDSSPack()
	e.packs[ComplianceISO27001] = e.createISO27001Pack()
}

func (e *CompliancePacksEngine) createHIPAAPack() *CompliancePack {
	return &CompliancePack{
		Standard:    ComplianceHIPAA,
		Name:        "HIPAA Compliance Pack",
		Description: "Health Insurance Portability and Accountability Act compliance controls",
		Version:     "1.0.0",
		Status:      PackDisabled,
		Settings: ComplianceSettings{
			EncryptionAtRest:    true,
			EncryptionAlgorithm: "AES-256-GCM",
			MinKeyLength:        256,
			AccessLogging:       true,
			QueryAuditLog:       true,
			DataRetentionDays:   2190, // 6 years
			ImmutableAuditLog:   true,
			PIIDetection:        true,
			DataMasking:         true,
			BreachNotification:  true,
			BackupEncryption:    true,
			BackupFrequency:     24 * time.Hour,
			SessionTimeout:      15 * time.Minute,
			MaxFailedLogins:     5,
			PasswordPolicy: PasswordPolicy{
				MinLength:      12,
				RequireUpper:   true,
				RequireLower:   true,
				RequireDigit:   true,
				RequireSpecial: true,
				MaxAge:         90 * 24 * time.Hour,
				HistoryCount:   12,
			},
			NetworkEncryption: true,
		},
		Requirements: []ComplianceRequirement{
			{ID: "HIPAA-001", Name: "Encryption at Rest", Description: "All PHI data must be encrypted at rest using AES-256", Category: "encryption", Severity: "critical", RemediationAction: "Enable AES-256 encryption for all storage"},
			{ID: "HIPAA-002", Name: "Access Control Audit Logging", Description: "All access to PHI must be logged with user identity", Category: "audit", Severity: "critical", RemediationAction: "Enable access audit logging"},
			{ID: "HIPAA-003", Name: "Automatic Session Timeout", Description: "Sessions must timeout after 15 minutes of inactivity", Category: "access_control", Severity: "high", RemediationAction: "Configure session timeout to 15 minutes"},
			{ID: "HIPAA-004", Name: "Backup and Recovery", Description: "Encrypted backups must be performed daily", Category: "data_handling", Severity: "critical", RemediationAction: "Enable encrypted daily backups"},
			{ID: "HIPAA-005", Name: "Data Integrity Verification", Description: "Data integrity checks must be performed on all PHI", Category: "data_handling", Severity: "high", RemediationAction: "Enable data integrity verification"},
			{ID: "HIPAA-006", Name: "Breach Notification", Description: "Breach notification capability must be enabled", Category: "audit", Severity: "critical", RemediationAction: "Enable breach notification system"},
			{ID: "HIPAA-007", Name: "Minimum Necessary Access", Description: "Access must be limited to minimum necessary for job function", Category: "access_control", Severity: "high", RemediationAction: "Configure role-based access controls"},
			{ID: "HIPAA-008", Name: "Business Associate Tracking", Description: "All business associate agreements must be tracked", Category: "audit", Severity: "high", RemediationAction: "Enable BAA tracking"},
			{ID: "HIPAA-009", Name: "PHI Identification and Protection", Description: "PHI data must be identified and protected with PII detection", Category: "data_handling", Severity: "critical", RemediationAction: "Enable PII detection for PHI"},
			{ID: "HIPAA-010", Name: "Audit Trail Immutability", Description: "Audit trails must be immutable and tamper-proof", Category: "audit", Severity: "critical", RemediationAction: "Enable immutable audit log"},
		},
	}
}

func (e *CompliancePacksEngine) createSOC2Pack() *CompliancePack {
	return &CompliancePack{
		Standard:    ComplianceSOC2,
		Name:        "SOC2 Compliance Pack",
		Description: "Service Organization Control 2 compliance controls",
		Version:     "1.0.0",
		Status:      PackDisabled,
		Settings: ComplianceSettings{
			EncryptionAtRest:    true,
			EncryptionAlgorithm: "AES-256-GCM",
			MinKeyLength:        256,
			AccessLogging:       true,
			QueryAuditLog:       true,
			DataRetentionDays:   365,
			ImmutableAuditLog:   true,
			BreachNotification:  true,
			BackupEncryption:    true,
			BackupFrequency:     24 * time.Hour,
			SessionTimeout:      30 * time.Minute,
			MaxFailedLogins:     5,
			PasswordPolicy: PasswordPolicy{
				MinLength:      10,
				RequireUpper:   true,
				RequireLower:   true,
				RequireDigit:   true,
				RequireSpecial: false,
				MaxAge:         90 * 24 * time.Hour,
				HistoryCount:   6,
			},
			NetworkEncryption: true,
		},
		Requirements: []ComplianceRequirement{
			{ID: "SOC2-001", Name: "Encryption at Rest and In Transit", Description: "Data must be encrypted at rest and in transit", Category: "encryption", Severity: "critical", RemediationAction: "Enable encryption at rest and network encryption"},
			{ID: "SOC2-002", Name: "Access Logging and Monitoring", Description: "All system access must be logged and monitored", Category: "audit", Severity: "critical", RemediationAction: "Enable access logging and monitoring"},
			{ID: "SOC2-003", Name: "Incident Response Procedures", Description: "Incident response procedures must be documented and active", Category: "audit", Severity: "high", RemediationAction: "Configure incident response workflow"},
			{ID: "SOC2-004", Name: "Change Management Audit Trail", Description: "All configuration changes must be tracked in audit trail", Category: "audit", Severity: "high", RemediationAction: "Enable change management audit logging"},
			{ID: "SOC2-005", Name: "Data Backup and Recovery", Description: "Regular encrypted backups with tested recovery", Category: "data_handling", Severity: "critical", RemediationAction: "Enable encrypted backups with recovery testing"},
			{ID: "SOC2-006", Name: "System Availability Monitoring", Description: "System availability must be continuously monitored", Category: "data_handling", Severity: "high", RemediationAction: "Enable availability monitoring"},
			{ID: "SOC2-007", Name: "Confidentiality Controls", Description: "Data confidentiality controls must be in place", Category: "access_control", Severity: "high", RemediationAction: "Enable data confidentiality controls"},
			{ID: "SOC2-008", Name: "Processing Integrity Verification", Description: "Data processing integrity must be verified", Category: "data_handling", Severity: "high", RemediationAction: "Enable processing integrity checks"},
		},
	}
}

func (e *CompliancePacksEngine) createGDPRPack() *CompliancePack {
	return &CompliancePack{
		Standard:    ComplianceGDPR,
		Name:        "GDPR Compliance Pack",
		Description: "General Data Protection Regulation compliance controls",
		Version:     "1.0.0",
		Status:      PackDisabled,
		Settings: ComplianceSettings{
			EncryptionAtRest:    true,
			EncryptionAlgorithm: "AES-256-GCM",
			MinKeyLength:        256,
			AccessLogging:       true,
			QueryAuditLog:       true,
			DataRetentionDays:   365,
			ImmutableAuditLog:   true,
			PIIDetection:        true,
			DataMasking:         true,
			RightToDelete:       true,
			ConsentTracking:     true,
			BreachNotification:  true,
			BackupEncryption:    true,
			BackupFrequency:     24 * time.Hour,
			SessionTimeout:      30 * time.Minute,
			MaxFailedLogins:     5,
			PasswordPolicy: PasswordPolicy{
				MinLength:      10,
				RequireUpper:   true,
				RequireLower:   true,
				RequireDigit:   true,
				RequireSpecial: false,
				MaxAge:         90 * 24 * time.Hour,
				HistoryCount:   6,
			},
			NetworkEncryption: true,
		},
		Requirements: []ComplianceRequirement{
			{ID: "GDPR-001", Name: "Right to Deletion", Description: "Support for complete data erasure on request", Category: "data_handling", Severity: "critical", RemediationAction: "Enable right to deletion support"},
			{ID: "GDPR-002", Name: "Data Portability", Description: "Data must be exportable in standard formats", Category: "data_handling", Severity: "high", RemediationAction: "Enable data export functionality"},
			{ID: "GDPR-003", Name: "Consent Tracking", Description: "All data processing consent must be tracked", Category: "audit", Severity: "critical", RemediationAction: "Enable consent tracking"},
			{ID: "GDPR-004", Name: "PII Detection and Masking", Description: "PII must be automatically detected and masked", Category: "data_handling", Severity: "critical", RemediationAction: "Enable PII detection and data masking"},
			{ID: "GDPR-005", Name: "Data Minimization", Description: "Only necessary data should be collected and retained", Category: "retention", Severity: "high", RemediationAction: "Configure data minimization policies"},
			{ID: "GDPR-006", Name: "Breach Notification", Description: "Breaches must be reported within 72 hours", Category: "audit", Severity: "critical", RemediationAction: "Enable 72-hour breach notification"},
			{ID: "GDPR-007", Name: "Data Processing Records", Description: "All data processing activities must be recorded", Category: "audit", Severity: "high", RemediationAction: "Enable data processing audit log"},
			{ID: "GDPR-008", Name: "Privacy Impact Assessment", Description: "Privacy impact assessments must be supported", Category: "audit", Severity: "high", RemediationAction: "Enable privacy impact assessment tooling"},
		},
	}
}

func (e *CompliancePacksEngine) createPCIDSSPack() *CompliancePack {
	return &CompliancePack{
		Standard:    CompliancePCIDSS,
		Name:        "PCI-DSS Compliance Pack",
		Description: "Payment Card Industry Data Security Standard compliance controls",
		Version:     "1.0.0",
		Status:      PackDisabled,
		Settings: ComplianceSettings{
			EncryptionAtRest:    true,
			EncryptionAlgorithm: "AES-256-GCM",
			MinKeyLength:        256,
			AccessLogging:       true,
			QueryAuditLog:       true,
			DataRetentionDays:   365,
			ImmutableAuditLog:   true,
			PIIDetection:        true,
			DataMasking:         true,
			BreachNotification:  true,
			BackupEncryption:    true,
			BackupFrequency:     24 * time.Hour,
			SessionTimeout:      15 * time.Minute,
			MaxFailedLogins:     6,
			PasswordPolicy: PasswordPolicy{
				MinLength:      12,
				RequireUpper:   true,
				RequireLower:   true,
				RequireDigit:   true,
				RequireSpecial: true,
				MaxAge:         90 * 24 * time.Hour,
				HistoryCount:   4,
			},
			NetworkEncryption: true,
		},
		Requirements: []ComplianceRequirement{
			{ID: "PCI-001", Name: "Strong Encryption", Description: "AES-256 encryption required for all cardholder data", Category: "encryption", Severity: "critical", RemediationAction: "Enable AES-256 encryption"},
			{ID: "PCI-002", Name: "Network Security", Description: "Network encryption must be enforced for all traffic", Category: "encryption", Severity: "critical", RemediationAction: "Enable network encryption"},
			{ID: "PCI-003", Name: "Access Control", Description: "Strict access controls with audit trail", Category: "access_control", Severity: "critical", RemediationAction: "Enable role-based access control"},
			{ID: "PCI-004", Name: "Regular Monitoring", Description: "Continuous monitoring of access to cardholder data", Category: "audit", Severity: "critical", RemediationAction: "Enable continuous access monitoring"},
			{ID: "PCI-005", Name: "Vulnerability Management", Description: "Regular vulnerability assessments must be performed", Category: "data_handling", Severity: "high", RemediationAction: "Enable vulnerability scanning"},
			{ID: "PCI-006", Name: "Cardholder Data Protection", Description: "Cardholder data must be masked and protected", Category: "data_handling", Severity: "critical", RemediationAction: "Enable PII detection and data masking"},
			{ID: "PCI-007", Name: "Authentication Policies", Description: "Strong authentication policies must be enforced", Category: "access_control", Severity: "critical", RemediationAction: "Configure strong password policy"},
			{ID: "PCI-008", Name: "Audit Trail Retention", Description: "Audit trails must be retained for at least one year", Category: "retention", Severity: "high", RemediationAction: "Configure 365-day audit retention"},
		},
	}
}

func (e *CompliancePacksEngine) createISO27001Pack() *CompliancePack {
	return &CompliancePack{
		Standard:    ComplianceISO27001,
		Name:        "ISO 27001 Compliance Pack",
		Description: "ISO/IEC 27001 Information Security Management System compliance controls",
		Version:     "1.0.0",
		Status:      PackDisabled,
		Settings: ComplianceSettings{
			EncryptionAtRest:    true,
			EncryptionAlgorithm: "AES-256-GCM",
			MinKeyLength:        256,
			AccessLogging:       true,
			QueryAuditLog:       true,
			DataRetentionDays:   1095, // 3 years
			ImmutableAuditLog:   true,
			BreachNotification:  true,
			BackupEncryption:    true,
			BackupFrequency:     24 * time.Hour,
			SessionTimeout:      30 * time.Minute,
			MaxFailedLogins:     5,
			PasswordPolicy: PasswordPolicy{
				MinLength:      10,
				RequireUpper:   true,
				RequireLower:   true,
				RequireDigit:   true,
				RequireSpecial: true,
				MaxAge:         90 * 24 * time.Hour,
				HistoryCount:   8,
			},
			NetworkEncryption: true,
		},
		Requirements: []ComplianceRequirement{
			{ID: "ISO-001", Name: "Information Security Policy", Description: "Information security policies must be defined and enforced", Category: "access_control", Severity: "critical", RemediationAction: "Define and enable security policies"},
			{ID: "ISO-002", Name: "Asset Management", Description: "All information assets must be inventoried and classified", Category: "data_handling", Severity: "high", RemediationAction: "Enable asset classification"},
			{ID: "ISO-003", Name: "Access Control", Description: "Access to information must be controlled and audited", Category: "access_control", Severity: "critical", RemediationAction: "Enable access control and audit logging"},
			{ID: "ISO-004", Name: "Cryptography", Description: "Cryptographic controls must protect data confidentiality and integrity", Category: "encryption", Severity: "critical", RemediationAction: "Enable AES-256 encryption"},
			{ID: "ISO-005", Name: "Operations Security", Description: "Operational procedures must be documented and monitored", Category: "audit", Severity: "high", RemediationAction: "Enable operational monitoring"},
			{ID: "ISO-006", Name: "Communications Security", Description: "Network services must be secured with encryption", Category: "encryption", Severity: "high", RemediationAction: "Enable network encryption"},
			{ID: "ISO-007", Name: "Incident Management", Description: "Security incidents must be reported and managed", Category: "audit", Severity: "critical", RemediationAction: "Enable incident management workflow"},
			{ID: "ISO-008", Name: "Business Continuity", Description: "Business continuity plans must include data backup and recovery", Category: "data_handling", Severity: "critical", RemediationAction: "Enable backup and recovery"},
			{ID: "ISO-009", Name: "Compliance Monitoring", Description: "Regular compliance reviews must be conducted", Category: "audit", Severity: "high", RemediationAction: "Enable compliance monitoring schedules"},
			{ID: "ISO-010", Name: "Supplier Relationships", Description: "Third-party access must be controlled and monitored", Category: "access_control", Severity: "high", RemediationAction: "Enable third-party access controls"},
		},
	}
}

// EnablePack enables a compliance pack by standard.
func (e *CompliancePacksEngine) EnablePack(standard ComplianceStandard) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	pack, ok := e.packs[standard]
	if !ok {
		return fmt.Errorf("unknown compliance standard: %s", standard)
	}
	pack.Enabled = true
	pack.EnabledAt = time.Now()
	pack.Status = PackEnabled

	if e.config.AuditLogEnabled {
		e.auditLog = append(e.auditLog, AuditLogEntry{
			ID:        fmt.Sprintf("audit-%d", len(e.auditLog)+1),
			Timestamp: time.Now(),
			Action:    "config_change",
			Details:   fmt.Sprintf("enabled compliance pack: %s", standard),
			Success:   true,
			Standard:  string(standard),
		})
	}
	return nil
}

// DisablePack disables a compliance pack by standard.
func (e *CompliancePacksEngine) DisablePack(standard ComplianceStandard) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	pack, ok := e.packs[standard]
	if !ok {
		return fmt.Errorf("unknown compliance standard: %s", standard)
	}
	pack.Enabled = false
	pack.Status = PackDisabled

	if e.config.AuditLogEnabled {
		e.auditLog = append(e.auditLog, AuditLogEntry{
			ID:        fmt.Sprintf("audit-%d", len(e.auditLog)+1),
			Timestamp: time.Now(),
			Action:    "config_change",
			Details:   fmt.Sprintf("disabled compliance pack: %s", standard),
			Success:   true,
			Standard:  string(standard),
		})
	}
	return nil
}

// ValidatePack validates a compliance pack and returns a report.
func (e *CompliancePacksEngine) ValidatePack(standard ComplianceStandard) (*CompliancePackReport, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	pack, ok := e.packs[standard]
	if !ok {
		return nil, fmt.Errorf("unknown compliance standard: %s", standard)
	}

	met := 0
	total := len(pack.Requirements)
	reqStatuses := make([]RequirementStatus, 0, total)
	findings := make([]ComplianceFinding, 0)

	for i := range pack.Requirements {
		req := &pack.Requirements[i]
		req.Met = e.checkRequirement(*req)
		status := "not_met"
		if req.Met {
			status = "met"
			met++
		}
		reqStatuses = append(reqStatuses, RequirementStatus{
			ID:       req.ID,
			Name:     req.Name,
			Status:   status,
			Evidence: req.Evidence,
		})
		if !req.Met {
			findings = append(findings, ComplianceFinding{
				Severity:    req.Severity,
				Category:    req.Category,
				Description: fmt.Sprintf("Requirement %s (%s) is not met", req.ID, req.Name),
				Remediation: req.RemediationAction,
				Deadline:    time.Now().Add(30 * 24 * time.Hour),
			})
		}
	}

	score := float64(0)
	if total > 0 {
		score = float64(met) / float64(total) * 100
	}

	overall := "non-compliant"
	if score == 100 {
		overall = "compliant"
		pack.Status = PackCompliant
	} else if score >= 50 {
		overall = "partial"
		pack.Status = PackPartial
	} else {
		pack.Status = PackNonCompliant
	}

	pack.LastValidated = time.Now()

	report := &CompliancePackReport{
		Standard:        string(standard),
		GeneratedAt:     time.Now(),
		OverallStatus:   overall,
		Score:           score,
		Requirements:    reqStatuses,
		Findings:        findings,
		Recommendations: e.generateRecommendations(findings),
		NextReviewDate:  time.Now().Add(90 * 24 * time.Hour),
	}
	return report, nil
}

func (e *CompliancePacksEngine) checkRequirement(req ComplianceRequirement) bool {
	// Simulated checks based on category and engine configuration
	switch req.Category {
	case "encryption":
		return e.config.Enabled
	case "audit":
		return e.config.AuditLogEnabled
	case "access_control":
		return e.config.Enabled
	case "data_handling":
		return e.config.Enabled
	case "retention":
		return e.config.Enabled
	default:
		return false
	}
}

func (e *CompliancePacksEngine) generateRecommendations(findings []ComplianceFinding) []string {
	recs := make([]string, 0)
	categories := make(map[string]int)
	for _, f := range findings {
		categories[f.Category]++
	}
	for cat, count := range categories {
		recs = append(recs, fmt.Sprintf("Address %d %s findings to improve compliance", count, cat))
	}
	if len(findings) == 0 {
		recs = append(recs, "All requirements met. Schedule next review in 90 days.")
	}
	return recs
}

// LogAudit writes an entry to the compliance audit log.
func (e *CompliancePacksEngine) LogAudit(entry AuditLogEntry) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if entry.ID == "" {
		entry.ID = fmt.Sprintf("audit-%d", len(e.auditLog)+1)
	}
	if entry.Timestamp.IsZero() {
		entry.Timestamp = time.Now()
	}
	e.auditLog = append(e.auditLog, entry)
}

// GetAuditLog returns audit log entries since the given time, up to limit.
func (e *CompliancePacksEngine) GetAuditLog(since time.Time, limit int) []AuditLogEntry {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]AuditLogEntry, 0)
	for _, entry := range e.auditLog {
		if !entry.Timestamp.Before(since) {
			result = append(result, entry)
			if limit > 0 && len(result) >= limit {
				break
			}
		}
	}
	return result
}

// ClassifyMetric classifies a metric's data sensitivity.
func (e *CompliancePacksEngine) ClassifyMetric(dc DataClassification) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if dc.MetricName == "" {
		return fmt.Errorf("metric name is required")
	}
	if dc.ClassifiedAt.IsZero() {
		dc.ClassifiedAt = time.Now()
	}
	e.classifications[dc.MetricName] = &dc

	if e.config.AuditLogEnabled {
		e.auditLog = append(e.auditLog, AuditLogEntry{
			ID:        fmt.Sprintf("audit-%d", len(e.auditLog)+1),
			Timestamp: time.Now(),
			Action:    "config_change",
			Resource:  dc.MetricName,
			Details:   fmt.Sprintf("classified metric as %s (PII: %v)", dc.Classification, dc.ContainsPII),
			Success:   true,
		})
	}
	return nil
}

// GetClassification returns the data classification for a metric.
func (e *CompliancePacksEngine) GetClassification(metric string) *DataClassification {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.classifications[metric]
}

// ListClassifications returns all data classifications.
func (e *CompliancePacksEngine) ListClassifications() []*DataClassification {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]*DataClassification, 0, len(e.classifications))
	for _, dc := range e.classifications {
		result = append(result, dc)
	}
	return result
}

// GenerateReport generates a full compliance report for a standard.
func (e *CompliancePacksEngine) GenerateReport(standard ComplianceStandard) (*CompliancePackReport, error) {
	return e.ValidatePack(standard)
}

// ListPacks returns all configured compliance packs.
func (e *CompliancePacksEngine) ListPacks() []*CompliancePack {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]*CompliancePack, 0, len(e.packs))
	for _, p := range e.packs {
		result = append(result, p)
	}
	return result
}

// GetPack returns a specific compliance pack.
func (e *CompliancePacksEngine) GetPack(standard ComplianceStandard) *CompliancePack {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.packs[standard]
}

// Stats returns aggregate statistics for the compliance packs engine.
func (e *CompliancePacksEngine) Stats() CompliancePacksStats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	stats := CompliancePacksStats{
		AuditLogEntries:   int64(len(e.auditLog)),
		ClassifiedMetrics: len(e.classifications),
	}
	for _, p := range e.packs {
		stats.TotalRequirements += len(p.Requirements)
		if p.Enabled {
			stats.EnabledPacks++
		}
		if p.Status == PackCompliant {
			stats.CompliantPacks++
		}
		for _, r := range p.Requirements {
			if r.Met {
				stats.MetRequirements++
			}
		}
		if !p.LastValidated.IsZero() && p.LastValidated.After(stats.LastValidation) {
			stats.LastValidation = p.LastValidated
		}
	}
	return stats
}

// RegisterHTTPHandlers registers compliance packs HTTP endpoints.
func (e *CompliancePacksEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/compliance/packs", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.ListPacks())
	})

	mux.HandleFunc("/api/v1/compliance/packs/get", func(w http.ResponseWriter, r *http.Request) {
		std := ComplianceStandard(r.URL.Query().Get("standard"))
		if std == "" {
			http.Error(w, "standard parameter required", http.StatusBadRequest)
			return
		}
		pack := e.GetPack(std)
		if pack == nil {
			http.Error(w, "pack not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(pack)
	})

	mux.HandleFunc("/api/v1/compliance/packs/enable", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		std := ComplianceStandard(r.URL.Query().Get("standard"))
		if std == "" {
			http.Error(w, "standard parameter required", http.StatusBadRequest)
			return
		}
		if err := e.EnablePack(std); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"status":"enabled","standard":"%s"}`, std)
	})

	mux.HandleFunc("/api/v1/compliance/packs/disable", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		std := ComplianceStandard(r.URL.Query().Get("standard"))
		if std == "" {
			http.Error(w, "standard parameter required", http.StatusBadRequest)
			return
		}
		if err := e.DisablePack(std); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"status":"disabled","standard":"%s"}`, std)
	})

	mux.HandleFunc("/api/v1/compliance/packs/validate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		std := ComplianceStandard(r.URL.Query().Get("standard"))
		if std == "" {
			http.Error(w, "standard parameter required", http.StatusBadRequest)
			return
		}
		report, err := e.ValidatePack(std)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(report)
	})

	mux.HandleFunc("/api/v1/compliance/packs/report", func(w http.ResponseWriter, r *http.Request) {
		std := ComplianceStandard(r.URL.Query().Get("standard"))
		if std == "" {
			http.Error(w, "standard parameter required", http.StatusBadRequest)
			return
		}
		report, err := e.GenerateReport(std)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(report)
	})

	mux.HandleFunc("/api/v1/compliance/packs/audit-log", func(w http.ResponseWriter, r *http.Request) {
		sinceStr := r.URL.Query().Get("since")
		since := time.Time{}
		if sinceStr != "" {
			if t, err := time.Parse(time.RFC3339, sinceStr); err == nil {
				since = t
			}
		}
		entries := e.GetAuditLog(since, 1000)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(entries)
	})

	mux.HandleFunc("/api/v1/compliance/packs/classify", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var dc DataClassification
		if err := json.NewDecoder(r.Body).Decode(&dc); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		if err := e.ClassifyMetric(dc); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusCreated)
	})

	mux.HandleFunc("/api/v1/compliance/packs/classifications", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.ListClassifications())
	})

	mux.HandleFunc("/api/v1/compliance/packs/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.Stats())
	})
}

// parseStandardFromPath extracts a compliance standard from a URL path segment.
func parseStandardFromPath(path, prefix string) ComplianceStandard {
	rest := strings.TrimPrefix(path, prefix)
	rest = strings.TrimPrefix(rest, "/")
	if idx := strings.Index(rest, "/"); idx >= 0 {
		rest = rest[:idx]
	}
	return ComplianceStandard(rest)
}
