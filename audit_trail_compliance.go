package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Compliance reporting, audit trail manager, and HTTP route handlers for the audit trail.

// ---------------------------------------------------------------------------
// ComplianceReporter
// ---------------------------------------------------------------------------

// ComplianceReporter generates compliance reports from audit data.
type ComplianceReporter struct {
	store      *AuditTrailStore
	frameworks []string
}

// NewComplianceReporter creates a reporter backed by the given store.
func NewComplianceReporter(store *AuditTrailStore, frameworks []string) *ComplianceReporter {
	return &ComplianceReporter{store: store, frameworks: frameworks}
}

// GenerateSOC2Report produces a SOC2 control-evidence report.
func (cr *ComplianceReporter) GenerateSOC2Report(start, end time.Time) *AuditTrailComplianceReport {
	entries := cr.store.ScanRange(start, end)
	findings := []AuditTrailComplianceFinding{
		{Control: "CC6.1", Status: "evaluated", Description: "Logical access controls", Evidence: fmt.Sprintf("%d access events reviewed", len(entries))},
		{Control: "CC6.2", Status: "evaluated", Description: "User authentication", Evidence: cr.authEvidence(entries)},
		{Control: "CC7.2", Status: "evaluated", Description: "System monitoring", Evidence: fmt.Sprintf("%d audit entries recorded", len(entries))},
	}
	return cr.buildReport("SOC2", start, end, entries, findings)
}

// GenerateHIPAAReport produces a HIPAA access-log report.
func (cr *ComplianceReporter) GenerateHIPAAReport(start, end time.Time) *AuditTrailComplianceReport {
	entries := cr.store.ScanRange(start, end)
	findings := []AuditTrailComplianceFinding{
		{Control: "§164.312(b)", Status: "evaluated", Description: "Audit controls", Evidence: fmt.Sprintf("%d audit entries", len(entries))},
		{Control: "§164.312(d)", Status: "evaluated", Description: "Person or entity authentication", Evidence: cr.authEvidence(entries)},
		{Control: "§164.308(a)(5)", Status: "evaluated", Description: "Security awareness training", Evidence: "audit trail enabled"},
	}
	return cr.buildReport("HIPAA", start, end, entries, findings)
}

// GenerateGDPRReport produces a GDPR data-access report.
func (cr *ComplianceReporter) GenerateGDPRReport(start, end time.Time) *AuditTrailComplianceReport {
	entries := cr.store.ScanRange(start, end)
	findings := []AuditTrailComplianceFinding{
		{Control: "Art.30", Status: "evaluated", Description: "Records of processing activities", Evidence: fmt.Sprintf("%d processing events", len(entries))},
		{Control: "Art.33", Status: "evaluated", Description: "Breach notification readiness", Evidence: cr.deniedEvidence(entries)},
		{Control: "Art.5(1)(f)", Status: "evaluated", Description: "Integrity and confidentiality", Evidence: "hash chain verification enabled"},
	}
	return cr.buildReport("GDPR", start, end, entries, findings)
}

func (cr *ComplianceReporter) buildReport(framework string, start, end time.Time, entries []AuditTrailEntry, findings []AuditTrailComplianceFinding) *AuditTrailComplianceReport {
	patterns := cr.accessPatterns(entries)
	anomalies := cr.detectAnomalies(entries)
	var mods int
	for _, e := range entries {
		if e.Action == "write" || e.Action == "delete" || e.Action == "schema_change" {
			mods++
		}
	}
	return &AuditTrailComplianceReport{
		Framework:         framework,
		GeneratedAt:       time.Now(),
		PeriodStart:       start,
		PeriodEnd:         end,
		TotalEvents:       len(entries),
		Summary:           fmt.Sprintf("%s compliance report: %d events in period", framework, len(entries)),
		Findings:          findings,
		AccessPatterns:    patterns,
		Anomalies:         anomalies,
		DataModifications: mods,
		Passed:            len(anomalies) == 0,
	}
}

func (cr *ComplianceReporter) accessPatterns(entries []AuditTrailEntry) []AuditTrailAccessPattern {
	type key struct{ actor, resource string }
	counts := make(map[key]*AuditTrailAccessPattern)
	for _, e := range entries {
		k := key{e.Actor, e.Resource}
		if p, ok := counts[k]; ok {
			p.AccessCount++
			if e.Timestamp.After(p.LastAccess) {
				p.LastAccess = e.Timestamp
			}
		} else {
			counts[k] = &AuditTrailAccessPattern{
				Actor:       e.Actor,
				Resource:    e.Resource,
				AccessCount: 1,
				LastAccess:  e.Timestamp,
			}
		}
	}
	patterns := make([]AuditTrailAccessPattern, 0, len(counts))
	for _, p := range counts {
		patterns = append(patterns, *p)
	}
	return patterns
}

func (cr *ComplianceReporter) detectAnomalies(entries []AuditTrailEntry) []AuditTrailAnomaly {
	var anomalies []AuditTrailAnomaly
	deniedCount := 0
	for _, e := range entries {
		if e.Outcome == AuditOutcomeDenied {
			deniedCount++
		}
	}
	if deniedCount > len(entries)/10 && deniedCount > 5 {
		anomalies = append(anomalies, AuditTrailAnomaly{
			Description: fmt.Sprintf("High denial rate: %d/%d events denied", deniedCount, len(entries)),
			Timestamp:   time.Now(),
			Severity:    "high",
		})
	}
	return anomalies
}

func (cr *ComplianceReporter) authEvidence(entries []AuditTrailEntry) string {
	actors := make(map[string]struct{})
	for _, e := range entries {
		actors[e.Actor] = struct{}{}
	}
	return fmt.Sprintf("%d unique actors observed", len(actors))
}

func (cr *ComplianceReporter) deniedEvidence(entries []AuditTrailEntry) string {
	denied := 0
	for _, e := range entries {
		if e.Outcome == AuditOutcomeDenied {
			denied++
		}
	}
	return fmt.Sprintf("%d denied access attempts", denied)
}

// ---------------------------------------------------------------------------
// AuditTrailManager – the main engine
// ---------------------------------------------------------------------------

// AuditTrailManager is the engine for the persistent, hash-linked audit trail.
type AuditTrailManager struct {
	db           *DB
	config       AuditTrailConfig
	mu           sync.RWMutex
	running      bool
	stopCh       chan struct{}
	chain        *HashChain
	store        *AuditTrailStore
	exporter     *SIEMExporter
	reporter     *ComplianceReporter
	seqNum       uint64
	lastVerified time.Time
	chainValid   bool
}

// NewAuditTrailManager creates a new audit trail manager.
func NewAuditTrailManager(db *DB, cfg AuditTrailConfig) *AuditTrailManager {
	store := NewAuditTrailStore(cfg.MaxEntriesPerFile, cfg.RotationInterval)

	// Load signing key if configured
	var signingKey []byte
	if cfg.SigningKeyPath != "" {
		if data, err := os.ReadFile(cfg.SigningKeyPath); err == nil {
			signingKey = data
		}
	}

	return &AuditTrailManager{
		db:         db,
		config:     cfg,
		stopCh:     make(chan struct{}),
		chain:      NewHashChain(cfg.HashAlgorithm, signingKey),
		store:      store,
		exporter:   NewSIEMExporter(cfg.SIEMEndpoint, cfg.SIEMFormat, cfg.SIEMBatchSize, cfg.SIEMFlushInterval),
		reporter:   NewComplianceReporter(store, cfg.ComplianceFrameworks),
		chainValid: true,
	}
}

// Start starts the audit trail manager.
func (m *AuditTrailManager) Start() {
	m.mu.Lock()
	if m.running {
		m.mu.Unlock()
		return
	}
	m.running = true
	m.stopCh = make(chan struct{})
	m.exporter.stopCh = make(chan struct{})
	m.mu.Unlock()
	m.exporter.StartBackgroundFlush()
}

// Stop stops the audit trail manager.
func (m *AuditTrailManager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.running {
		return
	}
	m.running = false
	m.exporter.Stop()
	close(m.stopCh)
}

// RecordEvent records an auditable event with hash-chain linking.
func (m *AuditTrailManager) RecordEvent(_ context.Context, action, actor, resource string, details map[string]string, outcome AuditTrailOutcome) (*AuditTrailEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.config.Enabled {
		return nil, fmt.Errorf("audit trail is disabled")
	}

	m.seqNum++
	entry := &AuditTrailEntry{
		SequenceNumber: m.seqNum,
		Timestamp:      time.Now().UTC(),
		Action:         action,
		Actor:          actor,
		Resource:       resource,
		Details:        details,
		Outcome:        outcome,
	}

	m.chain.Append(entry)
	m.store.Append(*entry)

	if m.config.SIEMEndpoint != "" {
		m.exporter.Enqueue(*entry)
	}

	return entry, nil
}

// VerifyIntegrity verifies the entire hash chain.
func (m *AuditTrailManager) VerifyIntegrity(_ context.Context) (bool, error) {
	entries := m.store.AllEntries()
	valid, err := m.chain.Verify(entries)

	m.mu.Lock()
	m.lastVerified = time.Now()
	m.chainValid = valid
	m.mu.Unlock()

	return valid, err
}

// VerifyRange verifies a range of entries by sequence number.
func (m *AuditTrailManager) VerifyRange(_ context.Context, startSeq, endSeq uint64) (bool, error) {
	entries := m.store.GetRange(startSeq, endSeq)
	if len(entries) == 0 {
		return true, nil
	}
	return m.chain.VerifyRange(entries)
}

// Search returns entries matching the given filter.
func (m *AuditTrailManager) Search(_ context.Context, filter AuditTrailFilter) []AuditTrailEntry {
	return m.store.Scan(filter)
}

// ExportToSIEM triggers a manual export of the given entries.
func (m *AuditTrailManager) ExportToSIEM(_ context.Context, entries []AuditTrailEntry) error {
	m.exporter.Enqueue(entries...)
	return m.exporter.Flush()
}

// GenerateComplianceReport generates a compliance report for the given framework.
func (m *AuditTrailManager) GenerateComplianceReport(_ context.Context, framework string, startTime, endTime time.Time) (*AuditTrailComplianceReport, error) {
	switch strings.ToUpper(framework) {
	case "SOC2":
		return m.reporter.GenerateSOC2Report(startTime, endTime), nil
	case "HIPAA":
		return m.reporter.GenerateHIPAAReport(startTime, endTime), nil
	case "GDPR":
		return m.reporter.GenerateGDPRReport(startTime, endTime), nil
	default:
		return nil, fmt.Errorf("unsupported compliance framework: %s", framework)
	}
}

// Status returns the current state of the audit trail.
func (m *AuditTrailManager) Status() AuditTrailStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var lastTime time.Time
	if m.seqNum > 0 {
		if e, ok := m.store.GetBySequence(m.seqNum); ok {
			lastTime = e.Timestamp
		}
	}

	return AuditTrailStatus{
		Running:          m.running,
		TotalEntries:     m.store.TotalEntries(),
		LastSequence:     m.seqNum,
		LastEntryTime:    lastTime,
		ChainIntegrity:   m.chainValid,
		LastVerifiedAt:   m.lastVerified,
		StoreSegments:    m.store.SegmentCount(),
		SIEMConnected:    m.exporter.IsConnected(),
		PendingSIEMBatch: m.exporter.PendingCount(),
	}
}

// ---------------------------------------------------------------------------
// HTTP Routes
// ---------------------------------------------------------------------------

// RegisterAuditTrailRoutes registers HTTP endpoints for the audit trail.
func (m *AuditTrailManager) RegisterAuditTrailRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/audit/events", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			m.handleRecordEvent(w, r)
		case http.MethodGet:
			m.handleSearchEvents(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/v1/audit/verify", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		m.handleVerify(w, r)
	})

	mux.HandleFunc("/api/v1/audit/compliance/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		m.handleComplianceReport(w, r)
	})

	mux.HandleFunc("/api/v1/audit/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(m.Status()); err != nil {
			internalError(w, err, "encoding response")
		}
	})
}

func (m *AuditTrailManager) handleRecordEvent(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Action   string            `json:"action"`
		Actor    string            `json:"actor"`
		Resource string            `json:"resource"`
		Details  map[string]string `json:"details"`
		Outcome  string            `json:"outcome"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Action == "" || req.Actor == "" {
		http.Error(w, "action and actor are required", http.StatusBadRequest)
		return
	}

	outcome := AuditTrailOutcome(req.Outcome)
	if outcome == "" {
		outcome = AuditOutcomeSuccess
	}

	entry, err := m.RecordEvent(r.Context(), req.Action, req.Actor, req.Resource, req.Details, outcome)
	if err != nil {
		internalError(w, err, "internal error")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(entry); err != nil {
		internalError(w, err, "encoding response")
	}
}

func (m *AuditTrailManager) handleSearchEvents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	filter := AuditTrailFilter{
		Action:   q.Get("action"),
		Actor:    q.Get("actor"),
		Resource: q.Get("resource"),
		Outcome:  AuditTrailOutcome(q.Get("outcome")),
	}
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			filter.Limit = n
		}
	}
	if v := q.Get("start_time"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			filter.StartTime = t
		}
	}
	if v := q.Get("end_time"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			filter.EndTime = t
		}
	}

	results := m.Search(r.Context(), filter)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(results); err != nil {
		internalError(w, err, "encoding response")
	}
}

func (m *AuditTrailManager) handleVerify(w http.ResponseWriter, r *http.Request) {
	var req struct {
		StartSeq uint64 `json:"start_seq"`
		EndSeq   uint64 `json:"end_seq"`
	}
	// Body is optional; empty body verifies the entire chain.
	_ = json.NewDecoder(r.Body).Decode(&req) //nolint:errcheck // best-effort audit recording

	var (
		valid bool
		err   error
	)
	if req.StartSeq > 0 || req.EndSeq > 0 {
		valid, err = m.VerifyRange(r.Context(), req.StartSeq, req.EndSeq)
	} else {
		valid, err = m.VerifyIntegrity(r.Context())
	}

	resp := map[string]interface{}{
		"valid":       valid,
		"verified_at": time.Now(),
	}
	if err != nil {
		resp["error"] = err.Error()
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		internalError(w, err, "encoding response")
	}
}

func (m *AuditTrailManager) handleComplianceReport(w http.ResponseWriter, r *http.Request) {
	// Extract framework from path: /api/v1/audit/compliance/{framework}
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/audit/compliance/")
	framework := strings.TrimRight(path, "/")
	if framework == "" {
		http.Error(w, "framework is required", http.StatusBadRequest)
		return
	}

	q := r.URL.Query()
	start := time.Now().Add(-30 * 24 * time.Hour)
	end := time.Now()
	if v := q.Get("start_time"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			start = t
		}
	}
	if v := q.Get("end_time"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			end = t
		}
	}

	report, err := m.GenerateComplianceReport(r.Context(), framework, start, end)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(report); err != nil {
		internalError(w, err, "encoding response")
	}
}
