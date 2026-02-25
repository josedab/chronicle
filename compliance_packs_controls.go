// compliance_packs_controls.go contains extended compliance packs functionality.
package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

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
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "enabled", "standard": string(std)})
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
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "disabled", "standard": string(std)})
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
			http.Error(w, "bad request", http.StatusBadRequest)
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
			http.Error(w, "bad request", http.StatusBadRequest)
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
			http.Error(w, "bad request", http.StatusBadRequest)
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
