package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Compliance report generation and legal hold management for blockchain audit.

// GenerateReport generates a compliance report for the given time range.
func (bat *BlockchainAuditTrail) GenerateReport(start, end time.Time) (*AuditReport, error) {
	bat.mu.RLock()
	defer bat.mu.RUnlock()

	entriesByType := make(map[AuditEventType]int)
	totalEntries := 0

	for _, entry := range bat.entries {
		if (entry.Timestamp.Equal(start) || entry.Timestamp.After(start)) && entry.Timestamp.Before(end) {
			totalEntries++
			entriesByType[entry.EventType]++
		}
	}

	var anchors []BlockchainAnchor
	for _, a := range bat.anchors {
		if (a.Timestamp.Equal(start) || a.Timestamp.After(start)) && a.Timestamp.Before(end) {
			anchors = append(anchors, a)
		}
	}

	// Run integrity check on the range
	intact := true
	var gaps []AuditGap
	for i, entry := range bat.entries {
		if entry.Timestamp.Before(start) || !entry.Timestamp.Before(end) {
			continue
		}
		var expectedSeq uint64
		if i == 0 {
			expectedSeq = 1
		} else {
			expectedSeq = bat.entries[i-1].SequenceNum + 1
		}
		if entry.SequenceNum != expectedSeq {
			gaps = append(gaps, AuditGap{
				ExpectedSeq: expectedSeq,
				ActualSeq:   entry.SequenceNum,
				Timestamp:   entry.Timestamp,
			})
			intact = false
		}
	}

	status := "verified"
	if !intact {
		status = "integrity_failure"
	}

	if anchors == nil {
		anchors = []BlockchainAnchor{}
	}
	if gaps == nil {
		gaps = []AuditGap{}
	}

	return &AuditReport{
		GeneratedAt:     time.Now().UTC(),
		Period:          AuditReportPeriod{Start: start, End: end},
		TotalEntries:    totalEntries,
		EntriesByType:   entriesByType,
		Anchors:         anchors,
		IntegrityStatus: status,
		TamperDetected:  !intact,
		Gaps:            gaps,
	}, nil
}

// CreateLegalHold creates a legal hold preventing deletion of audit data.
func (bat *BlockchainAuditTrail) CreateLegalHold(resource, reason, createdBy string) (*LegalHold, error) {
	bat.mu.Lock()
	defer bat.mu.Unlock()

	now := time.Now().UTC()
	hold := LegalHold{
		ID:        fmt.Sprintf("hold-%s", now.Format("20060102150405.000")),
		Resource:  resource,
		Reason:    reason,
		CreatedAt: now,
		CreatedBy: createdBy,
		Active:    true,
		AnchorIDs: []string{},
	}

	bat.legalHolds = append(bat.legalHolds, hold)
	return &hold, nil
}

// ReleaseLegalHold releases a legal hold by ID.
func (bat *BlockchainAuditTrail) ReleaseLegalHold(holdID string) error {
	bat.mu.Lock()
	defer bat.mu.Unlock()

	for i := range bat.legalHolds {
		if bat.legalHolds[i].ID == holdID {
			bat.legalHolds[i].Active = false
			return nil
		}
	}
	return fmt.Errorf("legal hold %q not found", holdID)
}

// ListLegalHolds returns all legal holds.
func (bat *BlockchainAuditTrail) ListLegalHolds() []LegalHold {
	bat.mu.RLock()
	defer bat.mu.RUnlock()

	result := make([]LegalHold, len(bat.legalHolds))
	copy(result, bat.legalHolds)
	return result
}

// GetEntry returns an audit entry by ID.
func (bat *BlockchainAuditTrail) GetEntry(id string) (*BlockchainAuditEntry, error) {
	bat.mu.RLock()
	defer bat.mu.RUnlock()

	for i := range bat.entries {
		if bat.entries[i].ID == id {
			e := bat.entries[i]
			return &e, nil
		}
	}
	return nil, fmt.Errorf("audit entry %q not found", id)
}

// QueryEntries queries audit entries with optional filters.
func (bat *BlockchainAuditTrail) QueryEntries(eventType AuditEventType, start, end time.Time, limit int) []BlockchainAuditEntry {
	bat.mu.RLock()
	defer bat.mu.RUnlock()

	var results []BlockchainAuditEntry
	for _, entry := range bat.entries {
		if eventType != "" && entry.EventType != eventType {
			continue
		}
		if !start.IsZero() && entry.Timestamp.Before(start) {
			continue
		}
		if !end.IsZero() && !entry.Timestamp.Before(end) {
			continue
		}
		results = append(results, entry)
		if limit > 0 && len(results) >= limit {
			break
		}
	}
	if results == nil {
		results = []BlockchainAuditEntry{}
	}
	return results
}

// Stats returns audit trail statistics.
func (bat *BlockchainAuditTrail) Stats() AuditStats {
	bat.mu.RLock()
	defer bat.mu.RUnlock()

	byType := make(map[AuditEventType]uint64)
	for _, e := range bat.entries {
		byType[e.EventType]++
	}

	var lastEntryTime time.Time
	if len(bat.entries) > 0 {
		lastEntryTime = bat.entries[len(bat.entries)-1].Timestamp
	}

	var lastAnchorTime time.Time
	if len(bat.anchors) > 0 {
		lastAnchorTime = bat.anchors[len(bat.anchors)-1].Timestamp
	}

	var lastAnchored uint64
	if len(bat.anchors) > 0 {
		lastAnchored = bat.anchors[len(bat.anchors)-1].LastEntry
	}
	pending := 0
	for _, e := range bat.entries {
		if e.SequenceNum > lastAnchored {
			pending++
		}
	}

	return AuditStats{
		TotalEntries:   uint64(len(bat.entries)),
		TotalAnchors:   len(bat.anchors),
		LastAnchorTime: lastAnchorTime,
		LastEntryTime:  lastEntryTime,
		PendingEntries: pending,
		EntriesByType:  byType,
	}
}

// ListAnchors returns all blockchain anchors.
func (bat *BlockchainAuditTrail) ListAnchors() []BlockchainAnchor {
	bat.mu.RLock()
	defer bat.mu.RUnlock()

	result := make([]BlockchainAnchor, len(bat.anchors))
	copy(result, bat.anchors)
	return result
}

// RegisterHTTPHandlers registers the audit trail HTTP endpoints.
func (bat *BlockchainAuditTrail) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/audit/record", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		var req struct {
			EventType AuditEventType    `json:"event_type"`
			Actor     string            `json:"actor"`
			Resource  string            `json:"resource"`
			Details   map[string]string `json:"details"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		entry, err := bat.RecordEvent(req.EventType, req.Actor, req.Resource, req.Details)
		if err != nil {
			internalError(w, err, "internal error")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(entry)
	})

	mux.HandleFunc("/api/v1/audit/entries", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		q := r.URL.Query()
		eventType := AuditEventType(q.Get("type"))
		var start, end time.Time
		if s := q.Get("start"); s != "" {
			start, _ = time.Parse(time.RFC3339, s) //nolint:errcheck // best-effort audit recording
		}
		if e := q.Get("end"); e != "" {
			end, _ = time.Parse(time.RFC3339, e) //nolint:errcheck // best-effort audit recording
		}
		limit := 100
		if l := q.Get("limit"); l != "" {
			if parsed, err := strconv.Atoi(l); err == nil {
				limit = parsed
			}
		}
		entries := bat.QueryEntries(eventType, start, end, limit)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(entries)
	})

	mux.HandleFunc("/api/v1/audit/entry/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		id := strings.TrimPrefix(r.URL.Path, "/api/v1/audit/entry/")
		if id == "" {
			http.Error(w, "missing entry id", http.StatusBadRequest)
			return
		}
		entry, err := bat.GetEntry(id)
		if err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(entry)
	})

	mux.HandleFunc("/api/v1/audit/anchor", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		anchor, err := bat.AnchorToBlockchain()
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(anchor)
	})

	mux.HandleFunc("/api/v1/audit/verify", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		intact, gaps, err := bat.VerifyIntegrity()
		if err != nil {
			internalError(w, err, "internal error")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"intact": intact,
			"gaps":   gaps,
		})
	})

	mux.HandleFunc("/api/v1/audit/proof/verify", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		var proof BlockchainMerkleProof
		if err := json.NewDecoder(r.Body).Decode(&proof); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		valid := bat.VerifyProof(&proof)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"valid": valid,
		})
	})

	mux.HandleFunc("/api/v1/audit/proof/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		entryID := strings.TrimPrefix(r.URL.Path, "/api/v1/audit/proof/")
		if entryID == "" || entryID == "verify" {
			return
		}
		proof, err := bat.GenerateProof(entryID)
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(proof)
	})

	mux.HandleFunc("/api/v1/audit/report", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		q := r.URL.Query()
		var start, end time.Time
		if s := q.Get("start"); s != "" {
			start, _ = time.Parse(time.RFC3339, s) //nolint:errcheck // best-effort audit recording
		}
		if e := q.Get("end"); e != "" {
			end, _ = time.Parse(time.RFC3339, e) //nolint:errcheck // best-effort audit recording
		}
		if start.IsZero() {
			start = time.Now().AddDate(0, -1, 0)
		}
		if end.IsZero() {
			end = time.Now()
		}
		report, err := bat.GenerateReport(start, end)
		if err != nil {
			internalError(w, err, "internal error")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(report)
	})

	mux.HandleFunc("/api/v1/audit/legal-holds", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(bat.ListLegalHolds())
	})

	mux.HandleFunc("/api/v1/audit/legal-hold/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		id := strings.TrimPrefix(r.URL.Path, "/api/v1/audit/legal-hold/")
		if id == "" {
			http.Error(w, "missing hold id", http.StatusBadRequest)
			return
		}
		if err := bat.ReleaseLegalHold(id); err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("/api/v1/audit/legal-hold", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Resource  string `json:"resource"`
			Reason    string `json:"reason"`
			CreatedBy string `json:"created_by"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		hold, err := bat.CreateLegalHold(req.Resource, req.Reason, req.CreatedBy)
		if err != nil {
			internalError(w, err, "internal error")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(hold)
	})

	mux.HandleFunc("/api/v1/audit/anchors", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(bat.ListAnchors())
	})

	mux.HandleFunc("/api/v1/audit/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(bat.Stats())
	})
}
