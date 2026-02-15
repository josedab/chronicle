package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// AuditLogConfig configures the audit log engine.
type AuditLogConfig struct {
	Enabled    bool `json:"enabled"`
	MaxEntries int  `json:"max_entries"`
	LogWrites  bool `json:"log_writes"`
	LogQueries bool `json:"log_queries"`
	LogAdmin   bool `json:"log_admin"`
}

// DefaultAuditLogConfig returns sensible defaults.
func DefaultAuditLogConfig() AuditLogConfig {
	return AuditLogConfig{
		Enabled:    true,
		MaxEntries: 10000,
		LogWrites:  true,
		LogQueries: true,
		LogAdmin:   true,
	}
}

// AuditRecord represents a single audit log entry.
type AuditRecord struct {
	ID        string    `json:"id"`
	Action    string    `json:"action"`
	Actor     string    `json:"actor"`
	Resource  string    `json:"resource"`
	Details   string    `json:"details"`
	Timestamp time.Time `json:"timestamp"`
	Success   bool      `json:"success"`
}

// AuditLogStats holds audit statistics.
type AuditLogStats struct {
	TotalEntries    int64            `json:"total_entries"`
	EntriesByAction map[string]int64 `json:"entries_by_action"`
}

// AuditLogEngine manages audit logging.
type AuditLogEngine struct {
	db      *DB
	config  AuditLogConfig
	mu      sync.RWMutex
	entries []AuditRecord
	nextID  int64
	running bool
	stopCh  chan struct{}
}

// NewAuditLogEngine creates a new engine.
func NewAuditLogEngine(db *DB, cfg AuditLogConfig) *AuditLogEngine {
	return &AuditLogEngine{
		db:     db,
		config: cfg,
		stopCh: make(chan struct{}),
	}
}

// Start starts the engine.
func (e *AuditLogEngine) Start() {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true
	e.mu.Unlock()
}

// Stop stops the engine.
func (e *AuditLogEngine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	close(e.stopCh)
}

func (e *AuditLogEngine) isActionEnabled(action string) bool {
	switch action {
	case "write":
		return e.config.LogWrites
	case "query":
		return e.config.LogQueries
	case "config_change", "backup", "schema_change":
		return e.config.LogAdmin
	default:
		return true
	}
}

// Log records an audit entry.
func (e *AuditLogEngine) Log(action, actor, resource, details string, success bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.config.Enabled {
		return
	}
	if !e.isActionEnabled(action) {
		return
	}

	e.nextID++
	entry := AuditRecord{
		ID:        fmt.Sprintf("audit_%d", e.nextID),
		Action:    action,
		Actor:     actor,
		Resource:  resource,
		Details:   details,
		Timestamp: time.Now(),
		Success:   success,
	}

	e.entries = append(e.entries, entry)

	// Cap entries
	if e.config.MaxEntries > 0 && len(e.entries) > e.config.MaxEntries {
		excess := len(e.entries) - e.config.MaxEntries
		e.entries = e.entries[excess:]
	}
}

// Query returns audit entries filtered by action.
func (e *AuditLogEngine) Query(action string, limit int) []AuditRecord {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var result []AuditRecord
	for i := len(e.entries) - 1; i >= 0 && len(result) < limit; i-- {
		if action == "" || e.entries[i].Action == action {
			result = append(result, e.entries[i])
		}
	}
	return result
}

// Clear removes all audit entries.
func (e *AuditLogEngine) Clear() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.entries = nil
}

// GetStats returns audit statistics.
func (e *AuditLogEngine) GetStats() AuditLogStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	byAction := make(map[string]int64)
	for _, entry := range e.entries {
		byAction[entry.Action]++
	}
	return AuditLogStats{
		TotalEntries:    int64(len(e.entries)),
		EntriesByAction: byAction,
	}
}

// RegisterHTTPHandlers registers HTTP endpoints.
func (e *AuditLogEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/audit/log/entries", func(w http.ResponseWriter, r *http.Request) {
		action := sanitizeAuditAction(r.URL.Query().Get("action"))
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(e.Query(action, 100)); err != nil {
			http.Error(w, "encoding response", http.StatusInternalServerError)
		}
	})
	mux.HandleFunc("/api/v1/audit/log/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(e.GetStats()); err != nil {
			http.Error(w, "encoding response", http.StatusInternalServerError)
		}
	})
}

// sanitizeAuditAction validates and sanitizes the action query parameter to
// prevent log injection. Only known action values and empty string (meaning
// "all") are accepted; anything else is mapped to empty string.
func sanitizeAuditAction(action string) string {
	switch action {
	case "", "write", "query", "config_change", "backup", "schema_change":
		return action
	default:
		return ""
	}
}
