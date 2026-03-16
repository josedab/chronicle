package chronicle

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
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
	// PersistPath is the file path for persisting audit entries as JSON lines.
	// When set, entries are appended to this file on each Log() call and
	// replayed on Start(). When empty, entries are in-memory only.
	PersistPath string `json:"persist_path,omitempty"`
	// MaxFileSize is the maximum size in bytes before the persist file is
	// rotated. The old file is renamed with a .1 suffix. Default: 50MB.
	MaxFileSize int64 `json:"max_file_size,omitempty"`
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
	file    *os.File // persist file, nil when PersistPath is empty
}

// NewAuditLogEngine creates a new engine.
func NewAuditLogEngine(db *DB, cfg AuditLogConfig) *AuditLogEngine {
	return &AuditLogEngine{
		db:     db,
		config: cfg,
		stopCh: make(chan struct{}),
	}
}

// Start starts the engine. If PersistPath is configured, existing entries are
// replayed from the file.
func (e *AuditLogEngine) Start() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.running {
		return
	}
	e.running = true

	if e.config.PersistPath != "" {
		e.replayFromFile()
		f, err := os.OpenFile(e.config.PersistPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
		if err == nil {
			e.file = f
		}
	}
}

// Stop stops the engine and closes the persist file if open.
func (e *AuditLogEngine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	if e.file != nil {
		e.file.Close()
		e.file = nil
	}
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

// Log records an audit entry. If file persistence is configured, the entry
// is also appended to the persist file as a JSON line.
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

	// Persist to file
	if e.file != nil {
		data, err := json.Marshal(entry)
		if err == nil {
			e.file.Write(append(data, '\n'))
		}
		e.maybeRotateFile()
	}
}

// AuditQueryFilter provides criteria for filtering audit log entries.
type AuditQueryFilter struct {
	Action string    // Filter by action type (empty matches all)
	Since  time.Time // Only entries at or after this time (zero means no lower bound)
	Until  time.Time // Only entries at or before this time (zero means no upper bound)
	Actor  string    // Filter by actor (empty matches all)
	Limit  int       // Maximum entries to return (0 means default of 100)
}

// Query returns audit entries filtered by action.
func (e *AuditLogEngine) Query(action string, limit int) []AuditRecord {
	return e.QueryWithFilter(AuditQueryFilter{
		Action: action,
		Limit:  limit,
	})
}

// QueryWithFilter returns audit entries matching the given filter criteria.
// Results are returned in reverse chronological order (newest first).
func (e *AuditLogEngine) QueryWithFilter(f AuditQueryFilter) []AuditRecord {
	e.mu.RLock()
	defer e.mu.RUnlock()

	limit := f.Limit
	if limit <= 0 {
		limit = 100
	}

	var result []AuditRecord
	for i := len(e.entries) - 1; i >= 0 && len(result) < limit; i-- {
		entry := e.entries[i]
		if f.Action != "" && entry.Action != f.Action {
			continue
		}
		if !f.Since.IsZero() && entry.Timestamp.Before(f.Since) {
			continue
		}
		if !f.Until.IsZero() && entry.Timestamp.After(f.Until) {
			continue
		}
		if f.Actor != "" && entry.Actor != f.Actor {
			continue
		}
		result = append(result, entry)
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
		q := r.URL.Query()
		filter := AuditQueryFilter{
			Action: sanitizeAuditAction(q.Get("action")),
			Actor:  q.Get("actor"),
			Limit:  100,
		}
		if sinceStr := q.Get("since"); sinceStr != "" {
			t, err := time.Parse(time.RFC3339, sinceStr)
			if err != nil {
				writeError(w, "invalid since parameter: expected RFC3339 format", http.StatusBadRequest)
				return
			}
			filter.Since = t
		}
		if untilStr := q.Get("until"); untilStr != "" {
			t, err := time.Parse(time.RFC3339, untilStr)
			if err != nil {
				writeError(w, "invalid until parameter: expected RFC3339 format", http.StatusBadRequest)
				return
			}
			filter.Until = t
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(e.QueryWithFilter(filter)); err != nil {
			internalError(w, err, "encoding response")
		}
	})
	mux.HandleFunc("/api/v1/audit/log/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(e.GetStats()); err != nil {
			internalError(w, err, "encoding response")
		}
	})
}

// replayFromFile reads JSON lines from the persist file and restores entries.
// Must be called with e.mu held.
func (e *AuditLogEngine) replayFromFile() {
	f, err := os.Open(e.config.PersistPath)
	if err != nil {
		return // file doesn't exist yet — nothing to replay
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		var entry AuditRecord
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			continue // skip corrupted lines
		}
		e.entries = append(e.entries, entry)
		e.nextID++
	}

	// Apply max entries cap after replay
	if e.config.MaxEntries > 0 && len(e.entries) > e.config.MaxEntries {
		excess := len(e.entries) - e.config.MaxEntries
		e.entries = e.entries[excess:]
	}
}

// maybeRotateFile rotates the persist file if it exceeds MaxFileSize.
// Must be called with e.mu held.
func (e *AuditLogEngine) maybeRotateFile() {
	maxSize := e.config.MaxFileSize
	if maxSize <= 0 {
		maxSize = 50 * 1024 * 1024 // 50MB default
	}

	info, err := e.file.Stat()
	if err != nil || info.Size() < maxSize {
		return
	}

	e.file.Close()
	os.Rename(e.config.PersistPath, e.config.PersistPath+".1")
	f, err := os.OpenFile(e.config.PersistPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err == nil {
		e.file = f
	} else {
		e.file = nil
	}
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
