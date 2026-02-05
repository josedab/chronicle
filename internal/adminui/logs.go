package adminui

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"
)

// Phase 11: Log Viewer API
func (ui *AdminUI) handleAPILogs(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		level := r.URL.Query().Get("level")
		source := r.URL.Query().Get("source")
		limitStr := r.URL.Query().Get("limit")
		limit := 100
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 1000 {
			limit = l
		}

		ui.mu.RLock()
		logs := make([]logEntry, 0)
		count := 0
		// Iterate in reverse to get most recent first
		for i := len(ui.logBuffer) - 1; i >= 0 && count < limit; i-- {
			entry := ui.logBuffer[i]
			if (level == "" || entry.Level == level) && (source == "" || entry.Source == source) {
				logs = append(logs, entry)
				count++
			}
		}
		ui.mu.RUnlock()
		writeJSON(w, logs)

	case http.MethodPost:
		// Allow posting log entries (useful for client-side logging)
		var entry logEntry
		if err := json.NewDecoder(r.Body).Decode(&entry); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		if entry.Message == "" {
			http.Error(w, "Message is required", http.StatusBadRequest)
			return
		}

		entry.Timestamp = time.Now()
		if entry.Level == "" {
			entry.Level = "info"
		}

		ui.mu.Lock()
		ui.logBuffer = append(ui.logBuffer, entry)
		// Keep buffer size limited
		if len(ui.logBuffer) > 1000 {
			ui.logBuffer = ui.logBuffer[len(ui.logBuffer)-1000:]
		}
		ui.mu.Unlock()

		writeJSON(w, map[string]string{"status": "logged"})

	case http.MethodDelete:
		// Clear logs
		if !ui.devMode {
			http.Error(w, "Clear logs only available in dev mode", http.StatusForbidden)
			return
		}

		ui.mu.Lock()
		ui.logBuffer = make([]logEntry, 0, 1000)
		ui.mu.Unlock()

		ui.logAudit(r, "ClearLogs", "All logs cleared")
		writeJSON(w, map[string]string{"status": "cleared"})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// AddLog adds a log entry to the admin UI log buffer
func (ui *AdminUI) AddLog(level, message, source string, fields map[string]interface{}) {
	entry := logEntry{
		Timestamp: time.Now(),
		Level:     level,
		Message:   message,
		Source:    source,
		Fields:    fields,
	}

	ui.mu.Lock()
	ui.logBuffer = append(ui.logBuffer, entry)
	if len(ui.logBuffer) > 1000 {
		ui.logBuffer = ui.logBuffer[len(ui.logBuffer)-1000:]
	}
	ui.mu.Unlock()
}
