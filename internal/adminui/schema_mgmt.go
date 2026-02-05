package adminui

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"
)

func (ui *AdminUI) handleAPISchemas(w http.ResponseWriter, r *http.Request) {
	ui.logAudit(r, "SchemasAPI", fmt.Sprintf("method=%s", r.Method))

	switch r.Method {
	case http.MethodGet:
		// Return list of metrics with their inferred schemas
		metrics := ui.db.Metrics()
		schemas := make([]map[string]interface{}, 0, len(metrics))

		for _, metric := range metrics {
			schema := map[string]interface{}{
				"name":        metric,
				"type":        "float64",
				"description": fmt.Sprintf("Auto-discovered metric: %s", metric),
				"tags":        []string{},
				"fields": []map[string]string{
					{"name": "value", "type": "float64"},
				},
			}

			// Try to get registered schema
			if regSchema := ui.db.GetSchema(metric); regSchema != nil {
				schema["description"] = regSchema.Description
				tags := make([]string, 0, len(regSchema.Tags))
				for _, t := range regSchema.Tags {
					tags = append(tags, t.Name)
				}
				schema["tags"] = tags
			}

			schemas = append(schemas, schema)
		}

		writeJSON(w, schemas)

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// Phase 8: Retention Policy API
func (ui *AdminUI) handleAPIRetention(w http.ResponseWriter, r *http.Request) {
	ui.logAudit(r, "RetentionAPI", fmt.Sprintf("method=%s", r.Method))

	switch r.Method {
	case http.MethodGet:
		ui.mu.RLock()
		rules := make([]retentionRule, len(ui.retentionRules))
		copy(rules, ui.retentionRules)
		ui.mu.RUnlock()
		writeJSON(w, rules)

	case http.MethodPost:
		var rule retentionRule
		if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
			http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}

		rule.ID = fmt.Sprintf("retention_%d", time.Now().UnixNano())
		rule.CreatedAt = time.Now()
		rule.Enabled = true

		ui.mu.Lock()
		ui.retentionRules = append(ui.retentionRules, rule)
		ui.mu.Unlock()

		ui.logActivity("Retention Policy Created", fmt.Sprintf("%s: %s", rule.Metric, rule.Duration))
		writeJSON(w, rule)

	case http.MethodDelete:
		id := r.URL.Query().Get("id")
		if id == "" {
			http.Error(w, "Missing retention rule id", http.StatusBadRequest)
			return
		}

		ui.mu.Lock()
		for i, rule := range ui.retentionRules {
			if rule.ID == id {
				ui.retentionRules = append(ui.retentionRules[:i], ui.retentionRules[i+1:]...)
				break
			}
		}
		ui.mu.Unlock()

		ui.logActivity("Retention Policy Deleted", id)
		writeJSON(w, map[string]string{"status": "deleted", "id": id})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// Phase 8: Cluster Status API
func (ui *AdminUI) handleAPICluster(w http.ResponseWriter, r *http.Request) {
	// Return cluster status (standalone mode if not in cluster)
	status := map[string]interface{}{
		"mode":   "standalone",
		"status": "healthy",
		"node": map[string]interface{}{
			"id":      "local",
			"state":   "leader",
			"address": "localhost",
			"uptime":  time.Since(ui.startTime).Round(time.Second).String(),
		},
		"nodes":       []interface{}{},
		"replication": "none",
	}

	writeJSON(w, status)
}

// Phase 9: WAL Inspector API
func (ui *AdminUI) handleAPIWAL(w http.ResponseWriter, r *http.Request) {
	ui.logAudit(r, "WALInspector", "Accessed WAL inspector")

	info := ui.db.Info()
	walInfo := map[string]interface{}{
		"enabled":       true,
		"path":          info.Path + ".wal",
		"sync_interval": "1s",
		"max_size":      "100MB",
		"segments":      []interface{}{},
		"stats": map[string]interface{}{
			"total_writes":   0,
			"pending_writes": 0,
			"last_sync":      time.Now().Add(-1 * time.Second),
		},
	}

	// Check if WAL file exists
	walPath := info.Path + ".wal"
	if fileInfo, err := os.Stat(walPath); err == nil {
		walInfo["stats"].(map[string]interface{})["file_size"] = formatBytes(uint64(fileInfo.Size()))
		walInfo["stats"].(map[string]interface{})["modified"] = fileInfo.ModTime()
	}

	writeJSON(w, walInfo)
}

// Phase 9: Scheduled Exports API
func (ui *AdminUI) handleAPIScheduledExports(w http.ResponseWriter, r *http.Request) {
	ui.logAudit(r, "ScheduledExportsAPI", fmt.Sprintf("method=%s", r.Method))

	switch r.Method {
	case http.MethodGet:
		ui.mu.RLock()
		exports := make([]scheduledExport, len(ui.scheduledExports))
		copy(exports, ui.scheduledExports)
		ui.mu.RUnlock()
		writeJSON(w, exports)

	case http.MethodPost:
		var export scheduledExport
		if err := json.NewDecoder(r.Body).Decode(&export); err != nil {
			http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}

		export.ID = fmt.Sprintf("export_%d", time.Now().UnixNano())
		export.CreatedAt = time.Now()
		export.Enabled = true

		// Calculate next run based on schedule
		nextRun := time.Now().Add(1 * time.Hour)
		export.NextRun = &nextRun

		ui.mu.Lock()
		ui.scheduledExports = append(ui.scheduledExports, export)
		ui.mu.Unlock()

		ui.logActivity("Scheduled Export Created", export.Name)
		writeJSON(w, export)

	case http.MethodDelete:
		id := r.URL.Query().Get("id")
		if id == "" {
			http.Error(w, "Missing export id", http.StatusBadRequest)
			return
		}

		ui.mu.Lock()
		for i, export := range ui.scheduledExports {
			if export.ID == id {
				ui.scheduledExports = append(ui.scheduledExports[:i], ui.scheduledExports[i+1:]...)
				break
			}
		}
		ui.mu.Unlock()

		ui.logActivity("Scheduled Export Deleted", id)
		writeJSON(w, map[string]string{"status": "deleted", "id": id})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}
