package adminui

import (
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"time"
)

func (ui *AdminUI) handleAPIPartitions(w http.ResponseWriter, r *http.Request) {
	partitions := make([]map[string]any, 0)

	info := ui.db.Info()
	if info.PartitionCount > 0 {
		// Get partition info from the index
		partitions = append(partitions, map[string]any{
			"count":              info.PartitionCount,
			"partition_duration": info.PartitionDuration,
		})
	}

	writeJSON(w, map[string]any{
		"partitions": partitions,
		"total":      len(partitions),
	})
}

// Phase 6: Backup endpoint
func (ui *AdminUI) handleAPIBackup(w http.ResponseWriter, r *http.Request) {
	info := ui.db.Info()
	if r.Method == http.MethodPost {
		// Trigger backup
		ui.logActivity("Backup", "Manual backup triggered")

		// Flush to ensure data is persisted
		if err := ui.db.Flush(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		writeJSON(w, map[string]any{
			"status":    "ok",
			"message":   "Backup completed (data synced to disk)",
			"timestamp": time.Now(),
			"path":      info.Path,
		})
		return
	}

	// GET - return backup status
	writeJSON(w, map[string]any{
		"path":        info.Path,
		"last_sync":   "available via Sync()",
		"auto_backup": false,
	})
}

// Phase 7: Server-Sent Events for real-time updates
func (ui *AdminUI) handleAPIEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "SSE not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	// CORS for SSE: only set if Origin matches a known host.
	// Callers should configure allowed origins at the HTTP layer.

	clientChan := make(chan []byte, 10)
	ui.sseMu.Lock()
	ui.sseClients[clientChan] = true
	ui.sseMu.Unlock()

	defer func() {
		ui.sseMu.Lock()
		delete(ui.sseClients, clientChan)
		ui.sseMu.Unlock()
		close(clientChan)
	}()

	// Send initial stats
	ui.sendStatsUpdate()

	// Start periodic updates
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case msg := <-clientChan:
			fmt.Fprintf(w, "data: %s\n\n", msg)
			flusher.Flush()
		case <-ticker.C:
			ui.sendStatsUpdate()
		}
	}
}

func (ui *AdminUI) sendStatsUpdate() {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	update := map[string]any{
		"type":        "stats",
		"timestamp":   time.Now().UnixMilli(),
		"memory":      formatBytes(memStats.Alloc),
		"goroutines":  runtime.NumGoroutine(),
		"gc_cycles":   memStats.NumGC,
		"uptime":      time.Since(ui.startTime).Round(time.Second).String(),
		"total_alloc": formatBytes(memStats.TotalAlloc),
	}

	data, _ := json.Marshal(update)

	ui.sseMu.RLock()
	for clientChan := range ui.sseClients {
		select {
		case clientChan <- data:
		default:
		}
	}
	ui.sseMu.RUnlock()
}

// Phase 7: Alerting API
func (ui *AdminUI) handleAPIAlerts(w http.ResponseWriter, r *http.Request) {
	ui.logAudit(r, "AlertsAPI", fmt.Sprintf("method=%s", r.Method))

	switch r.Method {
	case http.MethodGet:
		ui.mu.RLock()
		alerts := make([]adminAlertRule, len(ui.alertRules))
		copy(alerts, ui.alertRules)
		ui.mu.RUnlock()
		writeJSON(w, alerts)

	case http.MethodPost:
		var rule adminAlertRule
		if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
			http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}

		rule.ID = fmt.Sprintf("alert_%d", time.Now().UnixNano())
		rule.CreatedAt = time.Now()
		rule.State = "ok"
		if rule.Enabled == false {
			rule.Enabled = true
		}

		ui.mu.Lock()
		ui.alertRules = append(ui.alertRules, rule)
		ui.mu.Unlock()

		ui.logActivity("Alert Created", rule.Name)
		writeJSON(w, rule)

	case http.MethodDelete:
		id := r.URL.Query().Get("id")
		if id == "" {
			http.Error(w, "Missing alert id", http.StatusBadRequest)
			return
		}

		ui.mu.Lock()
		for i, rule := range ui.alertRules {
			if rule.ID == id {
				ui.alertRules = append(ui.alertRules[:i], ui.alertRules[i+1:]...)
				break
			}
		}
		ui.mu.Unlock()

		ui.logActivity("Alert Deleted", id)
		writeJSON(w, map[string]string{"status": "deleted", "id": id})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// Phase 7: Audit Log API
func (ui *AdminUI) handleAPIAuditLog(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= 500 {
			limit = n
		}
	}

	ui.mu.RLock()
	entries := make([]auditLogEntry, 0, limit)
	start := len(ui.auditLog) - limit
	if start < 0 {
		start = 0
	}
	for i := len(ui.auditLog) - 1; i >= start; i-- {
		entries = append(entries, ui.auditLog[i])
	}
	ui.mu.RUnlock()

	writeJSON(w, entries)
}

func (ui *AdminUI) logAudit(r *http.Request, action, details string) {
	entry := auditLogEntry{
		ID:        fmt.Sprintf("audit_%d", time.Now().UnixNano()),
		Action:    action,
		User:      r.Header.Get("X-User"),
		IP:        r.RemoteAddr,
		Details:   details,
		Timestamp: time.Now(),
		Success:   true,
	}
	if entry.User == "" {
		entry.User = "anonymous"
	}

	ui.mu.Lock()
	ui.auditLog = append(ui.auditLog, entry)
	if len(ui.auditLog) > 500 {
		ui.auditLog = ui.auditLog[1:]
	}
	ui.mu.Unlock()
}

// Phase 7: Query Explain API
func (ui *AdminUI) handleAPIQueryExplain(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("q")
	if query == "" {
		http.Error(w, "Missing query parameter 'q'", http.StatusBadRequest)
		return
	}

	result := ui.explainQuery(query)
	writeJSON(w, result)
}

func (ui *AdminUI) explainQuery(query string) queryExplainResult {
	result := queryExplainResult{
		Query: query,
		Steps: []string{},
	}

	// Parse the query
	query = strings.TrimSpace(query)
	upperQuery := strings.ToUpper(query)

	// Determine function
	if strings.Contains(upperQuery, "MEAN(") || strings.Contains(upperQuery, "AVG(") {
		result.ParsedFunction = "mean"
		result.Steps = append(result.Steps, "Aggregate: Calculate mean of values")
	} else if strings.Contains(upperQuery, "SUM(") {
		result.ParsedFunction = "sum"
		result.Steps = append(result.Steps, "Aggregate: Calculate sum of values")
	} else if strings.Contains(upperQuery, "COUNT(") {
		result.ParsedFunction = "count"
		result.Steps = append(result.Steps, "Aggregate: Count data points")
	} else if strings.Contains(upperQuery, "MAX(") {
		result.ParsedFunction = "max"
		result.Steps = append(result.Steps, "Aggregate: Find maximum value")
	} else if strings.Contains(upperQuery, "MIN(") {
		result.ParsedFunction = "min"
		result.Steps = append(result.Steps, "Aggregate: Find minimum value")
	} else {
		result.ParsedFunction = "raw"
		result.Steps = append(result.Steps, "Select: Return raw data points")
	}

	// Extract metric name
	if idx := strings.Index(upperQuery, "FROM "); idx != -1 {
		rest := query[idx+5:]
		parts := strings.Fields(rest)
		if len(parts) > 0 {
			result.ParsedMetric = parts[0]
		}
	}

	// Check for time range
	if strings.Contains(upperQuery, "WHERE") {
		result.TimeRange = "Custom time range specified"
		result.Steps = append(result.Steps, "Filter: Apply WHERE clause conditions")
	} else {
		result.TimeRange = "All time (no WHERE clause)"
	}

	// Estimate rows
	metrics := ui.db.Metrics()
	for _, m := range metrics {
		if m == result.ParsedMetric {
			result.IndexUsed = true
			result.EstimatedRows = 1000 // Placeholder estimate
			break
		}
	}

	result.Steps = append(result.Steps, "Scan: Read from metric index")
	if result.ParsedFunction != "raw" {
		result.Steps = append(result.Steps, "Reduce: Apply aggregation function")
	}
	result.Steps = append(result.Steps, "Return: Format and return results")

	return result
}

// Phase 8: Schema Registry API
