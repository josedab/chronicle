package adminui

import (
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

func (ui *AdminUI) handleAPIStats(w http.ResponseWriter, r *http.Request) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	info := ui.db.Info()

	stats := map[string]interface{}{
		"uptime":          time.Since(ui.startTime).Seconds(),
		"version":         "1.0.0",
		"go_version":      runtime.Version(),
		"num_cpu":         runtime.NumCPU(),
		"num_goroutine":   runtime.NumGoroutine(),
		"metric_count":    len(ui.db.Metrics()),
		"partition_count": info.PartitionCount,
		"memory": map[string]uint64{
			"alloc":       m.Alloc,
			"total_alloc": m.TotalAlloc,
			"sys":         m.Sys,
			"num_gc":      uint64(m.NumGC),
		},
		"db_status": func() string {
			if ui.db.IsClosed() {
				return "closed"
			}
			return "open"
		}(),
	}

	writeJSON(w, stats)
}

func (ui *AdminUI) handleAPIMetrics(w http.ResponseWriter, r *http.Request) {
	metrics := ui.db.Metrics()
	sort.Strings(metrics)

	search := strings.ToLower(r.URL.Query().Get("search"))

	result := make([]map[string]interface{}, 0, len(metrics))
	for _, m := range metrics {
		if search != "" && !strings.Contains(strings.ToLower(m), search) {
			continue
		}
		result = append(result, map[string]interface{}{
			"name": m,
		})
	}

	writeJSON(w, result)
}

func (ui *AdminUI) handleAPISeries(w http.ResponseWriter, r *http.Request) {
	metric := r.URL.Query().Get("metric")

	var series []map[string]interface{}

	if metric != "" {
		series = append(series, map[string]interface{}{
			"metric": metric,
		})
	} else {
		for _, m := range ui.db.Metrics() {
			series = append(series, map[string]interface{}{
				"metric": m,
			})
		}
	}

	writeJSON(w, series)
}

func (ui *AdminUI) handleAPIQuery(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("q")
	if query == "" {
		http.Error(w, "query parameter 'q' is required", http.StatusBadRequest)
		return
	}

	start := time.Now()
	q, err := ui.db.ParseQuery(query)
	if err != nil {
		ui.addQueryHistory(query, time.Since(start), false, err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	result, err := ui.db.Execute(q)
	duration := time.Since(start)
	if err != nil {
		ui.addQueryHistory(query, duration, false, err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	ui.addQueryHistory(query, duration, true, "")
	ui.logActivity("Query", query)

	writeJSON(w, result)
}

func (ui *AdminUI) handleAPIConfig(w http.ResponseWriter, r *http.Request) {
	info := ui.db.Info()
	config := map[string]interface{}{
		"path":               info.Path,
		"partition_duration": info.PartitionDuration,
		"buffer_size":        info.BufferSize,
		"sync_interval":      info.WALSyncInterval,
		"retention":          info.RetentionDuration,
		"dev_mode":           ui.devMode,
	}

	writeJSON(w, config)
}

func (ui *AdminUI) handleAPIHealth(w http.ResponseWriter, r *http.Request) {
	health := map[string]interface{}{
		"status": "healthy",
		"uptime": time.Since(ui.startTime).Seconds(),
		"checks": map[string]interface{}{
			"database":   "ok",
			"memory":     "ok",
			"goroutines": "ok",
		},
	}

	// Check if database is responsive
	if ui.db.IsClosed() {
		health["status"] = "unhealthy"
		health["checks"].(map[string]interface{})["database"] = "closed"
	}

	// Check memory usage
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	if m.Alloc > 1<<30 { // > 1GB
		health["checks"].(map[string]interface{})["memory"] = "warning"
	}

	// Check goroutine count
	if runtime.NumGoroutine() > 10000 {
		health["checks"].(map[string]interface{})["goroutines"] = "warning"
	}

	writeJSON(w, health)
}

// Phase 2: Activity log endpoint
func (ui *AdminUI) handleAPIActivity(w http.ResponseWriter, r *http.Request) {
	ui.mu.RLock()
	defer ui.mu.RUnlock()

	limit := 50
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 && parsed <= 100 {
			limit = parsed
		}
	}

	// Return most recent first
	result := make([]activityEntry, 0, limit)
	start := len(ui.activityLog) - limit
	if start < 0 {
		start = 0
	}
	for i := len(ui.activityLog) - 1; i >= start; i-- {
		result = append(result, ui.activityLog[i])
	}

	writeJSON(w, result)
}

// Phase 3: Metric details endpoint
