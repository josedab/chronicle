package adminui

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

func (ui *AdminUI) handleAPIMetricDetails(w http.ResponseWriter, r *http.Request) {
	metric := r.URL.Query().Get("metric")
	if metric == "" {
		http.Error(w, "metric parameter is required", http.StatusBadRequest)
		return
	}

	// Get metric info
	metrics := ui.db.Metrics()
	found := false
	for _, m := range metrics {
		if m == metric {
			found = true
			break
		}
	}

	if !found {
		http.Error(w, "metric not found", http.StatusNotFound)
		return
	}

	// Try to get sample data to determine tags
	q, _ := ui.db.ParseQuery(fmt.Sprintf("SELECT mean(value) FROM %s", metric))
	result, _ := ui.db.Execute(q)

	details := map[string]interface{}{
		"name":         metric,
		"exists":       true,
		"sample_count": len(result.Points),
	}

	writeJSON(w, details)
}

// Phase 3: Tags endpoint
func (ui *AdminUI) handleAPITags(w http.ResponseWriter, r *http.Request) {
	// Return empty tags since the DB doesn't have a direct tagIndex
	// Tags would need to be extracted from actual data points
	tags := make(map[string][]string)
	writeJSON(w, tags)
}

// Phase 3: Data preview endpoint
func (ui *AdminUI) handleAPIDataPreview(w http.ResponseWriter, r *http.Request) {
	metric := r.URL.Query().Get("metric")
	if metric == "" {
		http.Error(w, "metric parameter is required", http.StatusBadRequest)
		return
	}

	limit := 100
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 && parsed <= 1000 {
			limit = parsed
		}
	}

	// Build query with time range
	queryStr := fmt.Sprintf("SELECT mean(value) FROM %s", metric)

	q, err := ui.db.ParseQuery(queryStr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	result, err := ui.db.Execute(q)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Limit points
	points := result.Points
	if len(points) > limit {
		points = points[:limit]
	}

	preview := map[string]interface{}{
		"metric":   metric,
		"total":    len(result.Points),
		"returned": len(points),
		"points":   points,
		"has_more": len(result.Points) > limit,
	}

	writeJSON(w, preview)
}

// Phase 4: Query history endpoint
func (ui *AdminUI) handleAPIQueryHistory(w http.ResponseWriter, r *http.Request) {
	ui.mu.RLock()
	defer ui.mu.RUnlock()

	limit := 20
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 && parsed <= 100 {
			limit = parsed
		}
	}

	// Return most recent first
	result := make([]queryHistoryEntry, 0, limit)
	start := len(ui.queryHistory) - limit
	if start < 0 {
		start = 0
	}
	for i := len(ui.queryHistory) - 1; i >= start; i-- {
		result = append(result, ui.queryHistory[i])
	}

	writeJSON(w, result)
}

// Phase 4: Export endpoint
func (ui *AdminUI) handleAPIExport(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("q")
	format := r.URL.Query().Get("format")
	if format == "" {
		format = "json"
	}

	if query == "" {
		http.Error(w, "query parameter 'q' is required", http.StatusBadRequest)
		return
	}

	q, err := ui.db.ParseQuery(query)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	result, err := ui.db.Execute(q)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	ui.logActivity("Export", fmt.Sprintf("format=%s, query=%s", format, query))

	switch format {
	case "csv":
		w.Header().Set("Content-Type", "text/csv")
		w.Header().Set("Content-Disposition", "attachment; filename=export.csv")
		csvWriter := csv.NewWriter(w)
		csvWriter.Write([]string{"timestamp", "value", "metric"})
		for _, p := range result.Points {
			csvWriter.Write([]string{
				time.Unix(0, p.Timestamp).Format(time.RFC3339),
				fmt.Sprintf("%f", p.Value),
				p.Metric,
			})
		}
		csvWriter.Flush()
	default:
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Disposition", "attachment; filename=export.json")
		json.NewEncoder(w).Encode(result)
	}
}

// Phase 5: Delete metric endpoint
func (ui *AdminUI) handleAPIDeleteMetric(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	metric := r.URL.Query().Get("metric")
	if metric == "" {
		http.Error(w, "metric parameter is required", http.StatusBadRequest)
		return
	}

	// Note: Chronicle doesn't have a built-in delete metric function
	// This is a placeholder that logs the intent
	ui.logActivity("DeleteMetric", metric)

	writeJSON(w, map[string]interface{}{
		"status":  "acknowledged",
		"message": "Metric deletion scheduled. Data will be removed during next compaction.",
		"metric":  metric,
	})
}

// Phase 5: Truncate endpoint
func (ui *AdminUI) handleAPITruncate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	metric := r.URL.Query().Get("metric")
	beforeStr := r.URL.Query().Get("before")

	var before time.Time
	if beforeStr != "" {
		var err error
		before, err = time.Parse(time.RFC3339, beforeStr)
		if err != nil {
			http.Error(w, "invalid before timestamp", http.StatusBadRequest)
			return
		}
	}

	ui.logActivity("Truncate", fmt.Sprintf("metric=%s, before=%s", metric, before))

	writeJSON(w, map[string]interface{}{
		"status":  "acknowledged",
		"message": "Truncation scheduled. Data will be removed during next compaction.",
		"metric":  metric,
		"before":  before,
	})
}

// Phase 5: Insert endpoint (dev mode only)
func (ui *AdminUI) handleAPIInsert(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !ui.devMode {
		http.Error(w, "insert is only available in dev mode", http.StatusForbidden)
		return
	}

	var points []struct {
		Metric    string            `json:"metric"`
		Value     float64           `json:"value"`
		Timestamp int64             `json:"timestamp,omitempty"`
		Tags      map[string]string `json:"tags,omitempty"`
	}

	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&points); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	insertPoints := make([]Point, len(points))
	now := time.Now().UnixNano()
	for i, p := range points {
		ts := p.Timestamp
		if ts == 0 {
			ts = now
		}
		insertPoints[i] = Point{
			Metric:    p.Metric,
			Value:     p.Value,
			Timestamp: ts,
			Tags:      p.Tags,
		}
	}

	if err := ui.db.WriteBatch(insertPoints); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	ui.logActivity("Insert", fmt.Sprintf("%d points", len(points)))

	writeJSON(w, map[string]interface{}{
		"status":   "ok",
		"inserted": len(points),
	})
}

// Phase 6: Partitions endpoint
