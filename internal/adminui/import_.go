package adminui

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Phase 10: Data Import API
func (ui *AdminUI) handleAPIImport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !ui.devMode {
		http.Error(w, "Import only available in dev mode", http.StatusForbidden)
		return
	}

	ui.logAudit(r, "DataImport", "Import initiated")

	format := r.URL.Query().Get("format")
	if format == "" {
		format = "json"
	}

	var points []Point
	var err error

	switch format {
	case "json":
		var data []struct {
			Metric    string            `json:"metric"`
			Value     float64           `json:"value"`
			Timestamp int64             `json:"timestamp,omitempty"`
			Tags      map[string]string `json:"tags,omitempty"`
		}
		if err = json.NewDecoder(r.Body).Decode(&data); err != nil {
			http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}
		for _, d := range data {
			ts := d.Timestamp
			if ts == 0 {
				ts = time.Now().UnixNano()
			}
			points = append(points, Point{
				Metric:    d.Metric,
				Value:     d.Value,
				Timestamp: ts,
				Tags:      d.Tags,
			})
		}

	case "csv":
		reader := csv.NewReader(r.Body)
		records, err := reader.ReadAll()
		if err != nil {
			http.Error(w, "Invalid CSV: "+err.Error(), http.StatusBadRequest)
			return
		}

		// Expected format: metric,value,timestamp (optional)
		for i, record := range records {
			if i == 0 && (record[0] == "metric" || record[0] == "Metric") {
				continue // Skip header
			}
			if len(record) < 2 {
				continue
			}
			val, err := strconv.ParseFloat(record[1], 64)
			if err != nil {
				continue
			}
			ts := time.Now().UnixNano()
			if len(record) >= 3 {
				if parsed, err := strconv.ParseInt(record[2], 10, 64); err == nil {
					ts = parsed
				}
			}
			points = append(points, Point{
				Metric:    record[0],
				Value:     val,
				Timestamp: ts,
			})
		}

	case "line":
		// InfluxDB line protocol
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read body: "+err.Error(), http.StatusBadRequest)
			return
		}
		lines := strings.Split(string(body), "\n")
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			// Simple parsing: metric,tag=val value=123 timestamp
			parts := strings.Fields(line)
			if len(parts) < 2 {
				continue
			}
			// Parse metric and tags
			metricParts := strings.Split(parts[0], ",")
			metric := metricParts[0]
			tags := make(map[string]string)
			for i := 1; i < len(metricParts); i++ {
				kv := strings.SplitN(metricParts[i], "=", 2)
				if len(kv) == 2 {
					tags[kv[0]] = kv[1]
				}
			}
			// Parse value
			valueParts := strings.SplitN(parts[1], "=", 2)
			if len(valueParts) != 2 {
				continue
			}
			val, err := strconv.ParseFloat(valueParts[1], 64)
			if err != nil {
				continue
			}
			ts := time.Now().UnixNano()
			if len(parts) >= 3 {
				if parsed, err := strconv.ParseInt(parts[2], 10, 64); err == nil {
					ts = parsed
				}
			}
			points = append(points, Point{
				Metric:    metric,
				Value:     val,
				Timestamp: ts,
				Tags:      tags,
			})
		}

	default:
		http.Error(w, "Unsupported format: "+format, http.StatusBadRequest)
		return
	}

	if len(points) == 0 {
		http.Error(w, "No valid data points found", http.StatusBadRequest)
		return
	}

	// Write points
	if err = ui.db.WriteBatch(points); err != nil {
		http.Error(w, "Write failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	ui.logActivity("Data Import", fmt.Sprintf("Imported %d points", len(points)))
	writeJSON(w, map[string]any{
		"status":   "ok",
		"imported": len(points),
		"format":   format,
	})
}

// Phase 10: System Diagnostics API
