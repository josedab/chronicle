package adminui

import (
	"fmt"
	"net/http"
	"strings"
	"time"
)

// Phase 10: Multi-Metric Compare API
func (ui *AdminUI) handleAPICompare(w http.ResponseWriter, r *http.Request) {
	metricsParam := r.URL.Query().Get("metrics")
	if metricsParam == "" {
		http.Error(w, "Missing metrics parameter", http.StatusBadRequest)
		return
	}

	metricNames := strings.Split(metricsParam, ",")
	if len(metricNames) > 10 {
		metricNames = metricNames[:10] // Limit to 10 metrics
	}

	end := time.Now()
	start := end.Add(-1 * time.Hour)

	// Parse time range if provided
	if s := r.URL.Query().Get("start"); s != "" {
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			start = t
		}
	}
	if e := r.URL.Query().Get("end"); e != "" {
		if t, err := time.Parse(time.RFC3339, e); err == nil {
			end = t
		}
	}

	results := make(map[string]interface{})
	for _, metric := range metricNames {
		metric = strings.TrimSpace(metric)
		if metric == "" {
			continue
		}

		q, err := ui.db.ParseQuery(fmt.Sprintf("SELECT mean(value) FROM %s", metric))
		if err != nil {
			results[metric] = map[string]interface{}{
				"error": err.Error(),
			}
			continue
		}
		q.Start = start.UnixNano()
		q.End = end.UnixNano()

		result, err := ui.db.Execute(q)
		if err != nil {
			results[metric] = map[string]interface{}{
				"error": err.Error(),
			}
			continue
		}

		points := make([]map[string]interface{}, 0)
		var minVal, maxVal, sum float64
		if result != nil && len(result.Points) > 0 {
			minVal = result.Points[0].Value
			maxVal = result.Points[0].Value
			for _, p := range result.Points {
				points = append(points, map[string]interface{}{
					"timestamp": p.Timestamp,
					"value":     p.Value,
				})
				sum += p.Value
				if p.Value < minVal {
					minVal = p.Value
				}
				if p.Value > maxVal {
					maxVal = p.Value
				}
			}
		}

		meanVal := 0.0
		if len(result.Points) > 0 {
			meanVal = sum / float64(len(result.Points))
		}

		results[metric] = map[string]interface{}{
			"points": points,
			"min":    minVal,
			"max":    maxVal,
			"mean":   meanVal,
		}
	}

	writeJSON(w, map[string]interface{}{
		"metrics":    metricNames,
		"start":      start.Format(time.RFC3339),
		"end":        end.Format(time.RFC3339),
		"comparison": results,
	})
}
