package adminui

import (
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// Phase 10: Sparkline Data API
func (ui *AdminUI) handleAPISparkline(w http.ResponseWriter, r *http.Request) {
	metric := r.URL.Query().Get("metric")
	if metric == "" {
		http.Error(w, "Missing metric parameter", http.StatusBadRequest)
		return
	}

	points := 20 // Number of points for sparkline
	if p := r.URL.Query().Get("points"); p != "" {
		if n, err := strconv.Atoi(p); err == nil && n > 0 && n <= 100 {
			points = n
		}
	}

	// Get recent data for the metric
	end := time.Now()
	start := end.Add(-1 * time.Hour)

	q, err := ui.db.ParseQuery(fmt.Sprintf("SELECT mean(value) FROM %s", metric))
	if err != nil {
		writeJSON(w, map[string]any{
			"metric": metric,
			"values": []float64{},
		})
		return
	}
	q.Start = start.UnixNano()
	q.End = end.UnixNano()

	result, err := ui.db.Execute(q)
	if err != nil {
		writeJSON(w, map[string]any{
			"metric": metric,
			"values": []float64{},
		})
		return
	}

	values := make([]float64, 0, points)
	var minVal, maxVal float64
	if result != nil && len(result.Points) > 0 {
		minVal = result.Points[0].Value
		maxVal = result.Points[0].Value
		step := len(result.Points) / points
		if step < 1 {
			step = 1
		}
		for i := 0; i < len(result.Points) && len(values) < points; i += step {
			v := result.Points[i].Value
			values = append(values, v)
			if v < minVal {
				minVal = v
			}
			if v > maxVal {
				maxVal = v
			}
		}
	}

	writeJSON(w, map[string]any{
		"metric": metric,
		"values": values,
		"min":    minVal,
		"max":    maxVal,
	})
}
