package adminui

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Phase 11: Initialize built-in query templates
func initBuiltInTemplates() []queryTemplate {
	return []queryTemplate{
		{
			ID:          "tpl_avg_last_hour",
			Name:        "Average (Last Hour)",
			Description: "Calculate average value over the last hour",
			Category:    "aggregation",
			Query:       "SELECT mean(value) FROM {{metric}} WHERE time > now() - 1h",
			Variables:   []string{"metric"},
			BuiltIn:     true,
		},
		{
			ID:          "tpl_max_last_day",
			Name:        "Maximum (Last 24h)",
			Description: "Find maximum value in the last 24 hours",
			Category:    "aggregation",
			Query:       "SELECT max(value) FROM {{metric}} WHERE time > now() - 24h",
			Variables:   []string{"metric"},
			BuiltIn:     true,
		},
		{
			ID:          "tpl_min_last_day",
			Name:        "Minimum (Last 24h)",
			Description: "Find minimum value in the last 24 hours",
			Category:    "aggregation",
			Query:       "SELECT min(value) FROM {{metric}} WHERE time > now() - 24h",
			Variables:   []string{"metric"},
			BuiltIn:     true,
		},
		{
			ID:          "tpl_count_by_hour",
			Name:        "Count by Hour",
			Description: "Count data points grouped by hour",
			Category:    "analysis",
			Query:       "SELECT count(value) FROM {{metric}} GROUP BY time(1h)",
			Variables:   []string{"metric"},
			BuiltIn:     true,
		},
		{
			ID:          "tpl_rate_per_minute",
			Name:        "Rate per Minute",
			Description: "Calculate the rate of change per minute",
			Category:    "analysis",
			Query:       "SELECT derivative(mean(value), 1m) FROM {{metric}} GROUP BY time(1m)",
			Variables:   []string{"metric"},
			BuiltIn:     true,
		},
		{
			ID:          "tpl_percentile_95",
			Name:        "95th Percentile",
			Description: "Calculate 95th percentile over time",
			Category:    "analysis",
			Query:       "SELECT percentile(value, 95) FROM {{metric}} WHERE time > now() - 1h GROUP BY time(5m)",
			Variables:   []string{"metric"},
			BuiltIn:     true,
		},
		{
			ID:          "tpl_anomaly_detection",
			Name:        "Anomaly Detection",
			Description: "Find values outside 2 standard deviations",
			Category:    "monitoring",
			Query:       "SELECT value FROM {{metric}} WHERE value > mean(value) + 2*stddev(value) OR value < mean(value) - 2*stddev(value)",
			Variables:   []string{"metric"},
			BuiltIn:     true,
		},
		{
			ID:          "tpl_top_n",
			Name:        "Top N Values",
			Description: "Get top N highest values",
			Category:    "analysis",
			Query:       "SELECT top(value, {{n}}) FROM {{metric}} WHERE time > now() - 1h",
			Variables:   []string{"metric", "n"},
			BuiltIn:     true,
		},
		{
			ID:          "tpl_moving_average",
			Name:        "Moving Average",
			Description: "Calculate moving average over time windows",
			Category:    "analysis",
			Query:       "SELECT moving_average(mean(value), {{window}}) FROM {{metric}} GROUP BY time(1m)",
			Variables:   []string{"metric", "window"},
			BuiltIn:     true,
		},
		{
			ID:          "tpl_compare_periods",
			Name:        "Compare Time Periods",
			Description: "Compare current hour vs previous hour",
			Category:    "monitoring",
			Query:       "SELECT mean(value) FROM {{metric}} WHERE time > now() - 1h GROUP BY time(5m)",
			Variables:   []string{"metric"},
			BuiltIn:     true,
		},
	}
}

// Phase 11: Query Templates API
func (ui *AdminUI) handleAPITemplates(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		category := r.URL.Query().Get("category")
		ui.mu.RLock()
		templates := make([]queryTemplate, 0)
		for _, t := range ui.queryTemplates {
			if category == "" || t.Category == category {
				templates = append(templates, t)
			}
		}
		ui.mu.RUnlock()
		writeJSON(w, templates)

	case http.MethodPost:
		var tpl queryTemplate
		if err := json.NewDecoder(r.Body).Decode(&tpl); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		if tpl.Name == "" || tpl.Query == "" {
			http.Error(w, "Name and query are required", http.StatusBadRequest)
			return
		}

		tpl.ID = fmt.Sprintf("tpl_%d", time.Now().UnixNano())
		tpl.BuiltIn = false

		ui.mu.Lock()
		ui.queryTemplates = append(ui.queryTemplates, tpl)
		ui.mu.Unlock()

		ui.logAudit(r, "CreateTemplate", tpl.Name)
		writeJSON(w, tpl)

	case http.MethodDelete:
		id := r.URL.Query().Get("id")
		if id == "" {
			http.Error(w, "Missing id parameter", http.StatusBadRequest)
			return
		}

		ui.mu.Lock()
		for i, t := range ui.queryTemplates {
			if t.ID == id {
				if t.BuiltIn {
					ui.mu.Unlock()
					http.Error(w, "Cannot delete built-in templates", http.StatusForbidden)
					return
				}
				ui.queryTemplates = append(ui.queryTemplates[:i], ui.queryTemplates[i+1:]...)
				break
			}
		}
		ui.mu.Unlock()

		ui.logAudit(r, "DeleteTemplate", id)
		writeJSON(w, map[string]string{"status": "deleted"})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}
