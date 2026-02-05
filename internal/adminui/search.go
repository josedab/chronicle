package adminui

import (
	"net/http"
	"strings"
)

// Phase 9: Global Search API
func (ui *AdminUI) handleAPISearch(w http.ResponseWriter, r *http.Request) {
	query := strings.ToLower(r.URL.Query().Get("q"))
	if query == "" {
		writeJSON(w, []interface{}{})
		return
	}

	results := make([]map[string]interface{}, 0)

	// Search metrics
	metrics := ui.db.Metrics()
	for _, metric := range metrics {
		if strings.Contains(strings.ToLower(metric), query) {
			results = append(results, map[string]interface{}{
				"type":   "metric",
				"name":   metric,
				"action": "query",
				"icon":   "📊",
			})
		}
	}

	// Search pages
	pages := []struct{ name, icon string }{
		{"Dashboard", "🏠"},
		{"Health", "💚"},
		{"Explorer", "🔍"},
		{"Query Console", "⚡"},
		{"Management", "🗑️"},
		{"Configuration", "⚙️"},
		{"Backup", "💾"},
		{"Alerts", "🔔"},
		{"Audit Log", "📋"},
		{"Schema Registry", "📐"},
		{"Retention", "🕐"},
		{"Cluster", "🌐"},
		{"WAL Inspector", "💾"},
		{"Scheduled Exports", "📤"},
	}

	for _, page := range pages {
		if strings.Contains(strings.ToLower(page.name), query) {
			results = append(results, map[string]interface{}{
				"type":   "page",
				"name":   page.name,
				"action": "navigate",
				"icon":   page.icon,
			})
		}
	}

	// Limit results
	if len(results) > 10 {
		results = results[:10]
	}

	writeJSON(w, results)
}

// Phase 10: Query Autocomplete API
func (ui *AdminUI) handleAPIAutocomplete(w http.ResponseWriter, r *http.Request) {
	prefix := strings.ToLower(r.URL.Query().Get("prefix"))
	context := r.URL.Query().Get("context") // "metric", "function", "keyword"

	suggestions := make([]map[string]string, 0)

	switch context {
	case "function":
		functions := []struct{ name, desc string }{
			{"mean", "Calculate average value"},
			{"sum", "Calculate sum of values"},
			{"count", "Count data points"},
			{"max", "Find maximum value"},
			{"min", "Find minimum value"},
			{"first", "Get first value"},
			{"last", "Get last value"},
			{"median", "Calculate median value"},
			{"stddev", "Standard deviation"},
			{"percentile", "Calculate percentile"},
		}
		for _, f := range functions {
			if prefix == "" || strings.HasPrefix(f.name, prefix) {
				suggestions = append(suggestions, map[string]string{
					"value": f.name + "(value)",
					"label": f.name,
					"desc":  f.desc,
					"type":  "function",
				})
			}
		}

	case "keyword":
		keywords := []string{"SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "LIMIT", "AND", "OR", "time"}
		for _, kw := range keywords {
			if prefix == "" || strings.HasPrefix(strings.ToLower(kw), prefix) {
				suggestions = append(suggestions, map[string]string{
					"value": kw,
					"label": kw,
					"type":  "keyword",
				})
			}
		}

	default: // metrics
		metrics := ui.db.Metrics()
		for _, m := range metrics {
			if prefix == "" || strings.HasPrefix(strings.ToLower(m), prefix) {
				suggestions = append(suggestions, map[string]string{
					"value": m,
					"label": m,
					"type":  "metric",
				})
			}
		}
	}

	// Limit suggestions
	if len(suggestions) > 15 {
		suggestions = suggestions[:15]
	}

	writeJSON(w, suggestions)
}
