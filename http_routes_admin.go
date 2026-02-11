package chronicle

import (
	"encoding/json"
	"net/http"
)

// setupAdminRoutes configures admin and operational endpoints
func setupAdminRoutes(mux *http.ServeMux, db *DB, wrap middlewareWrapper) {
	mux.HandleFunc("/health", wrap(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]string{"status": "ok"})
	}))

	mux.HandleFunc("/metrics", wrap(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		writeJSON(w, db.Metrics())
	}))

	mux.HandleFunc("/schemas", wrap(func(w http.ResponseWriter, r *http.Request) {
		handleSchemas(db, w, r)
	}))

	adminUI := NewAdminUI(db, AdminConfig{Prefix: "/admin"})
	mux.Handle("/admin", wrap(adminUI.ServeHTTP))
	mux.Handle("/admin/", wrap(adminUI.ServeHTTP))
}

// setupAlertingRoutes configures alerting-related endpoints
func setupAlertingRoutes(mux *http.ServeMux, db *DB, wrap middlewareWrapper) {
	mux.HandleFunc("/api/v1/alerts", wrap(func(w http.ResponseWriter, r *http.Request) {
		handleAlerts(db, w, r)
	}))

	mux.HandleFunc("/api/v1/rules", wrap(func(w http.ResponseWriter, r *http.Request) {
		handleRules(db, w, r)
	}))
}

// handleSchemas handles schema registry operations
func handleSchemas(db *DB, w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		schemas := db.ListSchemas()
		writeJSON(w, schemas)

	case http.MethodPost:
		var schema MetricSchema
		if err := json.NewDecoder(r.Body).Decode(&schema); err != nil {
			writeError(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := db.RegisterSchema(schema); err != nil {
			writeError(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusCreated)

	case http.MethodDelete:
		name := r.URL.Query().Get("name")
		if name == "" {
			writeError(w, "name parameter required", http.StatusBadRequest)
			return
		}
		db.UnregisterSchema(name)
		w.WriteHeader(http.StatusNoContent)

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// handleAlerts handles alert listing
func handleAlerts(db *DB, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	am := db.AlertManager()
	if am == nil {
		writeError(w, "alert manager not initialized", http.StatusServiceUnavailable)
		return
	}
	alerts := am.ListAlerts()

	resp := make([]map[string]interface{}, len(alerts))
	for i, a := range alerts {
		resp[i] = map[string]interface{}{
			"name":    a.Rule.Name,
			"state":   a.State.String(),
			"value":   a.Value,
			"firedAt": a.FiredAt,
			"labels":  a.Labels,
		}
	}

	writeJSON(w, map[string]interface{}{
		"status": "success",
		"data":   map[string]interface{}{"alerts": resp},
	})
}

// handleRules handles alert rule management
func handleRules(db *DB, w http.ResponseWriter, r *http.Request) {
	am := db.AlertManager()
	if am == nil {
		writeError(w, "alert manager not initialized", http.StatusServiceUnavailable)
		return
	}

	switch r.Method {
	case http.MethodGet:
		rules := am.ListRules()
		resp := make([]map[string]interface{}, len(rules))
		for i, r := range rules {
			resp[i] = map[string]interface{}{
				"name":        r.Name,
				"metric":      r.Metric,
				"condition":   conditionString(r.Condition),
				"threshold":   r.Threshold,
				"forDuration": r.ForDuration.String(),
			}
		}
		writeJSON(w, map[string]interface{}{
			"status": "success",
			"data":   map[string]interface{}{"groups": []interface{}{map[string]interface{}{"rules": resp}}},
		})

	case http.MethodPost:
		var rule AlertRule
		if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
			writeError(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := am.AddRule(rule); err != nil {
			writeError(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusCreated)

	case http.MethodDelete:
		name := r.URL.Query().Get("name")
		if name == "" {
			writeError(w, "name parameter required", http.StatusBadRequest)
			return
		}
		am.RemoveRule(name)
		w.WriteHeader(http.StatusNoContent)

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}
