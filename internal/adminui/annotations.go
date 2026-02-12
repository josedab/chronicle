package adminui

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Phase 11: Metric Annotations API
func (ui *AdminUI) handleAPIAnnotations(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		metric := r.URL.Query().Get("metric")
		ui.mu.RLock()
		annotations := make([]metricAnnotation, 0)
		for _, a := range ui.annotations {
			if metric == "" || a.Metric == metric {
				annotations = append(annotations, a)
			}
		}
		ui.mu.RUnlock()
		writeJSON(w, annotations)

	case http.MethodPost:
		var ann metricAnnotation
		if err := json.NewDecoder(r.Body).Decode(&ann); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		if ann.Metric == "" || ann.Title == "" {
			http.Error(w, "Metric and title are required", http.StatusBadRequest)
			return
		}

		ann.ID = fmt.Sprintf("ann_%d", time.Now().UnixNano())
		ann.CreatedAt = time.Now()
		if ann.CreatedBy == "" {
			ann.CreatedBy = r.Header.Get("X-User")
			if ann.CreatedBy == "" {
				ann.CreatedBy = "anonymous"
			}
		}

		ui.mu.Lock()
		ui.annotations = append(ui.annotations, ann)
		ui.mu.Unlock()

		ui.logAudit(r, "CreateAnnotation", fmt.Sprintf("%s: %s", ann.Metric, ann.Title))
		writeJSON(w, ann)

	case http.MethodPut:
		var ann metricAnnotation
		if err := json.NewDecoder(r.Body).Decode(&ann); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		ui.mu.Lock()
		for i, a := range ui.annotations {
			if a.ID == ann.ID {
				ann.CreatedAt = a.CreatedAt
				ann.CreatedBy = a.CreatedBy
				ui.annotations[i] = ann
				break
			}
		}
		ui.mu.Unlock()

		ui.logAudit(r, "UpdateAnnotation", ann.ID)
		writeJSON(w, ann)

	case http.MethodDelete:
		id := r.URL.Query().Get("id")
		if id == "" {
			http.Error(w, "Missing id parameter", http.StatusBadRequest)
			return
		}

		ui.mu.Lock()
		for i, a := range ui.annotations {
			if a.ID == id {
				ui.annotations = append(ui.annotations[:i], ui.annotations[i+1:]...)
				break
			}
		}
		ui.mu.Unlock()

		ui.logAudit(r, "DeleteAnnotation", id)
		writeJSON(w, map[string]string{"status": "deleted"})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}
