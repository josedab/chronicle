package adminui

import (
	"fmt"
	"net/http"
	"time"
)

// Phase 10: Session Management API
func (ui *AdminUI) handleAPISessions(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		ui.mu.RLock()
		sessions := make([]sessionInfo, len(ui.sessions))
		copy(sessions, ui.sessions)
		ui.mu.RUnlock()

		// Filter active only if requested
		if r.URL.Query().Get("active") == "true" {
			filtered := make([]sessionInfo, 0)
			for _, s := range sessions {
				if s.Active {
					filtered = append(filtered, s)
				}
			}
			sessions = filtered
		}
		writeJSON(w, sessions)

	case http.MethodPost:
		// Record new session
		session := sessionInfo{
			ID:        fmt.Sprintf("sess_%d", time.Now().UnixNano()),
			User:      r.Header.Get("X-User"),
			IP:        r.RemoteAddr,
			UserAgent: r.UserAgent(),
			StartedAt: time.Now(),
			LastSeen:  time.Now(),
			Active:    true,
		}
		if session.User == "" {
			session.User = "anonymous"
		}

		ui.mu.Lock()
		ui.sessions = append(ui.sessions, session)
		// Keep only last 100 sessions
		if len(ui.sessions) > 100 {
			ui.sessions = ui.sessions[len(ui.sessions)-100:]
		}
		ui.mu.Unlock()

		writeJSON(w, session)

	case http.MethodDelete:
		id := r.URL.Query().Get("id")
		if id == "" {
			http.Error(w, "Missing session id", http.StatusBadRequest)
			return
		}

		ui.mu.Lock()
		for i := range ui.sessions {
			if ui.sessions[i].ID == id {
				ui.sessions[i].Active = false
				break
			}
		}
		ui.mu.Unlock()

		ui.logAudit(r, "SessionRevoked", id)
		writeJSON(w, map[string]string{"status": "revoked", "id": id})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}
