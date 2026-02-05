package adminui

import (
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// Phase 10: Alert History API
func (ui *AdminUI) handleAPIAlertHistory(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= 200 {
			limit = n
		}
	}

	alertID := r.URL.Query().Get("alert_id") // Optional filter

	ui.mu.RLock()
	entries := make([]alertHistoryEntry, 0, limit)
	count := 0
	for i := len(ui.alertHistory) - 1; i >= 0 && count < limit; i-- {
		entry := ui.alertHistory[i]
		if alertID == "" || entry.AlertID == alertID {
			entries = append(entries, entry)
			count++
		}
	}
	ui.mu.RUnlock()

	writeJSON(w, entries)
}

func (ui *AdminUI) recordAlertHistory(alertID, alertName, state string, value, threshold float64, message string) {
	entry := alertHistoryEntry{
		ID:        fmt.Sprintf("ah_%d", time.Now().UnixNano()),
		AlertID:   alertID,
		AlertName: alertName,
		State:     state,
		Value:     value,
		Threshold: threshold,
		Timestamp: time.Now(),
		Message:   message,
	}

	ui.mu.Lock()
	ui.alertHistory = append(ui.alertHistory, entry)
	if len(ui.alertHistory) > 200 {
		ui.alertHistory = ui.alertHistory[1:]
	}
	ui.mu.Unlock()
}
