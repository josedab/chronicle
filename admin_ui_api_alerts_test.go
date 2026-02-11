package chronicle

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAdminUI_APIAlerts(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest(http.MethodGet, "/admin/api/alerts", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var alerts []map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&alerts); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(alerts) != 0 {
		t.Errorf("expected 0 alerts, got %d", len(alerts))
	}

	body := `{"name": "Test Alert", "metric": "cpu", "condition": "above", "threshold": 80}`
	req = httptest.NewRequest(http.MethodPost, "/admin/api/alerts", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var created map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&created); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if created["name"] != "Test Alert" {
		t.Errorf("expected name 'Test Alert', got %v", created["name"])
	}
}

func TestAdminUI_APIAuditLog(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest(http.MethodGet, "/admin/api/audit-log", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var entries []map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&entries); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
}

func TestAdminUI_APIAlertHistory(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest("GET", "/admin/api/alert-history", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var history []map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&history); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if history == nil {
		t.Error("expected non-nil array")
	}
}
