package chronicle

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestNewAdminUI(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	if ui == nil {
		t.Fatal("NewAdminUI returned nil")
	}

	handler := ui.Handler()
	if handler == nil {
		t.Error("handler should be initialized")
	}
}

func TestNewAdminUI_CustomPrefix(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{Prefix: "/custom"})

	req := httptest.NewRequest(http.MethodGet, "/custom", nil)
	rec := httptest.NewRecorder()
	ui.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
}

func TestAdminUI_Dashboard(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest(http.MethodGet, "/admin", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	contentType := rec.Header().Get("Content-Type")
	if contentType != "text/html; charset=utf-8" {
		t.Errorf("expected text/html content type, got %s", contentType)
	}
}

func TestAdminUI_GetDashboardData(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	db.Write(Point{Metric: "test", Value: 1.0, Timestamp: time.Now().UnixNano()})

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	// Test dashboard renders correctly (verifies getDashboardData indirectly)
	req := httptest.NewRequest(http.MethodGet, "/admin", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "Chronicle Admin") {
		t.Error("dashboard should contain title")
	}
}

func TestAdminConfig_Defaults(t *testing.T) {
	config := AdminConfig{}

	if config.Prefix != "" {
		t.Error("prefix should default to empty")
	}
	if config.Username != "" {
		t.Error("username should default to empty")
	}
	if config.Password != "" {
		t.Error("password should default to empty")
	}
}

func TestAdminUI_APIActivity(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest(http.MethodGet, "/admin/api/activity", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var activity []map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&activity); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
}

func TestAdminUI_DevModeFlag(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{DevMode: true})
	handler := ui.Handler()

	// Verify dev mode by checking that insert endpoint works (only in dev mode)
	body := `[{"metric": "test", "value": 1.0}]`
	req := httptest.NewRequest(http.MethodPost, "/admin/api/insert", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200 in dev mode, got %d", rec.Code)
	}
}

func TestAdminUI_ActivityLogging(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	// Trigger an activity by making a query
	db.Write(Point{Metric: "test", Value: 1.0, Timestamp: time.Now().UnixNano()})
	req := httptest.NewRequest(http.MethodGet, "/admin/api/query?q=SELECT%20mean(value)%20FROM%20test", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	// Check activity log
	req = httptest.NewRequest(http.MethodGet, "/admin/api/activity", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var activities []map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&activities); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(activities) == 0 {
		t.Error("expected at least 1 activity after query")
	}
}

func TestAdminUI_QueryHistoryTracking(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	// Execute a query to populate history
	db.Write(Point{Metric: "test", Value: 1.0, Timestamp: time.Now().UnixNano()})
	req := httptest.NewRequest(http.MethodGet, "/admin/api/query?q=SELECT%20mean(value)%20FROM%20test", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	// Check query history
	req = httptest.NewRequest(http.MethodGet, "/admin/api/query-history", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var history []map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&history); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(history) == 0 {
		t.Error("expected at least 1 query in history")
	}

	_ = ui // keep reference
}

func TestAdminUI_HasPermission(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	body := `{"user":"analyst@test.com","role":"role_analyst"}`
	req := httptest.NewRequest("POST", "/admin/api/permissions", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if !ui.HasPermission("analyst@test.com", "read") {
		t.Error("analyst should have read permission")
	}

	if !ui.HasPermission("analyst@test.com", "export") {
		t.Error("analyst should have export permission")
	}

	if ui.HasPermission("analyst@test.com", "write") {
		t.Error("analyst should not have write permission")
	}

	if ui.HasPermission("unknown@test.com", "read") {
		t.Error("unknown user should not have any permission")
	}
}
