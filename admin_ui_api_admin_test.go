package chronicle

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAdminUI_APISchemas(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest(http.MethodGet, "/admin/api/schemas", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var schemas []map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&schemas); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
}

func TestAdminUI_APIRetention(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest(http.MethodGet, "/admin/api/retention", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	body := `{"metric": "*", "duration": "30d"}`
	req = httptest.NewRequest(http.MethodPost, "/admin/api/retention", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
}

func TestAdminUI_APIDiagnostics(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest("GET", "/admin/api/diagnostics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var result map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if _, ok := result["runtime"]; !ok {
		t.Error("expected runtime field in response")
	}
	if _, ok := result["memory"]; !ok {
		t.Error("expected memory field in response")
	}
}

func TestAdminUI_APISessions(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest("GET", "/admin/api/sessions", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var sessions []map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&sessions); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if sessions == nil {
		t.Error("expected non-nil sessions array")
	}
}

func TestAdminUI_APITemplates(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest("GET", "/admin/api/templates", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var templates []map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&templates); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(templates) == 0 {
		t.Error("expected built-in templates")
	}

	body := `{"name":"Test Template","category":"analysis","query":"SELECT * FROM test"}`
	req = httptest.NewRequest("POST", "/admin/api/templates", strings.NewReader(body))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
}

func TestAdminUI_APIAnnotations(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	body := `{"metric":"cpu","title":"Deployment v1.0","text":"Released new version"}`
	req := httptest.NewRequest("POST", "/admin/api/annotations", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	req = httptest.NewRequest("GET", "/admin/api/annotations?metric=cpu", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var annotations []map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&annotations); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(annotations) != 1 {
		t.Errorf("expected 1 annotation, got %d", len(annotations))
	}
}

func TestAdminUI_APIProfiling(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest("GET", "/admin/api/profiling?type=summary", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var result map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if _, ok := result["goroutines"]; !ok {
		t.Error("expected goroutines field")
	}

	req = httptest.NewRequest("GET", "/admin/api/profiling?type=memory", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
}

func TestAdminUI_APILogs(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{DevMode: true})
	handler := ui.Handler()

	body := `{"level":"info","message":"Test log message","source":"test"}`
	req := httptest.NewRequest("POST", "/admin/api/logs", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	req = httptest.NewRequest("GET", "/admin/api/logs", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var logs []map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&logs); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(logs) != 1 {
		t.Errorf("expected 1 log entry, got %d", len(logs))
	}
}

func TestAdminUI_APIRoles(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest("GET", "/admin/api/roles", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var roles []map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&roles); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(roles) < 4 {
		t.Errorf("expected at least 4 default roles, got %d", len(roles))
	}

	body := `{"name":"Developer","description":"Dev access","permissions":["read","write"]}`
	req = httptest.NewRequest("POST", "/admin/api/roles", strings.NewReader(body))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
}

func TestAdminUI_APIPermissions(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	body := `{"user":"test@example.com","role":"role_analyst"}`
	req := httptest.NewRequest("POST", "/admin/api/permissions", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	req = httptest.NewRequest("GET", "/admin/api/permissions?user=test@example.com", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var access map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&access); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if access["user"] != "test@example.com" {
		t.Error("expected user in response")
	}
}
