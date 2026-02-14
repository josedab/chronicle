package chronicle

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAdminUI_APIBackup_GET(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest(http.MethodGet, "/admin/api/backup", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
}

func TestAdminUI_APIBackup_POST(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest(http.MethodPost, "/admin/api/backup", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if result["status"] != "ok" {
		t.Errorf("expected status 'ok', got %v", result["status"])
	}
}

func TestAdminUI_APIInsert_DevMode(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{DevMode: true})
	handler := ui.Handler()

	body := `[{"metric": "test", "value": 42.5}]`
	req := httptest.NewRequest(http.MethodPost, "/admin/api/insert", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var result map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if result["status"] != "ok" {
		t.Errorf("expected status 'ok', got %v", result["status"])
	}
}

func TestAdminUI_APIInsert_NotDevMode(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{DevMode: false})
	handler := ui.Handler()

	body := `[{"metric": "test", "value": 42.5}]`
	req := httptest.NewRequest(http.MethodPost, "/admin/api/insert", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Errorf("expected status 403, got %d", rec.Code)
	}
}

func TestAdminUI_APIDeleteMetric(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest(http.MethodDelete, "/admin/api/delete-metric?metric=cpu", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
}

func TestAdminUI_APIDeleteMetric_WrongMethod(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest(http.MethodGet, "/admin/api/delete-metric?metric=cpu", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status 405, got %d", rec.Code)
	}
}

func TestAdminUI_APITruncate(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest(http.MethodPost, "/admin/api/truncate?metric=cpu", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
}

func TestAdminUI_APIExport_JSON(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	db.WriteBatch([]Point{{Metric: "cpu", Value: 42.5, Timestamp: time.Now().UnixNano()}})

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest(http.MethodGet, "/admin/api/export?q=SELECT%20mean(value)%20FROM%20cpu&format=json", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	contentType := rec.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("expected application/json content type, got %s", contentType)
	}
}

func TestAdminUI_APIExport_CSV(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	db.WriteBatch([]Point{{Metric: "cpu", Value: 42.5, Timestamp: time.Now().UnixNano()}})

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest(http.MethodGet, "/admin/api/export?q=SELECT%20mean(value)%20FROM%20cpu&format=csv", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	contentType := rec.Header().Get("Content-Type")
	if contentType != "text/csv" {
		t.Errorf("expected text/csv content type, got %s", contentType)
	}
}

func TestAdminUI_APIScheduledExports(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest(http.MethodGet, "/admin/api/scheduled-exports", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	body := `{"name": "Daily Report", "query": "SELECT mean(value) FROM cpu", "format": "json", "schedule": "daily"}`
	req = httptest.NewRequest(http.MethodPost, "/admin/api/scheduled-exports", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
}

func TestAdminUI_APIImport_JSON(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{DevMode: true})
	handler := ui.Handler()

	body := `[{"metric":"test","value":42,"timestamp":1000000000}]`
	req := httptest.NewRequest("POST", "/admin/api/import?format=json", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var result map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if result["imported"].(float64) != 1 {
		t.Errorf("expected 1 imported, got %v", result["imported"])
	}
}

func TestAdminUI_APIImport_NotDevMode(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	body := `[{"metric":"test","value":42,"timestamp":1000000000}]`
	req := httptest.NewRequest("POST", "/admin/api/import?format=json", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Errorf("expected status 403, got %d", rec.Code)
	}
}
