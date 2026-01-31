package chronicle

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func setupTestDB(t *testing.T) *DB {
	t.Helper()
	dir := t.TempDir()
	path := dir + "/test.db"
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		t.Fatalf("failed to open test db: %v", err)
	}
	return db
}

func TestNewAdminUI(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	if ui == nil {
		t.Fatal("NewAdminUI returned nil")
	}

	if ui.mux == nil {
		t.Error("mux should be initialized")
	}
}

func TestNewAdminUI_CustomPrefix(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{Prefix: "/custom"})

	// Should respond on custom prefix
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

func TestAdminUI_APIStats(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest(http.MethodGet, "/admin/api/stats", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var stats map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&stats); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if _, ok := stats["uptime"]; !ok {
		t.Error("expected uptime in stats")
	}
	if _, ok := stats["version"]; !ok {
		t.Error("expected version in stats")
	}
	if _, ok := stats["memory"]; !ok {
		t.Error("expected memory in stats")
	}
}

func TestAdminUI_APIMetrics(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Write some data using WriteBatch which flushes immediately
	db.WriteBatch([]Point{
		{Metric: "cpu", Value: 1.0, Timestamp: time.Now().UnixNano()},
		{Metric: "memory", Value: 2.0, Timestamp: time.Now().UnixNano()},
	})

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest(http.MethodGet, "/admin/api/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var metrics []map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&metrics); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(metrics) < 2 {
		t.Errorf("expected at least 2 metrics, got %d", len(metrics))
	}
}

func TestAdminUI_APISeries(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	db.WriteBatch([]Point{{Metric: "cpu", Value: 1.0, Timestamp: time.Now().UnixNano()}})

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	// Without filter
	req := httptest.NewRequest(http.MethodGet, "/admin/api/series", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	// With metric filter
	req = httptest.NewRequest(http.MethodGet, "/admin/api/series?metric=cpu", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
}

func TestAdminUI_APIQuery(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	now := time.Now()
	db.WriteBatch([]Point{{Metric: "cpu", Value: 1.0, Timestamp: now.UnixNano()}})

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	// Use a valid query format with aggregation
	req := httptest.NewRequest(http.MethodGet, "/admin/api/query?q=SELECT%20mean(value)%20FROM%20cpu", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d; body: %s", rec.Code, rec.Body.String())
	}
}

func TestAdminUI_APIQuery_MissingQuery(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest(http.MethodGet, "/admin/api/query", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", rec.Code)
	}
}

func TestAdminUI_APIQuery_InvalidQuery(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest(http.MethodGet, "/admin/api/query?q=INVALID", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", rec.Code)
	}
}

func TestAdminUI_APIConfig(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest(http.MethodGet, "/admin/api/config", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var config map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&config); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if _, ok := config["buffer_size"]; !ok {
		t.Error("expected buffer_size in config")
	}
}

func TestAdminUI_APIHealth(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest(http.MethodGet, "/admin/api/health", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var health map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&health); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	status, ok := health["status"].(string)
	if !ok || status != "healthy" {
		t.Errorf("expected healthy status, got %v", health["status"])
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		input    uint64
		expected string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1024, "1.0 KB"},
		{1024 * 1024, "1.0 MB"},
		{1024 * 1024 * 1024, "1.0 GB"},
		{1536, "1.5 KB"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := formatBytes(tt.input)
			if result != tt.expected {
				t.Errorf("formatBytes(%d) = %s, want %s", tt.input, result, tt.expected)
			}
		})
	}
}

func TestAdminUI_GetDashboardData(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	db.Write(Point{Metric: "test", Value: 1.0, Timestamp: time.Now().UnixNano()})

	ui := NewAdminUI(db, AdminConfig{})
	data := ui.getDashboardData()

	if data.Title == "" {
		t.Error("title should not be empty")
	}
	if data.Version == "" {
		t.Error("version should not be empty")
	}
	if data.GoVersion == "" {
		t.Error("go version should not be empty")
	}
	if data.NumCPU <= 0 {
		t.Error("num cpu should be positive")
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
