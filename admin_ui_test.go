package chronicle

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
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

// Tests for new Phase 2-7 endpoints

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

func TestAdminUI_APIQueryHistory(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	// Execute a query first to add to history
	req := httptest.NewRequest(http.MethodGet, "/admin/api/query?q=SELECT%20mean(value)%20FROM%20test", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	// Now check history
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
		t.Error("expected at least one query in history")
	}
}

func TestAdminUI_APITags(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest(http.MethodGet, "/admin/api/tags", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var tags map[string][]string
	if err := json.NewDecoder(rec.Body).Decode(&tags); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
}

func TestAdminUI_APIDataPreview(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	db.WriteBatch([]Point{{Metric: "cpu", Value: 42.5, Timestamp: time.Now().UnixNano()}})

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest(http.MethodGet, "/admin/api/data-preview?metric=cpu", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var preview map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&preview); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if preview["metric"] != "cpu" {
		t.Errorf("expected metric 'cpu', got %v", preview["metric"])
	}
}

func TestAdminUI_APIDataPreview_MissingMetric(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest(http.MethodGet, "/admin/api/data-preview", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", rec.Code)
	}
}

func TestAdminUI_APIMetricDetails(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	db.WriteBatch([]Point{{Metric: "cpu", Value: 42.5, Timestamp: time.Now().UnixNano()}})

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest(http.MethodGet, "/admin/api/metric-details?metric=cpu", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var details map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&details); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if details["name"] != "cpu" {
		t.Errorf("expected name 'cpu', got %v", details["name"])
	}
}

func TestAdminUI_APIMetricDetails_NotFound(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest(http.MethodGet, "/admin/api/metric-details?metric=nonexistent", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", rec.Code)
	}
}

func TestAdminUI_APIPartitions(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest(http.MethodGet, "/admin/api/partitions", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var partitions map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&partitions); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
}

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

func TestAdminUI_DevModeFlag(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{DevMode: true})
	data := ui.getDashboardData()

	if !data.DevMode {
		t.Error("expected DevMode to be true")
	}
}

func TestAdminUI_ActivityLogging(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})

	// Log an activity
	ui.logActivity("Test", "test details")

	// Verify it was recorded
	if len(ui.activityLog) != 1 {
		t.Errorf("expected 1 activity, got %d", len(ui.activityLog))
	}

	if ui.activityLog[0].Action != "Test" {
		t.Errorf("expected action 'Test', got %s", ui.activityLog[0].Action)
	}
}

func TestAdminUI_QueryHistoryTracking(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})

	// Add query to history
	ui.addQueryHistory("SELECT * FROM test", time.Millisecond*10, true, "")

	// Verify it was recorded
	if len(ui.queryHistory) != 1 {
		t.Errorf("expected 1 query in history, got %d", len(ui.queryHistory))
	}

	if ui.queryHistory[0].Query != "SELECT * FROM test" {
		t.Errorf("expected query 'SELECT * FROM test', got %s", ui.queryHistory[0].Query)
	}

	if !ui.queryHistory[0].Success {
		t.Error("expected query to be marked as success")
	}
}

// Tests for Phase 7-9 enhanced endpoints

func TestAdminUI_APIAlerts(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	// GET - initially empty
	req := httptest.NewRequest(http.MethodGet, "/admin/api/alerts", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var alerts []map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&alerts); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(alerts) != 0 {
		t.Errorf("expected 0 alerts, got %d", len(alerts))
	}

	// POST - create alert
	body := `{"name": "Test Alert", "metric": "cpu", "condition": "above", "threshold": 80}`
	req = httptest.NewRequest(http.MethodPost, "/admin/api/alerts", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var created map[string]interface{}
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

	var entries []map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&entries); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
}

func TestAdminUI_APIQueryExplain(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest(http.MethodGet, "/admin/api/query-explain?q=SELECT%20mean(value)%20FROM%20cpu", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if result["parsed_function"] != "mean" {
		t.Errorf("expected parsed_function 'mean', got %v", result["parsed_function"])
	}

	if result["parsed_metric"] != "cpu" {
		t.Errorf("expected parsed_metric 'cpu', got %v", result["parsed_metric"])
	}
}

func TestAdminUI_APIQueryExplain_MissingQuery(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest(http.MethodGet, "/admin/api/query-explain", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", rec.Code)
	}
}

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

	var schemas []map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&schemas); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
}

func TestAdminUI_APIRetention(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	// GET - initially empty
	req := httptest.NewRequest(http.MethodGet, "/admin/api/retention", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	// POST - create retention policy
	body := `{"metric": "*", "duration": "30d"}`
	req = httptest.NewRequest(http.MethodPost, "/admin/api/retention", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
}

func TestAdminUI_APICluster(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest(http.MethodGet, "/admin/api/cluster", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var status map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&status); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if status["mode"] != "standalone" {
		t.Errorf("expected mode 'standalone', got %v", status["mode"])
	}
}

func TestAdminUI_APIWAL(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest(http.MethodGet, "/admin/api/wal", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var walInfo map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&walInfo); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if walInfo["enabled"] != true {
		t.Errorf("expected enabled true, got %v", walInfo["enabled"])
	}
}

func TestAdminUI_APIScheduledExports(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	// GET - initially empty
	req := httptest.NewRequest(http.MethodGet, "/admin/api/scheduled-exports", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	// POST - create export
	body := `{"name": "Daily Report", "query": "SELECT mean(value) FROM cpu", "format": "json", "schedule": "daily"}`
	req = httptest.NewRequest(http.MethodPost, "/admin/api/scheduled-exports", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
}

func TestAdminUI_APISearch(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	db.WriteBatch([]Point{{Metric: "cpu_usage", Value: 42.5, Timestamp: time.Now().UnixNano()}})

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	// Search for metrics
	req := httptest.NewRequest(http.MethodGet, "/admin/api/search?q=cpu", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var results []map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&results); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	// Should find the cpu_usage metric
	found := false
	for _, r := range results {
		if r["name"] == "cpu_usage" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected to find cpu_usage metric in search results")
	}

	// Search for pages
	req = httptest.NewRequest(http.MethodGet, "/admin/api/search?q=dash", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	if err := json.NewDecoder(rec.Body).Decode(&results); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	found = false
	for _, r := range results {
		if r["name"] == "Dashboard" && r["type"] == "page" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected to find Dashboard page in search results")
	}
}

func TestAdminUI_APISearch_Empty(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest(http.MethodGet, "/admin/api/search?q=", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var results []interface{}
	if err := json.NewDecoder(rec.Body).Decode(&results); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("expected empty results for empty query, got %d", len(results))
	}
}

// Phase 10 Tests

func TestAdminUI_APIAutocomplete(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Add test data
	db.Write(Point{Metric: "cpu", Value: 50, Timestamp: time.Now().UnixNano()})
	db.Write(Point{Metric: "memory", Value: 70, Timestamp: time.Now().UnixNano()})

	ui := NewAdminUI(db, AdminConfig{})
	req := httptest.NewRequest("GET", "/admin/api/autocomplete?context=function", nil)
	rec := httptest.NewRecorder()
	ui.handleAPIAutocomplete(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var result []map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(result) == 0 {
		t.Error("expected non-empty autocomplete results")
	}
}

func TestAdminUI_APISavedQueries(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})

	// Create a saved query
	body := `{"name":"Test Query","query":"SELECT * FROM cpu"}`
	req := httptest.NewRequest("POST", "/admin/api/saved-queries", strings.NewReader(body))
	rec := httptest.NewRecorder()
	ui.handleAPISavedQueries(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	// List saved queries
	req = httptest.NewRequest("GET", "/admin/api/saved-queries", nil)
	rec = httptest.NewRecorder()
	ui.handleAPISavedQueries(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var queries []map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&queries); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(queries) != 1 {
		t.Errorf("expected 1 query, got %d", len(queries))
	}
}

func TestAdminUI_APISparkline(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Add test data
	now := time.Now()
	for i := 0; i < 10; i++ {
		db.Write(Point{Metric: "test_metric", Value: float64(i * 10), Timestamp: now.Add(-time.Duration(i) * time.Minute).UnixNano()})
	}

	ui := NewAdminUI(db, AdminConfig{})
	req := httptest.NewRequest("GET", "/admin/api/sparkline?metric=test_metric", nil)
	rec := httptest.NewRecorder()
	ui.handleAPISparkline(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if result["metric"] != "test_metric" {
		t.Errorf("expected metric name in response")
	}
}

func TestAdminUI_APICompare(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Add test data
	now := time.Now()
	for i := 0; i < 5; i++ {
		db.Write(Point{Metric: "cpu", Value: float64(i * 10), Timestamp: now.Add(-time.Duration(i) * time.Minute).UnixNano()})
		db.Write(Point{Metric: "memory", Value: float64(i * 20), Timestamp: now.Add(-time.Duration(i) * time.Minute).UnixNano()})
	}

	ui := NewAdminUI(db, AdminConfig{})
	req := httptest.NewRequest("GET", "/admin/api/compare?metrics=cpu,memory", nil)
	rec := httptest.NewRecorder()
	ui.handleAPICompare(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if _, ok := result["comparison"]; !ok {
		t.Error("expected comparison field in response")
	}
}

func TestAdminUI_APIFavorites(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})

	// Add a favorite
	body := `{"type":"metric","name":"cpu"}`
	req := httptest.NewRequest("POST", "/admin/api/favorites", strings.NewReader(body))
	rec := httptest.NewRecorder()
	ui.handleAPIFavorites(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	// List favorites
	req = httptest.NewRequest("GET", "/admin/api/favorites", nil)
	rec = httptest.NewRecorder()
	ui.handleAPIFavorites(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var favorites []map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&favorites); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(favorites) != 1 {
		t.Errorf("expected 1 favorite, got %d", len(favorites))
	}
}

func TestAdminUI_APIRecent(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})

	// Track a recent item
	body := `{"type":"metric","name":"cpu"}`
	req := httptest.NewRequest("POST", "/admin/api/recent", strings.NewReader(body))
	rec := httptest.NewRecorder()
	ui.handleAPIRecent(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	// List recent items
	req = httptest.NewRequest("GET", "/admin/api/recent", nil)
	rec = httptest.NewRecorder()
	ui.handleAPIRecent(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var recent []map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&recent); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(recent) != 1 {
		t.Errorf("expected 1 recent item, got %d", len(recent))
	}
}

func TestAdminUI_APIAlertHistory(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	req := httptest.NewRequest("GET", "/admin/api/alert-history", nil)
	rec := httptest.NewRecorder()
	ui.handleAPIAlertHistory(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var history []map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&history); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	// Should return empty array, not error
	if history == nil {
		t.Error("expected non-nil array")
	}
}

func TestAdminUI_APIImport_JSON(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{DevMode: true}) // dev mode for import

	// JSON array directly in body (format from query param)
	body := `[{"metric":"test","value":42,"timestamp":1000000000}]`
	req := httptest.NewRequest("POST", "/admin/api/import?format=json", strings.NewReader(body))
	rec := httptest.NewRecorder()
	ui.handleAPIImport(rec, req)

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

	ui := NewAdminUI(db, AdminConfig{}) // not dev mode

	body := `[{"metric":"test","value":42,"timestamp":1000000000}]`
	req := httptest.NewRequest("POST", "/admin/api/import?format=json", strings.NewReader(body))
	rec := httptest.NewRecorder()
	ui.handleAPIImport(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Errorf("expected status 403, got %d", rec.Code)
	}
}

func TestAdminUI_APIDiagnostics(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	req := httptest.NewRequest("GET", "/admin/api/diagnostics", nil)
	rec := httptest.NewRecorder()
	ui.handleAPIDiagnostics(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	// Check for nested structure
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
	req := httptest.NewRequest("GET", "/admin/api/sessions", nil)
	rec := httptest.NewRecorder()
	ui.handleAPISessions(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var sessions []map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&sessions); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	// Sessions array should be valid (may be empty)
	if sessions == nil {
		t.Error("expected non-nil sessions array")
	}
}

// Phase 11 Tests

func TestAdminUI_APITemplates(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})

	// Get built-in templates
	req := httptest.NewRequest("GET", "/admin/api/templates", nil)
	rec := httptest.NewRecorder()
	ui.handleAPITemplates(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var templates []map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&templates); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	// Should have built-in templates
	if len(templates) == 0 {
		t.Error("expected built-in templates")
	}

	// Create a custom template
	body := `{"name":"Test Template","category":"analysis","query":"SELECT * FROM test"}`
	req = httptest.NewRequest("POST", "/admin/api/templates", strings.NewReader(body))
	rec = httptest.NewRecorder()
	ui.handleAPITemplates(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
}

func TestAdminUI_APIAnnotations(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})

	// Create an annotation
	body := `{"metric":"cpu","title":"Deployment v1.0","text":"Released new version"}`
	req := httptest.NewRequest("POST", "/admin/api/annotations", strings.NewReader(body))
	rec := httptest.NewRecorder()
	ui.handleAPIAnnotations(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	// Get annotations
	req = httptest.NewRequest("GET", "/admin/api/annotations?metric=cpu", nil)
	rec = httptest.NewRecorder()
	ui.handleAPIAnnotations(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var annotations []map[string]interface{}
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

	// Test summary
	req := httptest.NewRequest("GET", "/admin/api/profiling?type=summary", nil)
	rec := httptest.NewRecorder()
	ui.handleAPIProfiling(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if _, ok := result["goroutines"]; !ok {
		t.Error("expected goroutines field")
	}

	// Test memory profile
	req = httptest.NewRequest("GET", "/admin/api/profiling?type=memory", nil)
	rec = httptest.NewRecorder()
	ui.handleAPIProfiling(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
}

func TestAdminUI_APILogs(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{DevMode: true})

	// Post a log entry
	body := `{"level":"info","message":"Test log message","source":"test"}`
	req := httptest.NewRequest("POST", "/admin/api/logs", strings.NewReader(body))
	rec := httptest.NewRecorder()
	ui.handleAPILogs(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	// Get logs
	req = httptest.NewRequest("GET", "/admin/api/logs", nil)
	rec = httptest.NewRecorder()
	ui.handleAPILogs(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var logs []map[string]interface{}
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

	// Get default roles
	req := httptest.NewRequest("GET", "/admin/api/roles", nil)
	rec := httptest.NewRecorder()
	ui.handleAPIRoles(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var roles []map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&roles); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	// Should have default roles (admin, operator, analyst, viewer)
	if len(roles) < 4 {
		t.Errorf("expected at least 4 default roles, got %d", len(roles))
	}

	// Create a custom role
	body := `{"name":"Developer","description":"Dev access","permissions":["read","write"]}`
	req = httptest.NewRequest("POST", "/admin/api/roles", strings.NewReader(body))
	rec = httptest.NewRecorder()
	ui.handleAPIRoles(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
}

func TestAdminUI_APIPermissions(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})

	// Assign a role to a user
	body := `{"user":"test@example.com","role":"role_analyst"}`
	req := httptest.NewRequest("POST", "/admin/api/permissions", strings.NewReader(body))
	rec := httptest.NewRecorder()
	ui.handleAPIPermissions(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	// Get user permissions
	req = httptest.NewRequest("GET", "/admin/api/permissions?user=test@example.com", nil)
	rec = httptest.NewRecorder()
	ui.handleAPIPermissions(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var access map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&access); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if access["user"] != "test@example.com" {
		t.Error("expected user in response")
	}
}

func TestAdminUI_HasPermission(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})

	// Assign analyst role
	body := `{"user":"analyst@test.com","role":"role_analyst"}`
	req := httptest.NewRequest("POST", "/admin/api/permissions", strings.NewReader(body))
	rec := httptest.NewRecorder()
	ui.handleAPIPermissions(rec, req)

	// Check permissions
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
