package chronicle

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

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

	var stats map[string]any
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

	var metrics []map[string]any
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

	req := httptest.NewRequest(http.MethodGet, "/admin/api/series", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

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

	var config map[string]any
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

	var health map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&health); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	status, ok := health["status"].(string)
	if !ok || status != "healthy" {
		t.Errorf("expected healthy status, got %v", health["status"])
	}
}

func TestAdminUI_APIQueryHistory(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest(http.MethodGet, "/admin/api/query?q=SELECT%20mean(value)%20FROM%20test", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	req = httptest.NewRequest(http.MethodGet, "/admin/api/query-history", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var history []map[string]any
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

	var preview map[string]any
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

	var details map[string]any
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

	var partitions map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&partitions); err != nil {
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

	var result map[string]any
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

	var status map[string]any
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

	var walInfo map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&walInfo); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if walInfo["enabled"] != true {
		t.Errorf("expected enabled true, got %v", walInfo["enabled"])
	}
}
