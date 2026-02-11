package chronicle

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAdminUI_APISparkline(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	now := time.Now()
	for i := 0; i < 10; i++ {
		db.Write(Point{Metric: "test_metric", Value: float64(i * 10), Timestamp: now.Add(-time.Duration(i) * time.Minute).UnixNano()})
	}

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest("GET", "/admin/api/sparkline?metric=test_metric", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var result map[string]any
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

	now := time.Now()
	for i := 0; i < 5; i++ {
		db.Write(Point{Metric: "cpu", Value: float64(i * 10), Timestamp: now.Add(-time.Duration(i) * time.Minute).UnixNano()})
		db.Write(Point{Metric: "memory", Value: float64(i * 20), Timestamp: now.Add(-time.Duration(i) * time.Minute).UnixNano()})
	}

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest("GET", "/admin/api/compare?metrics=cpu,memory", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var result map[string]any
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
	handler := ui.Handler()

	body := `{"type":"metric","name":"cpu"}`
	req := httptest.NewRequest("POST", "/admin/api/favorites", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	req = httptest.NewRequest("GET", "/admin/api/favorites", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var favorites []map[string]any
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
	handler := ui.Handler()

	body := `{"type":"metric","name":"cpu"}`
	req := httptest.NewRequest("POST", "/admin/api/recent", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	req = httptest.NewRequest("GET", "/admin/api/recent", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var recent []map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&recent); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(recent) != 1 {
		t.Errorf("expected 1 recent item, got %d", len(recent))
	}
}
