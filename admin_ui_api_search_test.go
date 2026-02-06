package chronicle

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAdminUI_APISearch(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	db.WriteBatch([]Point{{Metric: "cpu_usage", Value: 42.5, Timestamp: time.Now().UnixNano()}})

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

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

func TestAdminUI_APIAutocomplete(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	db.Write(Point{Metric: "cpu", Value: 50, Timestamp: time.Now().UnixNano()})
	db.Write(Point{Metric: "memory", Value: 70, Timestamp: time.Now().UnixNano()})

	ui := NewAdminUI(db, AdminConfig{})
	handler := ui.Handler()

	req := httptest.NewRequest("GET", "/admin/api/autocomplete?context=function", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

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
	handler := ui.Handler()

	body := `{"name":"Test Query","query":"SELECT * FROM cpu"}`
	req := httptest.NewRequest("POST", "/admin/api/saved-queries", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	req = httptest.NewRequest("GET", "/admin/api/saved-queries", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

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
