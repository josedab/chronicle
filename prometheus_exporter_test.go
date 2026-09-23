package chronicle

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestWritePrometheusMetrics(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Write sample points so series and metrics exist
	err := db.Write(Point{
		Metric:    "cpu_usage",
		Tags:      map[string]string{"host": "server-1", "region": "us-east"},
		Value:     42.5,
		Timestamp: time.Now().UnixNano(),
	})
	if err != nil {
		t.Fatalf("Write point failed: %v", err)
	}
	_ = db.Flush()

	httpMetrics := NewHTTPMetrics()
	httpMetrics.record("/write", http.StatusOK, 5*time.Millisecond)
	httpMetrics.record("/query", http.StatusOK, 12*time.Millisecond)
	httpMetrics.record("/api/v1/bad", http.StatusBadRequest, 2*time.Millisecond)

	var buf bytes.Buffer
	startTime := time.Now().Add(-10 * time.Second)
	WritePrometheusMetrics(&buf, db, httpMetrics, startTime)

	output := buf.String()

	// 1. Verify Prometheus HELP and TYPE annotations
	requiredMetrics := []string{
		"chronicle_build_info",
		"chronicle_uptime_seconds",
		"chronicle_series_total",
		"chronicle_metrics_total",
		"chronicle_http_requests_total",
		"chronicle_http_errors_total",
		"chronicle_http_requests_by_status_total",
		"chronicle_http_route_requests_total",
		"chronicle_http_route_duration_seconds",
		"chronicle_goroutines",
		"chronicle_memory_alloc_bytes",
		"chronicle_memory_sys_bytes",
		"chronicle_gc_runs_total",
	}

	for _, m := range requiredMetrics {
		if !strings.Contains(output, "# HELP "+m) {
			t.Errorf("expected output to contain HELP for %s", m)
		}
		if !strings.Contains(output, "# TYPE "+m) {
			t.Errorf("expected output to contain TYPE for %s", m)
		}
	}

	// 2. Verify values
	if !strings.Contains(output, "chronicle_series_total 1") {
		t.Errorf("expected chronicle_series_total 1 in output")
	}
	if !strings.Contains(output, "chronicle_metrics_total 1") {
		t.Errorf("expected chronicle_metrics_total 1 in output")
	}
	if !strings.Contains(output, "chronicle_http_requests_total 3") {
		t.Errorf("expected chronicle_http_requests_total 3 in output")
	}
	if !strings.Contains(output, "chronicle_http_errors_total 1") {
		t.Errorf("expected chronicle_http_errors_total 1 in output")
	}
	if !strings.Contains(output, "chronicle_http_requests_by_status_total{status=\"200\"} 2") {
		t.Errorf("expected status 200 count = 2 in output")
	}
	if !strings.Contains(output, "chronicle_http_requests_by_status_total{status=\"400\"} 1") {
		t.Errorf("expected status 400 count = 1 in output")
	}
}

func TestHTTPMetrics_PrometheusExposition(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	mux := http.NewServeMux()
	wrap := middlewareWrapper(func(h http.HandlerFunc) http.HandlerFunc { return h })
	setupAdminRoutes(mux, db, wrap, nil)

	// Test 1: Default GET /metrics (Prometheus scraper format)
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", w.Code)
	}
	ct := w.Header().Get("Content-Type")
	if !strings.Contains(ct, "text/plain") {
		t.Errorf("expected Content-Type text/plain, got %s", ct)
	}
	body := w.Body.String()
	if !strings.Contains(body, "chronicle_build_info") || !strings.Contains(body, "chronicle_uptime_seconds") {
		t.Errorf("expected Prometheus metrics in response, got: %s", body)
	}

	// Test 2: Explicit Accept: text/plain
	req = httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Accept", "text/plain; version=0.0.4")
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "chronicle_build_info") {
		t.Errorf("expected Prometheus metrics in response")
	}

	// Test 3: Query parameter ?format=prometheus
	req = httptest.NewRequest(http.MethodGet, "/metrics?format=prometheus", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "chronicle_build_info") {
		t.Errorf("expected Prometheus metrics in response")
	}
}

func TestHTTPMetrics_JSONBackwardCompatibility(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	_ = db.Write(Point{
		Metric:    "disk_io",
		Value:     100.0,
		Timestamp: time.Now().UnixNano(),
	})
	_ = db.Flush()

	mux := http.NewServeMux()
	wrap := middlewareWrapper(func(h http.HandlerFunc) http.HandlerFunc { return h })
	setupAdminRoutes(mux, db, wrap, nil)

	// Test 1: Accept: application/json
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Accept", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", w.Code)
	}
	ct := w.Header().Get("Content-Type")
	if !strings.Contains(ct, "application/json") {
		t.Errorf("expected Content-Type application/json, got %s", ct)
	}
	var metrics []string
	if err := json.NewDecoder(w.Body).Decode(&metrics); err != nil {
		t.Fatalf("failed to decode JSON metrics list: %v", err)
	}
	if len(metrics) != 1 || metrics[0] != "disk_io" {
		t.Errorf("expected ['disk_io'], got %v", metrics)
	}

	// Test 2: ?format=json
	req = httptest.NewRequest(http.MethodGet, "/metrics?format=json", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", w.Code)
	}
	var metricsQuery []string
	if err := json.NewDecoder(w.Body).Decode(&metricsQuery); err != nil {
		t.Fatalf("failed to decode JSON metrics list: %v", err)
	}
	if len(metricsQuery) != 1 || metricsQuery[0] != "disk_io" {
		t.Errorf("expected ['disk_io'], got %v", metricsQuery)
	}
}

func TestHTTPMetrics_MethodNotAllowed(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	mux := http.NewServeMux()
	wrap := middlewareWrapper(func(h http.HandlerFunc) http.HandlerFunc { return h })
	setupAdminRoutes(mux, db, wrap, nil)

	req := httptest.NewRequest(http.MethodPost, "/metrics", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 Method Not Allowed, got %d", w.Code)
	}
}
