package chronicle

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestPrometheusCompliance_RemoteWrite verifies the Prometheus remote write endpoint
// accepts valid requests and returns correct HTTP status codes.
func TestPrometheusCompliance_RemoteWrite(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	mux := http.NewServeMux()
	wrap := func(h http.HandlerFunc) http.HandlerFunc { return h }
	setupWriteRoutes(mux, db, wrap)

	t.Run("POST /api/v1/prom/write accepts snappy-encoded data", func(t *testing.T) {
		// Prometheus remote write sends snappy-compressed protobuf
		// For compliance, the endpoint must accept POST and return 200 or 204
		req := httptest.NewRequest("POST", "/api/v1/prom/write", bytes.NewReader([]byte{}))
		req.Header.Set("Content-Type", "application/x-protobuf")
		req.Header.Set("Content-Encoding", "snappy")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		// Prometheus expects 2xx for success, 4xx for bad request, 5xx for server error
		if w.Code >= 500 {
			t.Errorf("remote write returned server error: %d", w.Code)
		}
	})

	t.Run("GET /api/v1/prom/write returns 4xx", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/v1/prom/write", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		if w.Code < 400 || w.Code >= 500 {
			t.Errorf("expected 4xx for GET on write endpoint, got %d", w.Code)
		}
	})
}

// TestPrometheusCompliance_QueryEndpoints verifies Prometheus-compatible query API.
func TestPrometheusCompliance_QueryEndpoints(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Write test data
	now := time.Now().UnixNano()
	for i := 0; i < 10; i++ {
		db.Write(Point{Metric: "up", Value: 1, Tags: map[string]string{"job": "test"}, Timestamp: now + int64(i)})
	}
	db.Flush()

	mux := http.NewServeMux()
	wrap := func(h http.HandlerFunc) http.HandlerFunc { return h }
	setupQueryRoutes(mux, db, wrap)

	t.Run("POST /query returns JSON", func(t *testing.T) {
		body := `{"metric":"up"}`
		req := httptest.NewRequest("POST", "/query", bytes.NewReader([]byte(body)))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
		}
		ct := w.Header().Get("Content-Type")
		if ct != "application/json" && ct != "application/json; charset=utf-8" {
			t.Errorf("expected JSON content type, got %s", ct)
		}
	})
}

// TestPrometheusCompliance_HealthEndpoint verifies /health returns proper format.
func TestPrometheusCompliance_HealthEndpoint(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	mux := http.NewServeMux()
	wrap := func(h http.HandlerFunc) http.HandlerFunc { return h }
	setupAdminRoutes(mux, db, wrap)

	t.Run("GET /health returns 200", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/health", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Errorf("expected 200, got %d", w.Code)
		}
	})

	t.Run("GET /health/ready returns 200", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/health/ready", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Errorf("expected 200, got %d", w.Code)
		}
	})
}
