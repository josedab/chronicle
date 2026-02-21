package chronicle

import (
	"bytes"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestPrometheusCompliance_RemoteWrite verifies the Prometheus remote write endpoint
// accepts valid requests and returns correct HTTP status codes.
func TestPrometheusCompliance_RemoteWrite(t *testing.T) {
	db := setupTestDB(t)

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

// TestPrometheusCompliance_PromQLComplianceSuite verifies ≥90% pass rate.
func TestPrometheusCompliance_PromQLComplianceSuite(t *testing.T) {
	suite := NewPromQLComplianceSuite()
	results := suite.RunAll()

	passed := 0
	for _, r := range results {
		if r.Passed {
			passed++
		}
	}

	total := len(results)
	rate := float64(passed) / float64(total)
	t.Logf("PromQL compliance: %d/%d (%.1f%%)", passed, total, rate*100)

	if rate < 0.90 {
		for _, r := range results {
			if !r.Passed {
				t.Logf("  FAIL: %s - %s (err: %s)", r.Category, r.Name, r.Error)
			}
		}
		t.Errorf("compliance pass rate %.1f%% is below 90%% target", rate*100)
	}
}

// TestPrometheusCompliance_RangeFunction verifies range function evaluation.
func TestPrometheusCompliance_RangeFunction(t *testing.T) {
	tests := []struct {
		name     string
		fn       PromQLRangeFunc
		samples  []PromQLSample
		rangeMs  int64
		wantNaN  bool
		wantMin  float64
		wantMax  float64
	}{
		{
			name: "rate with counter reset",
			fn:   RangeFuncRate,
			samples: []PromQLSample{
				{Timestamp: 0, Value: 10},
				{Timestamp: 1000, Value: 20},
				{Timestamp: 2000, Value: 5}, // reset
				{Timestamp: 3000, Value: 15},
			},
			rangeMs: 3000,
			wantMin: 0,
			wantMax: 100,
		},
		{
			name:    "rate with single sample returns NaN",
			fn:      RangeFuncRate,
			samples: []PromQLSample{{Timestamp: 0, Value: 10}},
			rangeMs: 1000,
			wantNaN: true,
		},
		{
			name: "increase without per-second",
			fn:   RangeFuncIncrease,
			samples: []PromQLSample{
				{Timestamp: 0, Value: 100},
				{Timestamp: 60000, Value: 200},
			},
			rangeMs: 60000,
			wantMin: 90,
			wantMax: 120,
		},
		{
			name: "irate uses last two samples",
			fn:   RangeFuncIrate,
			samples: []PromQLSample{
				{Timestamp: 0, Value: 0},
				{Timestamp: 1000, Value: 100},
				{Timestamp: 2000, Value: 300},
			},
			rangeMs: 2000,
			wantMin: 199,
			wantMax: 201,
		},
		{
			name: "changes counts value changes",
			fn:   RangeFuncChanges,
			samples: []PromQLSample{
				{Timestamp: 0, Value: 1},
				{Timestamp: 1000, Value: 1},
				{Timestamp: 2000, Value: 2},
				{Timestamp: 3000, Value: 2},
				{Timestamp: 4000, Value: 3},
			},
			rangeMs: 4000,
			wantMin: 2,
			wantMax: 2,
		},
		{
			name: "resets counts counter resets",
			fn:   RangeFuncResets,
			samples: []PromQLSample{
				{Timestamp: 0, Value: 10},
				{Timestamp: 1000, Value: 20},
				{Timestamp: 2000, Value: 5},
				{Timestamp: 3000, Value: 15},
				{Timestamp: 4000, Value: 3},
			},
			rangeMs: 4000,
			wantMin: 2,
			wantMax: 2,
		},
		{
			name: "predict_linear extrapolates",
			fn:   RangeFuncPredictLinear,
			samples: []PromQLSample{
				{Timestamp: 0, Value: 0},
				{Timestamp: 1000, Value: 10},
				{Timestamp: 2000, Value: 20},
			},
			rangeMs: 1000,
			wantMin: 25,
			wantMax: 35,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := EvalRangeFunction(tc.fn, tc.samples, tc.rangeMs)
			if tc.wantNaN {
				if !math.IsNaN(got) {
					t.Errorf("expected NaN, got %f", got)
				}
				return
			}
			if math.IsNaN(got) {
				t.Errorf("unexpected NaN")
				return
			}
			if got < tc.wantMin || got > tc.wantMax {
				t.Errorf("got %f, want between %f and %f", got, tc.wantMin, tc.wantMax)
			}
		})
	}
}
