package chronicle

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestEBPFHTTPCollector_RecordEvent(t *testing.T) {
	collector := NewEBPFHTTPCollector(DefaultEBPFHTTPConfig())

	collector.RecordEvent(EBPFHTTPEvent{
		Method:     "GET",
		Path:       "/api/users",
		StatusCode: 200,
		LatencyNs:  5_000_000, // 5ms
	})
	collector.RecordEvent(EBPFHTTPEvent{
		Method:     "POST",
		Path:       "/api/users",
		StatusCode: 500,
		LatencyNs:  50_000_000, // 50ms
	})

	stats := collector.GetStats()
	if stats.TotalRequests != 2 {
		t.Errorf("expected 2 requests, got %d", stats.TotalRequests)
	}
	if stats.TotalErrors != 1 {
		t.Errorf("expected 1 error, got %d", stats.TotalErrors)
	}
	if stats.RequestsByMethod["GET"] != 1 {
		t.Errorf("expected 1 GET, got %d", stats.RequestsByMethod["GET"])
	}
	if stats.RequestsByStatus[200] != 1 {
		t.Errorf("expected 1 200, got %d", stats.RequestsByStatus[200])
	}
}

func TestEBPFHTTPCollector_PathNormalization(t *testing.T) {
	config := DefaultEBPFHTTPConfig()
	config.PathNormalization = true
	collector := NewEBPFHTTPCollector(config)

	collector.RecordEvent(EBPFHTTPEvent{
		Method:     "GET",
		Path:       "/api/users/12345",
		StatusCode: 200,
	})
	collector.RecordEvent(EBPFHTTPEvent{
		Method:     "GET",
		Path:       "/api/users/67890",
		StatusCode: 200,
	})

	stats := collector.GetStats()
	// Both should be normalized to the same path
	if stats.TopPaths["/api/users/:id"] != 2 {
		t.Errorf("expected 2 hits for normalized path, got %d (paths: %v)",
			stats.TopPaths["/api/users/:id"], stats.TopPaths)
	}
}

func TestEBPFHTTPCollector_RecentEvents(t *testing.T) {
	collector := NewEBPFHTTPCollector(DefaultEBPFHTTPConfig())

	for i := 0; i < 10; i++ {
		collector.RecordEvent(EBPFHTTPEvent{
			Method:     "GET",
			Path:       "/test",
			StatusCode: 200,
		})
	}

	events := collector.GetRecentEvents(5)
	if len(events) != 5 {
		t.Errorf("expected 5 recent events, got %d", len(events))
	}

	// Get all
	all := collector.GetRecentEvents(0)
	if len(all) != 10 {
		t.Errorf("expected 10 total events, got %d", len(all))
	}
}

func TestEBPFHTTPCollector_ToPoints(t *testing.T) {
	collector := NewEBPFHTTPCollector(DefaultEBPFHTTPConfig())
	collector.RecordEvent(EBPFHTTPEvent{Method: "GET", StatusCode: 200})
	collector.RecordEvent(EBPFHTTPEvent{Method: "POST", StatusCode: 201})

	points := collector.ToPoints()
	if len(points) == 0 {
		t.Error("expected points from ToPoints")
	}

	// Should have both method and status metrics
	hasMethod := false
	hasStatus := false
	for _, p := range points {
		if p.Metric == "ebpf_http_requests_total" {
			hasMethod = true
		}
		if p.Metric == "ebpf_http_responses_total" {
			hasStatus = true
		}
	}
	if !hasMethod {
		t.Error("expected ebpf_http_requests_total metric")
	}
	if !hasStatus {
		t.Error("expected ebpf_http_responses_total metric")
	}
}

func TestEBPFHTTPCollector_Middleware(t *testing.T) {
	collector := NewEBPFHTTPCollector(DefaultEBPFHTTPConfig())

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(1 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	wrapped := collector.HTTPMiddleware(handler)

	req := httptest.NewRequest("GET", "/api/test", nil)
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}

	stats := collector.GetStats()
	if stats.TotalRequests != 1 {
		t.Errorf("expected 1 request recorded, got %d", stats.TotalRequests)
	}
}

func TestEBPFHTTPCollector_EventBufferLimit(t *testing.T) {
	collector := NewEBPFHTTPCollector(DefaultEBPFHTTPConfig())

	// Write more than buffer size
	for i := 0; i < 10001; i++ {
		collector.RecordEvent(EBPFHTTPEvent{Method: "GET", StatusCode: 200})
	}

	events := collector.GetRecentEvents(0)
	if len(events) != 10000 {
		t.Errorf("expected buffer capped at 10000, got %d", len(events))
	}
}
