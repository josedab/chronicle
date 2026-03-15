package chronicle

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHttpCore(t *testing.T) {
	t.Run("rate_limiter_creation", func(t *testing.T) {
		rl := newRateLimiter(100, time.Second)
		if rl == nil {
			t.Fatal("expected non-nil rateLimiter")
		}
		defer rl.Stop()
	})

	t.Run("rate_limiter_allows_within_limit", func(t *testing.T) {
		rl := newRateLimiter(5, time.Minute)
		defer rl.Stop()

		for i := 0; i < 5; i++ {
			if !rl.allow("10.0.0.1") {
				t.Errorf("request %d should be allowed", i+1)
			}
		}
	})

	t.Run("rate_limiter_blocks_over_limit", func(t *testing.T) {
		rl := newRateLimiter(3, time.Minute)
		defer rl.Stop()

		for i := 0; i < 3; i++ {
			rl.allow("10.0.0.2")
		}
		if rl.allow("10.0.0.2") {
			t.Error("4th request should be blocked")
		}
	})

	t.Run("rate_limiter_independent_ips", func(t *testing.T) {
		rl := newRateLimiter(1, time.Minute)
		defer rl.Stop()

		if !rl.allow("10.0.0.3") {
			t.Error("first IP should be allowed")
		}
		if !rl.allow("10.0.0.4") {
			t.Error("second IP should be allowed independently")
		}
	})

	t.Run("rate_limiter_stop_idempotent", func(t *testing.T) {
		rl := newRateLimiter(10, time.Second)
		rl.Stop()
		rl.Stop() // should not panic
	})
}

func TestRequestIDMiddleware(t *testing.T) {
	t.Run("generates ID when not provided", func(t *testing.T) {
		handler := requestIDMiddleware(func(w http.ResponseWriter, r *http.Request) {
			reqID := RequestIDFromContext(r.Context())
			if reqID == "" {
				t.Error("expected request ID in context")
			}
			w.WriteHeader(http.StatusOK)
		})

		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		rec := httptest.NewRecorder()
		handler(rec, req)

		if rec.Header().Get("X-Request-ID") == "" {
			t.Error("expected X-Request-ID response header")
		}
	})

	t.Run("propagates client provided ID", func(t *testing.T) {
		handler := requestIDMiddleware(func(w http.ResponseWriter, r *http.Request) {
			reqID := RequestIDFromContext(r.Context())
			if reqID != "client-123" {
				t.Errorf("expected 'client-123', got %q", reqID)
			}
			w.WriteHeader(http.StatusOK)
		})

		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.Header.Set("X-Request-ID", "client-123")
		rec := httptest.NewRecorder()
		handler(rec, req)

		if rec.Header().Get("X-Request-ID") != "client-123" {
			t.Errorf("expected 'client-123' in response, got %q", rec.Header().Get("X-Request-ID"))
		}
	})

	t.Run("context returns empty when no middleware", func(t *testing.T) {
		id := RequestIDFromContext(context.Background())
		if id != "" {
			t.Errorf("expected empty, got %q", id)
		}
	})
}

func TestCORSMiddlewareGlobal(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}

	t.Run("no origin header passes through", func(t *testing.T) {
		m := corsMiddlewareGlobal([]string{"http://example.com"})
		rr := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/test", nil)
		m(handler)(rr, req)
		if rr.Header().Get("Access-Control-Allow-Origin") != "" {
			t.Error("expected no CORS header without Origin")
		}
	})

	t.Run("allowed origin gets headers", func(t *testing.T) {
		m := corsMiddlewareGlobal([]string{"http://example.com"})
		rr := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("Origin", "http://example.com")
		m(handler)(rr, req)
		if rr.Header().Get("Access-Control-Allow-Origin") != "http://example.com" {
			t.Error("expected CORS origin header")
		}
	})

	t.Run("wildcard allows any origin", func(t *testing.T) {
		m := corsMiddlewareGlobal([]string{"*"})
		rr := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("Origin", "http://anything.com")
		m(handler)(rr, req)
		if rr.Header().Get("Access-Control-Allow-Origin") != "http://anything.com" {
			t.Error("expected wildcard CORS to allow any origin")
		}
	})

	t.Run("disallowed origin gets no headers", func(t *testing.T) {
		m := corsMiddlewareGlobal([]string{"http://example.com"})
		rr := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("Origin", "http://evil.com")
		m(handler)(rr, req)
		if rr.Header().Get("Access-Control-Allow-Origin") != "" {
			t.Error("expected no CORS header for disallowed origin")
		}
	})

	t.Run("preflight OPTIONS returns 204", func(t *testing.T) {
		m := corsMiddlewareGlobal([]string{"*"})
		rr := httptest.NewRecorder()
		req := httptest.NewRequest("OPTIONS", "/test", nil)
		req.Header.Set("Origin", "http://example.com")
		m(handler)(rr, req)
		if rr.Code != http.StatusNoContent {
			t.Errorf("expected 204, got %d", rr.Code)
		}
	})
}

func TestHTTPMetricsMiddleware(t *testing.T) {
	metrics := NewHTTPMetrics()
	handler := func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/error" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}

	mw := httpMetricsMiddleware(metrics)

	rr := httptest.NewRecorder()
	mw(handler)(rr, httptest.NewRequest("GET", "/ok", nil))

	rr2 := httptest.NewRecorder()
	mw(handler)(rr2, httptest.NewRequest("GET", "/error", nil))

	snap := metrics.Snapshot()
	if snap.TotalRequests != 2 {
		t.Errorf("expected 2 total requests, got %d", snap.TotalRequests)
	}
	if snap.TotalErrors != 1 {
		t.Errorf("expected 1 error, got %d", snap.TotalErrors)
	}
	if snap.StatusCounts[200] != 1 {
		t.Errorf("expected 1x200, got %d", snap.StatusCounts[200])
	}
	if snap.StatusCounts[500] != 1 {
		t.Errorf("expected 1x500, got %d", snap.StatusCounts[500])
	}
	if rs, ok := snap.RouteStats["/ok"]; !ok || rs.Count != 1 {
		t.Error("expected route stats for /ok")
	}
}
