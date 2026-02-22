package chronicle

import (
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestIntegration_WriteValidationPipeline tests the full write path:
// PointValidator → WritePipeline hooks → Schema → Write → Post-hooks → AuditLog
func TestIntegration_WriteValidationPipeline(t *testing.T) {
	db := setupTestDB(t)

	pv := db.PointValidator()
	wp := db.WritePipeline()
	al := db.AuditLog()

	if pv == nil || wp == nil || al == nil {
		t.Fatal("expected all features to be available")
	}

	t.Run("valid point passes full pipeline", func(t *testing.T) {
		p := Point{
			Metric:    "cpu.usage",
			Value:     42.5,
			Tags:      map[string]string{"host": "prod-1"},
			Timestamp: time.Now().UnixNano(),
		}
		if err := db.Write(p); err != nil {
			t.Fatalf("valid point should pass pipeline: %v", err)
		}

		entries := al.Query("write", 10)
		found := false
		for _, e := range entries {
			if e.Resource == "cpu.usage" {
				found = true
				if !e.Success {
					t.Error("expected success=true in audit log")
				}
			}
		}
		if !found {
			t.Error("expected audit log entry for cpu.usage write")
		}
	})

	t.Run("NaN rejected by validator", func(t *testing.T) {
		err := db.Write(Point{Metric: "bad", Value: math.NaN(), Timestamp: time.Now().UnixNano()})
		if err == nil {
			t.Error("expected NaN to be rejected")
		}
	})

	t.Run("Inf rejected by validator", func(t *testing.T) {
		err := db.Write(Point{Metric: "bad", Value: math.Inf(1), Timestamp: time.Now().UnixNano()})
		if err == nil {
			t.Error("expected Inf to be rejected")
		}
	})

	t.Run("empty metric rejected by validator", func(t *testing.T) {
		err := db.Write(Point{Value: 42, Timestamp: time.Now().UnixNano()})
		if err == nil {
			t.Error("expected empty metric to be rejected")
		}
	})

	t.Run("pre-write hook modifies point", func(t *testing.T) {
		wp.Register(WriteHook{
			Name:  "add-env-tag",
			Phase: "pre",
			Handler: func(p Point) (Point, error) {
				if p.Tags == nil {
					p.Tags = map[string]string{}
				}
				p.Tags["env"] = "test"
				return p, nil
			},
		})
		defer wp.Unregister("add-env-tag")

		err := db.Write(Point{Metric: "enriched", Value: 100, Tags: map[string]string{"host": "a"}, Timestamp: time.Now().UnixNano()})
		if err != nil {
			t.Fatalf("write with hook should succeed: %v", err)
		}
	})

	t.Run("pre-write hook rejects point", func(t *testing.T) {
		wp.Register(WriteHook{
			Name:  "reject-negative",
			Phase: "pre",
			Handler: func(p Point) (Point, error) {
				if p.Value < 0 {
					return p, fmt.Errorf("negative values not allowed")
				}
				return p, nil
			},
		})
		defer wp.Unregister("reject-negative")

		err := db.Write(Point{Metric: "neg", Value: -5, Timestamp: time.Now().UnixNano()})
		if err == nil {
			t.Error("expected negative value to be rejected by hook")
		}
	})

	t.Run("batch write validates all points", func(t *testing.T) {
		points := []Point{
			{Metric: "batch.ok1", Value: 1},
			{Metric: "batch.ok2", Value: 2},
		}
		if err := db.WriteBatch(points); err != nil {
			t.Fatalf("valid batch should succeed: %v", err)
		}

		entries := al.Query("write_batch", 10)
		if len(entries) == 0 {
			t.Error("expected audit log entry for batch write")
		}
	})

	t.Run("batch rejects on invalid point", func(t *testing.T) {
		points := []Point{
			{Metric: "batch.ok", Value: 1},
			{Metric: "", Value: 2}, // empty metric
		}
		if db.WriteBatch(points) == nil {
			t.Error("expected batch with invalid point to fail")
		}
	})
}

// TestIntegration_QueryMiddleware tests the query path with middleware chain.
func TestIntegration_QueryMiddleware(t *testing.T) {
	db := setupTestDB(t)

	qm := db.QueryMiddleware()
	if qm == nil {
		t.Fatal("expected query middleware to be available")
	}

	now := time.Now().UnixNano()
	for i := 0; i < 10; i++ {
		db.Write(Point{Metric: "mw_test", Value: float64(i), Timestamp: now + int64(i)})
	}
	db.Flush()

	t.Run("query without middleware", func(t *testing.T) {
		result, err := db.Execute(&Query{Metric: "mw_test", Start: 0, End: now + 100})
		if err != nil {
			t.Fatalf("query should work: %v", err)
		}
		if result == nil || len(result.Points) == 0 {
			t.Error("expected results")
		}
	})

	t.Run("middleware intercepts query", func(t *testing.T) {
		intercepted := false
		qm.Use("logger", 1, func(q *Query, next func(*Query) (*Result, error)) (*Result, error) {
			intercepted = true
			return next(q)
		})

		result, err := db.Execute(&Query{Metric: "mw_test", Start: 0, End: now + 100})
		if err != nil {
			t.Fatalf("query with middleware should work: %v", err)
		}
		if !intercepted {
			t.Error("expected middleware to intercept")
		}
		if result == nil || len(result.Points) == 0 {
			t.Error("expected results through middleware")
		}
	})
}

// TestIntegration_HealthEndpoints tests /health, /health/ready, /health/live HTTP endpoints.
func TestIntegration_HealthEndpoints(t *testing.T) {
	db := setupTestDB(t)

	hc := db.HealthCheck()
	if hc == nil {
		t.Fatal("expected health check to be available")
	}
	hc.Start()

	mux := http.NewServeMux()
	wrap := func(h http.HandlerFunc) http.HandlerFunc { return h }
	setupAdminRoutes(mux, db, wrap, newAuthenticator(nil))

	t.Run("health returns detailed status", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/health", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Errorf("expected 200, got %d", w.Code)
		}
		if w.Body.Len() == 0 {
			t.Error("expected non-empty body")
		}
	})

	t.Run("ready endpoint", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/health/ready", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Errorf("expected 200, got %d", w.Code)
		}
	})

	t.Run("live endpoint", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/health/live", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Errorf("expected 200, got %d", w.Code)
		}
	})
}
