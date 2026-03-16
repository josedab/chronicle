package chronicle

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthCheckEngine(t *testing.T) {
	db := setupTestDB(t)

	e := NewHealthCheckEngine(db, DefaultHealthCheckConfig())
	e.Start()
	defer e.Stop()

	t.Run("healthy status by default", func(t *testing.T) {
		status := e.Check()
		if status.Overall != "healthy" {
			t.Errorf("expected healthy, got %s", status.Overall)
		}
		if len(status.Components) < 3 {
			t.Errorf("expected at least 3 components, got %d", len(status.Components))
		}
	})

	t.Run("built-in components present", func(t *testing.T) {
		status := e.Check()
		names := make(map[string]bool)
		for _, c := range status.Components {
			names[c.Name] = true
		}
		for _, name := range []string{"storage", "wal", "index"} {
			if !names[name] {
				t.Errorf("expected built-in component %s", name)
			}
		}
	})

	t.Run("component registration", func(t *testing.T) {
		e.AddComponent("custom", func() ComponentHealth {
			return ComponentHealth{Name: "custom", Status: "healthy", Message: "ok"}
		})
		status := e.Check()
		found := false
		for _, c := range status.Components {
			if c.Name == "custom" {
				found = true
			}
		}
		if !found {
			t.Error("expected custom component")
		}
	})

	t.Run("degraded component affects overall", func(t *testing.T) {
		e.AddComponent("degraded_svc", func() ComponentHealth {
			return ComponentHealth{Name: "degraded_svc", Status: "degraded", Message: "high latency"}
		})
		status := e.Check()
		if status.Overall != "degraded" {
			t.Errorf("expected degraded overall, got %s", status.Overall)
		}
	})

	t.Run("unhealthy component sets overall unhealthy", func(t *testing.T) {
		e.AddComponent("broken", func() ComponentHealth {
			return ComponentHealth{Name: "broken", Status: "unhealthy", Message: "down"}
		})
		status := e.Check()
		if status.Overall != "unhealthy" {
			t.Errorf("expected unhealthy overall, got %s", status.Overall)
		}
	})

	t.Run("is ready when not unhealthy", func(t *testing.T) {
		e2 := NewHealthCheckEngine(db, DefaultHealthCheckConfig())
		e2.Start()
		defer e2.Stop()
		if !e2.IsReady() {
			t.Error("expected ready")
		}
	})

	t.Run("is live when running", func(t *testing.T) {
		if !e.IsLive() {
			t.Error("expected live")
		}
	})

	t.Run("is not live when stopped", func(t *testing.T) {
		e3 := NewHealthCheckEngine(db, DefaultHealthCheckConfig())
		if e3.IsLive() {
			t.Error("expected not live before start")
		}
	})

	t.Run("version is set", func(t *testing.T) {
		status := e.Check()
		if status.Version == "" {
			t.Error("expected version to be set")
		}
	})
}

func TestHealthCheckMemoryAndDisk(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	e := NewHealthCheckEngine(db, DefaultHealthCheckConfig())
	e.Start()
	defer e.Stop()

	t.Run("MemoryCheckerPresent", func(t *testing.T) {
		status := e.Check()
		found := false
		for _, c := range status.Components {
			if c.Name == "memory" {
				found = true
				if c.Status != "healthy" && c.Status != "degraded" {
					t.Errorf("unexpected memory status: %s", c.Status)
				}
				if c.Message == "" {
					t.Error("expected memory message with stats")
				}
			}
		}
		if !found {
			t.Error("expected memory component")
		}
	})

	t.Run("DiskSpaceCheckerPresent", func(t *testing.T) {
		status := e.Check()
		found := false
		for _, c := range status.Components {
			if c.Name == "disk_space" {
				found = true
				if c.Status != "healthy" && c.Status != "degraded" {
					t.Errorf("unexpected disk_space status: %s", c.Status)
				}
			}
		}
		if !found {
			t.Error("expected disk_space component")
		}
	})

	t.Run("MemoryDegradedOnLowThreshold", func(t *testing.T) {
		cfg := DefaultHealthCheckConfig()
		cfg.MaxMemoryBytes = 1 // 1 byte — will always be exceeded
		e2 := NewHealthCheckEngine(db, cfg)
		e2.Start()
		defer e2.Stop()

		status := e2.Check()
		for _, c := range status.Components {
			if c.Name == "memory" && c.Status != "degraded" {
				t.Errorf("expected memory degraded with 1-byte threshold, got %s", c.Status)
			}
		}
	})
}

func TestHealthCheckHTTPEndpoints(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	e := NewHealthCheckEngine(db, DefaultHealthCheckConfig())
	e.Start()
	defer e.Stop()

	mux := http.NewServeMux()
	e.RegisterHTTPHandlers(mux)

	t.Run("Healthz_Live", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected 200, got %d", w.Code)
		}
		if w.Body.String() != "ok" {
			t.Errorf("expected 'ok', got %s", w.Body.String())
		}
	})

	t.Run("Readyz_Ready", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected 200, got %d", w.Code)
		}
	})

	t.Run("StatusEndpoint", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/health/status", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected 200, got %d", w.Code)
		}
		var status HealthCheckStatus
		json.Unmarshal(w.Body.Bytes(), &status)
		if status.Overall == "" {
			t.Error("expected overall status")
		}
	})

	t.Run("ComponentsEndpoint", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/health/components", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected 200, got %d", w.Code)
		}
		var components []ComponentHealth
		json.Unmarshal(w.Body.Bytes(), &components)
		if len(components) < 6 {
			t.Errorf("expected at least 6 components (including memory and disk_space), got %d", len(components))
		}
	})

	t.Run("Healthz_NotLiveWhenStopped", func(t *testing.T) {
		e2 := NewHealthCheckEngine(db, DefaultHealthCheckConfig())
		// Don't start e2 — it should report not live
		mux2 := http.NewServeMux()
		e2.RegisterHTTPHandlers(mux2)

		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		w := httptest.NewRecorder()
		mux2.ServeHTTP(w, req)

		if w.Code != http.StatusServiceUnavailable {
			t.Errorf("expected 503 when not live, got %d", w.Code)
		}
	})
}
