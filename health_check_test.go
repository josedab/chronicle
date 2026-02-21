package chronicle

import (
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
