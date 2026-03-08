package chronicle

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFeatureFlagEngine(t *testing.T) {
	db := setupTestDB(t)

	t.Run("all enabled by default", func(t *testing.T) {
		e := NewFeatureFlagEngine(db, DefaultFeatureFlagConfig())
		stats := e.GetStats()
		if stats.TotalFlags == 0 { t.Error("expected flags") }
		if stats.DisabledFlags != 0 { t.Errorf("expected 0 disabled, got %d", stats.DisabledFlags) }
	})

	t.Run("stable_only policy", func(t *testing.T) {
		cfg := DefaultFeatureFlagConfig()
		cfg.DefaultPolicy = "stable_only"
		e := NewFeatureFlagEngine(db, cfg)
		stats := e.GetStats()
		if stats.EnabledFlags != stats.StableCount { t.Error("only stable should be enabled") }
	})

	t.Run("stable_beta policy", func(t *testing.T) {
		cfg := DefaultFeatureFlagConfig()
		cfg.DefaultPolicy = "stable_beta"
		e := NewFeatureFlagEngine(db, cfg)
		stats := e.GetStats()
		expected := stats.StableCount + stats.BetaCount
		if stats.EnabledFlags != expected { t.Errorf("expected %d enabled, got %d", expected, stats.EnabledFlags) }
	})

	t.Run("enable disable", func(t *testing.T) {
		e := NewFeatureFlagEngine(db, DefaultFeatureFlagConfig())
		e.Disable("DB")
		if e.IsEnabled("DB") { t.Error("should be disabled") }
		e.Enable("DB")
		if !e.IsEnabled("DB") { t.Error("should be enabled") }
	})

	t.Run("unknown feature defaults enabled", func(t *testing.T) {
		e := NewFeatureFlagEngine(db, DefaultFeatureFlagConfig())
		if !e.IsEnabled("nonexistent") { t.Error("unknown should default to enabled") }
	})

	t.Run("disabled by config", func(t *testing.T) {
		cfg := DefaultFeatureFlagConfig()
		cfg.DisabledFeatures = []string{"DB", "Point"}
		e := NewFeatureFlagEngine(db, cfg)
		if e.IsEnabled("DB") { t.Error("DB should be disabled") }
		if e.IsEnabled("Point") { t.Error("Point should be disabled") }
	})

	t.Run("list sorted", func(t *testing.T) {
		e := NewFeatureFlagEngine(db, DefaultFeatureFlagConfig())
		flags := e.List()
		for i := 1; i < len(flags); i++ {
			if flags[i].Name < flags[i-1].Name { t.Error("flags not sorted"); break }
		}
	})

	t.Run("start stop", func(t *testing.T) {
		e := NewFeatureFlagEngine(db, DefaultFeatureFlagConfig())
		e.Start(); e.Start(); e.Stop(); e.Stop()
	})

	t.Run("check feature returns error when disabled", func(t *testing.T) {
		e := NewFeatureFlagEngine(db, DefaultFeatureFlagConfig())
		e.Disable("DB")
		err := e.CheckFeature("DB")
		if err == nil {
			t.Fatal("expected error for disabled feature")
		}
		if !errors.Is(err, ErrFeatureDisabled) {
			t.Errorf("expected ErrFeatureDisabled, got %v", err)
		}
	})

	t.Run("check feature returns nil when enabled", func(t *testing.T) {
		e := NewFeatureFlagEngine(db, DefaultFeatureFlagConfig())
		if err := e.CheckFeature("DB"); err != nil {
			t.Errorf("expected nil for enabled feature, got %v", err)
		}
	})
}

func TestFeatureGuard(t *testing.T) {
	db := setupTestDB(t)

	t.Run("allows request when feature enabled", func(t *testing.T) {
		called := false
		handler := featureGuard(db, "DB", func(w http.ResponseWriter, r *http.Request) {
			called = true
			w.WriteHeader(http.StatusOK)
		})
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		rec := httptest.NewRecorder()
		handler(rec, req)
		if !called {
			t.Error("handler should have been called")
		}
		if rec.Code != http.StatusOK {
			t.Errorf("expected 200, got %d", rec.Code)
		}
	})

	t.Run("blocks request when feature disabled", func(t *testing.T) {
		flags := db.FeatureFlags()
		flags.Disable("DB")
		defer flags.Enable("DB")

		called := false
		handler := featureGuard(db, "DB", func(w http.ResponseWriter, r *http.Request) {
			called = true
		})
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		rec := httptest.NewRecorder()
		handler(rec, req)
		if called {
			t.Error("handler should NOT have been called")
		}
		if rec.Code != http.StatusForbidden {
			t.Errorf("expected 403, got %d", rec.Code)
		}
	})
}
