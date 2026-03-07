package chronicle

import (
	"testing"
	"time"
)

func TestConfigReloadEngine(t *testing.T) {
	db := setupTestDB(t)

	e := NewConfigReloadEngine(db, DefaultConfigReloadConfig())
	e.Start()
	defer e.Stop()

	baseCfg := DefaultConfig(db.path)

	t.Run("apply safe changes", func(t *testing.T) {
		newCfg := baseCfg
		newCfg.HTTP.HTTPPort = 9090
		newCfg.Retention.RetentionDuration = 48 * time.Hour
		newCfg.Storage.BufferSize = 2048
		newCfg.syncLegacyFields()

		changes, err := e.Apply(newCfg)
		if err != nil {
			t.Fatalf("Apply() unexpected error: %v", err)
		}
		safeCount := 0
		for _, c := range changes {
			if c.Applied {
				safeCount++
			}
		}
		if safeCount == 0 {
			t.Error("expected at least one safe change to be applied")
		}
	})

	t.Run("reject unsafe changes", func(t *testing.T) {
		newCfg := baseCfg
		newCfg.HTTP.HTTPPort = 9090
		newCfg.Retention.RetentionDuration = 48 * time.Hour
		newCfg.Storage.BufferSize = 2048
		newCfg.Path = "/new/path"
		newCfg.Encryption = &EncryptionConfig{}
		newCfg.syncLegacyFields()

		changes, err := e.Apply(newCfg)
		if err != nil {
			t.Fatalf("Apply() unexpected error: %v", err)
		}
		for _, c := range changes {
			if (c.Field == "Path" || c.Field == "Encryption") && c.Applied {
				t.Errorf("unsafe field %s should not be applied", c.Field)
			}
		}
	})

	t.Run("reject invalid config", func(t *testing.T) {
		invalidCfg := baseCfg
		invalidCfg.Storage.MaxMemory = -1
		_, err := e.Apply(invalidCfg)
		if err == nil {
			t.Error("expected error for invalid config, got nil")
		}
	})

	t.Run("diff detection", func(t *testing.T) {
		a := Config{HTTPPort: 8080, BufferSize: 1024}
		b := Config{HTTPPort: 9090, BufferSize: 1024}
		changes := e.Diff(a, b)
		if len(changes) != 1 {
			t.Errorf("expected 1 change, got %d", len(changes))
		}
		if len(changes) > 0 && changes[0].Field != "HTTPPort" {
			t.Errorf("expected HTTPPort change, got %s", changes[0].Field)
		}
	})

	t.Run("no changes when identical", func(t *testing.T) {
		a := Config{HTTPPort: 8080}
		changes := e.Diff(a, a)
		if len(changes) != 0 {
			t.Errorf("expected 0 changes, got %d", len(changes))
		}
	})

	t.Run("stats track reloads", func(t *testing.T) {
		stats := e.GetStats()
		if stats.TotalReloads == 0 {
			t.Error("expected reload count > 0")
		}
		if stats.LastReloadAt.IsZero() {
			t.Error("expected last reload time to be set")
		}
	})

	t.Run("last reload time", func(t *testing.T) {
		lr := e.LastReload()
		if lr.IsZero() {
			t.Error("expected last reload to be non-zero")
		}
	})
}
