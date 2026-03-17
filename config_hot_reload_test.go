package chronicle

import (
	"encoding/json"
	"os"
	"path/filepath"
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

func TestConfigReloadEngine_FileWatch(t *testing.T) {
	db := setupTestDB(t)

	cfgDir := t.TempDir()
	cfgPath := filepath.Join(cfgDir, "chronicle.json")

	baseCfg := DefaultConfig(db.path)
	baseCfg.syncLegacyFields()
	data, err := json.Marshal(baseCfg)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	if err := os.WriteFile(cfgPath, data, 0644); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	// Seed the lastModTime so the initial file doesn't trigger a reload
	info, err := os.Stat(cfgPath)
	if err != nil {
		t.Fatalf("stat config file: %v", err)
	}

	reloadCh := make(chan []ConfigChange, 4)
	e := NewConfigReloadEngine(db, ConfigReloadConfig{
		Enabled:       true,
		WatchInterval: 50 * time.Millisecond,
		ConfigPath:    cfgPath,
	})
	e.currentCfg = baseCfg
	e.lastModTime = info.ModTime()
	e.onReload = func(changes []ConfigChange) {
		select {
		case reloadCh <- changes:
		default:
		}
	}

	e.Start()
	defer e.Stop()

	// Ensure mod time changes by sleeping then writing
	time.Sleep(200 * time.Millisecond)
	newCfg := baseCfg
	newCfg.Retention.RetentionDuration = 72 * time.Hour
	newCfg.RetentionDuration = 72 * time.Hour
	data, err = json.Marshal(newCfg)
	if err != nil {
		t.Fatalf("marshal new config: %v", err)
	}
	if err := os.WriteFile(cfgPath, data, 0644); err != nil {
		t.Fatalf("write new config: %v", err)
	}

	select {
	case changes := <-reloadCh:
		foundRetention := false
		for _, c := range changes {
			if c.Field == "RetentionDuration" && c.Applied {
				foundRetention = true
			}
		}
		if !foundRetention {
			t.Error("expected RetentionDuration change to be applied")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for config reload")
	}

	// Verify the DB config was actually updated
	db.mu.RLock()
	actual := db.config.Retention.RetentionDuration
	db.mu.RUnlock()
	if actual != 72*time.Hour {
		t.Errorf("expected DB retention to be 72h, got %v", actual)
	}
}

func TestConfigReloadEngine_CheckAndReload_MissingFile(t *testing.T) {
	db := setupTestDB(t)
	e := NewConfigReloadEngine(db, ConfigReloadConfig{
		Enabled:    true,
		ConfigPath: "/nonexistent/path/config.json",
	})
	err := e.checkAndReload()
	if err == nil {
		t.Error("expected error for missing config file")
	}
}

func TestConfigReloadEngine_CheckAndReload_InvalidJSON(t *testing.T) {
	db := setupTestDB(t)
	cfgPath := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(cfgPath, []byte("{invalid json"), 0644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	e := NewConfigReloadEngine(db, ConfigReloadConfig{
		Enabled:    true,
		ConfigPath: cfgPath,
	})
	err := e.checkAndReload()
	if err == nil {
		t.Error("expected error for invalid JSON config")
	}
}

func TestConfigReloadEngine_NoWatchWithoutPath(t *testing.T) {
	db := setupTestDB(t)
	e := NewConfigReloadEngine(db, ConfigReloadConfig{
		Enabled:       true,
		WatchInterval: 50 * time.Millisecond,
	})
	e.Start()
	defer e.Stop()

	// No path configured — should not crash, just run in manual mode
	time.Sleep(100 * time.Millisecond)
	stats := e.GetStats()
	if stats.WatchErrors != 0 {
		t.Errorf("expected 0 watch errors without path, got %d", stats.WatchErrors)
	}
}

func TestConfigReloadEngine_RejectsInvalidHotReload(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	baseCfg := db.config
	e := NewConfigReloadEngine(db, ConfigReloadConfig{Enabled: true})
	e.Start()
	defer e.Stop()

	t.Run("RejectsZeroBufferSize", func(t *testing.T) {
		badCfg := baseCfg
		badCfg.Storage.BufferSize = 0
		badCfg.BufferSize = 0
		_, err := e.Apply(badCfg)
		if err == nil {
			t.Error("expected error for zero BufferSize")
		}
	})

	t.Run("RejectsNegativeRateLimit", func(t *testing.T) {
		badCfg := baseCfg
		badCfg.RateLimitPerSecond = -1
		_, err := e.Apply(badCfg)
		if err == nil {
			t.Error("expected error for negative RateLimitPerSecond")
		}
	})

	t.Run("AcceptsValidReload", func(t *testing.T) {
		goodCfg := baseCfg
		goodCfg.RateLimitPerSecond = 2000
		_, err := e.Apply(goodCfg)
		if err != nil {
			t.Errorf("unexpected error for valid reload: %v", err)
		}
	})
}
