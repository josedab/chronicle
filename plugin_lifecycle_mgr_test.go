package chronicle

import (
	"context"
	"testing"
)

func newTestPluginLifecycleManager(t *testing.T) *PluginLifecycleManager {
	t.Helper()
	cfg := DefaultPluginLifecycleConfig()
	cfg.MaxPlugins = 5

	registry := NewPluginRegistry(DefaultPluginSDKConfig())
	marketplace := NewPluginMarketplace("", registry)

	// Publish a test plugin to the marketplace.
	_ = marketplace.Publish(PluginManifest{
		ID:      "test-plugin",
		Name:    "Test Plugin",
		Version: "1.0.0",
		Type:    PluginTypeAggregator,
		Author:  "test",
	})
	_ = marketplace.Publish(PluginManifest{
		ID:      "test-plugin-v2",
		Name:    "Test Plugin V2",
		Version: "2.0.0",
		Type:    PluginTypeAggregator,
		Author:  "test",
	})

	return NewPluginLifecycleManager(cfg, registry, marketplace)
}

func TestPluginInstallAndList(t *testing.T) {
	plm := newTestPluginLifecycleManager(t)

	err := plm.InstallPlugin("test-plugin", "1.0.0")
	if err != nil {
		t.Fatalf("InstallPlugin failed: %v", err)
	}

	installed := plm.ListInstalled()
	if len(installed) != 1 {
		t.Errorf("Expected 1 installed plugin, got %d", len(installed))
	}
	if installed[0].ID != "test-plugin" {
		t.Errorf("Expected test-plugin, got %s", installed[0].ID)
	}
	if installed[0].State != "installed" {
		t.Errorf("Expected state 'installed', got %s", installed[0].State)
	}
}

func TestPluginInstallEmptyID(t *testing.T) {
	plm := newTestPluginLifecycleManager(t)

	err := plm.InstallPlugin("", "1.0.0")
	if err == nil {
		t.Error("Expected error for empty plugin ID")
	}
}

func TestPluginInstallDuplicate(t *testing.T) {
	plm := newTestPluginLifecycleManager(t)

	_ = plm.InstallPlugin("test-plugin", "1.0.0")
	err := plm.InstallPlugin("test-plugin", "1.0.0")
	if err == nil {
		t.Error("Expected error for duplicate install")
	}
}

func TestPluginInstallExceedsMax(t *testing.T) {
	plm := newTestPluginLifecycleManager(t)
	plm.config.MaxPlugins = 1

	_ = plm.InstallPlugin("test-plugin", "1.0.0")
	err := plm.InstallPlugin("test-plugin-v2", "2.0.0")
	if err == nil {
		t.Error("Expected error when exceeding max plugins")
	}
}

func TestPluginInstallNotInMarketplace(t *testing.T) {
	plm := newTestPluginLifecycleManager(t)

	err := plm.InstallPlugin("nonexistent-plugin", "1.0.0")
	if err == nil {
		t.Error("Expected error for plugin not in marketplace")
	}
}

func TestPluginUninstall(t *testing.T) {
	plm := newTestPluginLifecycleManager(t)

	_ = plm.InstallPlugin("test-plugin", "1.0.0")
	err := plm.UninstallPlugin("test-plugin")
	if err != nil {
		t.Fatalf("UninstallPlugin failed: %v", err)
	}

	installed := plm.ListInstalled()
	if len(installed) != 0 {
		t.Errorf("Expected 0 installed after uninstall, got %d", len(installed))
	}
}

func TestPluginUninstallNotInstalled(t *testing.T) {
	plm := newTestPluginLifecycleManager(t)

	err := plm.UninstallPlugin("nonexistent")
	if err == nil {
		t.Error("Expected error uninstalling non-installed plugin")
	}
}

func TestPluginEnable(t *testing.T) {
	plm := newTestPluginLifecycleManager(t)

	_ = plm.InstallPlugin("test-plugin", "1.0.0")
	err := plm.EnablePlugin("test-plugin")
	if err != nil {
		t.Fatalf("EnablePlugin failed: %v", err)
	}

	info, _ := plm.GetPlugin("test-plugin")
	if info.State != "enabled" {
		t.Errorf("Expected state 'enabled', got %s", info.State)
	}
}

func TestPluginEnableAlreadyEnabled(t *testing.T) {
	plm := newTestPluginLifecycleManager(t)

	_ = plm.InstallPlugin("test-plugin", "1.0.0")
	_ = plm.EnablePlugin("test-plugin")
	err := plm.EnablePlugin("test-plugin")
	if err != nil {
		t.Errorf("EnablePlugin on already enabled should succeed, got: %v", err)
	}
}

func TestPluginEnableNotInstalled(t *testing.T) {
	plm := newTestPluginLifecycleManager(t)

	err := plm.EnablePlugin("nonexistent")
	if err == nil {
		t.Error("Expected error enabling non-installed plugin")
	}
}

func TestPluginDisable(t *testing.T) {
	plm := newTestPluginLifecycleManager(t)

	_ = plm.InstallPlugin("test-plugin", "1.0.0")
	_ = plm.EnablePlugin("test-plugin")
	err := plm.DisablePlugin("test-plugin")
	if err != nil {
		t.Fatalf("DisablePlugin failed: %v", err)
	}

	info, _ := plm.GetPlugin("test-plugin")
	if info.State != "disabled" {
		t.Errorf("Expected state 'disabled', got %s", info.State)
	}
}

func TestPluginDisableNotInstalled(t *testing.T) {
	plm := newTestPluginLifecycleManager(t)

	err := plm.DisablePlugin("nonexistent")
	if err == nil {
		t.Error("Expected error disabling non-installed plugin")
	}
}

func TestPluginGetPlugin(t *testing.T) {
	plm := newTestPluginLifecycleManager(t)

	_ = plm.InstallPlugin("test-plugin", "1.0.0")

	info, err := plm.GetPlugin("test-plugin")
	if err != nil {
		t.Fatalf("GetPlugin failed: %v", err)
	}
	if info.ID != "test-plugin" {
		t.Errorf("Expected test-plugin, got %s", info.ID)
	}
}

func TestPluginGetPluginNotInstalled(t *testing.T) {
	plm := newTestPluginLifecycleManager(t)

	_, err := plm.GetPlugin("nonexistent")
	if err == nil {
		t.Error("Expected error for non-installed plugin")
	}
}

func TestPluginEvents(t *testing.T) {
	plm := newTestPluginLifecycleManager(t)

	_ = plm.InstallPlugin("test-plugin", "1.0.0")
	_ = plm.EnablePlugin("test-plugin")
	_ = plm.DisablePlugin("test-plugin")
	_ = plm.UninstallPlugin("test-plugin")

	events := plm.Events()
	if len(events) < 4 {
		t.Errorf("Expected at least 4 events, got %d", len(events))
	}
}

func TestPluginStats(t *testing.T) {
	plm := newTestPluginLifecycleManager(t)

	_ = plm.InstallPlugin("test-plugin", "1.0.0")
	_ = plm.EnablePlugin("test-plugin")

	stats := plm.Stats()
	if stats.InstalledCount != 1 {
		t.Errorf("Expected 1 installed, got %d", stats.InstalledCount)
	}
	if stats.EnabledCount != 1 {
		t.Errorf("Expected 1 enabled, got %d", stats.EnabledCount)
	}
}

func TestPluginStartStop(t *testing.T) {
	plm := newTestPluginLifecycleManager(t)

	ctx := context.Background()
	err := plm.Start(ctx)
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Double start should fail.
	err = plm.Start(ctx)
	if err == nil {
		t.Error("Expected error on double start")
	}

	plm.Stop()
}

func TestPluginCheckUpdates(t *testing.T) {
	plm := newTestPluginLifecycleManager(t)

	_ = plm.InstallPlugin("test-plugin", "1.0.0")

	updates := plm.CheckUpdates()
	// Result depends on marketplace state, just verify no panic.
	_ = updates
}
