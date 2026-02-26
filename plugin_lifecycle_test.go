package chronicle

import "testing"

func TestPluginLifecycle(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "default_config"},
		{name: "new_plugin_lifecycle_manager"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			switch tt.name {
			case "default_config":
				cfg := DefaultPluginLifecycleConfig()
				if cfg.HealthCheckInterval == 0 {
					t.Error("expected non-zero health check interval")
				}
			case "new_plugin_lifecycle_manager":
				cfg := DefaultPluginLifecycleConfig()
				sdkCfg := DefaultPluginSDKConfig()
				registry := NewPluginRegistry(sdkCfg)
				mp := NewPluginMarketplace("http://example.com", registry)
				plm := NewPluginLifecycleManager(cfg, registry, mp)
				if plm == nil {
					t.Fatal("expected non-nil PluginLifecycleManager")
				}
			}
		})
	}
}
