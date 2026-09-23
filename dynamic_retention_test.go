package chronicle

import (
	"testing"
	"time"
)

func TestDynamicRetentionManager(t *testing.T) {
	mgr := NewDynamicRetentionManager(30 * 24 * time.Hour)

	mgr.AddRule(DynamicRetentionRule{
		MetricPrefix: "temp.",
		Retention:    1 * time.Hour,
	})

	mgr.AddRule(DynamicRetentionRule{
		Tags:      map[string]string{"env": "dev"},
		Retention: 24 * time.Hour,
	})

	if r := mgr.GetRetention("temp.cpu", nil); r != 1*time.Hour {
		t.Errorf("expected 1h for temp.cpu, got %v", r)
	}

	if r := mgr.GetRetention("system.memory", map[string]string{"env": "dev"}); r != 24*time.Hour {
		t.Errorf("expected 24h for env=dev, got %v", r)
	}

	if r := mgr.GetRetention("system.memory", map[string]string{"env": "prod"}); r != 30*24*time.Hour {
		t.Errorf("expected default 30d for prod, got %v", r)
	}
}
