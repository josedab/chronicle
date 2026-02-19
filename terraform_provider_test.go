package chronicle

import (
	"testing"
)

func TestChronicleProviderSchema(t *testing.T) {
	schema := ChronicleProviderSchema()
	if schema.Version != "1.0.0" {
		t.Errorf("version = %q", schema.Version)
	}
	if len(schema.Resources) != 3 {
		t.Errorf("resources = %d, want 3", len(schema.Resources))
	}

	types := map[TerraformResourceType]bool{}
	for _, r := range schema.Resources {
		types[r.Type] = true
		if len(r.Attributes) == 0 {
			t.Errorf("resource %q has no attributes", r.Type)
		}
	}
	if !types[TFResourceInstance] {
		t.Error("missing chronicle_instance")
	}
	if !types[TFResourceRetentionPolicy] {
		t.Error("missing chronicle_retention_policy")
	}
	if !types[TFResourceAlertRule] {
		t.Error("missing chronicle_alert_rule")
	}
}

func TestTerraformProvider_CreateRead(t *testing.T) {
	p := NewTerraformProvider(DefaultTFProviderConfig())

	state, err := p.Create(TFResourceInstance, map[string]any{
		"name":     "prod-db",
		"data_dir": "/var/chronicle",
	})
	if err != nil {
		t.Fatal(err)
	}
	if state.ID == "" {
		t.Error("empty ID")
	}
	if state.Type != TFResourceInstance {
		t.Errorf("type = %q", state.Type)
	}

	read, err := p.Read(state.ID)
	if err != nil {
		t.Fatal(err)
	}
	if read.Attributes["name"] != "prod-db" {
		t.Errorf("name = %v", read.Attributes["name"])
	}
}

func TestTerraformProvider_CreateMissingRequired(t *testing.T) {
	p := NewTerraformProvider(DefaultTFProviderConfig())

	// Missing 'name' which is required
	_, err := p.Create(TFResourceInstance, map[string]any{
		"data_dir": "/var/data",
	})
	if err == nil {
		t.Error("expected error for missing required field")
	}
}

func TestTerraformProvider_CreateNilAttrs(t *testing.T) {
	p := NewTerraformProvider(DefaultTFProviderConfig())
	_, err := p.Create(TFResourceInstance, nil)
	if err == nil {
		t.Error("expected error for nil attrs")
	}
}

func TestTerraformProvider_CreateUnknownType(t *testing.T) {
	p := NewTerraformProvider(DefaultTFProviderConfig())
	_, err := p.Create("unknown_type", map[string]any{"name": "x"})
	if err == nil {
		t.Error("expected error for unknown type")
	}
}

func TestTerraformProvider_Update(t *testing.T) {
	p := NewTerraformProvider(DefaultTFProviderConfig())

	state, _ := p.Create(TFResourceInstance, map[string]any{
		"name":     "dev-db",
		"data_dir": "/tmp/chronicle",
	})

	updated, err := p.Update(state.ID, map[string]any{
		"name": "staging-db",
	})
	if err != nil {
		t.Fatal(err)
	}
	if updated.Attributes["name"] != "staging-db" {
		t.Errorf("name = %v", updated.Attributes["name"])
	}
	// data_dir should be unchanged
	if updated.Attributes["data_dir"] != "/tmp/chronicle" {
		t.Errorf("data_dir changed")
	}
}

func TestTerraformProvider_UpdateNotFound(t *testing.T) {
	p := NewTerraformProvider(DefaultTFProviderConfig())
	_, err := p.Update("nonexistent", map[string]any{"name": "x"})
	if err == nil {
		t.Error("expected error")
	}
}

func TestTerraformProvider_Delete(t *testing.T) {
	p := NewTerraformProvider(DefaultTFProviderConfig())

	state, _ := p.Create(TFResourceInstance, map[string]any{
		"name":     "temp",
		"data_dir": "/tmp/temp",
	})

	err := p.Delete(state.ID)
	if err != nil {
		t.Fatal(err)
	}

	_, err = p.Read(state.ID)
	if err == nil {
		t.Error("expected not found after delete")
	}
}

func TestTerraformProvider_DeleteNotFound(t *testing.T) {
	p := NewTerraformProvider(DefaultTFProviderConfig())
	err := p.Delete("nonexistent")
	if err == nil {
		t.Error("expected error")
	}
}

func TestTerraformProvider_List(t *testing.T) {
	p := NewTerraformProvider(DefaultTFProviderConfig())

	p.Create(TFResourceInstance, map[string]any{
		"name": "db1", "data_dir": "/d1",
	})
	p.Create(TFResourceInstance, map[string]any{
		"name": "db2", "data_dir": "/d2",
	})
	p.Create(TFResourceRetentionPolicy, map[string]any{
		"name": "p1", "metric_pattern": "cpu.*", "retention_duration": "720h",
	})

	instances := p.List(TFResourceInstance)
	if len(instances) != 2 {
		t.Errorf("instances = %d, want 2", len(instances))
	}

	policies := p.List(TFResourceRetentionPolicy)
	if len(policies) != 1 {
		t.Errorf("policies = %d, want 1", len(policies))
	}
}

func TestTerraformProvider_Plan_NoOp(t *testing.T) {
	p := NewTerraformProvider(DefaultTFProviderConfig())

	state, _ := p.Create(TFResourceInstance, map[string]any{
		"name": "db1", "data_dir": "/d1",
	})

	plan, err := p.Plan(state.ID, map[string]any{
		"name": "db1", "data_dir": "/d1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Action != TFPlanNoOp {
		t.Errorf("action = %q, want no-op", plan.Action)
	}
}

func TestTerraformProvider_Plan_Update(t *testing.T) {
	p := NewTerraformProvider(DefaultTFProviderConfig())

	state, _ := p.Create(TFResourceInstance, map[string]any{
		"name": "db1", "data_dir": "/d1",
	})

	plan, err := p.Plan(state.ID, map[string]any{
		"name": "db2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Action != TFPlanUpdate {
		t.Errorf("action = %q, want update", plan.Action)
	}
	if len(plan.Changes) != 1 {
		t.Errorf("changes = %d, want 1", len(plan.Changes))
	}
}

func TestTerraformProvider_Plan_Create(t *testing.T) {
	p := NewTerraformProvider(DefaultTFProviderConfig())

	plan, err := p.Plan("nonexistent", map[string]any{
		"_type": string(TFResourceInstance),
		"name":  "new-db",
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Action != TFPlanCreate {
		t.Errorf("action = %q, want create", plan.Action)
	}
}

func TestTerraformProvider_ResourceCount(t *testing.T) {
	p := NewTerraformProvider(DefaultTFProviderConfig())
	if p.ResourceCount() != 0 {
		t.Error("expected 0")
	}

	p.Create(TFResourceAlertRule, map[string]any{
		"name": "a1", "metric": "cpu", "condition": "> 90", "duration": "5m",
	})
	if p.ResourceCount() != 1 {
		t.Errorf("count = %d", p.ResourceCount())
	}
}

func TestTerraformProviderSchema_MarshalJSON(t *testing.T) {
	schema := ChronicleProviderSchema()
	data, err := schema.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Error("empty json")
	}
}

func TestDefaultTFProviderConfig(t *testing.T) {
	cfg := DefaultTFProviderConfig()
	if cfg.Endpoint != "http://localhost:8080" {
		t.Errorf("endpoint = %q", cfg.Endpoint)
	}
	if cfg.Timeout != 30 {
		t.Errorf("timeout = %d", cfg.Timeout)
	}
}

func TestTerraformProvider_UpdateProtectsComputed(t *testing.T) {
	p := NewTerraformProvider(DefaultTFProviderConfig())

	state, _ := p.Create(TFResourceInstance, map[string]any{
		"name": "db1", "data_dir": "/d1",
	})
	originalID := state.Attributes["id"]

	updated, _ := p.Update(state.ID, map[string]any{
		"id": "hacked-id",
	})

	if updated.Attributes["id"] != originalID {
		t.Error("computed id should not be updateable")
	}
}
