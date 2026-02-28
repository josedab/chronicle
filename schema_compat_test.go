package chronicle

import (
	"testing"
)

func TestSchemaCompatEngine(t *testing.T) {
	registry := NewSchemaRegistry(false)
	engine := NewSchemaCompatEngine(registry, DefaultSchemaCompatConfig())

	baseSchema := MetricSchema{
		Name: "cpu_usage",
		Fields: []FieldSchema{
			{Name: "value", Type: FieldTypeFloat64, Required: true},
		},
		Tags: []TagSchema{
			{Name: "host", Required: true},
		},
	}

	t.Run("register first version", func(t *testing.T) {
		vs, err := engine.RegisterSchemaVersion(baseSchema, "initial schema")
		if err != nil {
			t.Fatalf("register failed: %v", err)
		}
		if vs.Version != 1 {
			t.Errorf("expected version 1, got %d", vs.Version)
		}
	})

	t.Run("backward compatible add optional field", func(t *testing.T) {
		newSchema := MetricSchema{
			Name: "cpu_usage",
			Fields: []FieldSchema{
				{Name: "value", Type: FieldTypeFloat64, Required: true},
				{Name: "extra", Type: FieldTypeFloat64, Required: false},
			},
			Tags: []TagSchema{
				{Name: "host", Required: true},
			},
		}
		vs, err := engine.RegisterSchemaVersion(newSchema, "added optional field")
		if err != nil {
			t.Fatalf("register failed: %v", err)
		}
		if vs.Version != 2 {
			t.Errorf("expected version 2, got %d", vs.Version)
		}
	})

	t.Run("backward incompatible remove field", func(t *testing.T) {
		newSchema := MetricSchema{
			Name: "cpu_usage",
			Fields: []FieldSchema{
				{Name: "extra", Type: FieldTypeFloat64},
			},
			Tags: []TagSchema{
				{Name: "host", Required: true},
			},
		}
		_, err := engine.RegisterSchemaVersion(newSchema, "removed field")
		if err == nil {
			t.Error("expected backward incompatibility error")
		}
	})

	t.Run("backward incompatible add required field", func(t *testing.T) {
		newSchema := MetricSchema{
			Name: "cpu_usage",
			Fields: []FieldSchema{
				{Name: "value", Type: FieldTypeFloat64, Required: true},
				{Name: "extra", Type: FieldTypeFloat64, Required: false},
				{Name: "mandatory", Type: FieldTypeFloat64, Required: true},
			},
			Tags: []TagSchema{
				{Name: "host", Required: true},
			},
		}
		_, err := engine.RegisterSchemaVersion(newSchema, "add required field")
		if err == nil {
			t.Error("expected backward incompatibility error for new required field")
		}
	})

	t.Run("compat none allows anything", func(t *testing.T) {
		engine.SetMetricCompatMode("free_metric", CompatNone)
		s1 := MetricSchema{Name: "free_metric", Fields: []FieldSchema{{Name: "a"}}}
		engine.RegisterSchemaVersion(s1, "v1")
		s2 := MetricSchema{Name: "free_metric", Fields: []FieldSchema{{Name: "b"}}}
		_, err := engine.RegisterSchemaVersion(s2, "v2 totally different")
		if err != nil {
			t.Errorf("CompatNone should allow any change: %v", err)
		}
	})

	t.Run("forward compat rejects new fields", func(t *testing.T) {
		engine.SetMetricCompatMode("fwd_metric", CompatForward)
		s1 := MetricSchema{Name: "fwd_metric", Fields: []FieldSchema{{Name: "a"}}}
		engine.RegisterSchemaVersion(s1, "v1")
		s2 := MetricSchema{Name: "fwd_metric", Fields: []FieldSchema{{Name: "a"}, {Name: "b"}}}
		_, err := engine.RegisterSchemaVersion(s2, "v2 add field")
		if err == nil {
			t.Error("expected forward incompatibility error for added field")
		}
	})

	t.Run("full compat", func(t *testing.T) {
		engine.SetMetricCompatMode("full_metric", CompatFull)
		s1 := MetricSchema{Name: "full_metric", Fields: []FieldSchema{{Name: "a", Required: true}}}
		engine.RegisterSchemaVersion(s1, "v1")
		// Same schema should be fine
		s2 := MetricSchema{Name: "full_metric", Fields: []FieldSchema{{Name: "a", Required: true}}}
		_, err := engine.RegisterSchemaVersion(s2, "v2 same")
		if err != nil {
			t.Errorf("same schema should be full-compatible: %v", err)
		}
	})

	t.Run("get version history", func(t *testing.T) {
		versions := engine.GetSchemaVersionHistory("cpu_usage")
		if len(versions) < 2 {
			t.Errorf("expected at least 2 versions, got %d", len(versions))
		}
	})

	t.Run("get specific version", func(t *testing.T) {
		v, err := engine.GetSchemaVersion("cpu_usage", 1)
		if err != nil {
			t.Fatal(err)
		}
		if v.Version != 1 {
			t.Errorf("expected version 1")
		}
	})

	t.Run("get nonexistent version", func(t *testing.T) {
		_, err := engine.GetSchemaVersion("cpu_usage", 999)
		if err == nil {
			t.Error("expected error")
		}
	})

	t.Run("get latest version", func(t *testing.T) {
		v, err := engine.GetLatestSchemaVersion("cpu_usage")
		if err != nil {
			t.Fatal(err)
		}
		if v.Version < 2 {
			t.Errorf("expected latest version >= 2, got %d", v.Version)
		}
	})

	t.Run("alter metric add field", func(t *testing.T) {
		err := engine.AlterMetricAddField("cpu_usage", FieldSchema{Name: "new_field", Type: FieldTypeFloat64})
		if err != nil {
			t.Fatalf("alter failed: %v", err)
		}
	})

	t.Run("alter metric add duplicate field", func(t *testing.T) {
		err := engine.AlterMetricAddField("cpu_usage", FieldSchema{Name: "new_field"})
		if err == nil {
			t.Error("expected error for duplicate field")
		}
	})

	t.Run("alter nonexistent metric", func(t *testing.T) {
		err := engine.AlterMetricAddField("nonexistent", FieldSchema{Name: "x"})
		if err == nil {
			t.Error("expected error")
		}
	})

	t.Run("describe metric", func(t *testing.T) {
		desc, err := engine.DescribeMetricSchema("cpu_usage")
		if err != nil {
			t.Fatal(err)
		}
		if desc.Name != "cpu_usage" {
			t.Errorf("expected cpu_usage, got %s", desc.Name)
		}
		if desc.TotalVersions == 0 {
			t.Error("expected version count > 0")
		}
	})

	t.Run("describe nonexistent", func(t *testing.T) {
		_, err := engine.DescribeMetricSchema("nonexistent")
		if err == nil {
			t.Error("expected error")
		}
	})

	t.Run("register empty name", func(t *testing.T) {
		_, err := engine.RegisterSchemaVersion(MetricSchema{}, "")
		if err == nil {
			t.Error("expected error for empty name")
		}
	})

	t.Run("coerce point nil safe", func(t *testing.T) {
		engine.CoercePoint(nil)
	})

	t.Run("set and get field default", func(t *testing.T) {
		engine.SetFieldDefault("cpu_usage", "extra", 42.0)
	})

	t.Run("compat mode string", func(t *testing.T) {
		modes := []SchemaCompatibilityMode{CompatNone, CompatBackward, CompatForward, CompatFull}
		expected := []string{"NONE", "BACKWARD", "FORWARD", "FULL"}
		for i, m := range modes {
			if m.String() != expected[i] {
				t.Errorf("expected %s, got %s", expected[i], m.String())
			}
		}
	})
}
