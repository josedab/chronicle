package chronicle

import (
	"testing"
)

func TestSchemaRegistry_Register(t *testing.T) {
	r := NewSchemaRegistry(false)

	schema := MetricSchema{
		Name:        "cpu",
		Description: "CPU usage metric",
		Tags: []TagSchema{
			{Name: "host", Required: true},
			{Name: "cpu", Required: false, Pattern: `^cpu\d+$`},
		},
		Fields: []FieldSchema{
			{Name: "value", Type: FieldTypeFloat64, MinValue: ptr(0.0), MaxValue: ptr(100.0)},
		},
	}

	if err := r.Register(schema); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	got := r.Get("cpu")
	if got == nil {
		t.Fatal("expected schema, got nil")
	}
	if got.Name != "cpu" {
		t.Errorf("expected name cpu, got %s", got.Name)
	}
}

func TestSchemaRegistry_ValidateRequiredTag(t *testing.T) {
	r := NewSchemaRegistry(false)
	_ = r.Register(MetricSchema{
		Name: "temp",
		Tags: []TagSchema{
			{Name: "sensor", Required: true},
		},
	})

	// Missing required tag
	err := r.Validate(Point{Metric: "temp", Value: 25.0})
	if err == nil {
		t.Error("expected validation error for missing required tag")
	}

	// With required tag
	err = r.Validate(Point{Metric: "temp", Value: 25.0, Tags: map[string]string{"sensor": "s1"}})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestSchemaRegistry_ValidateAllowedValues(t *testing.T) {
	r := NewSchemaRegistry(false)
	_ = r.Register(MetricSchema{
		Name: "status",
		Tags: []TagSchema{
			{Name: "level", AllowedVals: []string{"info", "warn", "error"}},
		},
	})

	// Valid value
	err := r.Validate(Point{Metric: "status", Tags: map[string]string{"level": "info"}})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Invalid value
	err = r.Validate(Point{Metric: "status", Tags: map[string]string{"level": "debug"}})
	if err == nil {
		t.Error("expected validation error for invalid allowed value")
	}
}

func TestSchemaRegistry_ValidatePattern(t *testing.T) {
	r := NewSchemaRegistry(false)
	_ = r.Register(MetricSchema{
		Name: "disk",
		Tags: []TagSchema{
			{Name: "device", Pattern: `^/dev/sd[a-z]+$`},
		},
	})

	// Valid pattern
	err := r.Validate(Point{Metric: "disk", Tags: map[string]string{"device": "/dev/sda"}})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Invalid pattern
	err = r.Validate(Point{Metric: "disk", Tags: map[string]string{"device": "C:\\"}})
	if err == nil {
		t.Error("expected validation error for pattern mismatch")
	}
}

func TestSchemaRegistry_ValidateValueRange(t *testing.T) {
	minVal := 0.0
	maxVal := 100.0
	r := NewSchemaRegistry(false)
	_ = r.Register(MetricSchema{
		Name: "percent",
		Fields: []FieldSchema{
			{Name: "value", MinValue: &minVal, MaxValue: &maxVal},
		},
	})

	// Within range
	err := r.Validate(Point{Metric: "percent", Value: 50.0})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Below minimum
	err = r.Validate(Point{Metric: "percent", Value: -10.0})
	if err == nil {
		t.Error("expected validation error for value below minimum")
	}

	// Above maximum
	err = r.Validate(Point{Metric: "percent", Value: 150.0})
	if err == nil {
		t.Error("expected validation error for value above maximum")
	}
}

func TestSchemaRegistry_StrictTags(t *testing.T) {
	r := NewSchemaRegistry(false)
	_ = r.Register(MetricSchema{
		Name: "mem",
		Tags: []TagSchema{
			{Name: "host"},
		},
		StrictTags: true,
	})

	// Known tag only
	err := r.Validate(Point{Metric: "mem", Tags: map[string]string{"host": "a"}})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Unknown tag
	err = r.Validate(Point{Metric: "mem", Tags: map[string]string{"host": "a", "extra": "b"}})
	if err == nil {
		t.Error("expected validation error for unknown tag in strict mode")
	}
}

func TestSchemaRegistry_StrictMode(t *testing.T) {
	r := NewSchemaRegistry(true) // strict mode

	// No schema registered, should fail in strict mode
	err := r.Validate(Point{Metric: "unknown", Value: 1.0})
	if err == nil {
		t.Error("expected validation error for unregistered metric in strict mode")
	}

	// Register schema, should pass
	_ = r.Register(MetricSchema{Name: "known"})
	err = r.Validate(Point{Metric: "known", Value: 1.0})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestSchemaRegistry_List(t *testing.T) {
	r := NewSchemaRegistry(false)
	_ = r.Register(MetricSchema{Name: "a"})
	_ = r.Register(MetricSchema{Name: "b"})

	list := r.List()
	if len(list) != 2 {
		t.Errorf("expected 2 schemas, got %d", len(list))
	}
}

func TestSchemaRegistry_Unregister(t *testing.T) {
	r := NewSchemaRegistry(false)
	_ = r.Register(MetricSchema{Name: "temp"})
	r.Unregister("temp")

	if r.Get("temp") != nil {
		t.Error("expected nil after unregister")
	}
}

func ptr(v float64) *float64 {
	return &v
}
