package chronicle

import (
	"testing"
	"time"
)

func TestSchemaDesignerCreate(t *testing.T) {
	sd := NewSchemaDesigner(nil, DefaultSchemaDesignerConfig())

	schema := SchemaDesign{
		ID:           "cpu-metrics",
		Name:         "CPU Metrics",
		MetricPrefix: "system.cpu",
		Fields: []SchemaDesignField{
			{Name: "usage", Type: SchemaFieldFloat, Required: true, Description: "CPU usage percentage"},
			{Name: "host", Type: SchemaFieldString, Required: true, IsTag: true},
		},
	}

	result, err := sd.CreateSchema(schema)
	if err != nil {
		t.Fatalf("CreateSchema failed: %v", err)
	}
	if result.Version != 1 {
		t.Errorf("expected version 1, got %d", result.Version)
	}

	got := sd.GetSchema("cpu-metrics")
	if got == nil {
		t.Fatal("expected schema to exist")
	}
	if got.Name != "CPU Metrics" {
		t.Errorf("expected CPU Metrics, got %s", got.Name)
	}
}

func TestSchemaDesignerVersioning(t *testing.T) {
	sd := NewSchemaDesigner(nil, DefaultSchemaDesignerConfig())

	sd.CreateSchema(SchemaDesign{ID: "s1", Name: "Test", Fields: []SchemaDesignField{{Name: "v", Type: SchemaFieldFloat}}})
	sd.CreateSchema(SchemaDesign{ID: "s1", Name: "Test V2", Fields: []SchemaDesignField{{Name: "v", Type: SchemaFieldFloat}}})

	got := sd.GetSchema("s1")
	if got.Version != 2 {
		t.Errorf("expected version 2, got %d", got.Version)
	}
	if got.Name != "Test V2" {
		t.Errorf("expected Test V2, got %s", got.Name)
	}
}

func TestSchemaDesignerValidation(t *testing.T) {
	sd := NewSchemaDesigner(nil, DefaultSchemaDesignerConfig())

	errors := sd.ValidateSchema(SchemaDesign{})
	if len(errors) < 3 {
		t.Errorf("expected at least 3 validation errors, got %d", len(errors))
	}

	errors = sd.ValidateSchema(SchemaDesign{
		ID: "s1", Name: "Test",
		Fields: []SchemaDesignField{
			{Name: "dup"},
			{Name: "dup"},
		},
	})
	hasDup := false
	for _, e := range errors {
		if e == "duplicate field name: dup" {
			hasDup = true
		}
	}
	if !hasDup {
		t.Error("expected duplicate field name error")
	}
}

func TestSchemaDesignerDelete(t *testing.T) {
	sd := NewSchemaDesigner(nil, DefaultSchemaDesignerConfig())
	sd.CreateSchema(SchemaDesign{ID: "s1", Name: "Test", Fields: []SchemaDesignField{{Name: "v", Type: SchemaFieldFloat}}})

	if err := sd.DeleteSchema("s1"); err != nil {
		t.Fatalf("DeleteSchema failed: %v", err)
	}
	if sd.GetSchema("s1") != nil {
		t.Error("expected schema to be deleted")
	}
}

func TestSchemaDesignerGoCodeGen(t *testing.T) {
	sd := NewSchemaDesigner(nil, DefaultSchemaDesignerConfig())

	sd.CreateSchema(SchemaDesign{
		ID:           "cpu",
		Name:         "CPU Metrics",
		MetricPrefix: "system.cpu",
		Fields: []SchemaDesignField{
			{Name: "usage", Type: SchemaFieldFloat, Description: "CPU usage percentage"},
			{Name: "host", Type: SchemaFieldString, IsTag: true},
		},
		RetentionPolicy: &SchemaRetention{MaxAge: 30 * 24 * time.Hour, Action: "delete"},
	})

	output, err := sd.GenerateGoCode("cpu")
	if err != nil {
		t.Fatalf("GenerateGoCode failed: %v", err)
	}
	if output.Language != "go" {
		t.Errorf("expected go language, got %s", output.Language)
	}
	if output.Code == "" {
		t.Error("expected non-empty code")
	}
	if output.FileName == "" {
		t.Error("expected non-empty filename")
	}
}

func TestSchemaDesignerYAMLCodeGen(t *testing.T) {
	sd := NewSchemaDesigner(nil, DefaultSchemaDesignerConfig())

	sd.CreateSchema(SchemaDesign{
		ID:   "mem",
		Name: "Memory Metrics",
		Fields: []SchemaDesignField{
			{Name: "used", Type: SchemaFieldFloat, Required: true},
		},
	})

	output, err := sd.GenerateYAMLConfig("mem")
	if err != nil {
		t.Fatalf("GenerateYAMLConfig failed: %v", err)
	}
	if output.Language != "yaml" {
		t.Errorf("expected yaml language, got %s", output.Language)
	}
}

func TestSchemaDesignerStats(t *testing.T) {
	sd := NewSchemaDesigner(nil, DefaultSchemaDesignerConfig())

	sd.CreateSchema(SchemaDesign{
		ID: "s1", Name: "Test",
		Fields: []SchemaDesignField{{Name: "f1", Type: SchemaFieldFloat}, {Name: "f2", Type: SchemaFieldString}},
	})

	stats := sd.Stats()
	if stats.TotalSchemas != 1 {
		t.Errorf("expected 1 schema, got %d", stats.TotalSchemas)
	}
	if stats.TotalFields != 2 {
		t.Errorf("expected 2 fields, got %d", stats.TotalFields)
	}
}

func TestSchemaDesignerMaxSchemas(t *testing.T) {
	cfg := DefaultSchemaDesignerConfig()
	cfg.MaxSchemas = 2
	sd := NewSchemaDesigner(nil, cfg)

	sd.CreateSchema(SchemaDesign{ID: "s1", Name: "T1", Fields: []SchemaDesignField{{Name: "v", Type: SchemaFieldFloat}}})
	sd.CreateSchema(SchemaDesign{ID: "s2", Name: "T2", Fields: []SchemaDesignField{{Name: "v", Type: SchemaFieldFloat}}})

	_, err := sd.CreateSchema(SchemaDesign{ID: "s3", Name: "T3", Fields: []SchemaDesignField{{Name: "v", Type: SchemaFieldFloat}}})
	if err == nil {
		t.Fatal("expected error when exceeding max schemas")
	}
}

func TestSchemaDesignerList(t *testing.T) {
	sd := NewSchemaDesigner(nil, DefaultSchemaDesignerConfig())

	sd.CreateSchema(SchemaDesign{ID: "b", Name: "B", Fields: []SchemaDesignField{{Name: "v", Type: SchemaFieldFloat}}})
	sd.CreateSchema(SchemaDesign{ID: "a", Name: "A", Fields: []SchemaDesignField{{Name: "v", Type: SchemaFieldFloat}}})

	schemas := sd.ListSchemas()
	if len(schemas) != 2 {
		t.Fatalf("expected 2 schemas, got %d", len(schemas))
	}
	if schemas[0].ID != "a" {
		t.Errorf("expected sorted order, first schema should be 'a', got '%s'", schemas[0].ID)
	}
}
