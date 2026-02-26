package chronicle

import "testing"

func TestDbSchema(t *testing.T) {
	db := setupTestDB(t)

	t.Run("register_and_get_schema", func(t *testing.T) {
		err := db.RegisterSchema(MetricSchema{Name: "cpu.usage"})
		if err != nil {
			t.Fatalf("RegisterSchema() error = %v", err)
		}
		s := db.GetSchema("cpu.usage")
		if s == nil {
			t.Fatal("expected schema to be found")
		}
		if s.Name != "cpu.usage" {
			t.Errorf("got schema name %q, want %q", s.Name, "cpu.usage")
		}
	})

	t.Run("get_missing_schema", func(t *testing.T) {
		s := db.GetSchema("nonexistent.metric")
		if s != nil {
			t.Error("expected nil for missing schema")
		}
	})

	t.Run("list_schemas", func(t *testing.T) {
		// Register a second schema
		if err := db.RegisterSchema(MetricSchema{Name: "mem.used"}); err != nil {
			t.Fatalf("RegisterSchema() error = %v", err)
		}
		schemas := db.ListSchemas()
		if len(schemas) < 2 {
			t.Errorf("expected at least 2 schemas, got %d", len(schemas))
		}
	})

	t.Run("unregister_schema", func(t *testing.T) {
		if err := db.RegisterSchema(MetricSchema{Name: "temp.metric"}); err != nil {
			t.Fatalf("RegisterSchema() error = %v", err)
		}
		db.UnregisterSchema("temp.metric")
		s := db.GetSchema("temp.metric")
		if s != nil {
			t.Error("expected schema to be removed after UnregisterSchema")
		}
	})
}
