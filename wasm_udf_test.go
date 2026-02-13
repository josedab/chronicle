package chronicle

import (
	"testing"
)

func TestWASMUDFEngine(t *testing.T) {
	db, err := Open(t.TempDir()+"/test.db", DefaultConfig(t.TempDir()+"/test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	t.Run("register and invoke", func(t *testing.T) {
		engine := NewWASMUDFEngine(db, DefaultWASMUDFConfig())

		def := UDFDefinition{
			Name:     "sum_values",
			Version:  "1.0.0",
			Type:     UDFTypeReduce,
			Language: UDFLanguageRust,
			Description: "Sums all values",
			InputSchema: []UDFParam{{Name: "values", Type: "[]float64"}},
			OutputSchema: []UDFParam{{Name: "result", Type: "float64"}},
		}

		if err := engine.Register(def); err != nil {
			t.Fatal(err)
		}

		result, err := engine.Invoke("sum_values", map[string]interface{}{
			"values": []float64{1, 2, 3, 4, 5},
		})
		if err != nil {
			t.Fatal(err)
		}
		if !result.Success {
			t.Errorf("expected success, got error: %s", result.Error)
		}
		if result.Output.(float64) != 15 {
			t.Errorf("expected 15, got %v", result.Output)
		}
	})

	t.Run("register duplicate", func(t *testing.T) {
		engine := NewWASMUDFEngine(db, DefaultWASMUDFConfig())
		def := UDFDefinition{Name: "dup", Type: UDFTypeMap}
		engine.Register(def)
		if err := engine.Register(def); err == nil {
			t.Error("expected error for duplicate")
		}
	})

	t.Run("register empty name", func(t *testing.T) {
		engine := NewWASMUDFEngine(db, DefaultWASMUDFConfig())
		if err := engine.Register(UDFDefinition{}); err == nil {
			t.Error("expected error for empty name")
		}
	})

	t.Run("max UDFs limit", func(t *testing.T) {
		cfg := DefaultWASMUDFConfig()
		cfg.MaxUDFs = 2
		engine := NewWASMUDFEngine(db, cfg)

		engine.Register(UDFDefinition{Name: "a", Type: UDFTypeMap})
		engine.Register(UDFDefinition{Name: "b", Type: UDFTypeMap})

		if err := engine.Register(UDFDefinition{Name: "c", Type: UDFTypeMap}); err == nil {
			t.Error("expected error for max UDFs")
		}
	})

	t.Run("invoke map UDF", func(t *testing.T) {
		engine := NewWASMUDFEngine(db, DefaultWASMUDFConfig())
		engine.Register(UDFDefinition{Name: "identity", Type: UDFTypeMap})

		result, err := engine.Invoke("identity", map[string]interface{}{
			"values": []float64{1, 2, 3},
		})
		if err != nil {
			t.Fatal(err)
		}
		output := result.Output.([]float64)
		if len(output) != 3 {
			t.Errorf("expected 3 values, got %d", len(output))
		}
	})

	t.Run("invoke filter UDF", func(t *testing.T) {
		engine := NewWASMUDFEngine(db, DefaultWASMUDFConfig())
		engine.Register(UDFDefinition{Name: "filter_gt", Type: UDFTypeFilter})

		result, err := engine.Invoke("filter_gt", map[string]interface{}{
			"values":    []float64{1, 5, 3, 8, 2},
			"threshold": 4.0,
		})
		if err != nil {
			t.Fatal(err)
		}
		output := result.Output.([]float64)
		if len(output) != 2 {
			t.Errorf("expected 2 values above threshold, got %d", len(output))
		}
	})

	t.Run("invoke aggregate UDF", func(t *testing.T) {
		engine := NewWASMUDFEngine(db, DefaultWASMUDFConfig())
		engine.Register(UDFDefinition{Name: "agg", Type: UDFTypeAggregate})

		result, err := engine.Invoke("agg", map[string]interface{}{
			"values": []float64{10, 20, 30},
		})
		if err != nil {
			t.Fatal(err)
		}
		output := result.Output.(map[string]float64)
		if output["sum"] != 60 {
			t.Errorf("expected sum=60, got %f", output["sum"])
		}
		if output["avg"] != 20 {
			t.Errorf("expected avg=20, got %f", output["avg"])
		}
	})

	t.Run("invoke transform UDF", func(t *testing.T) {
		engine := NewWASMUDFEngine(db, DefaultWASMUDFConfig())
		engine.Register(UDFDefinition{Name: "passthrough", Type: UDFTypeTransform})

		result, err := engine.Invoke("passthrough", map[string]interface{}{"key": "val"})
		if err != nil {
			t.Fatal(err)
		}
		if !result.Success {
			t.Error("expected success")
		}
	})

	t.Run("invoke trigger UDF", func(t *testing.T) {
		engine := NewWASMUDFEngine(db, DefaultWASMUDFConfig())
		engine.Register(UDFDefinition{Name: "alert_trigger", Type: UDFTypeTrigger})

		result, err := engine.Invoke("alert_trigger", map[string]interface{}{"value": 99.0})
		if err != nil {
			t.Fatal(err)
		}
		output := result.Output.(map[string]interface{})
		if output["triggered"] != true {
			t.Error("expected triggered=true")
		}
	})

	t.Run("invoke missing requires values", func(t *testing.T) {
		engine := NewWASMUDFEngine(db, DefaultWASMUDFConfig())
		engine.Register(UDFDefinition{Name: "needs_vals", Type: UDFTypeMap})

		result, _ := engine.Invoke("needs_vals", map[string]interface{}{})
		if result.Success {
			t.Error("expected failure for missing values")
		}
	})

	t.Run("invoke unknown UDF", func(t *testing.T) {
		engine := NewWASMUDFEngine(db, DefaultWASMUDFConfig())
		_, err := engine.Invoke("nonexistent", nil)
		if err == nil {
			t.Error("expected error for unknown UDF")
		}
	})

	t.Run("unregister", func(t *testing.T) {
		engine := NewWASMUDFEngine(db, DefaultWASMUDFConfig())
		engine.Register(UDFDefinition{Name: "to_remove", Type: UDFTypeMap})

		if err := engine.Unregister("to_remove"); err != nil {
			t.Fatal(err)
		}
		if err := engine.Unregister("to_remove"); err == nil {
			t.Error("expected error for missing UDF")
		}
	})

	t.Run("update UDF", func(t *testing.T) {
		engine := NewWASMUDFEngine(db, DefaultWASMUDFConfig())
		engine.Register(UDFDefinition{Name: "updatable", Type: UDFTypeMap, Version: "1.0.0"})

		err := engine.Update(UDFDefinition{Name: "updatable", Type: UDFTypeReduce, Version: "2.0.0"})
		if err != nil {
			t.Fatal(err)
		}

		inst := engine.Get("updatable")
		if inst.Definition.Version != "2.0.0" {
			t.Errorf("expected version 2.0.0, got %s", inst.Definition.Version)
		}
		if inst.Definition.Type != UDFTypeReduce {
			t.Errorf("expected reduce type, got %s", inst.Definition.Type)
		}
	})

	t.Run("update missing", func(t *testing.T) {
		engine := NewWASMUDFEngine(db, DefaultWASMUDFConfig())
		err := engine.Update(UDFDefinition{Name: "missing"})
		if err == nil {
			t.Error("expected error for missing UDF")
		}
	})

	t.Run("list and get", func(t *testing.T) {
		engine := NewWASMUDFEngine(db, DefaultWASMUDFConfig())
		engine.Register(UDFDefinition{Name: "a", Type: UDFTypeMap})
		engine.Register(UDFDefinition{Name: "b", Type: UDFTypeReduce})

		list := engine.List()
		if len(list) != 2 {
			t.Errorf("expected 2 UDFs, got %d", len(list))
		}

		inst := engine.Get("a")
		if inst == nil || inst.Definition.Name != "a" {
			t.Error("expected to find UDF 'a'")
		}
		if engine.Get("nonexistent") != nil {
			t.Error("expected nil for missing UDF")
		}
	})

	t.Run("stats", func(t *testing.T) {
		engine := NewWASMUDFEngine(db, DefaultWASMUDFConfig())
		engine.Register(UDFDefinition{Name: "stat_udf", Type: UDFTypeReduce})
		engine.Invoke("stat_udf", map[string]interface{}{"values": []float64{1}})

		stats := engine.GetStats()
		if stats.RegisteredUDFs != 1 {
			t.Errorf("expected 1 UDF, got %d", stats.RegisteredUDFs)
		}
		if stats.TotalInvocations != 1 {
			t.Errorf("expected 1 invocation, got %d", stats.TotalInvocations)
		}
	})

	t.Run("start stop", func(t *testing.T) {
		engine := NewWASMUDFEngine(db, DefaultWASMUDFConfig())
		engine.Start()
		engine.Start() // idempotent
		engine.Stop()
		engine.Stop() // idempotent
	})

	t.Run("config defaults", func(t *testing.T) {
		cfg := DefaultWASMUDFConfig()
		if cfg.MaxMemoryBytes != 64*1024*1024 {
			t.Error("unexpected max memory")
		}
		if cfg.MaxUDFs != 100 {
			t.Error("unexpected max UDFs")
		}
	})
}
