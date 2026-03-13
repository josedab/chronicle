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

	t.Run("map multiply", func(t *testing.T) {
		engine := NewWASMUDFEngine(db, DefaultWASMUDFConfig())
		engine.Register(UDFDefinition{
			Name: "scale", Type: UDFTypeMap,
			Metadata: map[string]string{"operation": "multiply", "factor": "2.5"},
		})
		result, err := engine.Invoke("scale", map[string]interface{}{
			"values": []float64{2, 4, 10},
		})
		if err != nil {
			t.Fatal(err)
		}
		output := result.Output.([]float64)
		if output[0] != 5.0 || output[1] != 10.0 || output[2] != 25.0 {
			t.Errorf("expected [5, 10, 25], got %v", output)
		}
	})

	t.Run("map abs", func(t *testing.T) {
		engine := NewWASMUDFEngine(db, DefaultWASMUDFConfig())
		engine.Register(UDFDefinition{Name: "abs_map", Type: UDFTypeMap})
		result, err := engine.Invoke("abs_map", map[string]interface{}{
			"values":    []float64{-3, 5, -1, 0},
			"operation": "abs",
		})
		if err != nil {
			t.Fatal(err)
		}
		output := result.Output.([]float64)
		if output[0] != 3 || output[1] != 5 || output[2] != 1 || output[3] != 0 {
			t.Errorf("expected [3, 5, 1, 0], got %v", output)
		}
	})

	t.Run("map clamp", func(t *testing.T) {
		engine := NewWASMUDFEngine(db, DefaultWASMUDFConfig())
		engine.Register(UDFDefinition{Name: "clamp", Type: UDFTypeMap})
		result, err := engine.Invoke("clamp", map[string]interface{}{
			"values":    []float64{-5, 3, 15, 7},
			"operation": "clamp",
			"min":       0.0,
			"max":       10.0,
		})
		if err != nil {
			t.Fatal(err)
		}
		output := result.Output.([]float64)
		if output[0] != 0 || output[1] != 3 || output[2] != 10 || output[3] != 7 {
			t.Errorf("expected [0, 3, 10, 7], got %v", output)
		}
	})

	t.Run("reduce operations", func(t *testing.T) {
		engine := NewWASMUDFEngine(db, DefaultWASMUDFConfig())
		vals := map[string]interface{}{"values": []float64{2, 3, 4}}

		// product
		engine.Register(UDFDefinition{Name: "prod", Type: UDFTypeReduce, Metadata: map[string]string{"operation": "product"}})
		r, _ := engine.Invoke("prod", vals)
		if r.Output.(float64) != 24 {
			t.Errorf("product: expected 24, got %v", r.Output)
		}

		// min
		engine.Register(UDFDefinition{Name: "rmin", Type: UDFTypeReduce, Metadata: map[string]string{"operation": "min"}})
		r, _ = engine.Invoke("rmin", vals)
		if r.Output.(float64) != 2 {
			t.Errorf("min: expected 2, got %v", r.Output)
		}

		// max
		engine.Register(UDFDefinition{Name: "rmax", Type: UDFTypeReduce, Metadata: map[string]string{"operation": "max"}})
		r, _ = engine.Invoke("rmax", vals)
		if r.Output.(float64) != 4 {
			t.Errorf("max: expected 4, got %v", r.Output)
		}

		// mean
		engine.Register(UDFDefinition{Name: "rmean", Type: UDFTypeReduce, Metadata: map[string]string{"operation": "mean"}})
		r, _ = engine.Invoke("rmean", vals)
		if r.Output.(float64) != 3 {
			t.Errorf("mean: expected 3, got %v", r.Output)
		}
	})

	t.Run("filter operators", func(t *testing.T) {
		engine := NewWASMUDFEngine(db, DefaultWASMUDFConfig())
		engine.Register(UDFDefinition{Name: "flt", Type: UDFTypeFilter})
		vals := []float64{1, 5, 3, 8, 2}

		// lt operator
		r, _ := engine.Invoke("flt", map[string]interface{}{
			"values": vals, "threshold": 4.0, "operator": "lt",
		})
		output := r.Output.([]float64)
		if len(output) != 3 { // 1, 3, 2
			t.Errorf("lt filter: expected 3 values, got %d", len(output))
		}

		// gte operator
		r, _ = engine.Invoke("flt", map[string]interface{}{
			"values": vals, "threshold": 5.0, "operator": "gte",
		})
		output = r.Output.([]float64)
		if len(output) != 2 { // 5, 8
			t.Errorf("gte filter: expected 2 values, got %d", len(output))
		}
	})

	t.Run("aggregate full stats", func(t *testing.T) {
		engine := NewWASMUDFEngine(db, DefaultWASMUDFConfig())
		engine.Register(UDFDefinition{Name: "stats", Type: UDFTypeAggregate})
		r, _ := engine.Invoke("stats", map[string]interface{}{
			"values": []float64{10, 20, 30},
		})
		output := r.Output.(map[string]float64)
		if output["min"] != 10 {
			t.Errorf("expected min=10, got %f", output["min"])
		}
		if output["max"] != 30 {
			t.Errorf("expected max=30, got %f", output["max"])
		}
		if output["range"] != 20 {
			t.Errorf("expected range=20, got %f", output["range"])
		}
		if output["stddev"] <= 0 {
			t.Errorf("expected positive stddev, got %f", output["stddev"])
		}
	})

	t.Run("transform select fields", func(t *testing.T) {
		engine := NewWASMUDFEngine(db, DefaultWASMUDFConfig())
		engine.Register(UDFDefinition{Name: "sel", Type: UDFTypeTransform})
		r, _ := engine.Invoke("sel", map[string]interface{}{
			"a": 1.0, "b": 2.0, "c": 3.0,
			"select": []interface{}{"a", "c"},
		})
		output := r.Output.(map[string]interface{})
		if _, ok := output["b"]; ok {
			t.Error("field 'b' should have been excluded by select")
		}
		if _, ok := output["a"]; !ok {
			t.Error("field 'a' should be present")
		}
	})

	t.Run("trigger conditional", func(t *testing.T) {
		engine := NewWASMUDFEngine(db, DefaultWASMUDFConfig())
		engine.Register(UDFDefinition{
			Name: "low_check", Type: UDFTypeTrigger,
			Metadata: map[string]string{"condition": "lt", "threshold": "10"},
		})

		// value=5 < threshold=10 → triggered
		r, _ := engine.Invoke("low_check", map[string]interface{}{"value": 5.0})
		output := r.Output.(map[string]interface{})
		if output["triggered"] != true {
			t.Error("expected triggered=true for 5 < 10")
		}

		// value=15 < threshold=10 → not triggered
		r, _ = engine.Invoke("low_check", map[string]interface{}{"value": 15.0})
		output = r.Output.(map[string]interface{})
		if output["triggered"] != false {
			t.Error("expected triggered=false for 15 < 10")
		}
	})
}
