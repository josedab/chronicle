package chronicle

import (
	"fmt"
	"testing"
)

func TestWritePipelineConfig(t *testing.T) {
	cfg := DefaultWritePipelineConfig()
	if !cfg.Enabled {
		t.Error("expected enabled")
	}
	if cfg.MaxHooks != 32 {
		t.Errorf("expected 32 max hooks, got %d", cfg.MaxHooks)
	}
}

func TestWritePipelineRegister(t *testing.T) {
	db := setupTestDB(t)

	engine := NewWritePipelineEngine(db, DefaultWritePipelineConfig())

	err := engine.Register(WriteHook{
		Name:    "double_value",
		Phase:   "pre",
		Handler: func(p Point) (Point, error) { p.Value *= 2; return p, nil },
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	hooks := engine.ListHooks()
	if len(hooks) != 1 {
		t.Fatalf("expected 1 hook, got %d", len(hooks))
	}
	if hooks[0].Name != "double_value" {
		t.Errorf("expected hook name double_value, got %s", hooks[0].Name)
	}
}

func TestWritePipelineInvalidPhase(t *testing.T) {
	db := setupTestDB(t)

	engine := NewWritePipelineEngine(db, DefaultWritePipelineConfig())

	err := engine.Register(WriteHook{
		Name:    "bad",
		Phase:   "invalid",
		Handler: func(p Point) (Point, error) { return p, nil },
	})
	if err == nil {
		t.Error("expected error for invalid phase")
	}
}

func TestWritePipelineUnregister(t *testing.T) {
	db := setupTestDB(t)

	engine := NewWritePipelineEngine(db, DefaultWritePipelineConfig())
	engine.Register(WriteHook{
		Name:    "hook1",
		Phase:   "pre",
		Handler: func(p Point) (Point, error) { return p, nil },
	})

	if !engine.Unregister("hook1") {
		t.Error("expected successful unregister")
	}
	if engine.Unregister("nonexistent") {
		t.Error("expected false for nonexistent hook")
	}
	if len(engine.ListHooks()) != 0 {
		t.Error("expected no hooks after unregister")
	}
}

func TestWritePipelineProcessPre(t *testing.T) {
	db := setupTestDB(t)

	engine := NewWritePipelineEngine(db, DefaultWritePipelineConfig())

	t.Run("modify point", func(t *testing.T) {
		engine.Register(WriteHook{
			Name:  "add_tag",
			Phase: "pre",
			Handler: func(p Point) (Point, error) {
				if p.Tags == nil {
					p.Tags = make(map[string]string)
				}
				p.Tags["processed"] = "true"
				return p, nil
			},
		})

		result, err := engine.ProcessPre(Point{Metric: "cpu", Value: 42.0})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Tags["processed"] != "true" {
			t.Error("expected tag to be added")
		}
		if result.Value != 42.0 {
			t.Errorf("expected value 42.0, got %f", result.Value)
		}
	})

	t.Run("reject point", func(t *testing.T) {
		engine.Register(WriteHook{
			Name:  "reject_negative",
			Phase: "pre",
			Handler: func(p Point) (Point, error) {
				if p.Value < 0 {
					return Point{}, fmt.Errorf("negative values not allowed")
				}
				return p, nil
			},
		})

		_, err := engine.ProcessPre(Point{Metric: "cpu", Value: -1.0})
		if err == nil {
			t.Error("expected rejection error")
		}

		stats := engine.Stats()
		if stats.TotalRejected == 0 {
			t.Error("expected rejected count > 0")
		}
		if stats.TotalErrors == 0 {
			t.Error("expected error count > 0")
		}
	})
}

func TestWritePipelineProcessPost(t *testing.T) {
	db := setupTestDB(t)

	engine := NewWritePipelineEngine(db, DefaultWritePipelineConfig())

	called := false
	engine.Register(WriteHook{
		Name:  "audit",
		Phase: "post",
		Handler: func(p Point) (Point, error) {
			called = true
			return p, nil
		},
	})

	engine.ProcessPost(Point{Metric: "cpu", Value: 42.0})
	if !called {
		t.Error("expected post hook to be called")
	}
}

func TestWritePipelineStats(t *testing.T) {
	db := setupTestDB(t)

	engine := NewWritePipelineEngine(db, DefaultWritePipelineConfig())
	engine.Register(WriteHook{
		Name:    "passthrough",
		Phase:   "pre",
		Handler: func(p Point) (Point, error) { return p, nil },
	})

	engine.ProcessPre(Point{Metric: "cpu", Value: 1.0})
	engine.ProcessPre(Point{Metric: "cpu", Value: 2.0})

	stats := engine.Stats()
	if stats.TotalProcessed != 2 {
		t.Errorf("expected 2 processed, got %d", stats.TotalProcessed)
	}
	if stats.HookCount != 1 {
		t.Errorf("expected 1 hook, got %d", stats.HookCount)
	}
}

func TestWritePipelineMaxHooks(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultWritePipelineConfig()
	cfg.MaxHooks = 2
	engine := NewWritePipelineEngine(db, cfg)

	for i := 0; i < 2; i++ {
		err := engine.Register(WriteHook{
			Name:    fmt.Sprintf("hook_%d", i),
			Phase:   "pre",
			Handler: func(p Point) (Point, error) { return p, nil },
		})
		if err != nil {
			t.Fatalf("unexpected error registering hook %d: %v", i, err)
		}
	}

	err := engine.Register(WriteHook{
		Name:    "overflow",
		Phase:   "pre",
		Handler: func(p Point) (Point, error) { return p, nil },
	})
	if err == nil {
		t.Error("expected error when exceeding max hooks")
	}
}
