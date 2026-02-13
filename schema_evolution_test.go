package chronicle

import (
	"testing"
)

func TestSchemaEvolutionEngine(t *testing.T) {
	db, err := Open(t.TempDir()+"/test.db", DefaultConfig(t.TempDir()+"/test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	t.Run("first observation creates version", func(t *testing.T) {
		engine := NewSchemaEvolutionEngine(db, DefaultSchemaEvolutionConfig())

		change := engine.Observe("cpu", map[string]string{"host": "a"})
		if change != nil {
			t.Error("first observation should not produce a change")
		}

		v := engine.GetCurrentVersion("cpu")
		if v == nil {
			t.Fatal("expected version")
		}
		if v.Version != 1 {
			t.Errorf("expected version 1, got %d", v.Version)
		}
	})

	t.Run("new tag detected", func(t *testing.T) {
		engine := NewSchemaEvolutionEngine(db, DefaultSchemaEvolutionConfig())

		engine.Observe("disk", map[string]string{"host": "a"})
		change := engine.Observe("disk", map[string]string{"host": "a", "device": "sda"})

		if change == nil {
			t.Fatal("expected change for new tag")
		}
		if change.ChangeType != SchemaChangeAddTag {
			t.Errorf("expected add_tag, got %s", change.ChangeType)
		}
		if change.Breaking {
			t.Error("adding a tag should not be breaking")
		}

		v := engine.GetCurrentVersion("disk")
		if v.Version != 2 {
			t.Errorf("expected version 2, got %d", v.Version)
		}
	})

	t.Run("removed tag is breaking", func(t *testing.T) {
		engine := NewSchemaEvolutionEngine(db, DefaultSchemaEvolutionConfig())

		engine.Observe("mem", map[string]string{"host": "a", "type": "used"})
		change := engine.Observe("mem", map[string]string{"host": "a"})

		if change == nil {
			t.Fatal("expected change for removed tag")
		}
		if !change.Breaking {
			t.Error("removing a tag should be breaking")
		}
	})

	t.Run("same schema no change", func(t *testing.T) {
		engine := NewSchemaEvolutionEngine(db, DefaultSchemaEvolutionConfig())

		engine.Observe("net", map[string]string{"iface": "eth0"})
		change := engine.Observe("net", map[string]string{"iface": "eth0"})
		if change != nil {
			t.Error("same schema should not produce change")
		}
	})

	t.Run("list changes", func(t *testing.T) {
		engine := NewSchemaEvolutionEngine(db, DefaultSchemaEvolutionConfig())

		engine.Observe("x", map[string]string{"a": "1"})
		engine.Observe("x", map[string]string{"a": "1", "b": "2"})
		engine.Observe("x", map[string]string{"a": "1"})

		all := engine.ListChanges(false)
		if len(all) < 2 {
			t.Errorf("expected at least 2 changes, got %d", len(all))
		}

		breaking := engine.ListChanges(true)
		if len(breaking) < 1 {
			t.Errorf("expected at least 1 breaking change, got %d", len(breaking))
		}
	})

	t.Run("diff versions", func(t *testing.T) {
		engine := NewSchemaEvolutionEngine(db, DefaultSchemaEvolutionConfig())

		engine.Observe("diff_m", map[string]string{"host": "a"})
		engine.Observe("diff_m", map[string]string{"host": "a", "region": "us"})

		diff := engine.DiffVersions("diff_m", 1, 2)
		if diff == nil {
			t.Fatal("expected diff")
		}
		if len(diff.AddedTags) == 0 {
			t.Error("expected added tags in diff")
		}

		// Unknown metric
		if engine.DiffVersions("nope", 1, 2) != nil {
			t.Error("expected nil for unknown metric")
		}
		// Unknown version
		if engine.DiffVersions("diff_m", 1, 99) != nil {
			t.Error("expected nil for unknown version")
		}
	})

	t.Run("tracked metrics", func(t *testing.T) {
		engine := NewSchemaEvolutionEngine(db, DefaultSchemaEvolutionConfig())

		engine.Observe("m1", map[string]string{"a": "1"})
		engine.Observe("m2", map[string]string{"b": "2"})

		metrics := engine.TrackedMetrics()
		if len(metrics) != 2 {
			t.Errorf("expected 2 metrics, got %d", len(metrics))
		}
	})

	t.Run("version history", func(t *testing.T) {
		engine := NewSchemaEvolutionEngine(db, DefaultSchemaEvolutionConfig())

		engine.Observe("versioned", map[string]string{"a": "1"})
		engine.Observe("versioned", map[string]string{"a": "1", "b": "2"})
		engine.Observe("versioned", map[string]string{"a": "1", "b": "2", "c": "3"})

		versions := engine.GetVersions("versioned")
		if len(versions) != 3 {
			t.Errorf("expected 3 versions, got %d", len(versions))
		}

		// Unknown metric
		if engine.GetVersions("nope") != nil {
			t.Error("expected nil for unknown metric")
		}
		if engine.GetCurrentVersion("nope") != nil {
			t.Error("expected nil for unknown metric")
		}
	})

	t.Run("stats", func(t *testing.T) {
		engine := NewSchemaEvolutionEngine(db, DefaultSchemaEvolutionConfig())

		engine.Observe("s1", map[string]string{"a": "1"})
		engine.Observe("s1", map[string]string{"a": "1", "b": "2"})

		stats := engine.GetStats()
		if stats.TrackedMetrics != 1 {
			t.Errorf("expected 1 metric, got %d", stats.TrackedMetrics)
		}
		if stats.TotalChanges != 1 {
			t.Errorf("expected 1 change, got %d", stats.TotalChanges)
		}
	})

	t.Run("auto detect disabled", func(t *testing.T) {
		cfg := DefaultSchemaEvolutionConfig()
		cfg.AutoDetect = false
		engine := NewSchemaEvolutionEngine(db, cfg)

		change := engine.Observe("disabled", map[string]string{"a": "1"})
		if change != nil {
			t.Error("expected nil when auto detect disabled")
		}
	})

	t.Run("max versions", func(t *testing.T) {
		cfg := DefaultSchemaEvolutionConfig()
		cfg.MaxVersions = 3
		engine := NewSchemaEvolutionEngine(db, cfg)

		engine.Observe("bounded", map[string]string{"a": "1"})
		engine.Observe("bounded", map[string]string{"a": "1", "b": "2"})
		engine.Observe("bounded", map[string]string{"a": "1", "b": "2", "c": "3"})
		engine.Observe("bounded", map[string]string{"a": "1", "b": "2", "c": "3", "d": "4"})

		versions := engine.GetVersions("bounded")
		if len(versions) > 3 {
			t.Errorf("expected max 3 versions, got %d", len(versions))
		}
	})

	t.Run("start stop", func(t *testing.T) {
		engine := NewSchemaEvolutionEngine(db, DefaultSchemaEvolutionConfig())
		engine.Start()
		engine.Start()
		engine.Stop()
		engine.Stop()
	})

	t.Run("config defaults", func(t *testing.T) {
		cfg := DefaultSchemaEvolutionConfig()
		if !cfg.AutoDetect {
			t.Error("auto detect should be enabled")
		}
		if cfg.MaxVersions != 100 {
			t.Error("unexpected max versions")
		}
	})
}
