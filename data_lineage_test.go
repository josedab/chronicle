package chronicle

import "testing"

func TestDataLineageEngine(t *testing.T) {
	path := t.TempDir() + "/test.db"
	db, err := Open(path, DefaultConfig(path))
	if err != nil { t.Fatal(err) }
	defer db.Close()

	t.Run("record write", func(t *testing.T) {
		e := NewDataLineageEngine(db, DefaultDataLineageConfig())
		e.RecordWrite("cpu", "iot-gateway", 100, map[string]string{"host": "a"})
		entries := e.GetEntries("cpu", 0)
		if len(entries) != 1 { t.Fatalf("expected 1 entry, got %d", len(entries)) }
		if entries[0].EventType != LineageWrite { t.Error("expected write type") }
		if entries[0].PointCount != 100 { t.Error("expected 100 points") }
	})

	t.Run("record query", func(t *testing.T) {
		e := NewDataLineageEngine(db, DefaultDataLineageConfig())
		e.RecordQuery("disk", "dashboard")
		stats := e.GetStats()
		if stats.QueryEvents != 1 { t.Error("expected 1 query event") }
	})

	t.Run("record transform builds graph", func(t *testing.T) {
		e := NewDataLineageEngine(db, DefaultDataLineageConfig())
		e.RecordTransform("raw_cpu", "cpu_5m_avg", "downsample_5m", 1000)

		srcGraph := e.GetGraph("raw_cpu")
		if srcGraph == nil { t.Fatal("expected source graph") }
		if len(srcGraph.Downstream) != 1 || srcGraph.Downstream[0] != "cpu_5m_avg" {
			t.Error("expected downstream link")
		}

		dstGraph := e.GetGraph("cpu_5m_avg")
		if dstGraph == nil { t.Fatal("expected dest graph") }
		if len(dstGraph.Upstream) != 1 || dstGraph.Upstream[0] != "raw_cpu" {
			t.Error("expected upstream link")
		}
	})

	t.Run("tracked metrics", func(t *testing.T) {
		e := NewDataLineageEngine(db, DefaultDataLineageConfig())
		e.RecordWrite("m1", "src", 1, nil)
		e.RecordWrite("m2", "src", 1, nil)
		metrics := e.TrackedMetrics()
		if len(metrics) != 2 { t.Errorf("expected 2 metrics, got %d", len(metrics)) }
	})

	t.Run("max entries cap", func(t *testing.T) {
		cfg := DefaultDataLineageConfig()
		cfg.MaxEntries = 3
		e := NewDataLineageEngine(db, cfg)
		for i := 0; i < 10; i++ { e.RecordWrite("x", "s", 1, nil) }
		entries := e.GetEntries("", 0)
		if len(entries) > 3 { t.Errorf("expected max 3, got %d", len(entries)) }
	})

	t.Run("disabled tracking", func(t *testing.T) {
		cfg := DefaultDataLineageConfig()
		cfg.TrackWrites = false
		cfg.TrackQueries = false
		cfg.TrackTransforms = false
		e := NewDataLineageEngine(db, cfg)
		e.RecordWrite("x", "s", 1, nil)
		e.RecordQuery("x", "c")
		e.RecordTransform("a", "b", "t", 1)
		if len(e.GetEntries("", 0)) != 0 { t.Error("expected no entries") }
	})

	t.Run("get graph nil", func(t *testing.T) {
		e := NewDataLineageEngine(db, DefaultDataLineageConfig())
		if e.GetGraph("nope") != nil { t.Error("expected nil") }
	})

	t.Run("entries with limit", func(t *testing.T) {
		e := NewDataLineageEngine(db, DefaultDataLineageConfig())
		for i := 0; i < 10; i++ { e.RecordWrite("x", "s", 1, nil) }
		entries := e.GetEntries("x", 3)
		if len(entries) != 3 { t.Errorf("expected 3, got %d", len(entries)) }
	})

	t.Run("start stop", func(t *testing.T) {
		e := NewDataLineageEngine(db, DefaultDataLineageConfig())
		e.Start(); e.Start(); e.Stop(); e.Stop()
	})
}
