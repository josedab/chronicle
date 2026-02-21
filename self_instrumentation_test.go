package chronicle

import "testing"

func TestSelfInstrumentationEngine(t *testing.T) {
	db := setupTestDB(t)

	t.Run("record and collect", func(t *testing.T) {
		e := NewSelfInstrumentationEngine(db, DefaultSelfInstrumentationConfig())
		e.RecordWrite(10, 240, 1000000, false) // 10 points, 240 bytes, 1ms, no error
		e.RecordWrite(5, 120, 500000, false)
		e.RecordQuery(2000000, false) // 2ms
		e.RecordWrite(1, 24, 100000, true) // error

		metrics := e.Collect()
		found := make(map[string]float64)
		for _, m := range metrics { found[m.Name] = m.Value }

		if found["chronicle_writes_total"] != 3 { t.Errorf("expected 3 writes, got %v", found["chronicle_writes_total"]) }
		if found["chronicle_write_errors_total"] != 1 { t.Error("expected 1 write error") }
		if found["chronicle_queries_total"] != 1 { t.Error("expected 1 query") }
		if found["chronicle_points_written_total"] != 16 { t.Error("expected 16 points") }
		if found["chronicle_bytes_written_total"] != 384 { t.Error("expected 384 bytes") }
	})

	t.Run("prometheus exposition", func(t *testing.T) {
		e := NewSelfInstrumentationEngine(db, DefaultSelfInstrumentationConfig())
		e.RecordWrite(1, 24, 1000000, false)
		text := e.PrometheusExposition()
		if len(text) == 0 { t.Error("empty prometheus output") }
		if !selfContainsStr(text, "chronicle_writes_total") { t.Error("missing writes_total") }
		if !selfContainsStr(text, "# HELP") { t.Error("missing HELP") }
		if !selfContainsStr(text, "# TYPE") { t.Error("missing TYPE") }
	})

	t.Run("zero state", func(t *testing.T) {
		e := NewSelfInstrumentationEngine(db, DefaultSelfInstrumentationConfig())
		metrics := e.Collect()
		if len(metrics) < 10 { t.Errorf("expected 10+ metrics, got %d", len(metrics)) }
		for _, m := range metrics {
			if m.Name == "" { t.Error("empty metric name") }
			if m.Type == "" { t.Error("empty metric type") }
		}
	})

	t.Run("start stop", func(t *testing.T) {
		e := NewSelfInstrumentationEngine(db, DefaultSelfInstrumentationConfig())
		e.Start(); e.Start(); e.Stop(); e.Stop()
	})
}

func selfContainsStr(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && selfContainsSub(s, substr))
}

func selfContainsSub(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub { return true }
	}
	return false
}
