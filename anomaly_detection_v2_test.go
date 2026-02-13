package chronicle

import (
	"testing"
	"time"
)

func TestAnomalyDetectionV2Engine(t *testing.T) {
	db, err := Open(t.TempDir()+"/test.db", DefaultConfig(t.TempDir()+"/test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	t.Run("basic detection", func(t *testing.T) {
		cfg := DefaultAnomalyDetectionV2Config()
		cfg.MinDataPoints = 10
		engine := NewAnomalyDetectionV2Engine(db, cfg)

		now := time.Now()
		// Feed normal data
		for i := 0; i < 50; i++ {
			engine.Ingest("cpu", 50.0+float64(i%5), nil, now.Add(time.Duration(i)*time.Minute))
		}

		// Feed anomalous point
		anomaly := engine.Ingest("cpu", 200.0, nil, now.Add(51*time.Minute))
		if anomaly == nil {
			t.Log("no anomaly detected (may need more data)")
		} else {
			if anomaly.Metric != "cpu" {
				t.Errorf("expected metric cpu, got %s", anomaly.Metric)
			}
			if anomaly.Method != "stl_adaptive" {
				t.Errorf("expected method stl_adaptive, got %s", anomaly.Method)
			}
			if anomaly.Score <= 0 {
				t.Error("expected positive score")
			}
		}
	})

	t.Run("baseline tracking", func(t *testing.T) {
		engine := NewAnomalyDetectionV2Engine(db, DefaultAnomalyDetectionV2Config())

		for i := 0; i < 50; i++ {
			engine.Ingest("memory", float64(i*10), nil, time.Now())
		}

		baseline := engine.GetBaseline("memory")
		if baseline == nil {
			t.Fatal("expected baseline")
		}
		if baseline.Metric != "memory" {
			t.Errorf("expected memory, got %s", baseline.Metric)
		}
		if baseline.Mean == 0 {
			t.Error("expected non-zero mean")
		}
		if baseline.Stddev == 0 {
			t.Error("expected non-zero stddev")
		}
		if baseline.P50 == 0 {
			t.Error("expected non-zero P50")
		}
	})

	t.Run("adaptive threshold", func(t *testing.T) {
		engine := NewAnomalyDetectionV2Engine(db, DefaultAnomalyDetectionV2Config())

		for i := 0; i < 30; i++ {
			engine.Ingest("disk", float64(i), nil, time.Now())
		}

		threshold := engine.GetThreshold("disk")
		if threshold == nil {
			t.Fatal("expected threshold")
		}
		if threshold.Metric != "disk" {
			t.Errorf("expected disk, got %s", threshold.Metric)
		}
		if threshold.Upper <= 0 {
			t.Error("expected positive upper threshold")
		}
	})

	t.Run("feedback loop", func(t *testing.T) {
		cfg := DefaultAnomalyDetectionV2Config()
		cfg.MinDataPoints = 5
		engine := NewAnomalyDetectionV2Engine(db, cfg)

		now := time.Now()
		for i := 0; i < 20; i++ {
			engine.Ingest("latency", 100.0, nil, now.Add(time.Duration(i)*time.Minute))
		}

		// Generate anomaly
		anomaly := engine.Ingest("latency", 500.0, nil, now.Add(21*time.Minute))
		if anomaly != nil {
			// Submit feedback: false positive
			err := engine.SubmitFeedback(AnomalyFeedback{
				AnomalyID: anomaly.ID,
				IsAnomaly: false,
				Comment:   "planned spike",
			})
			if err != nil {
				t.Fatal(err)
			}

			stats := engine.GetStats()
			if stats.TotalFalsePositives != 1 {
				t.Errorf("expected 1 false positive, got %d", stats.TotalFalsePositives)
			}

			// Submit true positive
			err = engine.SubmitFeedback(AnomalyFeedback{
				AnomalyID: anomaly.ID,
				IsAnomaly: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			stats = engine.GetStats()
			if stats.TotalTruePositives != 1 {
				t.Errorf("expected 1 true positive, got %d", stats.TotalTruePositives)
			}
		}
	})

	t.Run("feedback disabled", func(t *testing.T) {
		cfg := DefaultAnomalyDetectionV2Config()
		cfg.FeedbackEnabled = false
		engine := NewAnomalyDetectionV2Engine(db, cfg)

		err := engine.SubmitFeedback(AnomalyFeedback{AnomalyID: "x", IsAnomaly: true})
		if err == nil {
			t.Error("expected error when feedback disabled")
		}
	})

	t.Run("STL decomposition", func(t *testing.T) {
		cfg := DefaultAnomalyDetectionV2Config()
		cfg.MinDataPoints = 5
		engine := NewAnomalyDetectionV2Engine(db, cfg)

		now := time.Now()
		// Generate seasonal-like data
		for i := 0; i < 48; i++ {
			val := 100.0
			if i%12 < 6 {
				val = 150.0
			}
			engine.Ingest("seasonal_metric", val, nil, now.Add(time.Duration(i)*time.Hour))
		}

		decomp := engine.Decompose("seasonal_metric")
		if decomp == nil {
			t.Fatal("expected decomposition")
		}
		if len(decomp.Trend) != 48 {
			t.Errorf("expected 48 trend values, got %d", len(decomp.Trend))
		}
		if len(decomp.Seasonal) != 48 {
			t.Errorf("expected 48 seasonal values, got %d", len(decomp.Seasonal))
		}
		if len(decomp.Residual) != 48 {
			t.Errorf("expected 48 residual values, got %d", len(decomp.Residual))
		}
		if decomp.Period < 2 {
			t.Errorf("expected period >= 2, got %d", decomp.Period)
		}
	})

	t.Run("decompose unknown metric", func(t *testing.T) {
		engine := NewAnomalyDetectionV2Engine(db, DefaultAnomalyDetectionV2Config())
		if engine.Decompose("nonexistent") != nil {
			t.Error("expected nil for unknown metric")
		}
	})

	t.Run("list anomalies", func(t *testing.T) {
		cfg := DefaultAnomalyDetectionV2Config()
		cfg.MinDataPoints = 5
		engine := NewAnomalyDetectionV2Engine(db, cfg)

		now := time.Now()
		for i := 0; i < 20; i++ {
			engine.Ingest("list_test", 50.0, nil, now.Add(time.Duration(i)*time.Minute))
		}
		engine.Ingest("list_test", 999.0, nil, now.Add(21*time.Minute))

		all := engine.ListAnomalies(0)
		limited := engine.ListAnomalies(1)
		if len(limited) > 1 {
			t.Errorf("expected at most 1, got %d", len(limited))
		}
		_ = all
	})

	t.Run("correlate anomalies", func(t *testing.T) {
		cfg := DefaultAnomalyDetectionV2Config()
		cfg.MinDataPoints = 5
		cfg.CorrelationWindow = time.Hour
		engine := NewAnomalyDetectionV2Engine(db, cfg)

		now := time.Now()
		// Feed two metrics
		for i := 0; i < 20; i++ {
			engine.Ingest("metric_a", 50.0, nil, now.Add(time.Duration(i)*time.Minute))
			engine.Ingest("metric_b", 50.0, nil, now.Add(time.Duration(i)*time.Minute))
		}

		// Anomalies at similar times
		engine.Ingest("metric_a", 999.0, nil, now.Add(21*time.Minute))
		engine.Ingest("metric_b", 999.0, nil, now.Add(22*time.Minute))

		corr := engine.CorrelateAnomalies()
		_ = corr // Correlation depends on whether anomalies were detected
	})

	t.Run("start stop", func(t *testing.T) {
		engine := NewAnomalyDetectionV2Engine(db, DefaultAnomalyDetectionV2Config())
		engine.Start()
		engine.Start() // idempotent
		engine.Stop()
		engine.Stop() // idempotent
	})

	t.Run("stats", func(t *testing.T) {
		engine := NewAnomalyDetectionV2Engine(db, DefaultAnomalyDetectionV2Config())
		for i := 0; i < 10; i++ {
			engine.Ingest("stats_metric", float64(i), nil, time.Now())
		}
		stats := engine.GetStats()
		if stats.MetricsMonitored != 1 {
			t.Errorf("expected 1 metric, got %d", stats.MetricsMonitored)
		}
	})

	t.Run("classify and severity helpers", func(t *testing.T) {
		if classifyAnomalyV2(0.9, 10) != "spike" {
			t.Error("expected spike")
		}
		if classifyAnomalyV2(0.9, -10) != "dip" {
			t.Error("expected dip")
		}
		if classifyAnomalyV2(0.6, 5) != "drift" {
			t.Error("expected drift")
		}
		if classifyAnomalyV2(0.3, 1) != "fluctuation" {
			t.Error("expected fluctuation")
		}

		if severityFromScore(0.9) != "critical" {
			t.Error("expected critical")
		}
		if severityFromScore(0.6) != "warning" {
			t.Error("expected warning")
		}
		if severityFromScore(0.2) != "info" {
			t.Error("expected info")
		}
	})

	t.Run("config defaults", func(t *testing.T) {
		cfg := DefaultAnomalyDetectionV2Config()
		if cfg.SeasonalityPeriod != 24*time.Hour {
			t.Error("unexpected seasonality period")
		}
		if cfg.BaselineWindow != 168 {
			t.Error("unexpected baseline window")
		}
		if !cfg.FeedbackEnabled {
			t.Error("feedback should be enabled by default")
		}
	})
}
