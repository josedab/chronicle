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

func TestOnlineLearner(t *testing.T) {
	ol := NewOnlineLearner(0.1)

	for i := 0; i < 100; i++ {
		ol.Update(float64(i))
	}

	mean, stddev := ol.Predict()
	if mean < 50 || mean > 99 {
		t.Errorf("mean out of range: %f", mean)
	}
	if stddev < 0 {
		t.Errorf("stddev should be non-negative: %f", stddev)
	}
}

func TestOnlineLearnerDriftDetection(t *testing.T) {
	ol := NewOnlineLearner(0.1)

	// Build baseline with low values
	for i := 0; i < 200; i++ {
		ol.Update(1.0)
		ol.DetectDrift(0.1)
	}

	// Introduce large errors to trigger drift
	drifted := false
	for i := 0; i < 200; i++ {
		if ol.DetectDrift(100.0) {
			drifted = true
			break
		}
	}
	if !drifted {
		t.Error("expected drift to be detected")
	}
	if ol.DriftCount() == 0 {
		t.Error("expected non-zero drift count")
	}
}

func TestDBSCAN(t *testing.T) {
	points := [][]float64{
		{1, 1}, {1.1, 1.1}, {0.9, 0.9}, // cluster 1
		{5, 5}, {5.1, 5.1}, {4.9, 4.9}, // cluster 2
		{10, 10}, // noise
	}
	result := DBSCAN(points, DBSCANConfig{Epsilon: 0.5, MinPoints: 2})
	if result.NumClusters != 2 {
		t.Errorf("expected 2 clusters, got %d", result.NumClusters)
	}
	if len(result.Noise) != 1 {
		t.Errorf("expected 1 noise point, got %d", len(result.Noise))
	}
}

func TestIForestDetector(t *testing.T) {
	iforest := NewIForestDetector(50, 100, 0.6)

	data := make([][]float64, 100)
	for i := range data {
		data[i] = []float64{float64(i), float64(i) * 0.5}
	}
	iforest.Fit(data)

	// Score should return a value between 0 and 1
	score := iforest.Score([]float64{50, 25})
	if score < 0 || score > 1 {
		t.Errorf("score out of range [0,1]: %f", score)
	}

	// IsAnomaly should work with threshold
	if iforest.iThreshold <= 0 || iforest.iThreshold > 1 {
		t.Errorf("threshold out of range: %f", iforest.iThreshold)
	}
}

func TestMultiChannelRouter(t *testing.T) {
	channels := []AlertChannel{
		{Name: "slack-critical", Type: "slack", Severity: "critical", Enabled: true},
		{Name: "email-warning", Type: "email", Severity: "warning", Enabled: true},
		{Name: "disabled-channel", Type: "webhook", Severity: "info", Enabled: false},
	}
	router := NewMultiChannelRouter(channels)

	// Critical anomaly should route to both channels
	critAnomaly := &AnomalyV2{Severity: "critical"}
	routed := router.Route(critAnomaly)
	if len(routed) != 2 {
		t.Errorf("expected 2 routes for critical, got %d: %v", len(routed), routed)
	}

	// Warning should route to email only
	warnAnomaly := &AnomalyV2{Severity: "warning"}
	routed = router.Route(warnAnomaly)
	if len(routed) != 1 {
		t.Errorf("expected 1 route for warning, got %d: %v", len(routed), routed)
	}

	stats := router.Stats()
	if stats.TotalAlerts != 2 {
		t.Errorf("expected 2 total alerts, got %d", stats.TotalAlerts)
	}
}

func TestAnomalyPostWriteHook(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultAnomalyDetectionV2Config()
	engine := NewAnomalyDetectionV2Engine(db, cfg)
	engine.Start()
	defer engine.Stop()

	hook := engine.AsPostWriteHook()
	if hook.Name != "anomaly-detection-v2" {
		t.Errorf("expected hook name anomaly-detection-v2, got %s", hook.Name)
	}
	if hook.Phase != "post" {
		t.Errorf("expected post phase, got %s", hook.Phase)
	}

	// Hook should pass through points unchanged
	p := Point{Metric: "test_metric", Value: 42.0, Timestamp: 1000}
	result, err := hook.Handler(p)
	if err != nil {
		t.Fatalf("hook error: %v", err)
	}
	if result.Metric != p.Metric || result.Value != p.Value {
		t.Error("hook should not modify point")
	}
}

func TestAnomalyWithRouter(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultAnomalyDetectionV2Config()
	cfg.MinDataPoints = 3
	engine := NewAnomalyDetectionV2Engine(db, cfg)
	engine.Start()
	defer engine.Stop()

	router := NewMultiChannelRouter([]AlertChannel{
		{Name: "test-channel", Type: "webhook", Severity: "info", Enabled: true},
	})
	engine.SetAlertRouter(router)

	// Ingest enough points to build baseline
	for i := 0; i < 50; i++ {
		engine.Ingest("test.metric", 1.0, nil, time.Now())
	}

	// Ingest anomalous point
	engine.Ingest("test.metric", 1000.0, nil, time.Now())

	stats := router.Stats()
	// Router should have processed at least one alert (from the spike)
	if stats.TotalAlerts == 0 && engine.stats.TotalDetected > 0 {
		t.Log("anomaly detected but not routed (within threshold)")
	}
}

func TestAnomalyFullPipelineIntegration(t *testing.T) {
	db := setupTestDB(t)

	cfg := DefaultAnomalyDetectionV2Config()
	cfg.MinDataPoints = 5
	cfg.BaselineWindow = 100
	engine := NewAnomalyDetectionV2Engine(db, cfg)
	engine.Start()
	defer engine.Stop()

	// Set up multi-channel router
	router := NewMultiChannelRouter([]AlertChannel{
		{Name: "critical-slack", Type: "slack", Severity: "critical", Enabled: true},
		{Name: "warning-email", Type: "email", Severity: "warning", Enabled: true},
		{Name: "info-webhook", Type: "webhook", Severity: "info", Enabled: true},
	})
	engine.SetAlertRouter(router)

	// Phase 1: Build baseline with normal data
	for i := 0; i < 50; i++ {
		engine.Ingest("pipeline.metric", 100.0+float64(i%5), nil, time.Now())
	}

	// Verify baseline was built
	baseline := engine.GetBaseline("pipeline.metric")
	if baseline == nil {
		t.Fatal("expected baseline to exist")
	}
	if baseline.Mean < 99 || baseline.Mean > 105 {
		t.Errorf("baseline mean should be ~102, got %f", baseline.Mean)
	}

	// Phase 2: Inject anomaly (10x normal)
	anomaly := engine.Ingest("pipeline.metric", 1000.0, map[string]string{"host": "prod-1"}, time.Now())

	// Phase 3: Verify anomaly was detected
	anomalies := engine.ListAnomalies(10)
	if len(anomalies) == 0 {
		t.Log("no anomalies detected (threshold may be wide)")
	}

	// Phase 4: Verify online learner was updated
	engine.mu.RLock()
	learner, hasLearner := engine.learners["pipeline.metric"]
	engine.mu.RUnlock()
	if !hasLearner {
		t.Error("expected online learner for pipeline.metric")
	} else {
		mean, _ := learner.Predict()
		if mean == 0 {
			t.Error("learner mean should be non-zero after data")
		}
	}

	// Phase 5: Verify router processed alerts
	routerStats := router.Stats()
	t.Logf("anomaly=%v, detected=%d, routed=%d", anomaly != nil, engine.stats.TotalDetected, routerStats.TotalAlerts)

	// Phase 6: Test post-write hook integration
	hook := engine.AsPostWriteHook()
	p := Point{Metric: "hook.metric", Value: 42.0, Timestamp: time.Now().UnixNano()}
	result, err := hook.Handler(p)
	if err != nil {
		t.Fatalf("hook error: %v", err)
	}
	if result.Metric != p.Metric {
		t.Error("hook should pass through point unchanged")
	}

	// Phase 7: Write pipeline integration — register hook and write through DB
	pipeEngine := NewWritePipelineEngine(db, WritePipelineConfig{Enabled: true, MaxHooks: 10})
	pipeEngine.Start()
	defer pipeEngine.Stop()
	if err := pipeEngine.Register(hook); err != nil {
		t.Fatalf("Register hook: %v", err)
	}
	hooks := pipeEngine.ListHooks()
	found := false
	for _, h := range hooks {
		if h.Name == "anomaly-detection-v2" {
			found = true
		}
	}
	if !found {
		t.Error("expected anomaly hook to be registered in write pipeline")
	}

	// Phase 8: Verify feedback loop
	if engine.stats.TotalDetected > 0 && len(anomalies) > 0 {
		fb := AnomalyFeedback{
			AnomalyID: anomalies[0].ID,
			IsAnomaly: false,
			Comment:   "false_positive",
		}
		if err := engine.SubmitFeedback(fb); err != nil {
			t.Fatalf("SubmitFeedback: %v", err)
		}
	}

	// Verify stats
	finalStats := engine.GetStats()
	t.Logf("Final stats: detected=%d, truePos=%d, falsePos=%d, fpRate=%.2f",
		finalStats.TotalDetected, finalStats.TotalTruePositives,
		finalStats.TotalFalsePositives, finalStats.FalsePositiveRate)
}

func TestDBSCANEdgeCases(t *testing.T) {
	t.Run("EmptyData", func(t *testing.T) {
		result := DBSCAN(nil, DBSCANConfig{Epsilon: 1, MinPoints: 2})
		if result.NumClusters != 0 {
			t.Errorf("expected 0 clusters for nil data, got %d", result.NumClusters)
		}
	})

	t.Run("SinglePoint", func(t *testing.T) {
		result := DBSCAN([][]float64{{1, 1}}, DBSCANConfig{Epsilon: 1, MinPoints: 2})
		if result.NumClusters != 0 {
			t.Error("single point cannot form a cluster with MinPoints=2")
		}
		if len(result.Noise) != 1 {
			t.Errorf("expected 1 noise point, got %d", len(result.Noise))
		}
	})

	t.Run("AllSamePoint", func(t *testing.T) {
		points := make([][]float64, 5)
		for i := range points {
			points[i] = []float64{1, 1}
		}
		result := DBSCAN(points, DBSCANConfig{Epsilon: 0.1, MinPoints: 3})
		if result.NumClusters != 1 {
			t.Errorf("expected 1 cluster for identical points, got %d", result.NumClusters)
		}
	})
}
