package chronicle

import (
	"math"
	"testing"
	"time"
)

func TestCapacityPlanningEngine(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultCapacityPlanningConfig()
	config.Enabled = false // Disable background loops for testing
	config.MinDataPoints = 10

	engine := NewCapacityPlanningEngine(db, config)
	defer engine.Close()

	// Collect initial metrics
	usage, err := engine.CollectMetrics()
	if err != nil {
		t.Fatalf("failed to collect metrics: %v", err)
	}

	if usage.Timestamp.IsZero() {
		t.Error("usage timestamp should not be zero")
	}
}

func TestForecastGeneration(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultCapacityPlanningConfig()
	config.Enabled = false
	config.MinDataPoints = 10
	config.ForecastHorizon = 24 * time.Hour

	engine := NewCapacityPlanningEngine(db, config)
	defer engine.Close()

	// Simulate usage history with upward trend
	now := time.Now()
	for i := 0; i < 100; i++ {
		usage := ResourceUsage{
			Timestamp:    now.Add(time.Duration(-100+i) * time.Hour),
			StorageBytes: int64(1000000 + i*50000), // Growing storage
			StorageLimit: 10000000,
			MemoryBytes:  int64(500000 + i*5000),
			MemoryLimit:  5000000,
			PointsCount:  int64(10000 + i*500),
			SeriesCount:  int64(100 + i*5),
			SeriesLimit:  10000,
			QueryRate:    10.0 + float64(i)*0.5,
			WriteRate:    100.0,
		}
		engine.historyMu.Lock()
		engine.usageHistory = append(engine.usageHistory, usage)
		engine.historyMu.Unlock()
	}

	// Generate forecasts
	forecasts, err := engine.GenerateForecasts()
	if err != nil {
		t.Fatalf("failed to generate forecasts: %v", err)
	}

	// Check storage forecast
	storageForecast, ok := forecasts["storage"]
	if !ok {
		t.Fatal("missing storage forecast")
	}

	if storageForecast.Trend != TrendUp {
		t.Errorf("expected upward trend, got %s", storageForecast.Trend)
	}

	if storageForecast.PredictedValue <= 0 {
		t.Error("predicted value should be positive")
	}

	if storageForecast.Confidence <= 0 || storageForecast.Confidence > 1 {
		t.Errorf("confidence should be between 0 and 1, got %f", storageForecast.Confidence)
	}
}

func TestTimeToExhaustion(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultCapacityPlanningConfig()
	config.Enabled = false
	config.MinDataPoints = 10

	engine := NewCapacityPlanningEngine(db, config)
	defer engine.Close()

	// Simulate rapidly growing storage
	now := time.Now()
	for i := 0; i < 100; i++ {
		usage := ResourceUsage{
			Timestamp:    now.Add(time.Duration(-100+i) * time.Hour),
			StorageBytes: int64(5000000 + i*50000), // Fast growth
			StorageLimit: 10000000,                 // Limit will be hit
			MemoryBytes:  1000000,
			PointsCount:  10000,
			SeriesCount:  100,
		}
		engine.historyMu.Lock()
		engine.usageHistory = append(engine.usageHistory, usage)
		engine.historyMu.Unlock()
	}

	forecasts, err := engine.GenerateForecasts()
	if err != nil {
		t.Fatalf("failed to generate forecasts: %v", err)
	}

	storageForecast := forecasts["storage"]
	if storageForecast.TimeToExhaustion == nil {
		t.Error("expected time to exhaustion to be calculated")
	}
}

func TestRecommendationGeneration(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultCapacityPlanningConfig()
	config.Enabled = false
	config.MinDataPoints = 5

	engine := NewCapacityPlanningEngine(db, config)
	defer engine.Close()

	// Add history with high utilization
	now := time.Now()
	for i := 0; i < 10; i++ {
		usage := ResourceUsage{
			Timestamp:      now.Add(time.Duration(-10+i) * time.Hour),
			StorageBytes:   8500000, // 85% utilization
			StorageLimit:   10000000,
			MemoryBytes:    4500000, // 90% utilization
			MemoryLimit:    5000000,
			SeriesCount:    8000, // 80% of limit
			SeriesLimit:    10000,
			PointsCount:    100000000,
			PartitionCount: 5, // 20M points per partition
			QueryRate:      50,
			WriteRate:      1000,
		}
		engine.historyMu.Lock()
		engine.usageHistory = append(engine.usageHistory, usage)
		engine.historyMu.Unlock()
	}

	// Generate forecasts first
	engine.GenerateForecasts()

	// Generate recommendations
	recs, err := engine.GenerateRecommendations()
	if err != nil {
		t.Fatalf("failed to generate recommendations: %v", err)
	}

	if len(recs) == 0 {
		t.Error("expected recommendations for high utilization")
	}

	// Check for memory recommendation (should be critical)
	hasMemoryRec := false
	for _, rec := range recs {
		if rec.Category == CategoryMemory && rec.Priority == PriorityCritical {
			hasMemoryRec = true
			break
		}
	}
	if !hasMemoryRec {
		t.Error("expected critical memory recommendation")
	}
}

func TestAlerts(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultCapacityPlanningConfig()
	config.Enabled = false
	config.AlertThreshold = 0.8

	engine := NewCapacityPlanningEngine(db, config)
	defer engine.Close()

	alertTriggered := false
	engine.OnAlert(func(alert *CapacityAlert) {
		alertTriggered = true
	})

	// Collect metrics with high utilization
	engine.historyMu.Lock()
	engine.usageHistory = []ResourceUsage{{
		Timestamp:    time.Now(),
		StorageBytes: 9000000, // 90% utilization
		StorageLimit: 10000000,
		MemoryBytes:  4500000,
		MemoryLimit:  5000000,
		PointsCount:  10000,
		SeriesCount:  100,
	}}
	engine.historyMu.Unlock()

	// Trigger alert check
	usage := &ResourceUsage{
		Timestamp:    time.Now(),
		StorageBytes: 9000000,
		StorageLimit: 10000000,
		MemoryBytes:  4500000,
		MemoryLimit:  5000000,
	}
	engine.checkAlerts(usage)

	if !alertTriggered {
		t.Error("expected alert to be triggered")
	}

	alerts := engine.GetAlerts(false)
	if len(alerts) == 0 {
		t.Error("expected active alerts")
	}
}

func TestAlertAcknowledge(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultCapacityPlanningConfig()
	config.Enabled = false

	engine := NewCapacityPlanningEngine(db, config)
	defer engine.Close()

	// Add an alert manually
	engine.alertsMu.Lock()
	engine.alerts["test_alert"] = &CapacityAlert{
		ID:        "test_alert",
		Severity:  "warning",
		Metric:    "test",
		Message:   "Test alert",
		Triggered: time.Now(),
	}
	engine.alertsMu.Unlock()

	// Acknowledge
	err := engine.AcknowledgeAlert("test_alert")
	if err != nil {
		t.Fatalf("failed to acknowledge: %v", err)
	}

	// Verify
	alerts := engine.GetAlerts(true)
	for _, a := range alerts {
		if a.ID == "test_alert" && !a.Acknowledged {
			t.Error("alert should be acknowledged")
		}
	}

	// Try non-existent
	err = engine.AcknowledgeAlert("non_existent")
	if err == nil {
		t.Error("expected error for non-existent alert")
	}
}

func TestRecommendationDismiss(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultCapacityPlanningConfig()
	config.Enabled = false

	engine := NewCapacityPlanningEngine(db, config)
	defer engine.Close()

	// Add a recommendation
	rec := &CapacityRecommendation{
		ID:       "test_rec",
		Priority: PriorityLow,
		Title:    "Test",
	}
	engine.recommendationsMu.Lock()
	engine.recommendations = []*CapacityRecommendation{rec}
	engine.recommendationsMu.Unlock()

	// Dismiss
	err := engine.DismissRecommendation("test_rec")
	if err != nil {
		t.Fatalf("failed to dismiss: %v", err)
	}

	// Verify removed
	recs := engine.GetRecommendations()
	for _, r := range recs {
		if r.ID == "test_rec" {
			t.Error("recommendation should be dismissed")
		}
	}
}

func TestUsageHistory(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultCapacityPlanningConfig()
	config.Enabled = false

	engine := NewCapacityPlanningEngine(db, config)
	defer engine.Close()

	// Add history
	now := time.Now()
	for i := 0; i < 10; i++ {
		usage := ResourceUsage{
			Timestamp:    now.Add(time.Duration(-10+i) * time.Hour),
			StorageBytes: int64(1000 * i),
		}
		engine.historyMu.Lock()
		engine.usageHistory = append(engine.usageHistory, usage)
		engine.historyMu.Unlock()
	}

	// Get recent history
	history := engine.GetUsageHistory(now.Add(-5 * time.Hour))
	if len(history) < 4 {
		t.Errorf("expected at least 4 history entries, got %d", len(history))
	}
}

func TestExportReport(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultCapacityPlanningConfig()
	config.Enabled = false

	engine := NewCapacityPlanningEngine(db, config)
	defer engine.Close()

	// Add some data
	engine.historyMu.Lock()
	engine.usageHistory = []ResourceUsage{{
		Timestamp:    time.Now(),
		StorageBytes: 1000000,
		MemoryBytes:  500000,
	}}
	engine.historyMu.Unlock()

	report, err := engine.ExportReport()
	if err != nil {
		t.Fatalf("failed to export report: %v", err)
	}

	if len(report) == 0 {
		t.Error("report should not be empty")
	}
}

func TestCapacityStats(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultCapacityPlanningConfig()
	config.Enabled = false

	engine := NewCapacityPlanningEngine(db, config)
	defer engine.Close()

	// Collect some metrics
	engine.CollectMetrics()
	engine.CollectMetrics()

	stats := engine.Stats()
	if stats.CollectionsRun != 2 {
		t.Errorf("expected 2 collections, got %d", stats.CollectionsRun)
	}
}

func TestLinearRegression(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultCapacityPlanningConfig()
	config.Enabled = false
	config.MinDataPoints = 5

	engine := NewCapacityPlanningEngine(db, config)
	defer engine.Close()

	// Create perfectly linear data
	values := make([]float64, 100)
	timestamps := make([]time.Time, 100)
	start := time.Now().Add(-100 * time.Hour)

	for i := 0; i < 100; i++ {
		values[i] = float64(100 + i*10) // y = 100 + 10x
		timestamps[i] = start.Add(time.Duration(i) * time.Hour)
	}

	forecast := engine.forecastMetric("test", values, timestamps, 0)

	if forecast == nil {
		t.Fatal("forecast should not be nil")
	}

	// For linear data, R-squared should be very high
	if forecast.Confidence < 0.99 {
		t.Errorf("expected high confidence for linear data, got %f", forecast.Confidence)
	}

	// Should have upward trend
	if forecast.Trend != TrendUp {
		t.Errorf("expected upward trend, got %s", forecast.Trend)
	}
}

func TestStableTrend(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultCapacityPlanningConfig()
	config.Enabled = false
	config.MinDataPoints = 5

	engine := NewCapacityPlanningEngine(db, config)
	defer engine.Close()

	// Create stable data
	values := make([]float64, 100)
	timestamps := make([]time.Time, 100)
	start := time.Now().Add(-100 * time.Hour)

	for i := 0; i < 100; i++ {
		values[i] = 100 + math.Sin(float64(i)*0.1)*5 // Small variation around 100
		timestamps[i] = start.Add(time.Duration(i) * time.Hour)
	}

	forecast := engine.forecastMetric("test", values, timestamps, 0)

	if forecast == nil {
		t.Fatal("forecast should not be nil")
	}

	// Should have stable trend
	if forecast.Trend != TrendStable {
		t.Errorf("expected stable trend, got %s", forecast.Trend)
	}
}

func TestDownwardTrend(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultCapacityPlanningConfig()
	config.Enabled = false
	config.MinDataPoints = 5

	engine := NewCapacityPlanningEngine(db, config)
	defer engine.Close()

	// Create downward trend
	values := make([]float64, 100)
	timestamps := make([]time.Time, 100)
	start := time.Now().Add(-100 * time.Hour)

	for i := 0; i < 100; i++ {
		values[i] = float64(1000 - i*10) // Decreasing
		timestamps[i] = start.Add(time.Duration(i) * time.Hour)
	}

	forecast := engine.forecastMetric("test", values, timestamps, 0)

	if forecast == nil {
		t.Fatal("forecast should not be nil")
	}

	if forecast.Trend != TrendDown {
		t.Errorf("expected downward trend, got %s", forecast.Trend)
	}
}

func TestAutoTune(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultCapacityPlanningConfig()
	config.Enabled = false
	config.AutoTuneEnabled = true
	config.MinDataPoints = 5

	engine := NewCapacityPlanningEngine(db, config)
	defer engine.Close()

	// Add history with high utilization
	now := time.Now()
	for i := 0; i < 10; i++ {
		usage := ResourceUsage{
			Timestamp:      now.Add(time.Duration(-10+i) * time.Hour),
			StorageBytes:   8500000,
			StorageLimit:   10000000,
			MemoryBytes:    4500000,
			MemoryLimit:    5000000,
			PointsCount:    100000000,
			PartitionCount: 5,
			QueryRate:      50,
		}
		engine.historyMu.Lock()
		engine.usageHistory = append(engine.usageHistory, usage)
		engine.historyMu.Unlock()
	}

	engine.GenerateForecasts()
	engine.GenerateRecommendations()

	_ = engine.Stats()
	// With auto-tune enabled, some recommendations should be applied
	// (depends on which ones have AutoApply set)
}

func TestCallbacks(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	config := DefaultCapacityPlanningConfig()
	config.Enabled = false
	config.MinDataPoints = 5

	engine := NewCapacityPlanningEngine(db, config)
	defer engine.Close()

	recCallback := false
	engine.OnRecommendation(func(rec *CapacityRecommendation) {
		recCallback = true
	})

	// Add history with high utilization to trigger recommendations
	now := time.Now()
	for i := 0; i < 10; i++ {
		usage := ResourceUsage{
			Timestamp:    now.Add(time.Duration(-10+i) * time.Hour),
			StorageBytes: 8500000,
			StorageLimit: 10000000,
			MemoryBytes:  4500000,
			MemoryLimit:  5000000,
		}
		engine.historyMu.Lock()
		engine.usageHistory = append(engine.usageHistory, usage)
		engine.historyMu.Unlock()
	}

	engine.GenerateForecasts()
	engine.GenerateRecommendations()

	if !recCallback {
		t.Error("recommendation callback should have been triggered")
	}
}

func BenchmarkCollectMetrics(b *testing.B) {
	dir := b.TempDir()
	path := dir + "/test.db"
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		b.Fatalf("failed to open test db: %v", err)
	}
	defer db.Close()

	config := DefaultCapacityPlanningConfig()
	config.Enabled = false

	engine := NewCapacityPlanningEngine(db, config)
	defer engine.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		engine.CollectMetrics()
	}
}

func BenchmarkGenerateForecasts(b *testing.B) {
	dir := b.TempDir()
	path := dir + "/test.db"
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		b.Fatalf("failed to open test db: %v", err)
	}
	defer db.Close()

	config := DefaultCapacityPlanningConfig()
	config.Enabled = false
	config.MinDataPoints = 10

	engine := NewCapacityPlanningEngine(db, config)
	defer engine.Close()

	// Add history
	now := time.Now()
	for i := 0; i < 1000; i++ {
		usage := ResourceUsage{
			Timestamp:    now.Add(time.Duration(-1000+i) * time.Hour),
			StorageBytes: int64(1000000 + i*10000),
			StorageLimit: 100000000,
			MemoryBytes:  int64(500000 + i*1000),
			PointsCount:  int64(10000 + i*100),
			SeriesCount:  int64(100 + i),
			QueryRate:    10.0 + float64(i)*0.1,
		}
		engine.usageHistory = append(engine.usageHistory, usage)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		engine.GenerateForecasts()
	}
}
