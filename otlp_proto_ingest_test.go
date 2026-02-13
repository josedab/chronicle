package chronicle

import (
	"testing"
)

func TestOTLPProtoEngine(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultOTLPProtoConfig()
	engine := NewOTLPProtoEngine(db, cfg)
	engine.Start()
	defer engine.Stop()

	t.Run("IngestMetrics", func(t *testing.T) {
		points := []ProtoMetricPoint{
			{Name: "cpu.usage", Value: 85.5, Timestamp: 1000, Labels: map[string]string{"host": "a"}, Type: "gauge"},
			{Name: "mem.used", Value: 1024, Timestamp: 1000, Labels: map[string]string{"host": "a"}, Type: "sum"},
		}
		result, err := engine.IngestMetrics(points)
		if err != nil {
			t.Fatalf("IngestMetrics failed: %v", err)
		}
		if result.PointsAccepted != 2 {
			t.Errorf("expected 2 accepted, got %d", result.PointsAccepted)
		}
		if result.PointsRejected != 0 {
			t.Errorf("expected 0 rejected, got %d", result.PointsRejected)
		}
	})

	t.Run("IngestMetricsWithHistogram", func(t *testing.T) {
		points := []ProtoMetricPoint{
			{Name: "http.latency", Value: 0.5, Timestamp: 1000, Type: "histogram"},
			{Name: "http.latency", Value: 1.0, Timestamp: 1001, Type: "histogram"},
		}
		result, err := engine.IngestMetrics(points)
		if err != nil {
			t.Fatalf("IngestMetrics failed: %v", err)
		}
		if result.HistogramsProcessed != 2 {
			t.Errorf("expected 2 histograms processed, got %d", result.HistogramsProcessed)
		}
	})

	t.Run("IngestHistograms", func(t *testing.T) {
		histograms := []ProtoHistogramPoint{
			{
				Name:      "request.duration",
				Buckets:   []ProtoBucket{{UpperBound: 0.1, Count: 10}, {UpperBound: 0.5, Count: 25}},
				Sum:       15.5,
				Count:     35,
				Timestamp: 2000,
				Labels:    map[string]string{"service": "api"},
			},
		}
		result, err := engine.IngestHistograms(histograms)
		if err != nil {
			t.Fatalf("IngestHistograms failed: %v", err)
		}
		if result.PointsAccepted != 1 {
			t.Errorf("expected 1 accepted, got %d", result.PointsAccepted)
		}
		if result.HistogramsProcessed != 1 {
			t.Errorf("expected 1 histogram processed, got %d", result.HistogramsProcessed)
		}
	})

	t.Run("ConvertToPoints", func(t *testing.T) {
		proto := []ProtoMetricPoint{
			{Name: "cpu.usage", Value: 42.0, Timestamp: 3000, Labels: map[string]string{"host": "b"}, Type: "gauge"},
			{Name: "disk.io", Value: 100, Timestamp: 3001, Labels: map[string]string{"dev": "sda"}, Type: "sum"},
		}
		points := engine.ConvertToPoints(proto)
		if len(points) != 2 {
			t.Fatalf("expected 2 points, got %d", len(points))
		}
		if points[0].Metric != "cpu.usage" {
			t.Errorf("expected metric cpu.usage, got %s", points[0].Metric)
		}
		if points[0].Tags["__type__"] != "gauge" {
			t.Errorf("expected type tag gauge, got %s", points[0].Tags["__type__"])
		}
		if points[0].Tags["host"] != "b" {
			t.Errorf("expected host tag b, got %s", points[0].Tags["host"])
		}
	})

	t.Run("EmptyBatch", func(t *testing.T) {
		result, err := engine.IngestMetrics(nil)
		if err != nil {
			t.Fatalf("IngestMetrics failed: %v", err)
		}
		if result.PointsAccepted != 0 {
			t.Errorf("expected 0 accepted, got %d", result.PointsAccepted)
		}
	})

	t.Run("RejectEmptyName", func(t *testing.T) {
		points := []ProtoMetricPoint{
			{Name: "", Value: 1.0, Timestamp: 1000, Type: "gauge"},
		}
		result, err := engine.IngestMetrics(points)
		if err != nil {
			t.Fatalf("IngestMetrics failed: %v", err)
		}
		if result.PointsRejected != 1 {
			t.Errorf("expected 1 rejected, got %d", result.PointsRejected)
		}
	})

	t.Run("MaxBatchSize", func(t *testing.T) {
		db2 := setupTestDB(t)
		defer db2.Close()
		smallCfg := DefaultOTLPProtoConfig()
		smallCfg.MaxBatchSize = 2
		e2 := NewOTLPProtoEngine(db2, smallCfg)
		e2.Start()
		defer e2.Stop()

		points := []ProtoMetricPoint{
			{Name: "m1", Value: 1, Timestamp: 1, Type: "gauge"},
			{Name: "m2", Value: 2, Timestamp: 2, Type: "gauge"},
			{Name: "m3", Value: 3, Timestamp: 3, Type: "gauge"},
		}
		result, _ := e2.IngestMetrics(points)
		if result.PointsAccepted != 2 {
			t.Errorf("expected 2 accepted, got %d", result.PointsAccepted)
		}
		if result.PointsRejected != 1 {
			t.Errorf("expected 1 rejected, got %d", result.PointsRejected)
		}
	})

	t.Run("Stats", func(t *testing.T) {
		stats := engine.GetStats()
		if stats.TotalIngested == 0 {
			t.Error("expected non-zero total ingested")
		}
		if stats.HistogramsProcessed == 0 {
			t.Error("expected non-zero histograms processed")
		}
	})
}
