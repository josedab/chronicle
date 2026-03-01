package chronicle

import (
	"context"
	"testing"
	"time"
)

func newTestGRPCEngine(t *testing.T) *GRPCIngestionEngine {
	t.Helper()
	db := setupTestDB(t)
	cfg := DefaultGRPCIngestionConfig()
	cfg.ListenAddr = "" // don't bind
	return NewGRPCIngestionEngine(db, cfg)
}

func TestGRPCHandleWriteValidPoints(t *testing.T) {
	g := newTestGRPCEngine(t)
	ctx := context.Background()

	req := &WriteRequest{
		TimeSeries: []ProtoTimeSeries{
			{
				Labels: []ProtoLabel{
					{Name: "__name__", Value: "cpu_usage"},
					{Name: "host", Value: "server1"},
				},
				Samples: []ProtoSample{
					{Value: 42.5, Timestamp: time.Now().UnixNano()},
					{Value: 43.0, Timestamp: time.Now().Add(time.Second).UnixNano()},
				},
			},
		},
	}

	err := g.HandleWrite(ctx, req)
	if err != nil {
		t.Fatalf("HandleWrite failed: %v", err)
	}

	stats := g.Stats()
	if stats.TotalWriteRequests != 1 {
		t.Errorf("Expected 1 write request, got %d", stats.TotalWriteRequests)
	}
	if stats.TotalPointsWritten != 2 {
		t.Errorf("Expected 2 points written, got %d", stats.TotalPointsWritten)
	}
}

func TestGRPCHandleWriteNilRequest(t *testing.T) {
	g := newTestGRPCEngine(t)
	ctx := context.Background()

	err := g.HandleWrite(ctx, nil)
	if err == nil {
		t.Error("Expected error for nil request")
	}
}

func TestGRPCHandleWriteEmptyMetric(t *testing.T) {
	g := newTestGRPCEngine(t)
	ctx := context.Background()

	req := &WriteRequest{
		TimeSeries: []ProtoTimeSeries{
			{
				Labels: []ProtoLabel{
					{Name: "host", Value: "server1"}, // no __name__
				},
				Samples: []ProtoSample{
					{Value: 1.0, Timestamp: time.Now().UnixNano()},
				},
			},
		},
	}

	err := g.HandleWrite(ctx, req)
	if err != nil {
		t.Fatalf("HandleWrite should succeed (skip unnamed series): %v", err)
	}

	stats := g.Stats()
	if stats.TotalPointsWritten != 0 {
		t.Errorf("Expected 0 points (skipped), got %d", stats.TotalPointsWritten)
	}
}

func TestGRPCHandleWriteBatch(t *testing.T) {
	g := newTestGRPCEngine(t)
	ctx := context.Background()

	// Build a batch of 1000+ points.
	samples := make([]ProtoSample, 1100)
	now := time.Now()
	for i := range samples {
		samples[i] = ProtoSample{
			Value:     float64(i),
			Timestamp: now.Add(time.Duration(i) * time.Millisecond).UnixNano(),
		}
	}

	req := &WriteRequest{
		TimeSeries: []ProtoTimeSeries{
			{
				Labels: []ProtoLabel{
					{Name: "__name__", Value: "batch_metric"},
				},
				Samples: samples,
			},
		},
	}

	err := g.HandleWrite(ctx, req)
	if err != nil {
		t.Fatalf("Batch write failed: %v", err)
	}

	stats := g.Stats()
	if stats.TotalPointsWritten != 1100 {
		t.Errorf("Expected 1100 points, got %d", stats.TotalPointsWritten)
	}
}

func TestGRPCHandleWriteMultipleSeries(t *testing.T) {
	g := newTestGRPCEngine(t)
	ctx := context.Background()

	req := &WriteRequest{
		TimeSeries: []ProtoTimeSeries{
			{
				Labels:  []ProtoLabel{{Name: "__name__", Value: "metric_a"}},
				Samples: []ProtoSample{{Value: 1.0, Timestamp: time.Now().UnixNano()}},
			},
			{
				Labels:  []ProtoLabel{{Name: "__name__", Value: "metric_b"}},
				Samples: []ProtoSample{{Value: 2.0, Timestamp: time.Now().UnixNano()}},
			},
			{
				Labels:  []ProtoLabel{{Name: "__name__", Value: "metric_c"}},
				Samples: []ProtoSample{{Value: 3.0, Timestamp: time.Now().UnixNano()}},
			},
		},
	}

	err := g.HandleWrite(ctx, req)
	if err != nil {
		t.Fatalf("Multi-series write failed: %v", err)
	}

	stats := g.Stats()
	if stats.TotalPointsWritten != 3 {
		t.Errorf("Expected 3 points, got %d", stats.TotalPointsWritten)
	}
}

func TestGRPCHandleQueryNilRequest(t *testing.T) {
	g := newTestGRPCEngine(t)
	ctx := context.Background()

	_, err := g.HandleQuery(ctx, nil)
	if err == nil {
		t.Error("Expected error for nil query request")
	}
}

func TestGRPCHandleQueryReturnsResults(t *testing.T) {
	g := newTestGRPCEngine(t)
	ctx := context.Background()

	// Write some data first.
	now := time.Now()
	writeReq := &WriteRequest{
		TimeSeries: []ProtoTimeSeries{
			{
				Labels: []ProtoLabel{{Name: "__name__", Value: "query_test"}},
				Samples: []ProtoSample{
					{Value: 10.0, Timestamp: now.UnixNano()},
					{Value: 20.0, Timestamp: now.Add(time.Second).UnixNano()},
				},
			},
		},
	}
	_ = g.HandleWrite(ctx, writeReq)
	_ = g.db.Flush()

	queryReq := &QueryRequest{
		Metric: "query_test",
	}

	resp, err := g.HandleQuery(ctx, queryReq)
	if err != nil {
		t.Fatalf("HandleQuery failed: %v", err)
	}
	if resp == nil {
		t.Fatal("Expected non-nil response")
	}
	// Verify stats were updated.
	stats := g.Stats()
	if stats.TotalQueryRequests != 1 {
		t.Errorf("Expected 1 query request, got %d", stats.TotalQueryRequests)
	}
}

func TestGRPCHandleQueryWithAggregation(t *testing.T) {
	g := newTestGRPCEngine(t)
	ctx := context.Background()

	// Write data.
	now := time.Now()
	writeReq := &WriteRequest{
		TimeSeries: []ProtoTimeSeries{
			{
				Labels: []ProtoLabel{{Name: "__name__", Value: "agg_test"}},
				Samples: []ProtoSample{
					{Value: 10.0, Timestamp: now.UnixNano()},
					{Value: 30.0, Timestamp: now.Add(time.Second).UnixNano()},
				},
			},
		},
	}
	_ = g.HandleWrite(ctx, writeReq)
	_ = g.db.Flush()

	queryReq := &QueryRequest{
		Metric:    "agg_test",
		Aggregate: "avg",
	}

	resp, err := g.HandleQuery(ctx, queryReq)
	if err != nil {
		t.Fatalf("HandleQuery with aggregation failed: %v", err)
	}
	if resp == nil {
		t.Fatal("Expected non-nil response")
	}
}

func TestGRPCStats(t *testing.T) {
	g := newTestGRPCEngine(t)

	stats := g.Stats()
	if stats.TotalWriteRequests != 0 {
		t.Errorf("Expected 0 write requests initially, got %d", stats.TotalWriteRequests)
	}
	if stats.TotalQueryRequests != 0 {
		t.Errorf("Expected 0 query requests initially, got %d", stats.TotalQueryRequests)
	}
}

func TestGRPCAddInterceptor(t *testing.T) {
	g := newTestGRPCEngine(t)

	interceptor := func(ctx context.Context, method string, req interface{}) (interface{}, error) {
		return req, nil
	}
	g.AddInterceptor(interceptor)

	g.mu.RLock()
	count := len(g.unaryInterceptors)
	g.mu.RUnlock()

	if count != 1 {
		t.Errorf("Expected 1 interceptor, got %d", count)
	}
}

func TestGRPCEncodeDecodeWriteRequest(t *testing.T) {
	req := &WriteRequest{
		TimeSeries: []ProtoTimeSeries{
			{
				Labels: []ProtoLabel{
					{Name: "__name__", Value: "test_metric"},
					{Name: "env", Value: "prod"},
				},
				Samples: []ProtoSample{
					{Value: 99.5, Timestamp: 1234567890},
				},
			},
		},
	}

	encoded := EncodeWriteRequest(req)
	if len(encoded) == 0 {
		t.Fatal("Encoded request is empty")
	}

	decoded, err := DecodeWriteRequest(encoded)
	if err != nil {
		t.Fatalf("DecodeWriteRequest failed: %v", err)
	}
	if len(decoded.TimeSeries) != 1 {
		t.Errorf("Expected 1 time series, got %d", len(decoded.TimeSeries))
	}
}
