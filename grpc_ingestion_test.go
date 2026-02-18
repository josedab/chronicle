package chronicle

import (
	"context"
	"testing"
	"time"
)

func TestGRPCIngestionEngine(t *testing.T) {
	db, err := Open(t.TempDir()+"/test.db", DefaultConfig(t.TempDir()+"/test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	engine := NewGRPCIngestionEngine(db, DefaultGRPCIngestionConfig())

	t.Run("write and query", func(t *testing.T) {
		req := &WriteRequest{
			TimeSeries: []ProtoTimeSeries{
				{
					Labels: []ProtoLabel{
						{Name: "__name__", Value: "cpu_usage"},
						{Name: "host", Value: "server1"},
					},
					Samples: []ProtoSample{
						{Value: 42.5, Timestamp: time.Now().UnixNano()},
						{Value: 43.0, Timestamp: time.Now().UnixNano() + 1},
					},
				},
			},
		}

		ctx := context.Background()
		if err := engine.HandleWrite(ctx, req); err != nil {
			t.Fatalf("HandleWrite: %v", err)
		}

		stats := engine.Stats()
		if stats.TotalWriteRequests != 1 {
			t.Errorf("expected 1 write request, got %d", stats.TotalWriteRequests)
		}
		if stats.TotalPointsWritten != 2 {
			t.Errorf("expected 2 points written, got %d", stats.TotalPointsWritten)
		}

		db.Flush()

		qReq := &QueryRequest{
			Metric:    "cpu_usage",
			Labels:    map[string]string{"host": "server1"},
			StartTime: 0,
			EndTime:   time.Now().UnixNano() + 1000,
		}

		resp, err := engine.HandleQuery(ctx, qReq)
		if err != nil {
			t.Fatalf("HandleQuery: %v", err)
		}
		if resp.PointCount < 1 {
			t.Errorf("expected points in response, got %d", resp.PointCount)
		}
		if resp.QueryTimeMs < 0 {
			t.Error("query time should be non-negative")
		}

		stats = engine.Stats()
		if stats.TotalQueryRequests != 1 {
			t.Errorf("expected 1 query request, got %d", stats.TotalQueryRequests)
		}
	})

	t.Run("nil request", func(t *testing.T) {
		ctx := context.Background()
		if err := engine.HandleWrite(ctx, nil); err == nil {
			t.Error("expected error for nil write request")
		}
		if _, err := engine.HandleQuery(ctx, nil); err == nil {
			t.Error("expected error for nil query request")
		}
	})

	t.Run("empty metric skipped", func(t *testing.T) {
		req := &WriteRequest{
			TimeSeries: []ProtoTimeSeries{
				{
					Labels:  []ProtoLabel{{Name: "host", Value: "x"}},
					Samples: []ProtoSample{{Value: 1.0, Timestamp: 1}},
				},
			},
		}
		if err := engine.HandleWrite(context.Background(), req); err != nil {
			t.Fatalf("HandleWrite: %v", err)
		}
	})

	t.Run("services reflection", func(t *testing.T) {
		services := engine.Services()
		if len(services) != 4 {
			t.Errorf("expected 4 services, got %d", len(services))
		}
		found := false
		for _, s := range services {
			if s.Name == "chronicle.WriteService" {
				found = true
				if len(s.Methods) != 3 {
					t.Errorf("expected 3 methods, got %d", len(s.Methods))
				}
			}
		}
		if !found {
			t.Error("WriteService not found")
		}
	})

	t.Run("interceptor", func(t *testing.T) {
		called := false
		engine.AddInterceptor(func(ctx context.Context, method string, req interface{}) (interface{}, error) {
			called = true
			return nil, nil
		})
		if len(engine.unaryInterceptors) != 1 {
			t.Errorf("expected 1 interceptor, got %d", len(engine.unaryInterceptors))
		}
		_ = called
	})

	t.Run("query with aggregation", func(t *testing.T) {
		now := time.Now().UnixNano()
		req := &WriteRequest{
			TimeSeries: []ProtoTimeSeries{
				{
					Labels: []ProtoLabel{{Name: "__name__", Value: "grpc_agg_test"}},
					Samples: []ProtoSample{
						{Value: 10, Timestamp: now},
						{Value: 20, Timestamp: now + 1},
						{Value: 30, Timestamp: now + 2},
					},
				},
			},
		}
		ctx := context.Background()
		if err := engine.HandleWrite(ctx, req); err != nil {
			t.Fatal(err)
		}
		db.Flush()

		qReq := &QueryRequest{
			Metric:    "grpc_agg_test",
			StartTime: 0,
			EndTime:   now + 1000,
			Aggregate: "sum",
		}
		resp, err := engine.HandleQuery(ctx, qReq)
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.Series) == 0 {
			t.Error("expected series in response")
		}
	})

	t.Run("config defaults", func(t *testing.T) {
		cfg := DefaultGRPCIngestionConfig()
		if cfg.ListenAddr != ":9095" {
			t.Errorf("unexpected listen addr: %s", cfg.ListenAddr)
		}
		if cfg.MaxMessageSize != 16*1024*1024 {
			t.Errorf("unexpected max message size: %d", cfg.MaxMessageSize)
		}
		if cfg.MaxConcurrent != 100 {
			t.Errorf("unexpected max concurrent: %d", cfg.MaxConcurrent)
		}
	})
}
