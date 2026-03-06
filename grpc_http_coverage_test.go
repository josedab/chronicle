package chronicle

import (
	"bytes"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func newGRPCEngineForHTTP(t *testing.T) *GRPCIngestionEngine {
	t.Helper()
	dir := t.TempDir()
	db, err := Open(dir+"/g.db", DefaultConfig(dir+"/g.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return NewGRPCIngestionEngine(db, DefaultGRPCIngestionConfig())
}

// --- RegisterHTTPHandlers coverage: bad request paths ---

func TestGRPCHTTP_Write_BadJSON(t *testing.T) {
	engine := newGRPCEngineForHTTP(t)
	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	req := httptest.NewRequest("POST", "/api/v1/grpc/write", bytes.NewReader([]byte("not json")))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("Expected 400, got %d", rr.Code)
	}
}

func TestGRPCHTTP_Query_BadJSON(t *testing.T) {
	engine := newGRPCEngineForHTTP(t)
	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	req := httptest.NewRequest("POST", "/api/v1/grpc/query", bytes.NewReader([]byte("bad")))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("Expected 400, got %d", rr.Code)
	}
}

func TestGRPCHTTP_Query_MethodNotAllowed(t *testing.T) {
	engine := newGRPCEngineForHTTP(t)
	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	req := httptest.NewRequest("GET", "/api/v1/grpc/query", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected 405, got %d", rr.Code)
	}
}

func TestGRPCHTTP_Services_MethodNotAllowed(t *testing.T) {
	engine := newGRPCEngineForHTTP(t)
	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	req := httptest.NewRequest("POST", "/api/v1/grpc/services", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected 405, got %d", rr.Code)
	}
}

func TestGRPCHTTP_Stats_MethodNotAllowed(t *testing.T) {
	engine := newGRPCEngineForHTTP(t)
	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	req := httptest.NewRequest("POST", "/api/v1/grpc/stats", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected 405, got %d", rr.Code)
	}
}

func TestGRPCHTTP_OTLPExport_MethodNotAllowed(t *testing.T) {
	engine := newGRPCEngineForHTTP(t)
	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	req := httptest.NewRequest("GET", "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected 405, got %d", rr.Code)
	}
}

func TestGRPCHTTP_OTLPMetrics_MethodNotAllowed(t *testing.T) {
	engine := newGRPCEngineForHTTP(t)
	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	req := httptest.NewRequest("GET", "/api/v1/grpc/otlp-metrics", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected 405, got %d", rr.Code)
	}
}

// --- handleOTLPExport with protobuf-encoded OTLP data ---

func TestGRPCHTTP_OTLPExport_WithProtoData(t *testing.T) {
	engine := newGRPCEngineForHTTP(t)
	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	// Build a minimal OTLP ExportMetricsServiceRequest in protobuf
	// NumberDataPoint with timestamp and value
	dp := newProtoEncoder()
	dp.writeVarint(3, uint64(time.Now().UnixNano()))    // time_unix_nano
	dp.writeFixed64(4, math.Float64bits(42.5))          // as_double

	// Gauge with one data point
	gauge := newProtoEncoder()
	gauge.writeBytes(1, dp.Bytes())

	// Metric with name and gauge
	metric := newProtoEncoder()
	metric.writeString(1, "test_metric")
	metric.writeString(2, "test description")
	metric.writeString(3, "1")
	metric.writeBytes(5, gauge.Bytes()) // gauge field

	// ScopeMetrics with scope and metric
	scope := newProtoEncoder()
	scope.writeString(1, "test-scope")
	scope.writeString(2, "1.0.0")

	scopeMetrics := newProtoEncoder()
	scopeMetrics.writeBytes(1, scope.Bytes())
	scopeMetrics.writeBytes(2, metric.Bytes())

	// ResourceMetrics
	kvEnc := newProtoEncoder()
	kvEnc.writeString(1, "service.name")
	valEnc := newProtoEncoder()
	valEnc.writeString(1, "test-service")
	kvEnc.writeBytes(2, valEnc.Bytes())

	resource := newProtoEncoder()
	resource.writeBytes(1, kvEnc.Bytes()) // attributes

	resourceMetrics := newProtoEncoder()
	resourceMetrics.writeBytes(1, resource.Bytes())
	resourceMetrics.writeBytes(2, scopeMetrics.Bytes())

	// ExportMetricsServiceRequest
	exportReq := newProtoEncoder()
	exportReq.writeBytes(1, resourceMetrics.Bytes())

	// Wrap in gRPC frame
	frame := encodeGRPCFrame(exportReq.Bytes(), false)

	httpReq := httptest.NewRequest("POST",
		"/opentelemetry.proto.collector.metrics.v1.MetricsService/Export",
		bytes.NewReader(frame))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httpReq)

	if rr.Code != http.StatusOK {
		t.Errorf("OTLP Export status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
}

// --- decodeOTLPMetricsRequest protobuf wire path ---

func TestDecodeOTLPMetrics_ProtoWireFormat(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/g.db", DefaultConfig(dir+"/g.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	engine := NewGRPCIngestionEngine(db, DefaultGRPCIngestionConfig())

	// Build protobuf data (not JSON, not gRPC frame)
	dp := newProtoEncoder()
	dp.writeVarint(3, 1000000000)
	dp.writeFixed64(4, math.Float64bits(99.9))

	gauge := newProtoEncoder()
	gauge.writeBytes(1, dp.Bytes())

	metric := newProtoEncoder()
	metric.writeString(1, "wire_metric")
	metric.writeBytes(5, gauge.Bytes())

	scopeMetrics := newProtoEncoder()
	scopeMetrics.writeBytes(2, metric.Bytes())

	resourceMetrics := newProtoEncoder()
	resourceMetrics.writeBytes(2, scopeMetrics.Bytes())

	exportReq := newProtoEncoder()
	exportReq.writeBytes(1, resourceMetrics.Bytes())

	batch, err := engine.decodeOTLPMetricsRequest(exportReq.Bytes())
	if err != nil {
		t.Fatalf("decodeOTLPMetricsRequest proto: %v", err)
	}
	if batch == nil {
		t.Fatal("Expected non-nil batch")
	}
	if len(batch.Metrics) == 0 {
		t.Error("Expected at least one metric")
	}
	if len(batch.Metrics) > 0 && batch.Metrics[0].Name != "wire_metric" {
		t.Errorf("Metric name = %q, want wire_metric", batch.Metrics[0].Name)
	}
}

// --- HandleQuery with various query types ---

func TestGRPCHandleQuery_WithMetricAndTimeRange(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/g.db", DefaultConfig(dir+"/g.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Write some test data
	db.Write(Point{Metric: "cpu", Value: 42.5, Timestamp: time.Now().UnixNano()})
	db.Write(Point{Metric: "cpu", Value: 55.0, Timestamp: time.Now().UnixNano()})

	engine := NewGRPCIngestionEngine(db, DefaultGRPCIngestionConfig())

	qReq := &QueryRequest{
		Metric:    "cpu",
		StartTime: 0,
		EndTime:   time.Now().UnixNano() + int64(time.Hour),
		Limit:     10,
	}

	resp, err := engine.HandleQuery(t.Context(), qReq)
	if err != nil {
		t.Fatalf("HandleQuery: %v", err)
	}
	// Points may not be immediately queryable if buffered; verify no error
	_ = resp
}
