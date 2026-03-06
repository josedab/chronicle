package chronicle

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestGrpcIngestionServer_Smoke(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}

func newGRPCTestEngine(t *testing.T) *GRPCIngestionEngine {
	t.Helper()
	dir := t.TempDir()
	db, err := Open(dir+"/grpc.db", DefaultConfig(dir+"/grpc.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	cfg := DefaultGRPCIngestionConfig()
	cfg.ListenAddr = ""
	engine := NewGRPCIngestionEngine(db, cfg)
	return engine
}

func TestGRPCRegisterHTTPHandlers_Write(t *testing.T) {
	engine := newGRPCTestEngine(t)
	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	req := WriteRequest{
		TimeSeries: []ProtoTimeSeries{{
			Labels:  []ProtoLabel{{Name: "__name__", Value: "cpu"}},
			Samples: []ProtoSample{{Value: 42.5, Timestamp: time.Now().UnixNano()}},
		}},
	}
	body, _ := json.Marshal(req)

	httpReq := httptest.NewRequest("POST", "/api/v1/grpc/write", bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httpReq)

	if rr.Code != http.StatusOK {
		t.Errorf("Write status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
}

func TestGRPCRegisterHTTPHandlers_Write_MethodNotAllowed(t *testing.T) {
	engine := newGRPCTestEngine(t)
	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	httpReq := httptest.NewRequest("GET", "/api/v1/grpc/write", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httpReq)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected 405, got %d", rr.Code)
	}
}

func TestGRPCRegisterHTTPHandlers_Query(t *testing.T) {
	engine := newGRPCTestEngine(t)
	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	qReq := QueryRequest{Metric: "cpu", StartTime: 0, EndTime: time.Now().UnixNano() + int64(time.Hour)}
	body, _ := json.Marshal(qReq)

	httpReq := httptest.NewRequest("POST", "/api/v1/grpc/query", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httpReq)

	if rr.Code != http.StatusOK {
		t.Errorf("Query status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
}

func TestGRPCRegisterHTTPHandlers_Services(t *testing.T) {
	engine := newGRPCTestEngine(t)
	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	httpReq := httptest.NewRequest("GET", "/api/v1/grpc/services", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httpReq)

	if rr.Code != http.StatusOK {
		t.Errorf("Services status = %d, want 200", rr.Code)
	}
}

func TestGRPCRegisterHTTPHandlers_Stats(t *testing.T) {
	engine := newGRPCTestEngine(t)
	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	httpReq := httptest.NewRequest("GET", "/api/v1/grpc/stats", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httpReq)

	if rr.Code != http.StatusOK {
		t.Errorf("Stats status = %d, want 200", rr.Code)
	}
}

func TestGRPCHandleProtobufFrame_Valid(t *testing.T) {
	engine := newGRPCTestEngine(t)

	req := &WriteRequest{
		TimeSeries: []ProtoTimeSeries{{
			Labels:  []ProtoLabel{{Name: "__name__", Value: "test_metric"}},
			Samples: []ProtoSample{{Value: 1.0, Timestamp: time.Now().UnixNano()}},
		}},
	}
	payload := EncodeWriteRequest(req)
	frame := encodeGRPCFrame(payload, false)

	ctx := context.Background()
	err := engine.handleProtobufFrame(ctx, frame)
	if err != nil {
		t.Fatalf("handleProtobufFrame: %v", err)
	}
}

func TestGRPCHandleProtobufFrame_TooShort(t *testing.T) {
	engine := newGRPCTestEngine(t)
	ctx := context.Background()

	err := engine.handleProtobufFrame(ctx, []byte{0x00, 0x01})
	if err == nil {
		t.Error("Expected error for short frame")
	}
}

func TestGRPCHandleProtobufFrame_IncompleteFrame(t *testing.T) {
	engine := newGRPCTestEngine(t)
	ctx := context.Background()

	data := make([]byte, 15)
	data[0] = 0x00
	binary.BigEndian.PutUint32(data[1:5], 100)

	err := engine.handleProtobufFrame(ctx, data)
	if err == nil {
		t.Error("Expected error for incomplete frame")
	}
}

func TestGRPCHandleOTLPHTTPMetrics(t *testing.T) {
	engine := newGRPCTestEngine(t)
	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	batch := map[string]any{"resource_metrics": []any{}}
	body, _ := json.Marshal(batch)

	httpReq := httptest.NewRequest("POST", "/api/v1/grpc/otlp-metrics", bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httpReq)

	if rr.Code != http.StatusOK {
		t.Errorf("OTLP HTTP status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
}

func TestGRPCHandleOTLPHTTPMetrics_BadJSON(t *testing.T) {
	engine := newGRPCTestEngine(t)
	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	httpReq := httptest.NewRequest("POST", "/api/v1/grpc/otlp-metrics", bytes.NewReader([]byte("not json")))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httpReq)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("Expected 400, got %d", rr.Code)
	}
}

func TestGRPCDecodeOTLPMetricsRequest_Empty(t *testing.T) {
	engine := newGRPCTestEngine(t)

	batch, err := engine.decodeOTLPMetricsRequest([]byte{})
	if err != nil {
		t.Fatalf("decodeOTLPMetricsRequest empty: %v", err)
	}
	if batch == nil {
		t.Error("Expected non-nil batch")
	}
}

func TestGRPCDecodeOTLPMetricsRequest_WithGRPCFrame(t *testing.T) {
	engine := newGRPCTestEngine(t)

	frame := encodeGRPCFrame([]byte{}, false)
	batch, err := engine.decodeOTLPMetricsRequest(frame)
	if err != nil {
		t.Fatalf("decodeOTLPMetricsRequest with frame: %v", err)
	}
	if batch == nil {
		t.Error("Expected non-nil batch")
	}
}

func TestGRPCHandleOTLPExport(t *testing.T) {
	engine := newGRPCTestEngine(t)
	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	// Send empty gRPC frame
	frame := encodeGRPCFrame([]byte{}, false)
	httpReq := httptest.NewRequest("POST",
		"/opentelemetry.proto.collector.metrics.v1.MetricsService/Export",
		bytes.NewReader(frame))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httpReq)

	if rr.Code != http.StatusOK {
		t.Errorf("OTLP Export status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
}
