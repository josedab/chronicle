package chronicle

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWireFrameEncodeDecode(t *testing.T) {
	original := &WireFrame{
		Version:    1,
		OpCode:     WireOpWrite,
		PayloadLen: 5,
		Payload:    []byte("hello"),
	}

	encoded := original.Encode()
	decoded, err := DecodeWireFrame(encoded)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if decoded.Version != original.Version {
		t.Errorf("version: got %d, want %d", decoded.Version, original.Version)
	}
	if decoded.OpCode != original.OpCode {
		t.Errorf("opcode: got %d, want %d", decoded.OpCode, original.OpCode)
	}
	if decoded.PayloadLen != original.PayloadLen {
		t.Errorf("payload len: got %d, want %d", decoded.PayloadLen, original.PayloadLen)
	}
	if string(decoded.Payload) != string(original.Payload) {
		t.Errorf("payload: got %q, want %q", decoded.Payload, original.Payload)
	}
}

func TestWireFrameDecodeTooShort(t *testing.T) {
	_, err := DecodeWireFrame([]byte{1, 2})
	if err == nil {
		t.Fatal("expected error for short frame")
	}
}

func TestWireProtocolEngineStartStop(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultWireProtocolConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	engine := NewWireProtocolEngine(db, cfg)

	if err := engine.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Idempotent start
	if err := engine.Start(); err != nil {
		t.Fatalf("second start: %v", err)
	}

	if err := engine.Stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}

	// Idempotent stop
	if err := engine.Stop(); err != nil {
		t.Fatalf("second stop: %v", err)
	}
}

func TestWireProtocolStats(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultWireProtocolConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	engine := NewWireProtocolEngine(db, cfg)

	stats := engine.Stats()
	if stats.TotalConnections != 0 {
		t.Errorf("initial total connections: got %d, want 0", stats.TotalConnections)
	}
	if stats.ActiveConnections != 0 {
		t.Errorf("initial active connections: got %d, want 0", stats.ActiveConnections)
	}
	if stats.TotalFrames != 0 {
		t.Errorf("initial total frames: got %d, want 0", stats.TotalFrames)
	}
}

func TestWireProtocolDispatch(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultWireProtocolConfig()
	engine := NewWireProtocolEngine(db, cfg)

	// Test ping dispatch
	ping := &WireFrame{Version: 1, OpCode: WireOpPing, PayloadLen: 0}
	resp := engine.dispatchFrame(ping)
	if resp == nil {
		t.Fatal("expected response for ping")
	}
	if resp.OpCode != WireOpResult {
		t.Errorf("ping response opcode: got %d, want %d", resp.OpCode, WireOpResult)
	}
	if string(resp.Payload) != "pong" {
		t.Errorf("ping response payload: got %q, want %q", resp.Payload, "pong")
	}

	// Test unknown opcode
	unknown := &WireFrame{Version: 1, OpCode: 99, PayloadLen: 0}
	resp = engine.dispatchFrame(unknown)
	if resp == nil {
		t.Fatal("expected response for unknown opcode")
	}
	if resp.OpCode != WireOpError {
		t.Errorf("unknown opcode response: got %d, want %d", resp.OpCode, WireOpError)
	}
}

func TestWireProtocolHTTPHandlers(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := DefaultWireProtocolConfig()
	engine := NewWireProtocolEngine(db, cfg)

	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	req := httptest.NewRequest("GET", "/api/v1/wire/stats", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("stats status: got %d, want %d", w.Code, http.StatusOK)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type: got %q, want %q", ct, "application/json")
	}
}

func TestWireProtocolDefaultConfig(t *testing.T) {
	cfg := DefaultWireProtocolConfig()
	if cfg.ListenAddr != ":9096" {
		t.Errorf("listen addr: got %q, want %q", cfg.ListenAddr, ":9096")
	}
	if cfg.MaxConnections != 100 {
		t.Errorf("max connections: got %d, want %d", cfg.MaxConnections, 100)
	}
	if cfg.MaxMessageSize != 16*1024*1024 {
		t.Errorf("max message size: got %d, want %d", cfg.MaxMessageSize, 16*1024*1024)
	}
}
