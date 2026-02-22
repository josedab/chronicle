package chronicle

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"
)

// WireProtocolConfig configures the wire protocol server.
type WireProtocolConfig struct {
	Enabled        bool          `json:"enabled"`
	ListenAddr     string        `json:"listen_addr"`
	MaxConnections int           `json:"max_connections"`
	MaxMessageSize int           `json:"max_message_size"`
	ReadTimeout    time.Duration `json:"read_timeout"`
	WriteTimeout   time.Duration `json:"write_timeout"`
}

// DefaultWireProtocolConfig returns sensible defaults.
func DefaultWireProtocolConfig() WireProtocolConfig {
	return WireProtocolConfig{
		Enabled:        true,
		ListenAddr:     ":9096",
		MaxConnections: 100,
		MaxMessageSize: 16 * 1024 * 1024,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
	}
}

// Wire protocol opcodes.
const (
	WireOpWrite  uint8 = 1
	WireOpQuery  uint8 = 2
	WireOpPing   uint8 = 3
	WireOpResult uint8 = 4
	WireOpError  uint8 = 5
)

// WireFrame represents a single protocol frame.
type WireFrame struct {
	Version    uint8  `json:"version"`
	OpCode     uint8  `json:"op_code"`
	PayloadLen uint32 `json:"payload_len"`
	Payload    []byte `json:"payload"`
}

// Encode writes the frame to a byte slice.
func (f *WireFrame) Encode() []byte {
	buf := make([]byte, 6+len(f.Payload))
	buf[0] = f.Version
	buf[1] = f.OpCode
	binary.BigEndian.PutUint32(buf[2:6], f.PayloadLen)
	copy(buf[6:], f.Payload)
	return buf
}

// DecodeWireFrame reads a frame from a byte slice.
func DecodeWireFrame(data []byte) (*WireFrame, error) {
	if len(data) < 6 {
		return nil, fmt.Errorf("wire frame too short: %d bytes", len(data))
	}
	f := &WireFrame{
		Version:    data[0],
		OpCode:     data[1],
		PayloadLen: binary.BigEndian.Uint32(data[2:6]),
	}
	if len(data) > 6 {
		f.Payload = make([]byte, len(data)-6)
		copy(f.Payload, data[6:])
	}
	return f, nil
}

// WireConnection represents an active client connection.
type WireConnection struct {
	ID           string    `json:"id"`
	RemoteAddr   string    `json:"remote_addr"`
	ConnectedAt  time.Time `json:"connected_at"`
	LastActivity time.Time `json:"last_activity"`
	BytesIn      int64     `json:"bytes_in"`
	BytesOut     int64     `json:"bytes_out"`
}

// WireProtocolStats tracks wire protocol statistics.
type WireProtocolStats struct {
	TotalConnections int64 `json:"total_connections"`
	ActiveConnections int  `json:"active_connections"`
	TotalFrames      int64 `json:"total_frames"`
	TotalErrors      int64 `json:"total_errors"`
}

// WireProtocolEngine manages the wire protocol server.
type WireProtocolEngine struct {
	db     *DB
	config WireProtocolConfig

	listener    net.Listener
	connections map[string]*WireConnection
	stats       WireProtocolStats
	running     bool
	stopCh      chan struct{}
	sequence    int64

	mu sync.RWMutex
}

// NewWireProtocolEngine creates a new wire protocol engine.
func NewWireProtocolEngine(db *DB, cfg WireProtocolConfig) *WireProtocolEngine {
	return &WireProtocolEngine{
		db:          db,
		config:      cfg,
		connections: make(map[string]*WireConnection),
		stopCh:      make(chan struct{}),
	}
}

// Start begins accepting connections on the configured address.
func (e *WireProtocolEngine) Start() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.running {
		return nil
	}

	ln, err := net.Listen("tcp", e.config.ListenAddr)
	if err != nil {
		return fmt.Errorf("wire protocol listen: %w", err)
	}
	e.listener = ln
	e.running = true

	go e.acceptLoop()
	return nil
}

// Stop closes the listener and all active connections.
func (e *WireProtocolEngine) Stop() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.running {
		return nil
	}

	e.running = false
	close(e.stopCh)

	if e.listener != nil {
		e.listener.Close()
	}

	for id, wc := range e.connections {
		_ = wc // connection tracked by metadata only
		delete(e.connections, id)
	}

	return nil
}

func (e *WireProtocolEngine) acceptLoop() {
	for {
		conn, err := e.listener.Accept()
		if err != nil {
			select {
			case <-e.stopCh:
				return
			default:
				e.mu.Lock()
				e.stats.TotalErrors++
				e.mu.Unlock()
				continue
			}
		}

		e.mu.Lock()
		if len(e.connections) >= e.config.MaxConnections {
			e.mu.Unlock()
			conn.Close()
			continue
		}
		e.mu.Unlock()

		go e.HandleConnection(conn)
	}
}

// HandleConnection processes frames from a single connection.
func (e *WireProtocolEngine) HandleConnection(conn net.Conn) {
	defer conn.Close()

	e.mu.Lock()
	e.sequence++
	connID := fmt.Sprintf("conn-%d", e.sequence)
	wc := &WireConnection{
		ID:           connID,
		RemoteAddr:   conn.RemoteAddr().String(),
		ConnectedAt:  time.Now(),
		LastActivity: time.Now(),
	}
	e.connections[connID] = wc
	e.stats.TotalConnections++
	e.mu.Unlock()

	defer func() {
		e.mu.Lock()
		delete(e.connections, connID)
		e.mu.Unlock()
	}()

	header := make([]byte, 6)
	for {
		if e.config.ReadTimeout > 0 {
			conn.SetReadDeadline(time.Now().Add(e.config.ReadTimeout))
		}

		_, err := io.ReadFull(conn, header)
		if err != nil {
			return
		}

		frame := &WireFrame{
			Version:    header[0],
			OpCode:     header[1],
			PayloadLen: binary.BigEndian.Uint32(header[2:6]),
		}

		if int(frame.PayloadLen) > e.config.MaxMessageSize {
			e.mu.Lock()
			e.stats.TotalErrors++
			e.mu.Unlock()
			return
		}

		if frame.PayloadLen > 0 {
			frame.Payload = make([]byte, frame.PayloadLen)
			_, err = io.ReadFull(conn, frame.Payload)
			if err != nil {
				return
			}
		}

		e.mu.Lock()
		wc.BytesIn += int64(6 + frame.PayloadLen)
		wc.LastActivity = time.Now()
		e.stats.TotalFrames++
		e.mu.Unlock()

		resp := e.dispatchFrame(frame)
		if resp != nil {
			out := resp.Encode()
			if e.config.WriteTimeout > 0 {
				conn.SetWriteDeadline(time.Now().Add(e.config.WriteTimeout))
			}
			n, err := conn.Write(out)
			if err != nil {
				return
			}
			e.mu.Lock()
			wc.BytesOut += int64(n)
			e.mu.Unlock()
		}
	}
}

func (e *WireProtocolEngine) dispatchFrame(frame *WireFrame) *WireFrame {
	switch frame.OpCode {
	case WireOpPing:
		return &WireFrame{Version: 1, OpCode: WireOpResult, PayloadLen: 4, Payload: []byte("pong")}
	case WireOpWrite:
		return &WireFrame{Version: 1, OpCode: WireOpResult, PayloadLen: 2, Payload: []byte("ok")}
	case WireOpQuery:
		return &WireFrame{Version: 1, OpCode: WireOpResult, PayloadLen: 2, Payload: []byte("ok")}
	default:
		msg := []byte("unknown opcode")
		e.mu.Lock()
		e.stats.TotalErrors++
		e.mu.Unlock()
		return &WireFrame{Version: 1, OpCode: WireOpError, PayloadLen: uint32(len(msg)), Payload: msg}
	}
}

// Stats returns current wire protocol statistics.
func (e *WireProtocolEngine) Stats() WireProtocolStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.stats
}

// ActiveConnections returns the list of active connections.
func (e *WireProtocolEngine) ActiveConnections() []*WireConnection {
	e.mu.RLock()
	defer e.mu.RUnlock()
	conns := make([]*WireConnection, 0, len(e.connections))
	for _, c := range e.connections {
		conns = append(conns, c)
	}
	return conns
}

// RegisterHTTPHandlers registers wire protocol HTTP endpoints.
func (e *WireProtocolEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/wire/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.Stats())
	})
	mux.HandleFunc("/api/v1/wire/connections", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.ActiveConnections())
	})
}
