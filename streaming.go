package chronicle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// StreamConfig configures the streaming API.
type StreamConfig struct {
	// Enabled turns on WebSocket streaming
	Enabled bool
	// BufferSize is the channel buffer size per subscription
	BufferSize int
	// PingInterval is how often to ping clients
	PingInterval time.Duration
	// WriteTimeout for WebSocket writes
	WriteTimeout time.Duration
	// AllowedOrigins is the list of allowed WebSocket origins.
	// If empty, only same-origin requests are allowed.
	AllowedOrigins []string
}

// DefaultStreamConfig returns default streaming configuration.
func DefaultStreamConfig() StreamConfig {
	return StreamConfig{
		Enabled:      true,
		BufferSize:   1000,
		PingInterval: 30 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
}

// Subscription represents an active stream subscription.
type Subscription struct {
	ID      string
	Metric  string
	Tags    map[string]string
	ch      chan Point
	done    chan struct{}
	closed  bool
	mu      sync.Mutex
	created time.Time
}

// C returns the channel for receiving points.
func (s *Subscription) C() <-chan Point {
	return s.ch
}

// Close closes the subscription.
func (s *Subscription) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	close(s.done)
	close(s.ch)
}

// StreamHub manages real-time subscriptions.
type StreamHub struct {
	db     *DB
	config StreamConfig
	mu     sync.RWMutex
	subs   map[string]*Subscription
	nextID uint64
}

// NewStreamHub creates a new streaming hub.
func NewStreamHub(db *DB, cfg StreamConfig) *StreamHub {
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 1000
	}
	return &StreamHub{
		db:     db,
		config: cfg,
		subs:   make(map[string]*Subscription),
	}
}

// Subscribe creates a new subscription for a metric pattern.
func (h *StreamHub) Subscribe(metric string, tags map[string]string) *Subscription {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.nextID++
	id := fmt.Sprintf("sub-%d", h.nextID)

	sub := &Subscription{
		ID:      id,
		Metric:  metric,
		Tags:    tags,
		ch:      make(chan Point, h.config.BufferSize),
		done:    make(chan struct{}),
		created: time.Now(),
	}

	h.subs[id] = sub
	return sub
}

// Unsubscribe removes a subscription.
func (h *StreamHub) Unsubscribe(id string) {
	h.mu.Lock()
	sub, ok := h.subs[id]
	if ok {
		delete(h.subs, id)
	}
	h.mu.Unlock()

	if ok {
		sub.Close()
	}
}

// Publish sends a point to all matching subscriptions.
func (h *StreamHub) Publish(p Point) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for _, sub := range h.subs {
		if !h.matches(sub, p) {
			continue
		}

		select {
		case sub.ch <- p:
		default:
			// Buffer full, drop the point
		}
	}
}

// PublishBatch sends multiple points to matching subscriptions.
func (h *StreamHub) PublishBatch(points []Point) {
	for _, p := range points {
		h.Publish(p)
	}
}

// matches checks if a point matches a subscription filter.
func (h *StreamHub) matches(sub *Subscription, p Point) bool {
	if sub.Metric != "" && sub.Metric != p.Metric {
		return false
	}
	for k, v := range sub.Tags {
		if p.Tags[k] != v {
			return false
		}
	}
	return true
}

// Count returns the number of active subscriptions.
func (h *StreamHub) Count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.subs)
}

// List returns all active subscription IDs.
func (h *StreamHub) List() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	ids := make([]string, 0, len(h.subs))
	for id := range h.subs {
		ids = append(ids, id)
	}
	return ids
}

// WebSocket handling

// newUpgrader creates a WebSocket upgrader that validates the request origin
// against the configured AllowedOrigins list. If no origins are configured,
// only same-origin requests are permitted (the default gorilla/websocket behavior).
func newUpgrader(allowedOrigins []string) *websocket.Upgrader {
	return &websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			if len(allowedOrigins) == 0 {
				return false
			}
			origin := r.Header.Get("Origin")
			if origin == "" {
				return true // non-browser client
			}
			for _, allowed := range allowedOrigins {
				if allowed == "*" || allowed == origin {
					return true
				}
			}
			return false
		},
	}
}

// StreamMessage is the JSON format for WebSocket messages.
type StreamMessage struct {
	Type   string            `json:"type"`
	Metric string            `json:"metric,omitempty"`
	Tags   map[string]string `json:"tags,omitempty"`
	Point  *Point            `json:"point,omitempty"`
	SubID  string            `json:"sub_id,omitempty"`
	Error  string            `json:"error,omitempty"`
}

// WebSocketHandler returns an HTTP handler for WebSocket connections.
func (h *StreamHub) WebSocketHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		up := newUpgrader(h.config.AllowedOrigins)
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		defer func() { _ = conn.Close() }() //nolint:errcheck // best-effort stream operation

		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()

		// Map of active subscriptions for this connection
		connSubs := make(map[string]*Subscription)
		var connMu sync.Mutex

		// Read commands from client
		var readerWg sync.WaitGroup
		readerWg.Add(1)
		go func() {
			defer readerWg.Done()
			defer cancel()
			for {
				_, msg, err := conn.ReadMessage()
				if err != nil {
					return
				}

				var cmd StreamMessage
				if err := json.Unmarshal(msg, &cmd); err != nil {
					h.sendError(conn, "invalid message format")
					continue
				}

				switch cmd.Type {
				case "subscribe":
					sub := h.Subscribe(cmd.Metric, cmd.Tags)
					connMu.Lock()
					connSubs[sub.ID] = sub
					connMu.Unlock()

					resp, _ := json.Marshal(StreamMessage{
						Type:  "subscribed",
						SubID: sub.ID,
					})
					if err := conn.WriteMessage(websocket.TextMessage, resp); err != nil {
						return
					}

					// Start forwarding points for this subscription
					go h.forwardPoints(ctx, conn, sub)

				case "unsubscribe":
					connMu.Lock()
					if sub, ok := connSubs[cmd.SubID]; ok {
						delete(connSubs, cmd.SubID)
						h.Unsubscribe(sub.ID)
					}
					connMu.Unlock()

					resp, _ := json.Marshal(StreamMessage{
						Type:  "unsubscribed",
						SubID: cmd.SubID,
					})
					if err := conn.WriteMessage(websocket.TextMessage, resp); err != nil {
						return
					}

				default:
					h.sendError(conn, "unknown command: "+cmd.Type)
				}
			}
		}()

		// Wait for context cancellation
		<-ctx.Done()

		// Wait for reader goroutine to finish
		readerWg.Wait()

		// Cleanup subscriptions
		connMu.Lock()
		for _, sub := range connSubs {
			h.Unsubscribe(sub.ID)
		}
		connMu.Unlock()
	}
}

func (h *StreamHub) forwardPoints(ctx context.Context, conn *websocket.Conn, sub *Subscription) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-sub.done:
			return
		case p, ok := <-sub.ch:
			if !ok {
				return
			}
			msg, _ := json.Marshal(StreamMessage{
				Type:  "point",
				SubID: sub.ID,
				Point: &p,
			})
			if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		}
	}
}

func (h *StreamHub) sendError(conn *websocket.Conn, msg string) {
	resp, _ := json.Marshal(StreamMessage{
		Type:  "error",
		Error: msg,
	})
	if err := conn.WriteMessage(websocket.TextMessage, resp); err != nil {
		_ = conn.Close() //nolint:errcheck // best-effort stream operation
	}
}

// StreamingDB wraps a DB with streaming capabilities.
type StreamingDB struct {
	*DB
	hub *StreamHub
}

// NewStreamingDB creates a streaming-enabled database wrapper.
func NewStreamingDB(db *DB, cfg StreamConfig) *StreamingDB {
	return &StreamingDB{
		DB:  db,
		hub: NewStreamHub(db, cfg),
	}
}

// Write writes a point and publishes to subscribers.
func (s *StreamingDB) Write(p Point) error {
	if err := s.DB.Write(p); err != nil {
		return err
	}
	s.hub.Publish(p)
	return nil
}

// WriteBatch writes points and publishes to subscribers.
func (s *StreamingDB) WriteBatch(points []Point) error {
	if err := s.DB.WriteBatch(points); err != nil {
		return err
	}
	s.hub.PublishBatch(points)
	return nil
}

// Subscribe creates a subscription for real-time updates.
func (s *StreamingDB) Subscribe(metric string, tags map[string]string) *Subscription {
	return s.hub.Subscribe(metric, tags)
}

// Hub returns the underlying stream hub.
func (s *StreamingDB) Hub() *StreamHub {
	return s.hub
}

// ErrStreamingDisabled is returned when streaming operations are attempted on a non-streaming DB.
var ErrStreamingDisabled = errors.New("streaming not enabled")
