package chronicle

import (
	"context"
	"fmt"
	"sync"
)

// ---------------------------------------------------------------------------
// ETLStreamSource – reads from a StreamHub subscription
// ---------------------------------------------------------------------------

// ETLStreamSource is an ETL source that reads from a StreamHub subscription.
// It bridges the real-time streaming system with the ETL pipeline.
type ETLStreamSource struct {
	hub    *StreamHub
	sub    *Subscription
	metric string
	tags   map[string]string
	ctx    context.Context
	cancel context.CancelFunc
}

// NewETLStreamSource creates a source that subscribes to a StreamHub metric.
func NewETLStreamSource(hub *StreamHub, metric string, tags map[string]string) *ETLStreamSource {
	return &ETLStreamSource{
		hub:    hub,
		metric: metric,
		tags:   tags,
	}
}

// Open subscribes to the StreamHub.
func (s *ETLStreamSource) Open(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.sub = s.hub.Subscribe(s.metric, s.tags)
	return nil
}

// Read returns the next point from the subscription.
func (s *ETLStreamSource) Read(ctx context.Context) (*Point, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	case p, ok := <-s.sub.C():
		if !ok {
			return nil, fmt.Errorf("etl stream source: subscription closed")
		}
		return &p, nil
	}
}

// Close unsubscribes and cleans up.
func (s *ETLStreamSource) Close() error {
	if s.cancel != nil {
		s.cancel()
	}
	if s.sub != nil {
		s.hub.Unsubscribe(s.sub.ID)
	}
	return nil
}

// ---------------------------------------------------------------------------
// ETLStreamSink – publishes to a StreamHub
// ---------------------------------------------------------------------------

// ETLStreamSink is an ETL sink that publishes points to a StreamHub.
type ETLStreamSink struct {
	hub *StreamHub
	ctx context.Context
}

// NewETLStreamSink creates a sink that publishes points to a StreamHub.
func NewETLStreamSink(hub *StreamHub) *ETLStreamSink {
	return &ETLStreamSink{hub: hub}
}

func (s *ETLStreamSink) Open(ctx context.Context) error {
	s.ctx = ctx
	return nil
}

func (s *ETLStreamSink) Write(ctx context.Context, point *Point) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		s.hub.Publish(*point)
		return nil
	}
}

func (s *ETLStreamSink) Flush(_ context.Context) error { return nil }
func (s *ETLStreamSink) Close() error                  { return nil }

// ---------------------------------------------------------------------------
// ETLWriteHook – intercepts DB writes and feeds ETL pipelines
// ---------------------------------------------------------------------------

// ETLWriteHook wraps a DB to intercept writes and feed them into ETL pipelines.
// This enables real-time ETL processing of all incoming data.
type ETLWriteHook struct {
	db       *DB
	registry *ETLRegistry
	hub      *StreamHub
	enabled  bool
	mu       sync.RWMutex
}

// NewETLWriteHook creates a hook that publishes writes to the given registry's
// StreamHub so that any subscribed ETL pipeline receives the data.
func NewETLWriteHook(db *DB, registry *ETLRegistry) *ETLWriteHook {
	return &ETLWriteHook{
		db:       db,
		registry: registry,
		hub:      NewStreamHub(db, DefaultStreamConfig()),
		enabled:  true,
	}
}

// Hub returns the underlying StreamHub used by the hook.
func (h *ETLWriteHook) Hub() *StreamHub { return h.hub }

// OnWrite is called after each point is written to the DB.
// It publishes to the StreamHub for any subscribed ETL pipelines.
func (h *ETLWriteHook) OnWrite(point *Point) {
	h.mu.RLock()
	ok := h.enabled
	h.mu.RUnlock()
	if ok {
		h.hub.Publish(*point)
	}
}

// OnWriteBatch handles batch writes.
func (h *ETLWriteHook) OnWriteBatch(points []Point) {
	h.mu.RLock()
	ok := h.enabled
	h.mu.RUnlock()
	if ok {
		h.hub.PublishBatch(points)
	}
}

// Enable enables the hook.
func (h *ETLWriteHook) Enable() {
	h.mu.Lock()
	h.enabled = true
	h.mu.Unlock()
}

// Disable disables the hook.
func (h *ETLWriteHook) Disable() {
	h.mu.Lock()
	h.enabled = false
	h.mu.Unlock()
}

// IsEnabled reports whether the hook is active.
func (h *ETLWriteHook) IsEnabled() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.enabled
}
