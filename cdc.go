package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// CDCConfig configures the change data capture engine.
type CDCConfig struct {
	Enabled        bool
	BufferSize     int
	MaxSubscribers int
	FlushInterval  time.Duration
	IncludeMetrics []string // empty = all metrics
	ExcludeMetrics []string
}

// DefaultCDCConfig returns production-ready CDC defaults.
func DefaultCDCConfig() CDCConfig {
	return CDCConfig{
		Enabled:        true,
		BufferSize:     4096,
		MaxSubscribers: 64,
		FlushInterval:  100 * time.Millisecond,
	}
}

// CDCSubscription represents an active CDC subscriber.
type CDCSubscription struct {
	ID      string
	Filter  CDCFilter
	Events  chan ChangeEvent
	created time.Time
	cancel  context.CancelFunc
	closed  int32
}

// Close terminates the subscription.
func (s *CDCSubscription) Close() {
	if atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		s.cancel()
		close(s.Events)
	}
}

// CDCFilter allows subscribers to filter change events.
type CDCFilter struct {
	Metrics    []string          // empty = all
	Operations []CDCOp           // empty = all
	Tags       map[string]string // events must match all specified tags
}

func (f CDCFilter) matches(e *ChangeEvent) bool {
	if len(f.Metrics) > 0 {
		found := false
		for _, m := range f.Metrics {
			if m == e.Metric {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	if len(f.Operations) > 0 {
		found := false
		for _, op := range f.Operations {
			if op == e.Operation {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	if len(f.Tags) > 0 {
		point := e.After
		if point == nil {
			point = e.Before
		}
		if point == nil {
			return false
		}
		for k, v := range f.Tags {
			if point.Tags[k] != v {
				return false
			}
		}
	}

	return true
}

// CDCStats provides runtime statistics for the CDC engine.
type CDCStats struct {
	TotalEvents      int64 `json:"total_events"`
	EventsPublished  int64 `json:"events_published"`
	EventsDropped    int64 `json:"events_dropped"`
	ActiveSubscribers int  `json:"active_subscribers"`
	BufferUtilization float64 `json:"buffer_utilization"`
}

// CDCEngine provides change data capture with fan-out to subscribers.
//
// 🔬 BETA: API may evolve between minor versions with migration guidance.
// See api_stability.go for stability classifications.
type CDCEngine struct {
	config CDCConfig

	mu          sync.RWMutex
	subscribers map[string]*CDCSubscription
	nextID      int64

	buffer    []ChangeEvent
	bufferMu  sync.Mutex

	totalEvents     int64
	eventsPublished int64
	eventsDropped   int64

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewCDCEngine creates a new change data capture engine.
func NewCDCEngine(config CDCConfig) *CDCEngine {
	if config.BufferSize <= 0 {
		config.BufferSize = 4096
	}
	if config.MaxSubscribers <= 0 {
		config.MaxSubscribers = 64
	}
	if config.FlushInterval <= 0 {
		config.FlushInterval = 100 * time.Millisecond
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &CDCEngine{
		config:      config,
		subscribers: make(map[string]*CDCSubscription),
		buffer:      make([]ChangeEvent, 0, config.BufferSize),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// Start begins the background flush loop.
func (e *CDCEngine) Start() {
	e.wg.Add(1)
	go e.flushLoop()
}

// Stop gracefully shuts down the CDC engine.
func (e *CDCEngine) Stop() {
	e.cancel()
	e.wg.Wait()

	e.mu.Lock()
	for _, sub := range e.subscribers {
		sub.Close()
	}
	e.subscribers = make(map[string]*CDCSubscription)
	e.mu.Unlock()
}

// Subscribe creates a new CDC subscription with the given filter.
func (e *CDCEngine) Subscribe(filter CDCFilter) (*CDCSubscription, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(e.subscribers) >= e.config.MaxSubscribers {
		return nil, fmt.Errorf("cdc: max subscribers (%d) reached", e.config.MaxSubscribers)
	}

	id := fmt.Sprintf("cdc-%d", atomic.AddInt64(&e.nextID, 1))
	ctx, cancel := context.WithCancel(e.ctx)

	sub := &CDCSubscription{
		ID:      id,
		Filter:  filter,
		Events:  make(chan ChangeEvent, e.config.BufferSize),
		created: time.Now(),
		cancel:  cancel,
	}

	// Cleanup goroutine
	go func() {
		<-ctx.Done()
		e.mu.Lock()
		delete(e.subscribers, id)
		e.mu.Unlock()
	}()

	e.subscribers[id] = sub
	return sub, nil
}

// Unsubscribe removes a subscription by ID.
func (e *CDCEngine) Unsubscribe(id string) {
	e.mu.Lock()
	sub, ok := e.subscribers[id]
	e.mu.Unlock()
	if ok {
		sub.Close()
	}
}

// Emit publishes a change event to all matching subscribers.
func (e *CDCEngine) Emit(event ChangeEvent) {
	atomic.AddInt64(&e.totalEvents, 1)

	if !e.matchesGlobalFilter(&event) {
		return
	}

	e.bufferMu.Lock()
	e.buffer = append(e.buffer, event)
	shouldFlush := len(e.buffer) >= e.config.BufferSize
	e.bufferMu.Unlock()

	if shouldFlush {
		e.flush()
	}
}

// EmitInsert is a convenience method for insert events.
func (e *CDCEngine) EmitInsert(p Point) {
	e.Emit(ChangeEvent{
		ID:        fmt.Sprintf("cdc-%d", time.Now().UnixNano()),
		Timestamp: time.Now().UnixNano(),
		Operation: CDCOpInsert,
		Metric:    p.Metric,
		After:     &p,
	})
}

// Stats returns CDC engine statistics.
func (e *CDCEngine) Stats() CDCStats {
	e.mu.RLock()
	active := len(e.subscribers)
	e.mu.RUnlock()

	e.bufferMu.Lock()
	bufLen := len(e.buffer)
	e.bufferMu.Unlock()

	return CDCStats{
		TotalEvents:       atomic.LoadInt64(&e.totalEvents),
		EventsPublished:   atomic.LoadInt64(&e.eventsPublished),
		EventsDropped:     atomic.LoadInt64(&e.eventsDropped),
		ActiveSubscribers: active,
		BufferUtilization: float64(bufLen) / float64(e.config.BufferSize),
	}
}

// MarshalEvent serializes a ChangeEvent to JSON.
func MarshalEvent(e ChangeEvent) ([]byte, error) {
	return json.Marshal(e)
}

func (e *CDCEngine) matchesGlobalFilter(event *ChangeEvent) bool {
	if len(e.config.ExcludeMetrics) > 0 {
		for _, m := range e.config.ExcludeMetrics {
			if m == event.Metric {
				return false
			}
		}
	}
	if len(e.config.IncludeMetrics) > 0 {
		found := false
		for _, m := range e.config.IncludeMetrics {
			if m == event.Metric {
				found = true
				break
			}
		}
		return found
	}
	return true
}

func (e *CDCEngine) flushLoop() {
	defer e.wg.Done()
	ticker := time.NewTicker(e.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			e.flush() // final flush
			return
		case <-ticker.C:
			e.flush()
		}
	}
}

func (e *CDCEngine) flush() {
	e.bufferMu.Lock()
	if len(e.buffer) == 0 {
		e.bufferMu.Unlock()
		return
	}
	events := e.buffer
	e.buffer = make([]ChangeEvent, 0, e.config.BufferSize)
	e.bufferMu.Unlock()

	e.mu.RLock()
	subs := make([]*CDCSubscription, 0, len(e.subscribers))
	for _, sub := range e.subscribers {
		subs = append(subs, sub)
	}
	e.mu.RUnlock()

	for _, event := range events {
		for _, sub := range subs {
			if atomic.LoadInt32(&sub.closed) == 1 {
				continue
			}
			if !sub.Filter.matches(&event) {
				continue
			}
			select {
			case sub.Events <- event:
				atomic.AddInt64(&e.eventsPublished, 1)
			default:
				atomic.AddInt64(&e.eventsDropped, 1)
			}
		}
	}
}
