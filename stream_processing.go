package chronicle

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// StreamWindowType defines the type of processing window for stream pipelines.
type StreamWindowType int

const (
	// StreamTumblingWindow is a fixed-size, non-overlapping window.
	StreamTumblingWindow StreamWindowType = iota
	// StreamSlidingWindow is a fixed-size window that slides by a configurable interval.
	StreamSlidingWindow
	// StreamSessionWindow groups events by activity separated by a gap.
	StreamSessionWindow
	// StreamCountWindow triggers after a fixed number of events.
	StreamCountWindow
)

// StreamPipelineState tracks the state of a stream pipeline.
type StreamPipelineState int

const (
	PipelineCreated StreamPipelineState = iota
	PipelineRunning
	PipelinePaused
	PipelineStopped
	PipelineError
)

func (s StreamPipelineState) String() string {
	switch s {
	case PipelineCreated:
		return "created"
	case PipelineRunning:
		return "running"
	case PipelinePaused:
		return "paused"
	case PipelineStopped:
		return "stopped"
	case PipelineError:
		return "error"
	default:
		return "unknown"
	}
}

// StreamProcessingConfig holds configuration for the stream processing engine.
type StreamProcessingConfig struct {
	Enabled            bool
	MaxPipelines       int
	CheckpointInterval time.Duration
	StateBackendType   string // "memory" or "wal"
	MaxBufferSize      int
	WorkerCount        int
}

// DefaultStreamProcessingConfig returns a StreamProcessingConfig with sensible defaults.
func DefaultStreamProcessingConfig() StreamProcessingConfig {
	return StreamProcessingConfig{
		Enabled:            true,
		MaxPipelines:       100,
		CheckpointInterval: 30 * time.Second,
		StateBackendType:   "memory",
		MaxBufferSize:      10000,
		WorkerCount:        4,
	}
}

// WindowSpec defines a window specification for stream processing.
type WindowSpec struct {
	Type  StreamWindowType
	Size  time.Duration
	Slide time.Duration // For sliding windows
	Gap   time.Duration // For session windows
	Count int           // For count windows
}

// StreamOperator is the interface for all stream processing operators.
type StreamOperator interface {
	Process(ctx context.Context, event *StreamEvent) ([]*StreamEvent, error)
	Name() string
}

// StreamEvent is the unit of data flowing through a processing pipeline.
type StreamEvent struct {
	Key       string
	Value     float64
	Timestamp int64
	Metric    string
	Tags      map[string]string
	Watermark int64
}

// StreamPipelineStats tracks pipeline statistics.
type StreamPipelineStats struct {
	EventsIn       int64
	EventsOut      int64
	EventsFiltered int64
	Errors         int64
	LastProcessed  time.Time
	Latency        time.Duration
}

// StreamPipeline is a configured processing pipeline.
type StreamPipeline struct {
	ID        string
	Name      string
	State     StreamPipelineState
	Operators []StreamOperator
	Window    *WindowSpec
	Source    string // source metric
	Sink      string // output metric name
	CreatedAt time.Time
	Stats     StreamPipelineStats

	mu sync.Mutex
}

// WindowState holds aggregation state for a window.
type SPWindowState struct {
	Key    string
	Start  int64
	End    int64
	Count  int64
	Sum    float64
	Min    float64
	Max    float64
	Values []float64
}

// StreamCheckpoint stores checkpoint data for exactly-once semantics.
type StreamCheckpoint struct {
	PipelineID   string
	Offset       int64
	WindowStates map[string]*SPWindowState
	Timestamp    int64
}

// StreamProcessingStats holds engine-wide statistics.
type StreamProcessingStats struct {
	TotalPipelines  int
	ActivePipelines int
	TotalEventsIn   int64
	TotalEventsOut  int64
	TotalErrors     int64
	CheckpointCount int64
	LastCheckpoint  time.Time
}

// StreamProcessingEngine is the main stream processing engine.
//
// 🧪 EXPERIMENTAL: This API may change or be removed without notice.
// See api_stability.go for stability classifications.
type StreamProcessingEngine struct {
	db          *DB
	config      StreamProcessingConfig
	mu          sync.RWMutex
	pipelines   map[string]*StreamPipeline
	checkpoints map[string]*StreamCheckpoint
	windows     map[string]map[string]*SPWindowState // pipelineID -> key -> state
	stopCh      chan struct{}
	running     bool

	// Atomic stats counters
	totalEventsIn   int64
	totalEventsOut  int64
	totalErrors     int64
	checkpointCount int64
	lastCheckpoint  int64 // unix nano
}

// NewStreamProcessingEngine creates a new stream processing engine.
func NewStreamProcessingEngine(db *DB, cfg StreamProcessingConfig) *StreamProcessingEngine {
	return &StreamProcessingEngine{
		db:          db,
		config:      cfg,
		pipelines:   make(map[string]*StreamPipeline),
		checkpoints: make(map[string]*StreamCheckpoint),
		windows:     make(map[string]map[string]*SPWindowState),
		stopCh:      make(chan struct{}),
	}
}

// Start begins the stream processing engine's background workers.
func (e *StreamProcessingEngine) Start() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.running {
		return fmt.Errorf("stream processing engine already running")
	}
	if !e.config.Enabled {
		return fmt.Errorf("stream processing engine is disabled")
	}

	e.running = true
	e.stopCh = make(chan struct{})

	go e.checkpointLoop()

	return nil
}

// Stop shuts down the stream processing engine.
func (e *StreamProcessingEngine) Stop() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.running {
		return nil
	}

	close(e.stopCh)
	e.running = false

	// Stop all running pipelines
	for _, p := range e.pipelines {
		p.mu.Lock()
		if p.State == PipelineRunning {
			p.State = PipelineStopped
		}
		p.mu.Unlock()
	}

	return nil
}

func (e *StreamProcessingEngine) checkpointLoop() {
	ticker := time.NewTicker(e.config.CheckpointInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			e.mu.RLock()
			ids := make([]string, 0, len(e.pipelines))
			for id, p := range e.pipelines {
				p.mu.Lock()
				if p.State == PipelineRunning {
					ids = append(ids, id)
				}
				p.mu.Unlock()
			}
			e.mu.RUnlock()

			for _, id := range ids {
				_ = e.checkpoint(id) //nolint:errcheck // best-effort periodic checkpoint
			}
		}
	}
}

func streamProcessingGenerateID() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("crypto/rand failed: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// CreatePipeline creates a new stream processing pipeline.
func (e *StreamProcessingEngine) CreatePipeline(name string, source string, sink string, window *WindowSpec) (*StreamPipeline, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(e.pipelines) >= e.config.MaxPipelines {
		return nil, fmt.Errorf("maximum number of pipelines (%d) reached", e.config.MaxPipelines)
	}

	if name == "" {
		return nil, fmt.Errorf("pipeline name is required")
	}
	if source == "" {
		return nil, fmt.Errorf("pipeline source is required")
	}
	if sink == "" {
		return nil, fmt.Errorf("pipeline sink is required")
	}

	genID, err := streamProcessingGenerateID()
	if err != nil {
		return nil, err
	}
	id := "sp-" + genID
	p := &StreamPipeline{
		ID:        id,
		Name:      name,
		State:     PipelineCreated,
		Operators: make([]StreamOperator, 0),
		Window:    window,
		Source:    source,
		Sink:      sink,
		CreatedAt: time.Now(),
	}

	e.pipelines[id] = p
	e.windows[id] = make(map[string]*SPWindowState)

	return p, nil
}

// AddOperator adds an operator to an existing pipeline.
func (e *StreamProcessingEngine) AddOperator(pipelineID string, op StreamOperator) error {
	e.mu.RLock()
	p, ok := e.pipelines[pipelineID]
	e.mu.RUnlock()

	if !ok {
		return fmt.Errorf("pipeline %q not found", pipelineID)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.State == PipelineRunning {
		return fmt.Errorf("cannot add operator to a running pipeline")
	}

	p.Operators = append(p.Operators, op)
	return nil
}

// StartPipeline starts a pipeline for processing.
func (e *StreamProcessingEngine) StartPipeline(id string) error {
	e.mu.RLock()
	p, ok := e.pipelines[id]
	e.mu.RUnlock()

	if !ok {
		return fmt.Errorf("pipeline %q not found", id)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.State == PipelineRunning {
		return fmt.Errorf("pipeline %q is already running", id)
	}

	p.State = PipelineRunning
	return nil
}

// StopPipeline stops a running pipeline.
func (e *StreamProcessingEngine) StopPipeline(id string) error {
	e.mu.RLock()
	p, ok := e.pipelines[id]
	e.mu.RUnlock()

	if !ok {
		return fmt.Errorf("pipeline %q not found", id)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.State != PipelineRunning && p.State != PipelinePaused {
		return fmt.Errorf("pipeline %q is not running or paused", id)
	}

	p.State = PipelineStopped
	return nil
}

// PausePipeline pauses a running pipeline.
func (e *StreamProcessingEngine) PausePipeline(id string) error {
	e.mu.RLock()
	p, ok := e.pipelines[id]
	e.mu.RUnlock()

	if !ok {
		return fmt.Errorf("pipeline %q not found", id)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.State != PipelineRunning {
		return fmt.Errorf("pipeline %q is not running", id)
	}

	p.State = PipelinePaused
	return nil
}

// ProcessEvent processes a single event through a pipeline.
func (e *StreamProcessingEngine) ProcessEvent(pipelineID string, event *StreamEvent) ([]*StreamEvent, error) {
	e.mu.RLock()
	p, ok := e.pipelines[pipelineID]
	e.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("pipeline %q not found", pipelineID)
	}

	p.mu.Lock()
	if p.State != PipelineRunning {
		p.mu.Unlock()
		return nil, fmt.Errorf("pipeline %q is not running", pipelineID)
	}
	p.mu.Unlock()

	atomic.AddInt64(&e.totalEventsIn, 1)
	p.mu.Lock()
	p.Stats.EventsIn++
	p.mu.Unlock()

	start := time.Now()

	// Run through operators
	events := []*StreamEvent{event}
	for _, op := range p.Operators {
		var next []*StreamEvent
		for _, ev := range events {
			// Use a timeout context to prevent unbounded operator execution
			opCtx, opCancel := context.WithTimeout(context.Background(), 30*time.Second)
			result, err := op.Process(opCtx, ev)
			opCancel()
			if err != nil {
				atomic.AddInt64(&e.totalErrors, 1)
				p.mu.Lock()
				p.Stats.Errors++
				p.mu.Unlock()
				return nil, fmt.Errorf("operator %q failed: %w", op.Name(), err)
			}
			next = append(next, result...)
		}
		events = next
	}

	// Apply windowing if configured
	if p.Window != nil {
		var windowed []*StreamEvent
		for _, ev := range events {
			result, err := e.processWindow(p, ev)
			if err != nil {
				atomic.AddInt64(&e.totalErrors, 1)
				p.mu.Lock()
				p.Stats.Errors++
				p.mu.Unlock()
				return nil, fmt.Errorf("window processing failed: %w", err)
			}
			windowed = append(windowed, result...)
		}
		events = windowed
	}

	elapsed := time.Since(start)
	outCount := int64(len(events))
	filtered := int64(1) - outCount
	if filtered < 0 {
		filtered = 0
	}

	atomic.AddInt64(&e.totalEventsOut, outCount)
	p.mu.Lock()
	p.Stats.EventsOut += outCount
	p.Stats.EventsFiltered += filtered
	p.Stats.LastProcessed = time.Now()
	p.Stats.Latency = elapsed
	p.mu.Unlock()

	return events, nil
}

// processWindow applies window aggregation logic.
func (e *StreamProcessingEngine) processWindow(pipeline *StreamPipeline, event *StreamEvent) ([]*StreamEvent, error) {
	e.mu.Lock()
	windowStates, ok := e.windows[pipeline.ID]
	if !ok {
		windowStates = make(map[string]*SPWindowState)
		e.windows[pipeline.ID] = windowStates
	}

	key := event.Key
	if key == "" {
		key = event.Metric
	}

	ws, ok := windowStates[key]
	if !ok {
		ws = &SPWindowState{
			Key:   key,
			Start: event.Timestamp,
			End:   event.Timestamp,
			Min:   math.MaxFloat64,
			Max:   -math.MaxFloat64,
		}
		windowStates[key] = ws
	}
	e.mu.Unlock()

	ws.Count++
	ws.Sum += event.Value
	if event.Value < ws.Min {
		ws.Min = event.Value
	}
	if event.Value > ws.Max {
		ws.Max = event.Value
	}
	ws.Values = append(ws.Values, event.Value)
	if event.Timestamp > ws.End {
		ws.End = event.Timestamp
	}

	spec := pipeline.Window
	var emitted []*StreamEvent

	switch spec.Type {
	case StreamTumblingWindow:
		windowEnd := ws.Start + spec.Size.Nanoseconds()
		if event.Timestamp >= windowEnd {
			emitted = append(emitted, e.emitWindowResult(pipeline, ws))
			// Reset window
			e.mu.Lock()
			windowStates[key] = &SPWindowState{
				Key:   key,
				Start: event.Timestamp,
				End:   event.Timestamp,
				Min:   math.MaxFloat64,
				Max:   -math.MaxFloat64,
			}
			e.mu.Unlock()
		}

	case StreamSlidingWindow:
		windowEnd := ws.Start + spec.Size.Nanoseconds()
		if event.Timestamp >= windowEnd {
			emitted = append(emitted, e.emitWindowResult(pipeline, ws))
			// Slide the window
			e.mu.Lock()
			newStart := ws.Start + spec.Slide.Nanoseconds()
			windowStates[key] = &SPWindowState{
				Key:   key,
				Start: newStart,
				End:   event.Timestamp,
				Min:   math.MaxFloat64,
				Max:   -math.MaxFloat64,
			}
			e.mu.Unlock()
		}

	case StreamSessionWindow:
		if event.Timestamp-ws.End > spec.Gap.Nanoseconds() {
			emitted = append(emitted, e.emitWindowResult(pipeline, ws))
			e.mu.Lock()
			windowStates[key] = &SPWindowState{
				Key:    key,
				Start:  event.Timestamp,
				End:    event.Timestamp,
				Count:  1,
				Sum:    event.Value,
				Min:    event.Value,
				Max:    event.Value,
				Values: []float64{event.Value},
			}
			e.mu.Unlock()
		}

	case StreamCountWindow:
		if ws.Count >= int64(spec.Count) {
			emitted = append(emitted, e.emitWindowResult(pipeline, ws))
			e.mu.Lock()
			windowStates[key] = &SPWindowState{
				Key:   key,
				Start: event.Timestamp,
				End:   event.Timestamp,
				Min:   math.MaxFloat64,
				Max:   -math.MaxFloat64,
			}
			e.mu.Unlock()
		}
	}

	return emitted, nil
}

func (e *StreamProcessingEngine) emitWindowResult(pipeline *StreamPipeline, ws *SPWindowState) *StreamEvent {
	avg := float64(0)
	if ws.Count > 0 {
		avg = ws.Sum / float64(ws.Count)
	}

	return &StreamEvent{
		Key:       ws.Key,
		Value:     avg,
		Timestamp: ws.End,
		Metric:    pipeline.Sink,
		Tags: map[string]string{
			"window_start": fmt.Sprintf("%d", ws.Start),
			"window_end":   fmt.Sprintf("%d", ws.End),
			"count":        fmt.Sprintf("%d", ws.Count),
			"sum":          fmt.Sprintf("%f", ws.Sum),
			"min":          fmt.Sprintf("%f", ws.Min),
			"max":          fmt.Sprintf("%f", ws.Max),
		},
		Watermark: ws.End,
	}
}
