package chronicle

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strings"
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
				_ = e.checkpoint(id)
			}
		}
	}
}

func streamProcessingGenerateID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
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

	id := "sp-" + streamProcessingGenerateID()
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
			result, err := op.Process(context.Background(), ev)
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

// checkpoint saves a checkpoint for a pipeline.
func (e *StreamProcessingEngine) checkpoint(pipelineID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	_, ok := e.pipelines[pipelineID]
	if !ok {
		return fmt.Errorf("pipeline %q not found", pipelineID)
	}

	windowStates := make(map[string]*SPWindowState)
	if ws, ok := e.windows[pipelineID]; ok {
		for k, v := range ws {
			cpy := *v
			cpy.Values = make([]float64, len(v.Values))
			copy(cpy.Values, v.Values)
			windowStates[k] = &cpy
		}
	}

	e.checkpoints[pipelineID] = &StreamCheckpoint{
		PipelineID:   pipelineID,
		Offset:       atomic.LoadInt64(&e.totalEventsIn),
		WindowStates: windowStates,
		Timestamp:    time.Now().UnixNano(),
	}

	atomic.AddInt64(&e.checkpointCount, 1)
	atomic.StoreInt64(&e.lastCheckpoint, time.Now().UnixNano())

	return nil
}

// ListPipelines returns all registered pipelines.
func (e *StreamProcessingEngine) ListPipelines() []*StreamPipeline {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]*StreamPipeline, 0, len(e.pipelines))
	for _, p := range e.pipelines {
		result = append(result, p)
	}
	return result
}

// GetPipeline returns a pipeline by ID, or nil if not found.
func (e *StreamProcessingEngine) GetPipeline(id string) *StreamPipeline {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.pipelines[id]
}

// DeletePipeline removes a pipeline. It must be stopped first.
func (e *StreamProcessingEngine) DeletePipeline(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	p, ok := e.pipelines[id]
	if !ok {
		return fmt.Errorf("pipeline %q not found", id)
	}

	p.mu.Lock()
	state := p.State
	p.mu.Unlock()

	if state == PipelineRunning {
		return fmt.Errorf("cannot delete running pipeline %q; stop it first", id)
	}

	delete(e.pipelines, id)
	delete(e.windows, id)
	delete(e.checkpoints, id)

	return nil
}

// Stats returns engine-wide statistics.
func (e *StreamProcessingEngine) Stats() StreamProcessingStats {
	e.mu.RLock()
	total := len(e.pipelines)
	active := 0
	for _, p := range e.pipelines {
		p.mu.Lock()
		if p.State == PipelineRunning {
			active++
		}
		p.mu.Unlock()
	}
	e.mu.RUnlock()

	cpCount := atomic.LoadInt64(&e.checkpointCount)
	lastCP := atomic.LoadInt64(&e.lastCheckpoint)
	var lastCPTime time.Time
	if lastCP > 0 {
		lastCPTime = time.Unix(0, lastCP)
	}

	return StreamProcessingStats{
		TotalPipelines:  total,
		ActivePipelines: active,
		TotalEventsIn:   atomic.LoadInt64(&e.totalEventsIn),
		TotalEventsOut:  atomic.LoadInt64(&e.totalEventsOut),
		TotalErrors:     atomic.LoadInt64(&e.totalErrors),
		CheckpointCount: cpCount,
		LastCheckpoint:  lastCPTime,
	}
}

// RegisterHTTPHandlers registers stream processing HTTP endpoints.
func (e *StreamProcessingEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/streams", e.handleStreams)
	mux.HandleFunc("/api/v1/streams/stats", e.handleStreamStats)
	mux.HandleFunc("/api/v1/streams/", e.handleStreamAction)
}

func (e *StreamProcessingEngine) handleStreams(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		pipelines := e.ListPipelines()
		type pipelineInfo struct {
			ID        string `json:"id"`
			Name      string `json:"name"`
			State     string `json:"state"`
			Source    string `json:"source"`
			Sink      string `json:"sink"`
			CreatedAt string `json:"created_at"`
		}
		result := make([]pipelineInfo, 0, len(pipelines))
		for _, p := range pipelines {
			p.mu.Lock()
			state := p.State.String()
			p.mu.Unlock()
			result = append(result, pipelineInfo{
				ID:        p.ID,
				Name:      p.Name,
				State:     state,
				Source:    p.Source,
				Sink:      p.Sink,
				CreatedAt: p.CreatedAt.Format(time.RFC3339),
			})
		}
		writeJSON(w, result)

	case http.MethodPost:
		var req struct {
			Name        string `json:"name"`
			Source      string `json:"source"`
			Sink        string `json:"sink"`
			WindowType  string `json:"window_type"`
			WindowSize  string `json:"window_size"`
			WindowSlide string `json:"window_slide"`
			WindowGap   string `json:"window_gap"`
			WindowCount int    `json:"window_count"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, err.Error(), http.StatusBadRequest)
			return
		}

		var windowSpec *WindowSpec
		if req.WindowType != "" {
			ws := &WindowSpec{}
			switch req.WindowType {
			case "tumbling":
				ws.Type = StreamTumblingWindow
			case "sliding":
				ws.Type = StreamSlidingWindow
			case "session":
				ws.Type = StreamSessionWindow
			case "count":
				ws.Type = StreamCountWindow
			default:
				writeError(w, fmt.Sprintf("unknown window type: %s", req.WindowType), http.StatusBadRequest)
				return
			}
			if req.WindowSize != "" {
				d, err := time.ParseDuration(req.WindowSize)
				if err != nil {
					writeError(w, fmt.Sprintf("invalid window_size: %s", err), http.StatusBadRequest)
					return
				}
				ws.Size = d
			}
			if req.WindowSlide != "" {
				d, err := time.ParseDuration(req.WindowSlide)
				if err != nil {
					writeError(w, fmt.Sprintf("invalid window_slide: %s", err), http.StatusBadRequest)
					return
				}
				ws.Slide = d
			}
			if req.WindowGap != "" {
				d, err := time.ParseDuration(req.WindowGap)
				if err != nil {
					writeError(w, fmt.Sprintf("invalid window_gap: %s", err), http.StatusBadRequest)
					return
				}
				ws.Gap = d
			}
			ws.Count = req.WindowCount
			windowSpec = ws
		}

		p, err := e.CreatePipeline(req.Name, req.Source, req.Sink, windowSpec)
		if err != nil {
			writeError(w, err.Error(), http.StatusBadRequest)
			return
		}

		writeJSONStatus(w, http.StatusCreated, map[string]any{
			"id":     p.ID,
			"name":   p.Name,
			"state":  p.State.String(),
			"source": p.Source,
			"sink":   p.Sink,
		})

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (e *StreamProcessingEngine) handleStreamStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, e.Stats())
}

func (e *StreamProcessingEngine) handleStreamAction(w http.ResponseWriter, r *http.Request) {
	// Parse: /api/v1/streams/{id} or /api/v1/streams/{id}/{action}
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/streams/")
	if path == "" || path == "stats" {
		return
	}

	parts := strings.SplitN(path, "/", 2)
	id := parts[0]
	action := ""
	if len(parts) == 2 {
		action = parts[1]
	}

	switch {
	case action == "" && r.Method == http.MethodGet:
		p := e.GetPipeline(id)
		if p == nil {
			writeError(w, fmt.Sprintf("pipeline %q not found", id), http.StatusNotFound)
			return
		}
		p.mu.Lock()
		stats := p.Stats
		state := p.State.String()
		p.mu.Unlock()
		writeJSON(w, map[string]any{
			"id":         p.ID,
			"name":       p.Name,
			"state":      state,
			"source":     p.Source,
			"sink":       p.Sink,
			"created_at": p.CreatedAt.Format(time.RFC3339),
			"stats": map[string]any{
				"events_in":       stats.EventsIn,
				"events_out":      stats.EventsOut,
				"events_filtered": stats.EventsFiltered,
				"errors":          stats.Errors,
			},
		})

	case action == "start" && r.Method == http.MethodPost:
		if err := e.StartPipeline(id); err != nil {
			writeError(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, map[string]string{"status": "started", "id": id})

	case action == "stop" && r.Method == http.MethodPost:
		if err := e.StopPipeline(id); err != nil {
			writeError(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, map[string]string{"status": "stopped", "id": id})

	case action == "events" && r.Method == http.MethodPost:
		var ev StreamEvent
		if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
			writeError(w, err.Error(), http.StatusBadRequest)
			return
		}
		results, err := e.ProcessEvent(id, &ev)
		if err != nil {
			writeError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]any{
			"results": results,
			"count":   len(results),
		})

	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

// --- Built-in Operators ---

// MapOperator transforms events using a callback function.
type MapOperator struct {
	name string
	fn   func(*StreamEvent) (*StreamEvent, error)
}

// NewMapOperator creates a new MapOperator.
func NewMapOperator(name string, fn func(*StreamEvent) (*StreamEvent, error)) *MapOperator {
	return &MapOperator{name: name, fn: fn}
}

// Process applies the map function to an event.
func (m *MapOperator) Process(_ context.Context, event *StreamEvent) ([]*StreamEvent, error) {
	result, err := m.fn(event)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, nil
	}
	return []*StreamEvent{result}, nil
}

// Name returns the operator name.
func (m *MapOperator) Name() string { return m.name }

// FilterOperator filters events using a predicate function.
type SPFilterOperator struct {
	name      string
	predicate func(*StreamEvent) bool
}

// NewFilterOperator creates a new FilterOperator.
func NewSPFilterOperator(name string, predicate func(*StreamEvent) bool) *SPFilterOperator {
	return &SPFilterOperator{name: name, predicate: predicate}
}

// Process applies the filter predicate to an event.
func (f *SPFilterOperator) Process(_ context.Context, event *StreamEvent) ([]*StreamEvent, error) {
	if f.predicate(event) {
		return []*StreamEvent{event}, nil
	}
	return nil, nil
}

// Name returns the operator name.
func (f *SPFilterOperator) Name() string { return f.name }

// AggregateType defines the type of aggregation.
type SPAggregateType int

const (
	SPAggregateSum SPAggregateType = iota
	SPAggregateAvg
	SPAggregateMin
	SPAggregateMax
	SPAggregateCount
)

// AggregateOperator aggregates events within windows.
type SPAggregateOperator struct {
	name    string
	aggType SPAggregateType
	mu      sync.Mutex
	state   map[string]*SPWindowState
}

// NewAggregateOperator creates a new AggregateOperator.
func NewSPAggregateOperator(name string, aggType SPAggregateType) *SPAggregateOperator {
	return &SPAggregateOperator{
		name:    name,
		aggType: aggType,
		state:   make(map[string]*SPWindowState),
	}
}

// Process aggregates an event into the running state and emits the current aggregate.
func (a *SPAggregateOperator) Process(_ context.Context, event *StreamEvent) ([]*StreamEvent, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	key := event.Key
	if key == "" {
		key = event.Metric
	}

	ws, ok := a.state[key]
	if !ok {
		ws = &SPWindowState{
			Key: key,
			Min: math.MaxFloat64,
			Max: -math.MaxFloat64,
		}
		a.state[key] = ws
	}

	ws.Count++
	ws.Sum += event.Value
	if event.Value < ws.Min {
		ws.Min = event.Value
	}
	if event.Value > ws.Max {
		ws.Max = event.Value
	}

	var value float64
	switch a.aggType {
	case SPAggregateSum:
		value = ws.Sum
	case SPAggregateAvg:
		value = ws.Sum / float64(ws.Count)
	case SPAggregateMin:
		value = ws.Min
	case SPAggregateMax:
		value = ws.Max
	case SPAggregateCount:
		value = float64(ws.Count)
	}

	return []*StreamEvent{{
		Key:       event.Key,
		Value:     value,
		Timestamp: event.Timestamp,
		Metric:    event.Metric,
		Tags:      event.Tags,
		Watermark: event.Watermark,
	}}, nil
}

// Name returns the operator name.
func (a *SPAggregateOperator) Name() string { return a.name }

// JoinOperator joins two streams by key within a time window.
type JoinOperator struct {
	name        string
	joinWindow  time.Duration
	mu          sync.Mutex
	leftBuf     map[string][]*StreamEvent
	rightBuf    map[string][]*StreamEvent
	rightStream string
}

// NewJoinOperator creates a new JoinOperator.
func NewJoinOperator(name string, rightStream string, joinWindow time.Duration) *JoinOperator {
	return &JoinOperator{
		name:        name,
		joinWindow:  joinWindow,
		leftBuf:     make(map[string][]*StreamEvent),
		rightBuf:    make(map[string][]*StreamEvent),
		rightStream: rightStream,
	}
}

// Process handles join logic. Events matching the right stream go to the right buffer;
// others are matched against the right buffer.
func (j *JoinOperator) Process(_ context.Context, event *StreamEvent) ([]*StreamEvent, error) {
	j.mu.Lock()
	defer j.mu.Unlock()

	windowNanos := j.joinWindow.Nanoseconds()
	key := event.Key

	if event.Metric == j.rightStream {
		j.rightBuf[key] = append(j.rightBuf[key], event)
		// Try to match with left buffer
		var results []*StreamEvent
		for _, left := range j.leftBuf[key] {
			diff := event.Timestamp - left.Timestamp
			if diff < 0 {
				diff = -diff
			}
			if diff <= windowNanos {
				results = append(results, &StreamEvent{
					Key:       key,
					Value:     left.Value + event.Value,
					Timestamp: event.Timestamp,
					Metric:    left.Metric + "_joined",
					Tags:      spMergeTags(left.Tags, event.Tags),
					Watermark: event.Watermark,
				})
			}
		}
		return results, nil
	}

	// Left stream event
	j.leftBuf[key] = append(j.leftBuf[key], event)
	var results []*StreamEvent
	for _, right := range j.rightBuf[key] {
		diff := event.Timestamp - right.Timestamp
		if diff < 0 {
			diff = -diff
		}
		if diff <= windowNanos {
			results = append(results, &StreamEvent{
				Key:       key,
				Value:     event.Value + right.Value,
				Timestamp: event.Timestamp,
				Metric:    event.Metric + "_joined",
				Tags:      spMergeTags(event.Tags, right.Tags),
				Watermark: event.Watermark,
			})
		}
	}
	return results, nil
}

// Name returns the operator name.
func (j *JoinOperator) Name() string { return j.name }

func spMergeTags(a, b map[string]string) map[string]string {
	result := make(map[string]string, len(a)+len(b))
	for k, v := range a {
		result[k] = v
	}
	for k, v := range b {
		result[k] = v
	}
	return result
}
