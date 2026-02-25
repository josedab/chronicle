package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Checkpoint management, pipeline CRUD, HTTP handlers, and built-in operators (map, filter, aggregate, join) for stream processing.

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
			internalError(w, err, "internal error")
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
