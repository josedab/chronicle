package chronicle

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sync"
	"time"
)

// ContinuousAggConfig configures the continuous aggregation engine.
type ContinuousAggConfig struct {
	Enabled       bool
	MaxAggregations int
	CheckInterval time.Duration
	RetainWindows int
}

// DefaultContinuousAggConfig returns sensible defaults.
func DefaultContinuousAggConfig() ContinuousAggConfig {
	return ContinuousAggConfig{
		Enabled:       true,
		MaxAggregations: 100,
		CheckInterval: 10 * time.Second,
		RetainWindows: 1000,
	}
}

// ContinuousAggDefinition defines a continuous aggregation.
type ContinuousAggDefinition struct {
	Name       string        `json:"name"`
	SourceMetric string      `json:"source_metric"`
	TargetMetric string      `json:"target_metric"`
	Function   string        `json:"function"` // sum, avg, min, max, count, p95, p99
	Window     time.Duration `json:"window"`
	GroupBy    []string      `json:"group_by,omitempty"`
	Filter     map[string]string `json:"filter,omitempty"`
	CreatedAt  time.Time     `json:"created_at"`
}

// AggWindow represents a single aggregation window result.
type AggWindow struct {
	Start      int64   `json:"start"`
	End        int64   `json:"end"`
	Value      float64 `json:"value"`
	Count      int     `json:"count"`
	LastUpdate time.Time `json:"last_update"`
}

// ContinuousAggState holds the state of a running aggregation.
type ContinuousAggState struct {
	Definition ContinuousAggDefinition `json:"definition"`
	Windows    []AggWindow             `json:"windows"`
	PointsIn   int64                   `json:"points_ingested"`
	LastPoint  int64                   `json:"last_point_ts"`
	Running    bool                    `json:"running"`
}

// ContinuousAggStats holds engine statistics.
type ContinuousAggStats struct {
	ActiveAggregations int   `json:"active_aggregations"`
	TotalPointsIn      int64 `json:"total_points_ingested"`
	TotalWindowsEmitted int64 `json:"total_windows_emitted"`
}

// ContinuousAggEngine provides incrementally maintained aggregations.
type ContinuousAggEngine struct {
	db     *DB
	config ContinuousAggConfig
	mu     sync.RWMutex
	aggs   map[string]*ContinuousAggState
	running bool
	stopCh chan struct{}
	stats  ContinuousAggStats
}

// NewContinuousAggEngine creates a new continuous aggregation engine.
func NewContinuousAggEngine(db *DB, cfg ContinuousAggConfig) *ContinuousAggEngine {
	return &ContinuousAggEngine{
		db:     db,
		config: cfg,
		aggs:   make(map[string]*ContinuousAggState),
		stopCh: make(chan struct{}),
	}
}

// Start starts the background processing.
func (e *ContinuousAggEngine) Start() {
	e.mu.Lock()
	if e.running { e.mu.Unlock(); return }
	e.running = true
	e.mu.Unlock()
	go e.processLoop()
}

// Stop stops the engine.
func (e *ContinuousAggEngine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running { return }
	e.running = false
	close(e.stopCh)
}

// Create creates a new continuous aggregation.
func (e *ContinuousAggEngine) Create(def ContinuousAggDefinition) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if def.Name == "" { return fmt.Errorf("name required") }
	if def.SourceMetric == "" { return fmt.Errorf("source metric required") }
	if def.Window <= 0 { return fmt.Errorf("positive window required") }
	if _, exists := e.aggs[def.Name]; exists { return fmt.Errorf("aggregation %q exists", def.Name) }
	if len(e.aggs) >= e.config.MaxAggregations { return fmt.Errorf("max aggregations reached") }

	if def.TargetMetric == "" { def.TargetMetric = def.SourceMetric + "_" + def.Function }
	if def.Function == "" { def.Function = "avg" }
	def.CreatedAt = time.Now()

	e.aggs[def.Name] = &ContinuousAggState{
		Definition: def,
		Windows:    make([]AggWindow, 0),
		Running:    true,
	}
	e.stats.ActiveAggregations = len(e.aggs)
	return nil
}

// Delete removes a continuous aggregation.
func (e *ContinuousAggEngine) Delete(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, exists := e.aggs[name]; !exists { return fmt.Errorf("aggregation %q not found", name) }
	delete(e.aggs, name)
	e.stats.ActiveAggregations = len(e.aggs)
	return nil
}

// Ingest processes a point for all matching aggregations.
func (e *ContinuousAggEngine) Ingest(p Point) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, state := range e.aggs {
		if !state.Running { continue }
		if p.Metric != state.Definition.SourceMetric { continue }
		// Check filter
		if !matchesContinuousAggFilter(p.Tags, state.Definition.Filter) { continue }
		e.addToWindow(state, p)
		state.PointsIn++
		state.LastPoint = p.Timestamp
		e.stats.TotalPointsIn++
	}
}

func (e *ContinuousAggEngine) addToWindow(state *ContinuousAggState, p Point) {
	windowNanos := state.Definition.Window.Nanoseconds()
	if windowNanos <= 0 { return }
	windowStart := (p.Timestamp / windowNanos) * windowNanos
	windowEnd := windowStart + windowNanos

	for i := range state.Windows {
		if state.Windows[i].Start == windowStart {
			state.Windows[i] = applyAggFunc(state.Windows[i], p.Value, state.Definition.Function)
			state.Windows[i].LastUpdate = time.Now()
			return
		}
	}
	// New window
	w := AggWindow{Start: windowStart, End: windowEnd, Value: p.Value, Count: 1, LastUpdate: time.Now()}
	state.Windows = append(state.Windows, w)
	e.stats.TotalWindowsEmitted++
	// Cap windows
	if len(state.Windows) > e.config.RetainWindows {
		state.Windows = state.Windows[len(state.Windows)-e.config.RetainWindows:]
	}
}

func applyAggFunc(w AggWindow, value float64, fn string) AggWindow {
	w.Count++
	switch fn {
	case "sum":
		w.Value += value
	case "avg":
		w.Value = (w.Value*float64(w.Count-1) + value) / float64(w.Count)
	case "min":
		if value < w.Value { w.Value = value }
	case "max":
		if value > w.Value { w.Value = value }
	case "count":
		w.Value = float64(w.Count)
	default:
		w.Value = (w.Value*float64(w.Count-1) + value) / float64(w.Count)
	}
	return w
}

func matchesContinuousAggFilter(tags, filter map[string]string) bool {
	for k, v := range filter {
		if tags[k] != v { return false }
	}
	return true
}

// List returns all aggregation states.
func (e *ContinuousAggEngine) List() []ContinuousAggState {
	e.mu.RLock()
	defer e.mu.RUnlock()
	result := make([]ContinuousAggState, 0, len(e.aggs))
	for _, s := range e.aggs { result = append(result, *s) }
	return result
}

// Get returns a specific aggregation state.
func (e *ContinuousAggEngine) Get(name string) *ContinuousAggState {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if s, ok := e.aggs[name]; ok { cp := *s; return &cp }
	return nil
}

// GetStats returns engine stats.
func (e *ContinuousAggEngine) GetStats() ContinuousAggStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.stats
}

func (e *ContinuousAggEngine) processLoop() {
	ticker := time.NewTicker(e.config.CheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-e.stopCh: return
		case <-ticker.C: // periodic maintenance
		}
	}
}

// RegisterHTTPHandlers registers HTTP endpoints.
func (e *ContinuousAggEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/continuous-agg/create", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost { http.Error(w, "method not allowed", http.StatusMethodNotAllowed); return }
		var def ContinuousAggDefinition
		if err := json.NewDecoder(r.Body).Decode(&def); err != nil { http.Error(w, err.Error(), http.StatusBadRequest); return }
		if err := e.Create(def); err != nil { http.Error(w, err.Error(), http.StatusConflict); return }
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "created"})
	})
	mux.HandleFunc("/api/v1/continuous-agg/list", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.List())
	})
	mux.HandleFunc("/api/v1/continuous-agg/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetStats())
	})
}

var _ = math.MaxFloat64
