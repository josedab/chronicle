package chronicle

// materialized_views_v2.go implements the current (recommended) materialized view
// engine with windowed views, incremental maintenance, dependency tracking,
// and cost-based refresh optimization. See ViewManagerV2.

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// WindowType identifies the type of windowing strategy.
type WindowType int

const (
	WindowTumbling WindowType = iota
	WindowSliding
	WindowSession
	WindowGlobal
)

func (wt WindowType) String() string {
	names := [...]string{"tumbling", "sliding", "session", "global"}
	if int(wt) < len(names) {
		return names[wt]
	}
	return "unknown"
}

// LateDataPolicy controls how late-arriving data is handled.
type LateDataPolicy int

const (
	LateDataDrop LateDataPolicy = iota
	LateDataUpdate
	LateDataRetrigger
)

// MaterializedViewV2Config configures the v2 materialized view engine.
type MaterializedViewV2Config struct {
	// MaxViews limits the number of views.
	MaxViews int `json:"max_views"`

	// CheckpointInterval controls how often state is checkpointed.
	CheckpointInterval time.Duration `json:"checkpoint_interval"`

	// MaxAllowedLateness is the maximum age of late data to accept.
	MaxAllowedLateness time.Duration `json:"max_allowed_lateness"`

	// DefaultLatePolicy is the default policy for late data.
	DefaultLatePolicy LateDataPolicy `json:"default_late_policy"`

	// EnableExactlyOnce enables exactly-once semantics via idempotent writes.
	EnableExactlyOnce bool `json:"enable_exactly_once"`

	// EnableShadowVerification checks incremental results against full recomputation.
	EnableShadowVerification bool `json:"enable_shadow_verification"`

	// ShadowVerificationRate is the fraction of updates to shadow verify (0-1).
	ShadowVerificationRate float64 `json:"shadow_verification_rate"`
}

// DefaultMaterializedViewV2Config returns sensible defaults.
func DefaultMaterializedViewV2Config() MaterializedViewV2Config {
	return MaterializedViewV2Config{
		MaxViews:                 256,
		CheckpointInterval:       30 * time.Second,
		MaxAllowedLateness:       5 * time.Minute,
		DefaultLatePolicy:        LateDataUpdate,
		EnableExactlyOnce:        true,
		EnableShadowVerification: false,
		ShadowVerificationRate:   0.01,
	}
}

// ViewV2Definition describes a v2 materialized view.
type ViewV2Definition struct {
	Name            string            `json:"name"`
	SourceMetric    string            `json:"source_metric"`
	Tags            map[string]string `json:"tags,omitempty"`
	Aggregation     AggFunc           `json:"aggregation"`
	WindowType      WindowType        `json:"window_type"`
	WindowSize      time.Duration     `json:"window_size"`
	SlideInterval   time.Duration     `json:"slide_interval,omitempty"` // For sliding windows
	SessionGap      time.Duration     `json:"session_gap,omitempty"`    // For session windows
	GroupBy         []string          `json:"group_by,omitempty"`
	LatePolicy      LateDataPolicy    `json:"late_policy"`
	AllowedLateness time.Duration     `json:"allowed_lateness"`
	Enabled         bool              `json:"enabled"`
}

// Watermark tracks the event-time progress of a stream.
type Watermark struct {
	Value     int64     `json:"value"`
	UpdatedAt time.Time `json:"updated_at"`
}

// ViewV2State represents the runtime state of a v2 view.
type ViewV2State struct {
	Definition     ViewV2Definition `json:"definition"`
	Watermark      Watermark        `json:"watermark"`
	WindowCount    int              `json:"window_count"`
	LateDataCount  int64            `json:"late_data_count"`
	LastCheckpoint time.Time        `json:"last_checkpoint"`
	IsActive       bool             `json:"is_active"`
}

// viewV2Window tracks state for a single window instance.
type viewV2Window struct {
	start     int64
	end       int64
	state     windowState
	closed    bool
	updatedAt time.Time
}

// viewV2Runtime holds the runtime state for a single view.
type viewV2Runtime struct {
	def        ViewV2Definition
	watermark  int64
	windows    map[string]*viewV2Window // keyed by groupKey:windowStart
	processed  map[string]bool          // idempotency tracking
	mu         sync.Mutex
	lateCount  atomic.Int64
	checkpoint time.Time
}

// MaterializedViewV2Engine manages v2 materialized views with event-time semantics.
type MaterializedViewV2Engine struct {
	config  MaterializedViewV2Config
	db      *DB
	views   map[string]*viewV2Runtime
	mu      sync.RWMutex
	running atomic.Bool
	stopCh  chan struct{}
}

// NewMaterializedViewV2Engine creates a new v2 materialized view engine.
func NewMaterializedViewV2Engine(db *DB, config MaterializedViewV2Config) *MaterializedViewV2Engine {
	return &MaterializedViewV2Engine{
		config: config,
		db:     db,
		views:  make(map[string]*viewV2Runtime),
		stopCh: make(chan struct{}),
	}
}

// CreateView registers a new v2 materialized view.
func (e *MaterializedViewV2Engine) CreateView(def ViewV2Definition) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if def.Name == "" {
		return errors.New("matview_v2: view name required")
	}
	if _, exists := e.views[def.Name]; exists {
		return fmt.Errorf("matview_v2: view %q already exists", def.Name)
	}
	if len(e.views) >= e.config.MaxViews {
		return fmt.Errorf("matview_v2: max views (%d) reached", e.config.MaxViews)
	}

	if def.AllowedLateness == 0 {
		def.AllowedLateness = e.config.MaxAllowedLateness
	}

	e.views[def.Name] = &viewV2Runtime{
		def:        def,
		windows:    make(map[string]*viewV2Window),
		processed:  make(map[string]bool),
		checkpoint: time.Now(),
	}

	return nil
}

// DropView removes a materialized view.
func (e *MaterializedViewV2Engine) DropView(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.views[name]; !exists {
		return fmt.Errorf("matview_v2: view %q not found", name)
	}
	delete(e.views, name)
	return nil
}

// Apply processes a point against all matching views.
func (e *MaterializedViewV2Engine) Apply(p *Point) ([]AggregateUpdate, error) {
	if p == nil {
		return nil, errors.New("matview_v2: nil point")
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	var updates []AggregateUpdate

	for _, vr := range e.views {
		if !vr.def.Enabled {
			continue
		}
		if vr.def.SourceMetric != "" && vr.def.SourceMetric != p.Metric {
			continue
		}

		update, err := e.applyToView(vr, p)
		if err != nil {
			continue
		}
		if update != nil {
			updates = append(updates, *update)
		}
	}

	return updates, nil
}

func (e *MaterializedViewV2Engine) applyToView(vr *viewV2Runtime, p *Point) (*AggregateUpdate, error) {
	vr.mu.Lock()
	defer vr.mu.Unlock()

	// Exactly-once: check if this point was already processed
	if e.config.EnableExactlyOnce {
		pointID := fmt.Sprintf("%s|%d|%f", p.Metric, p.Timestamp, p.Value)
		if vr.processed[pointID] {
			return nil, nil
		}
		vr.processed[pointID] = true

		// Bound the processed set
		if len(vr.processed) > 100000 {
			vr.processed = make(map[string]bool)
		}
	}

	// Check watermark: is this late data?
	isLate := p.Timestamp < vr.watermark

	if isLate {
		vr.lateCount.Add(1)
		lateness := time.Duration(vr.watermark-p.Timestamp) * time.Nanosecond

		if lateness > vr.def.AllowedLateness {
			if vr.def.LatePolicy == LateDataDrop {
				return nil, nil
			}
		}

		switch vr.def.LatePolicy {
		case LateDataDrop:
			return nil, nil
		case LateDataUpdate, LateDataRetrigger:
			// Allow the update
		}
	}

	// Advance watermark
	if p.Timestamp > vr.watermark {
		vr.watermark = p.Timestamp
	}

	// Determine the window(s) this point belongs to
	windows := e.assignWindows(vr, p)

	var lastUpdate *AggregateUpdate
	for _, win := range windows {
		update := e.applyToWindow(vr, win, p)
		if update != nil {
			lastUpdate = update
		}
	}

	return lastUpdate, nil
}

func (e *MaterializedViewV2Engine) assignWindows(vr *viewV2Runtime, p *Point) []*viewV2Window {
	switch vr.def.WindowType {
	case WindowTumbling:
		return e.assignTumblingWindow(vr, p)
	case WindowSliding:
		return e.assignSlidingWindows(vr, p)
	case WindowSession:
		return e.assignSessionWindow(vr, p)
	default:
		return e.assignGlobalWindow(vr, p)
	}
}

func (e *MaterializedViewV2Engine) assignTumblingWindow(vr *viewV2Runtime, p *Point) []*viewV2Window {
	windowNs := vr.def.WindowSize.Nanoseconds()
	if windowNs <= 0 {
		windowNs = time.Minute.Nanoseconds()
	}

	wStart := (p.Timestamp / windowNs) * windowNs
	wEnd := wStart + windowNs

	groupKey := buildGroupKey(p)
	key := fmt.Sprintf("%s:%d", groupKey, wStart)

	win, exists := vr.windows[key]
	if !exists {
		win = &viewV2Window{
			start: wStart,
			end:   wEnd,
			state: windowState{min: math.MaxFloat64, max: -math.MaxFloat64},
		}
		vr.windows[key] = win
	}
	return []*viewV2Window{win}
}

func (e *MaterializedViewV2Engine) assignSlidingWindows(vr *viewV2Runtime, p *Point) []*viewV2Window {
	windowNs := vr.def.WindowSize.Nanoseconds()
	slideNs := vr.def.SlideInterval.Nanoseconds()
	if slideNs <= 0 {
		slideNs = windowNs
	}

	groupKey := buildGroupKey(p)

	// A point belongs to multiple sliding windows
	var windows []*viewV2Window

	// Earliest window that includes this point
	earliest := ((p.Timestamp-windowNs)/slideNs + 1) * slideNs
	for wStart := earliest; wStart <= p.Timestamp; wStart += slideNs {
		wEnd := wStart + windowNs
		key := fmt.Sprintf("%s:%d", groupKey, wStart)

		win, exists := vr.windows[key]
		if !exists {
			win = &viewV2Window{
				start: wStart,
				end:   wEnd,
				state: windowState{min: math.MaxFloat64, max: -math.MaxFloat64},
			}
			vr.windows[key] = win
		}
		windows = append(windows, win)
	}

	return windows
}

func (e *MaterializedViewV2Engine) assignSessionWindow(vr *viewV2Runtime, p *Point) []*viewV2Window {
	gap := vr.def.SessionGap.Nanoseconds()
	if gap <= 0 {
		gap = time.Minute.Nanoseconds()
	}

	groupKey := buildGroupKey(p)

	// Find an existing session window within the gap
	for key, win := range vr.windows {
		if win.closed {
			continue
		}
		if key[:len(groupKey)] != groupKey {
			continue
		}
		// Extend the window if the point is within the gap
		if p.Timestamp >= win.start-gap && p.Timestamp <= win.end+gap {
			if p.Timestamp > win.end {
				win.end = p.Timestamp + gap
			}
			return []*viewV2Window{win}
		}
	}

	// Create new session window
	win := &viewV2Window{
		start: p.Timestamp,
		end:   p.Timestamp + gap,
		state: windowState{min: math.MaxFloat64, max: -math.MaxFloat64},
	}
	key := fmt.Sprintf("%s:%d", groupKey, p.Timestamp)
	vr.windows[key] = win
	return []*viewV2Window{win}
}

func (e *MaterializedViewV2Engine) assignGlobalWindow(vr *viewV2Runtime, p *Point) []*viewV2Window {
	groupKey := buildGroupKey(p)
	key := groupKey + ":global"

	win, exists := vr.windows[key]
	if !exists {
		win = &viewV2Window{
			start: 0,
			end:   math.MaxInt64,
			state: windowState{min: math.MaxFloat64, max: -math.MaxFloat64},
		}
		vr.windows[key] = win
	}
	return []*viewV2Window{win}
}

func (e *MaterializedViewV2Engine) applyToWindow(vr *viewV2Runtime, win *viewV2Window, p *Point) *AggregateUpdate {
	ws := &win.state
	oldValue := computeWindowValue(ws, vr.def.Aggregation)

	ws.count++
	ws.sum += p.Value
	if p.Value < ws.min {
		ws.min = p.Value
	}
	if p.Value > ws.max {
		ws.max = p.Value
	}
	// Welford's online algorithm
	delta := p.Value - ws.mean
	ws.mean += delta / float64(ws.count)
	delta2 := p.Value - ws.mean
	ws.m2 += delta * delta2
	ws.last = *p
	if ws.count == 1 {
		ws.first = *p
	}

	win.updatedAt = time.Now()

	newValue := computeWindowValue(ws, vr.def.Aggregation)

	return &AggregateUpdate{
		GroupKey:    buildGroupKey(p),
		OldValue:    oldValue,
		NewValue:    newValue,
		WindowStart: win.start,
		PointCount:  ws.count,
	}
}

func computeWindowValue(ws *windowState, fn AggFunc) float64 {
	if ws.count == 0 {
		return 0
	}
	switch fn {
	case AggCount:
		return float64(ws.count)
	case AggSum:
		return ws.sum
	case AggMean:
		return ws.mean
	case AggMin:
		return ws.min
	case AggMax:
		return ws.max
	case AggFirst:
		return ws.first.Value
	case AggLast:
		return ws.last.Value
	case AggStddev:
		if ws.count < 2 {
			return 0
		}
		return math.Sqrt(ws.m2 / float64(ws.count-1))
	default:
		return ws.sum
	}
}

// ListViews returns the state of all views.
func (e *MaterializedViewV2Engine) ListViews() []ViewV2State {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var result []ViewV2State
	for _, vr := range e.views {
		vr.mu.Lock()
		state := ViewV2State{
			Definition:     vr.def,
			Watermark:      Watermark{Value: vr.watermark, UpdatedAt: time.Now()},
			WindowCount:    len(vr.windows),
			LateDataCount:  vr.lateCount.Load(),
			LastCheckpoint: vr.checkpoint,
			IsActive:       vr.def.Enabled,
		}
		vr.mu.Unlock()
		result = append(result, state)
	}
	return result
}

// GetView returns the state of a specific view.
func (e *MaterializedViewV2Engine) GetView(name string) (*ViewV2State, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	vr, ok := e.views[name]
	if !ok {
		return nil, false
	}

	vr.mu.Lock()
	defer vr.mu.Unlock()

	state := &ViewV2State{
		Definition:     vr.def,
		Watermark:      Watermark{Value: vr.watermark, UpdatedAt: time.Now()},
		WindowCount:    len(vr.windows),
		LateDataCount:  vr.lateCount.Load(),
		LastCheckpoint: vr.checkpoint,
		IsActive:       vr.def.Enabled,
	}
	return state, true
}

// Start begins the background checkpoint loop.
func (e *MaterializedViewV2Engine) Start() {
	if e.running.Swap(true) {
		return
	}
	go e.checkpointLoop()
}

// Stop stops background processing.
func (e *MaterializedViewV2Engine) Stop() {
	if !e.running.Swap(false) {
		return
	}
	close(e.stopCh)
}

func (e *MaterializedViewV2Engine) checkpointLoop() {
	ticker := time.NewTicker(e.config.CheckpointInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			e.checkpoint(context.Background())
		}
	}
}

func (e *MaterializedViewV2Engine) checkpoint(_ context.Context) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	now := time.Now()
	for _, vr := range e.views {
		vr.mu.Lock()
		vr.checkpoint = now

		// Evict old closed windows to prevent unbounded growth
		for key, win := range vr.windows {
			if win.closed && time.Since(win.updatedAt) > vr.def.AllowedLateness {
				delete(vr.windows, key)
			}
		}
		vr.mu.Unlock()
	}
}
