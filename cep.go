package chronicle

import (
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"sync"
	"time"
)

// CEPConfig configures the Complex Event Processing engine.
type CEPConfig struct {
	// MaxWindowSize is the maximum window size allowed.
	MaxWindowSize time.Duration

	// MaxPendingEvents is the maximum events buffered per window.
	MaxPendingEvents int

	// WatermarkDelay is how long to wait for late events.
	WatermarkDelay time.Duration

	// CheckpointInterval is how often to checkpoint state.
	CheckpointInterval time.Duration

	// EnableLateEvents allows processing of late-arriving events.
	EnableLateEvents bool

	// MaxLateDelay is the maximum delay for late events.
	MaxLateDelay time.Duration

	// OutputBufferSize is the size of the output channel buffer.
	OutputBufferSize int
}

// DefaultCEPConfig returns default CEP configuration.
func DefaultCEPConfig() CEPConfig {
	return CEPConfig{
		MaxWindowSize:      time.Hour,
		MaxPendingEvents:   100000,
		WatermarkDelay:     10 * time.Second,
		CheckpointInterval: time.Minute,
		EnableLateEvents:   true,
		MaxLateDelay:       time.Minute,
		OutputBufferSize:   10000,
	}
}

// CEPEngine provides Complex Event Processing capabilities.
type CEPEngine struct {
	db     *DB
	config CEPConfig

	// Registered patterns
	patterns   map[string]*EventPattern
	patternsMu sync.RWMutex

	// Windows
	windows   map[string]*CEPWindow
	windowsMu sync.RWMutex

	// Queries
	queries   map[string]*CEPQuery
	queriesMu sync.RWMutex

	// Watermark tracking
	watermark int64
	waterMu   sync.RWMutex

	// Output channels
	outputs   map[string]chan *CEPResult
	outputsMu sync.RWMutex

	// State management
	stateStore *CEPStateStore

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// EventPattern defines a pattern to match in event streams.
type EventPattern struct {
	Name        string
	Sequence    []PatternStep
	WithinTime  time.Duration
	Condition   string
	OnMatch     func(events []Point, context map[string]interface{})
	PartitionBy []string
}

// PatternStep defines a single step in an event pattern.
type PatternStep struct {
	Name       string
	Metric     string
	Tags       map[string]string
	Condition  func(Point) bool
	Quantifier StepQuantifier
}

// StepQuantifier defines how many times a step can match.
type StepQuantifier int

const (
	// QuantifierOne matches exactly one event.
	QuantifierOne StepQuantifier = iota
	// QuantifierOneOrMore matches one or more events.
	QuantifierOneOrMore
	// QuantifierZeroOrMore matches zero or more events.
	QuantifierZeroOrMore
	// QuantifierZeroOrOne matches zero or one event.
	QuantifierZeroOrOne
)

// CEPWindow represents a windowed computation.
type CEPWindow struct {
	ID       string
	Type     WindowType
	Size     time.Duration
	Slide    time.Duration
	Metric   string
	Tags     map[string]string
	Function AggFunc
	GroupBy  []string

	// State
	buffer    []windowedEvent
	bufferMu  sync.RWMutex
	lastEmit  time.Time
	panes     map[int64]*WindowPane
	nextPaneID int64

	// Output
	output chan *CEPResult
}

// WindowType defines the type of window.
type WindowType int

const (
	// WindowTumbling creates non-overlapping fixed-size windows.
	WindowTumbling WindowType = iota
	// WindowSliding creates overlapping windows.
	WindowSliding
	// WindowSession creates session-based windows with gap detection.
	WindowSession
	// WindowCount creates windows based on event count.
	WindowCount
)

func (w WindowType) String() string {
	switch w {
	case WindowTumbling:
		return "tumbling"
	case WindowSliding:
		return "sliding"
	case WindowSession:
		return "session"
	case WindowCount:
		return "count"
	default:
		return "unknown"
	}
}

// WindowPane represents a single pane in a window.
type WindowPane struct {
	ID        int64
	StartTime int64
	EndTime   int64
	Events    []windowedEvent
	State     map[string]float64
	Emitted   bool
}

type windowedEvent struct {
	Point     Point
	EventTime int64
	IngestTime int64
}

// CEPQuery represents a continuous query.
type CEPQuery struct {
	ID          string
	Name        string
	SQL         string
	Parsed      *ParsedCEPQuery
	Destination string
	Interval    time.Duration
	Window      *CEPWindow
	GroupBy     []string
	Having      string
	LastRun     time.Time
	Running     bool

	// Output
	output chan *CEPResult
}

// ParsedCEPQuery is a parsed CEP query.
type ParsedCEPQuery struct {
	Select    []CEPSelectItem
	From      string
	Where     []CEPCondition
	GroupBy   []string
	Having    *CEPCondition
	Window    CEPWindowSpec
	Emit      CEPEmitSpec
}

// CEPSelectItem is an item in the SELECT clause.
type CEPSelectItem struct {
	Expression string
	Alias      string
	Function   string
	Field      string
}

// CEPCondition is a WHERE/HAVING condition.
type CEPCondition struct {
	Field    string
	Operator string
	Value    interface{}
	And      *CEPCondition
	Or       *CEPCondition
}

// CEPWindowSpec specifies window parameters.
type CEPWindowSpec struct {
	Type     WindowType
	Size     time.Duration
	Slide    time.Duration
	GapTime  time.Duration
}

// CEPEmitSpec specifies when to emit results.
type CEPEmitSpec struct {
	OnWindowClose bool
	OnWatermark   bool
	OnInterval    time.Duration
	OnChange      bool
}

// CEPResult is the output of a CEP computation.
type CEPResult struct {
	QueryID     string                 `json:"query_id"`
	PatternID   string                 `json:"pattern_id,omitempty"`
	WindowStart int64                  `json:"window_start"`
	WindowEnd   int64                  `json:"window_end"`
	Timestamp   int64                  `json:"timestamp"`
	Values      map[string]float64     `json:"values"`
	GroupKey    map[string]string      `json:"group_key,omitempty"`
	MatchedEvents []Point              `json:"matched_events,omitempty"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// CEPStateStore manages state for CEP operations.
type CEPStateStore struct {
	states   map[string][]byte
	statesMu sync.RWMutex
}

// NewCEPEngine creates a new CEP engine.
func NewCEPEngine(db *DB, config CEPConfig) *CEPEngine {
	if config.MaxPendingEvents <= 0 {
		config.MaxPendingEvents = 100000
	}
	if config.OutputBufferSize <= 0 {
		config.OutputBufferSize = 10000
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &CEPEngine{
		db:         db,
		config:     config,
		patterns:   make(map[string]*EventPattern),
		windows:    make(map[string]*CEPWindow),
		queries:    make(map[string]*CEPQuery),
		outputs:    make(map[string]chan *CEPResult),
		stateStore: &CEPStateStore{states: make(map[string][]byte)},
		ctx:        ctx,
		cancel:     cancel,
	}
}

// Start starts the CEP engine.
func (e *CEPEngine) Start() error {
	// Start watermark advancement
	e.wg.Add(1)
	go e.watermarkLoop()

	// Start window evaluation
	e.wg.Add(1)
	go e.windowEvaluationLoop()

	// Start pattern matching
	e.wg.Add(1)
	go e.patternMatchingLoop()

	// Start checkpoint
	if e.config.CheckpointInterval > 0 {
		e.wg.Add(1)
		go e.checkpointLoop()
	}

	return nil
}

// Stop stops the CEP engine.
func (e *CEPEngine) Stop() error {
	e.cancel()
	e.wg.Wait()

	// Close all output channels
	e.outputsMu.Lock()
	for _, ch := range e.outputs {
		close(ch)
	}
	e.outputsMu.Unlock()

	return nil
}

// ProcessEvent processes a single event through the CEP engine.
func (e *CEPEngine) ProcessEvent(p Point) error {
	eventTime := p.Timestamp
	ingestTime := time.Now().UnixNano()

	event := windowedEvent{
		Point:      p,
		EventTime:  eventTime,
		IngestTime: ingestTime,
	}

	// Update watermark
	e.updateWatermark(eventTime)

	// Add to windows
	e.windowsMu.RLock()
	for _, window := range e.windows {
		if e.matchesWindow(p, window) {
			e.addToWindow(window, event)
		}
	}
	e.windowsMu.RUnlock()

	// Check patterns
	e.patternsMu.RLock()
	for _, pattern := range e.patterns {
		e.checkPattern(pattern, p)
	}
	e.patternsMu.RUnlock()

	return nil
}

// ProcessBatch processes multiple events.
func (e *CEPEngine) ProcessBatch(points []Point) error {
	for _, p := range points {
		if err := e.ProcessEvent(p); err != nil {
			return err
		}
	}
	return nil
}

// CreateWindow creates a new windowed computation.
func (e *CEPEngine) CreateWindow(id string, config WindowConfig) (*CEPWindow, error) {
	if id == "" {
		return nil, errors.New("window ID required")
	}

	window := &CEPWindow{
		ID:       id,
		Type:     config.Type,
		Size:     config.Size,
		Slide:    config.Slide,
		Metric:   config.Metric,
		Tags:     config.Tags,
		Function: config.Function,
		GroupBy:  config.GroupBy,
		buffer:   make([]windowedEvent, 0),
		panes:    make(map[int64]*WindowPane),
		output:   make(chan *CEPResult, e.config.OutputBufferSize),
	}

	if window.Slide == 0 {
		window.Slide = window.Size
	}

	e.windowsMu.Lock()
	e.windows[id] = window
	e.windowsMu.Unlock()

	e.outputsMu.Lock()
	e.outputs[id] = window.output
	e.outputsMu.Unlock()

	return window, nil
}

// WindowConfig configures a window.
type WindowConfig struct {
	Type     WindowType
	Size     time.Duration
	Slide    time.Duration
	Metric   string
	Tags     map[string]string
	Function AggFunc
	GroupBy  []string
}

// DeleteWindow removes a window.
func (e *CEPEngine) DeleteWindow(id string) {
	e.windowsMu.Lock()
	delete(e.windows, id)
	e.windowsMu.Unlock()

	e.outputsMu.Lock()
	if ch, ok := e.outputs[id]; ok {
		close(ch)
		delete(e.outputs, id)
	}
	e.outputsMu.Unlock()
}

// RegisterPattern registers an event pattern.
func (e *CEPEngine) RegisterPattern(pattern *EventPattern) error {
	if pattern.Name == "" {
		return errors.New("pattern name required")
	}
	if len(pattern.Sequence) == 0 {
		return errors.New("pattern sequence required")
	}

	e.patternsMu.Lock()
	e.patterns[pattern.Name] = pattern
	e.patternsMu.Unlock()

	return nil
}

// UnregisterPattern removes a pattern.
func (e *CEPEngine) UnregisterPattern(name string) {
	e.patternsMu.Lock()
	delete(e.patterns, name)
	e.patternsMu.Unlock()
}

// Subscribe returns a channel for receiving results.
func (e *CEPEngine) Subscribe(id string) <-chan *CEPResult {
	e.outputsMu.RLock()
	ch, ok := e.outputs[id]
	e.outputsMu.RUnlock()

	if !ok {
		// Create new output channel
		ch = make(chan *CEPResult, e.config.OutputBufferSize)
		e.outputsMu.Lock()
		e.outputs[id] = ch
		e.outputsMu.Unlock()
	}

	return ch
}

// RegisterQuery registers a continuous query.
func (e *CEPEngine) RegisterQuery(query *CEPQuery) error {
	if query.ID == "" {
		return errors.New("query ID required")
	}

	// Parse the SQL if provided
	if query.SQL != "" {
		parsed, err := e.parseCEPSQL(query.SQL)
		if err != nil {
			return fmt.Errorf("failed to parse query: %w", err)
		}
		query.Parsed = parsed
	}

	// Create associated window
	if query.Parsed != nil && query.Parsed.Window.Size > 0 {
		windowConfig := WindowConfig{
			Type:     query.Parsed.Window.Type,
			Size:     query.Parsed.Window.Size,
			Slide:    query.Parsed.Window.Slide,
			Metric:   query.Parsed.From,
			GroupBy:  query.Parsed.GroupBy,
		}

		window, err := e.CreateWindow(query.ID+"-window", windowConfig)
		if err != nil {
			return err
		}
		query.Window = window
	}

	query.output = make(chan *CEPResult, e.config.OutputBufferSize)

	e.queriesMu.Lock()
	e.queries[query.ID] = query
	e.queriesMu.Unlock()

	e.outputsMu.Lock()
	e.outputs[query.ID] = query.output
	e.outputsMu.Unlock()

	return nil
}

// UnregisterQuery removes a continuous query.
func (e *CEPEngine) UnregisterQuery(id string) {
	e.queriesMu.Lock()
	query, ok := e.queries[id]
	if ok {
		delete(e.queries, id)
	}
	e.queriesMu.Unlock()

	if ok && query.output != nil {
		close(query.output)
	}

	// Remove associated window
	e.DeleteWindow(id + "-window")
}

// Internal methods

func (e *CEPEngine) updateWatermark(eventTime int64) {
	e.waterMu.Lock()
	defer e.waterMu.Unlock()

	// Watermark = max event time - delay
	newWatermark := eventTime - int64(e.config.WatermarkDelay)
	if newWatermark > e.watermark {
		e.watermark = newWatermark
	}
}

func (e *CEPEngine) matchesWindow(p Point, w *CEPWindow) bool {
	if w.Metric != "" && w.Metric != p.Metric {
		return false
	}
	for k, v := range w.Tags {
		if p.Tags[k] != v {
			return false
		}
	}
	return true
}

func (e *CEPEngine) addToWindow(w *CEPWindow, event windowedEvent) {
	w.bufferMu.Lock()
	defer w.bufferMu.Unlock()

	// Add to buffer
	w.buffer = append(w.buffer, event)

	// Determine pane
	paneStart := (event.EventTime / int64(w.Size)) * int64(w.Size)
	pane, ok := w.panes[paneStart]
	if !ok {
		pane = &WindowPane{
			ID:        w.nextPaneID,
			StartTime: paneStart,
			EndTime:   paneStart + int64(w.Size),
			Events:    make([]windowedEvent, 0),
			State:     make(map[string]float64),
		}
		w.nextPaneID++
		w.panes[paneStart] = pane
	}

	pane.Events = append(pane.Events, event)

	// Update incremental state
	e.updatePaneState(pane, event)

	// Limit buffer size
	if len(w.buffer) > e.config.MaxPendingEvents {
		w.buffer = w.buffer[len(w.buffer)-e.config.MaxPendingEvents:]
	}
}

func (e *CEPEngine) updatePaneState(pane *WindowPane, event windowedEvent) {
	value := event.Point.Value

	count, _ := pane.State["count"]
	sum, _ := pane.State["sum"]
	min, hasMin := pane.State["min"]
	max, hasMax := pane.State["max"]

	pane.State["count"] = count + 1
	pane.State["sum"] = sum + value

	if !hasMin || value < min {
		pane.State["min"] = value
	}
	if !hasMax || value > max {
		pane.State["max"] = value
	}

	// Update mean
	if pane.State["count"] > 0 {
		pane.State["mean"] = pane.State["sum"] / pane.State["count"]
	}
}

func (e *CEPEngine) watermarkLoop() {
	defer e.wg.Done()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.advanceWatermark()
		}
	}
}

func (e *CEPEngine) advanceWatermark() {
	e.waterMu.RLock()
	watermark := e.watermark
	e.waterMu.RUnlock()

	// Trigger window evaluations for closed windows
	e.windowsMu.RLock()
	windows := make([]*CEPWindow, 0, len(e.windows))
	for _, w := range e.windows {
		windows = append(windows, w)
	}
	e.windowsMu.RUnlock()

	for _, w := range windows {
		e.evaluateWindow(w, watermark)
	}
}

func (e *CEPEngine) windowEvaluationLoop() {
	defer e.wg.Done()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.evaluateAllWindows()
		}
	}
}

func (e *CEPEngine) evaluateAllWindows() {
	e.waterMu.RLock()
	watermark := e.watermark
	e.waterMu.RUnlock()

	e.windowsMu.RLock()
	for _, w := range e.windows {
		e.evaluateWindow(w, watermark)
	}
	e.windowsMu.RUnlock()
}

func (e *CEPEngine) evaluateWindow(w *CEPWindow, watermark int64) {
	w.bufferMu.Lock()
	defer w.bufferMu.Unlock()

	// Find closed panes
	var closedPanes []*WindowPane
	for start, pane := range w.panes {
		if pane.EndTime <= watermark && !pane.Emitted {
			closedPanes = append(closedPanes, pane)
			pane.Emitted = true
		}
		// Clean up old panes
		if pane.EndTime < watermark-int64(e.config.MaxLateDelay) {
			delete(w.panes, start)
		}
	}

	// Emit results for closed panes
	for _, pane := range closedPanes {
		result := e.computeWindowResult(w, pane)
		if result != nil {
			select {
			case w.output <- result:
			default:
				// Buffer full, drop result
			}
		}
	}
}

func (e *CEPEngine) computeWindowResult(w *CEPWindow, pane *WindowPane) *CEPResult {
	if len(pane.Events) == 0 {
		return nil
	}

	result := &CEPResult{
		QueryID:     w.ID,
		WindowStart: pane.StartTime,
		WindowEnd:   pane.EndTime,
		Timestamp:   time.Now().UnixNano(),
		Values:      make(map[string]float64),
		Metadata:    make(map[string]interface{}),
	}

	// Copy state values
	for k, v := range pane.State {
		result.Values[k] = v
	}

	result.Metadata["event_count"] = len(pane.Events)
	result.Metadata["window_type"] = w.Type.String()

	return result
}

func (e *CEPEngine) patternMatchingLoop() {
	defer e.wg.Done()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			// Pattern matching is done incrementally in ProcessEvent
		}
	}
}

func (e *CEPEngine) checkPattern(pattern *EventPattern, p Point) {
	// Get pattern state
	key := fmt.Sprintf("pattern:%s:%s", pattern.Name, getPartitionKey(p, pattern.PartitionBy))
	state := e.getPatternState(key)

	// Check if event matches current step
	currentStep := state.CurrentStep
	if currentStep >= len(pattern.Sequence) {
		state.CurrentStep = 0
		currentStep = 0
	}

	step := pattern.Sequence[currentStep]
	if e.matchesStep(p, step) {
		state.MatchedEvents = append(state.MatchedEvents, p)
		state.LastMatch = p.Timestamp

		// Check if pattern is complete
		if currentStep == len(pattern.Sequence)-1 {
			// Pattern matched!
			if pattern.OnMatch != nil {
				go pattern.OnMatch(state.MatchedEvents, nil)
			}

			// Emit result
			e.emitPatternMatch(pattern, state.MatchedEvents)

			// Reset state
			state.CurrentStep = 0
			state.MatchedEvents = nil
		} else {
			state.CurrentStep++
		}
	}

	// Check timeout
	if pattern.WithinTime > 0 && state.LastMatch > 0 {
		if p.Timestamp-state.LastMatch > int64(pattern.WithinTime) {
			// Timeout, reset
			state.CurrentStep = 0
			state.MatchedEvents = nil
		}
	}

	e.savePatternState(key, state)
}

func (e *CEPEngine) matchesStep(p Point, step PatternStep) bool {
	if step.Metric != "" && step.Metric != p.Metric {
		return false
	}
	for k, v := range step.Tags {
		if p.Tags[k] != v {
			return false
		}
	}
	if step.Condition != nil && !step.Condition(p) {
		return false
	}
	return true
}

func (e *CEPEngine) emitPatternMatch(pattern *EventPattern, events []Point) {
	result := &CEPResult{
		PatternID:     pattern.Name,
		Timestamp:     time.Now().UnixNano(),
		MatchedEvents: events,
		Values:        make(map[string]float64),
		Metadata:      make(map[string]interface{}),
	}

	if len(events) > 0 {
		result.WindowStart = events[0].Timestamp
		result.WindowEnd = events[len(events)-1].Timestamp
	}

	result.Metadata["pattern_name"] = pattern.Name
	result.Metadata["match_count"] = len(events)

	e.outputsMu.RLock()
	if ch, ok := e.outputs[pattern.Name]; ok {
		select {
		case ch <- result:
		default:
		}
	}
	e.outputsMu.RUnlock()
}

type patternState struct {
	CurrentStep   int
	MatchedEvents []Point
	LastMatch     int64
}

func (e *CEPEngine) getPatternState(key string) *patternState {
	e.stateStore.statesMu.RLock()
	data, ok := e.stateStore.states[key]
	e.stateStore.statesMu.RUnlock()

	if !ok {
		return &patternState{}
	}

	var state patternState
	if err := json.Unmarshal(data, &state); err != nil {
		return &patternState{}
	}
	return &state
}

func (e *CEPEngine) savePatternState(key string, state *patternState) {
	data, err := json.Marshal(state)
	if err != nil {
		return
	}

	e.stateStore.statesMu.Lock()
	e.stateStore.states[key] = data
	e.stateStore.statesMu.Unlock()
}

func getPartitionKey(p Point, partitionBy []string) string {
	if len(partitionBy) == 0 {
		return "default"
	}

	key := ""
	for _, field := range partitionBy {
		if v, ok := p.Tags[field]; ok {
			key += field + "=" + v + ","
		}
	}
	return key
}

func (e *CEPEngine) checkpointLoop() {
	defer e.wg.Done()

	ticker := time.NewTicker(e.config.CheckpointInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.checkpoint()
		}
	}
}

func (e *CEPEngine) checkpoint() {
	// Checkpoint window state
	e.windowsMu.RLock()
	windowState := make(map[string]interface{})
	for id, w := range e.windows {
		w.bufferMu.RLock()
		windowState[id] = map[string]interface{}{
			"pane_count":  len(w.panes),
			"buffer_size": len(w.buffer),
		}
		w.bufferMu.RUnlock()
	}
	e.windowsMu.RUnlock()

	// Store checkpoint (in production, would persist to disk/remote storage)
	_ = windowState
}

// parseCEPSQL parses a CEP SQL query (simplified parser).
func (e *CEPEngine) parseCEPSQL(sql string) (*ParsedCEPQuery, error) {
	parsed := &ParsedCEPQuery{
		Select: make([]CEPSelectItem, 0),
	}

	// Simple regex-based parsing (production would use a proper parser)
	selectRe := regexp.MustCompile(`(?i)SELECT\s+(.+?)\s+FROM`)
	fromRe := regexp.MustCompile(`(?i)FROM\s+(\w+)`)
	windowRe := regexp.MustCompile(`(?i)WINDOW\s+(\w+)\s*\(\s*(\d+)\s*(\w+)\s*\)`)
	groupByRe := regexp.MustCompile(`(?i)GROUP\s+BY\s+(.+?)(?:\s+WINDOW|\s+HAVING|\s*$)`)

	// Parse SELECT
	if matches := selectRe.FindStringSubmatch(sql); len(matches) > 1 {
		items := matches[1]
		// Parse individual select items
		parsed.Select = append(parsed.Select, CEPSelectItem{
			Expression: items,
		})
	}

	// Parse FROM
	if matches := fromRe.FindStringSubmatch(sql); len(matches) > 1 {
		parsed.From = matches[1]
	}

	// Parse WINDOW
	if matches := windowRe.FindStringSubmatch(sql); len(matches) > 3 {
		windowType := matches[1]
		var duration time.Duration
		switch matches[3] {
		case "s", "second", "seconds":
			duration, _ = time.ParseDuration(matches[2] + "s")
		case "m", "minute", "minutes":
			duration, _ = time.ParseDuration(matches[2] + "m")
		case "h", "hour", "hours":
			duration, _ = time.ParseDuration(matches[2] + "h")
		}

		switch windowType {
		case "TUMBLING", "tumbling":
			parsed.Window.Type = WindowTumbling
		case "SLIDING", "sliding":
			parsed.Window.Type = WindowSliding
		case "SESSION", "session":
			parsed.Window.Type = WindowSession
		}
		parsed.Window.Size = duration
		parsed.Window.Slide = duration // Default
	}

	// Parse GROUP BY
	if matches := groupByRe.FindStringSubmatch(sql); len(matches) > 1 {
		parsed.GroupBy = []string{matches[1]}
	}

	return parsed, nil
}

// JoinWindow creates a temporal join between two streams.
func (e *CEPEngine) JoinWindow(left, right string, joinKey []string, withinTime time.Duration) (*CEPWindow, error) {
	joinID := fmt.Sprintf("join-%s-%s", left, right)

	window := &CEPWindow{
		ID:      joinID,
		Type:    WindowSliding,
		Size:    withinTime,
		Slide:   withinTime / 10,
		buffer:  make([]windowedEvent, 0),
		panes:   make(map[int64]*WindowPane),
		output:  make(chan *CEPResult, e.config.OutputBufferSize),
		GroupBy: joinKey,
	}

	e.windowsMu.Lock()
	e.windows[joinID] = window
	e.windowsMu.Unlock()

	e.outputsMu.Lock()
	e.outputs[joinID] = window.output
	e.outputsMu.Unlock()

	return window, nil
}

// Stats returns CEP engine statistics.
func (e *CEPEngine) Stats() CEPStats {
	e.windowsMu.RLock()
	windowCount := len(e.windows)
	e.windowsMu.RUnlock()

	e.patternsMu.RLock()
	patternCount := len(e.patterns)
	e.patternsMu.RUnlock()

	e.queriesMu.RLock()
	queryCount := len(e.queries)
	e.queriesMu.RUnlock()

	e.waterMu.RLock()
	watermark := e.watermark
	e.waterMu.RUnlock()

	return CEPStats{
		WindowCount:  windowCount,
		PatternCount: patternCount,
		QueryCount:   queryCount,
		Watermark:    watermark,
	}
}

// CEPStats contains CEP engine statistics.
type CEPStats struct {
	WindowCount  int   `json:"window_count"`
	PatternCount int   `json:"pattern_count"`
	QueryCount   int   `json:"query_count"`
	Watermark    int64 `json:"watermark"`
}

// CEPDB wraps a DB with CEP capabilities.
type CEPDB struct {
	*DB
	cep *CEPEngine
}

// NewCEPDB creates a CEP-enabled database wrapper.
func NewCEPDB(db *DB, config CEPConfig) (*CEPDB, error) {
	cep := NewCEPEngine(db, config)

	return &CEPDB{
		DB:  db,
		cep: cep,
	}, nil
}

// Start starts the CEP engine.
func (c *CEPDB) Start() error {
	return c.cep.Start()
}

// Stop stops the CEP engine.
func (c *CEPDB) Stop() error {
	return c.cep.Stop()
}

// Write writes a point and processes it through CEP.
func (c *CEPDB) Write(p Point) error {
	if err := c.DB.Write(p); err != nil {
		return err
	}
	return c.cep.ProcessEvent(p)
}

// CEP returns the underlying CEP engine.
func (c *CEPDB) CEP() *CEPEngine {
	return c.cep
}

// ========== Priority Queue for Event Ordering ==========

type eventHeap []windowedEvent

func (h eventHeap) Len() int           { return len(h) }
func (h eventHeap) Less(i, j int) bool { return h[i].EventTime < h[j].EventTime }
func (h eventHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *eventHeap) Push(x interface{}) {
	*h = append(*h, x.(windowedEvent))
}

func (h *eventHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

var _ heap.Interface = (*eventHeap)(nil)

// ========== Pattern Builder ==========

// PatternBuilder provides a fluent API for building event patterns.
type PatternBuilder struct {
	pattern *EventPattern
}

// NewPattern creates a new pattern builder.
func NewPattern(name string) *PatternBuilder {
	return &PatternBuilder{
		pattern: &EventPattern{
			Name:     name,
			Sequence: make([]PatternStep, 0),
		},
	}
}

// Begin adds the first step in the pattern.
func (b *PatternBuilder) Begin(name string) *PatternBuilder {
	b.pattern.Sequence = append(b.pattern.Sequence, PatternStep{
		Name:       name,
		Quantifier: QuantifierOne,
	})
	return b
}

// FollowedBy adds a step that must follow the previous step.
func (b *PatternBuilder) FollowedBy(name string) *PatternBuilder {
	b.pattern.Sequence = append(b.pattern.Sequence, PatternStep{
		Name:       name,
		Quantifier: QuantifierOne,
	})
	return b
}

// Where adds a condition to the current step.
func (b *PatternBuilder) Where(metric string, condition func(Point) bool) *PatternBuilder {
	if len(b.pattern.Sequence) > 0 {
		step := &b.pattern.Sequence[len(b.pattern.Sequence)-1]
		step.Metric = metric
		step.Condition = condition
	}
	return b
}

// WithTags adds tag filters to the current step.
func (b *PatternBuilder) WithTags(tags map[string]string) *PatternBuilder {
	if len(b.pattern.Sequence) > 0 {
		b.pattern.Sequence[len(b.pattern.Sequence)-1].Tags = tags
	}
	return b
}

// Within sets the maximum time for the entire pattern.
func (b *PatternBuilder) Within(duration time.Duration) *PatternBuilder {
	b.pattern.WithinTime = duration
	return b
}

// PartitionBy sets the partition key for the pattern.
func (b *PatternBuilder) PartitionBy(fields ...string) *PatternBuilder {
	b.pattern.PartitionBy = fields
	return b
}

// OnMatch sets the callback for pattern matches.
func (b *PatternBuilder) OnMatch(fn func(events []Point, context map[string]interface{})) *PatternBuilder {
	b.pattern.OnMatch = fn
	return b
}

// Build returns the constructed pattern.
func (b *PatternBuilder) Build() *EventPattern {
	return b.pattern
}

// ========== Window Builder ==========

// WindowBuilder provides a fluent API for creating windows.
type WindowBuilder struct {
	config WindowConfig
}

// NewWindowBuilder creates a new window builder.
func NewWindowBuilder() *WindowBuilder {
	return &WindowBuilder{
		config: WindowConfig{
			Type: WindowTumbling,
		},
	}
}

// Tumbling creates a tumbling window.
func (b *WindowBuilder) Tumbling(size time.Duration) *WindowBuilder {
	b.config.Type = WindowTumbling
	b.config.Size = size
	return b
}

// Sliding creates a sliding window.
func (b *WindowBuilder) Sliding(size, slide time.Duration) *WindowBuilder {
	b.config.Type = WindowSliding
	b.config.Size = size
	b.config.Slide = slide
	return b
}

// Session creates a session window.
func (b *WindowBuilder) Session(gap time.Duration) *WindowBuilder {
	b.config.Type = WindowSession
	b.config.Size = gap
	return b
}

// OnMetric sets the metric filter.
func (b *WindowBuilder) OnMetric(metric string) *WindowBuilder {
	b.config.Metric = metric
	return b
}

// WithTags sets tag filters.
func (b *WindowBuilder) WithTags(tags map[string]string) *WindowBuilder {
	b.config.Tags = tags
	return b
}

// Aggregate sets the aggregation function.
func (b *WindowBuilder) Aggregate(fn AggFunc) *WindowBuilder {
	b.config.Function = fn
	return b
}

// GroupBy sets grouping fields.
func (b *WindowBuilder) GroupBy(fields ...string) *WindowBuilder {
	b.config.GroupBy = fields
	return b
}

// Build returns the window configuration.
func (b *WindowBuilder) Build() WindowConfig {
	return b.config
}

// Sorted helper for deterministic output
type byTimestamp []Point

func (a byTimestamp) Len() int           { return len(a) }
func (a byTimestamp) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byTimestamp) Less(i, j int) bool { return a[i].Timestamp < a[j].Timestamp }

var _ sort.Interface = byTimestamp{}
