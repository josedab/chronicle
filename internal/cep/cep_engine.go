package cep

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	chronicle "github.com/chronicle-db/chronicle"
)

// Start starts the CEP engine.
func (e *CEPEngine) Start() error {

	e.wg.Add(1)
	go e.watermarkLoop()

	e.wg.Add(1)
	go e.windowEvaluationLoop()

	e.wg.Add(1)
	go e.patternMatchingLoop()

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

	e.outputsMu.Lock()
	for _, ch := range e.outputs {
		close(ch)
	}
	e.outputsMu.Unlock()

	return nil
}

// ProcessEvent processes a single event through the CEP engine.
func (e *CEPEngine) ProcessEvent(p chronicle.Point) error {
	eventTime := p.Timestamp
	ingestTime := time.Now().UnixNano()

	event := windowedEvent{
		Point:      p,
		EventTime:  eventTime,
		IngestTime: ingestTime,
	}

	e.updateWatermark(eventTime)

	e.windowsMu.RLock()
	for _, window := range e.windows {
		if e.matchesWindow(p, window) {
			e.addToWindow(window, event)
		}
	}
	e.windowsMu.RUnlock()

	e.patternsMu.RLock()
	for _, pattern := range e.patterns {
		e.checkPattern(pattern, p)
	}
	e.patternsMu.RUnlock()

	return nil
}

// ProcessBatch processes multiple events.
func (e *CEPEngine) ProcessBatch(points []chronicle.Point) error {
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

	if query.SQL != "" {
		parsed, err := e.parseCEPSQL(query.SQL)
		if err != nil {
			return fmt.Errorf("failed to parse query: %w", err)
		}
		query.Parsed = parsed
	}

	if query.Parsed != nil && query.Parsed.Window.Size > 0 {
		windowConfig := WindowConfig{
			Type:    query.Parsed.Window.Type,
			Size:    query.Parsed.Window.Size,
			Slide:   query.Parsed.Window.Slide,
			Metric:  query.Parsed.From,
			GroupBy: query.Parsed.GroupBy,
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

	e.DeleteWindow(id + "-window")
}

func (e *CEPEngine) updateWatermark(eventTime int64) {
	e.waterMu.Lock()
	defer e.waterMu.Unlock()

	newWatermark := eventTime - int64(e.config.WatermarkDelay)
	if newWatermark > e.watermark {
		e.watermark = newWatermark
	}
}

func (e *CEPEngine) matchesWindow(p chronicle.Point, w *CEPWindow) bool {
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

	w.buffer = append(w.buffer, event)

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

	e.updatePaneState(pane, event)

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

		if pane.EndTime < watermark-int64(e.config.MaxLateDelay) {
			delete(w.panes, start)
		}
	}

	for _, pane := range closedPanes {
		result := e.computeWindowResult(w, pane)
		if result != nil {
			select {
			case w.output <- result:
			default:

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
		Metadata:    make(map[string]any),
	}

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

		}
	}
}

func (e *CEPEngine) checkPattern(pattern *EventPattern, p chronicle.Point) {

	key := fmt.Sprintf("pattern:%s:%s", pattern.Name, getPartitionKey(p, pattern.PartitionBy))
	state := e.getPatternState(key)

	currentStep := state.CurrentStep
	if currentStep >= len(pattern.Sequence) {
		state.CurrentStep = 0
		currentStep = 0
	}

	step := pattern.Sequence[currentStep]
	if e.matchesStep(p, step) {
		state.MatchedEvents = append(state.MatchedEvents, p)
		state.LastMatch = p.Timestamp

		if currentStep == len(pattern.Sequence)-1 {

			if pattern.OnMatch != nil {
				go pattern.OnMatch(state.MatchedEvents, nil)
			}

			e.emitPatternMatch(pattern, state.MatchedEvents)

			state.CurrentStep = 0
			state.MatchedEvents = nil
		} else {
			state.CurrentStep++
		}
	}

	if pattern.WithinTime > 0 && state.LastMatch > 0 {
		if p.Timestamp-state.LastMatch > int64(pattern.WithinTime) {

			state.CurrentStep = 0
			state.MatchedEvents = nil
		}
	}

	e.savePatternState(key, state)
}

func (e *CEPEngine) matchesStep(p chronicle.Point, step PatternStep) bool {
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

func (e *CEPEngine) emitPatternMatch(pattern *EventPattern, events []chronicle.Point) {
	result := &CEPResult{
		PatternID:     pattern.Name,
		Timestamp:     time.Now().UnixNano(),
		MatchedEvents: events,
		Values:        make(map[string]float64),
		Metadata:      make(map[string]any),
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

	e.windowsMu.RLock()
	windowState := make(map[string]any)
	for id, w := range e.windows {
		w.bufferMu.RLock()
		windowState[id] = map[string]any{
			"pane_count":  len(w.panes),
			"buffer_size": len(w.buffer),
		}
		w.bufferMu.RUnlock()
	}
	e.windowsMu.RUnlock()

	_ = windowState
}

// parseCEPSQL parses a CEP SQL query (simplified parser).
func (e *CEPEngine) parseCEPSQL(sql string) (*ParsedCEPQuery, error) {
	parsed := &ParsedCEPQuery{
		Select: make([]CEPSelectItem, 0),
	}

	selectRe := regexp.MustCompile(`(?i)SELECT\s+(.+?)\s+FROM`)
	fromRe := regexp.MustCompile(`(?i)FROM\s+(\w+)`)
	windowRe := regexp.MustCompile(`(?i)WINDOW\s+(\w+)\s*\(\s*(\d+)\s*(\w+)\s*\)`)
	groupByRe := regexp.MustCompile(`(?i)GROUP\s+BY\s+(.+?)(?:\s+WINDOW|\s+HAVING|\s*$)`)

	if matches := selectRe.FindStringSubmatch(sql); len(matches) > 1 {
		items := matches[1]

		parsed.Select = append(parsed.Select, CEPSelectItem{
			Expression: items,
		})
	}

	if matches := fromRe.FindStringSubmatch(sql); len(matches) > 1 {
		parsed.From = matches[1]
	}

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
		parsed.Window.Slide = duration
	}

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
