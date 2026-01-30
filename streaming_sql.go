package chronicle

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// StreamingSQLConfig configures the streaming SQL engine.
type StreamingSQLConfig struct {
	// Enabled enables streaming SQL
	Enabled bool `json:"enabled"`

	// BufferSize for internal channels
	BufferSize int `json:"buffer_size"`

	// WindowTimeout for tumbling windows
	WindowTimeout time.Duration `json:"window_timeout"`

	// MaxConcurrentQueries limits concurrent streaming queries
	MaxConcurrentQueries int `json:"max_concurrent_queries"`

	// StateStoreSize for aggregation state
	StateStoreSize int `json:"state_store_size"`

	// EmitInterval how often to emit results for continuous queries
	EmitInterval time.Duration `json:"emit_interval"`
}

// DefaultStreamingSQLConfig returns default configuration.
func DefaultStreamingSQLConfig() StreamingSQLConfig {
	return StreamingSQLConfig{
		Enabled:              true,
		BufferSize:           10000,
		WindowTimeout:        time.Minute,
		MaxConcurrentQueries: 100,
		StateStoreSize:       100000,
		EmitInterval:         time.Second,
	}
}

// StreamingSQLEngine provides KSQL-style streaming SQL capabilities.
type StreamingSQLEngine struct {
	db       *DB
	config   StreamingSQLConfig
	hub      *StreamHub
	mu       sync.RWMutex

	// Active streaming queries
	queries  map[string]*StreamingQuery
	queryMu  sync.RWMutex

	// State store for aggregations
	stateStore *StateStore

	// Background context
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// StreamingQuery represents an active streaming SQL query.
type StreamingQuery struct {
	ID          string                 `json:"id"`
	SQL         string                 `json:"sql"`
	Parsed      *ParsedStreamingSQL    `json:"parsed"`
	State       StreamingQueryState    `json:"state"`
	Created     time.Time              `json:"created"`
	LastEmit    time.Time              `json:"last_emit"`
	Results     chan *StreamingResult  `json:"-"`
	cancel      context.CancelFunc
	mu          sync.Mutex
}

// StreamingQueryState represents query execution state.
type StreamingQueryState int

const (
	StreamingQueryStateCreated StreamingQueryState = iota
	StreamingQueryStateRunning
	StreamingQueryStatePaused
	StreamingQueryStateStopped
	StreamingQueryStateError
)

func (s StreamingQueryState) String() string {
	switch s {
	case StreamingQueryStateCreated:
		return "created"
	case StreamingQueryStateRunning:
		return "running"
	case StreamingQueryStatePaused:
		return "paused"
	case StreamingQueryStateStopped:
		return "stopped"
	case StreamingQueryStateError:
		return "error"
	default:
		return "unknown"
	}
}

// ParsedStreamingSQL represents a parsed streaming SQL statement.
type ParsedStreamingSQL struct {
	Type        StreamingSQLType       `json:"type"`
	Source      string                 `json:"source"`
	Sink        string                 `json:"sink,omitempty"`
	Select      []SelectField          `json:"select"`
	Joins       []StreamJoin           `json:"joins,omitempty"`
	Where       *WhereClause           `json:"where,omitempty"`
	GroupBy     *GroupByClause         `json:"group_by,omitempty"`
	Window      *WindowClause          `json:"window,omitempty"`
	Having      *HavingClause          `json:"having,omitempty"`
	Emit        *EmitClause            `json:"emit,omitempty"`
}

// StreamingSQLType identifies the SQL statement type.
type StreamingSQLType int

const (
	StreamingSQLTypeSelect StreamingSQLType = iota
	StreamingSQLTypeCreateStream
	StreamingSQLTypeCreateTable
	StreamingSQLTypeInsertInto
)

// SelectField represents a field in SELECT.
type SelectField struct {
	Expression string `json:"expression"`
	Alias      string `json:"alias,omitempty"`
	Function   string `json:"function,omitempty"`
	Field      string `json:"field,omitempty"`
}

// StreamJoin represents a stream join.
type StreamJoin struct {
	Type      JoinType `json:"type"`
	Stream    string   `json:"stream"`
	Condition string   `json:"condition"`
	Window    string   `json:"window,omitempty"`
}

// JoinType identifies join types.
type JoinType int

const (
	JoinTypeInner JoinType = iota
	JoinTypeLeft
	JoinTypeOuter
)

// WhereClause represents WHERE conditions.
type WhereClause struct {
	Conditions []Condition `json:"conditions"`
}

// Condition represents a filter condition.
type Condition struct {
	Field    string      `json:"field"`
	Operator string      `json:"operator"`
	Value    interface{} `json:"value"`
	And      *Condition  `json:"and,omitempty"`
	Or       *Condition  `json:"or,omitempty"`
}

// GroupByClause represents GROUP BY.
type GroupByClause struct {
	Fields []string `json:"fields"`
}

// WindowClause represents window specifications.
type WindowClause struct {
	Type     StreamingWindowType `json:"type"`
	Size     time.Duration       `json:"size"`
	Advance  time.Duration       `json:"advance,omitempty"`
	Grace    time.Duration       `json:"grace,omitempty"`
}

// StreamingWindowType identifies window types for streaming SQL.
type StreamingWindowType int

const (
	StreamingWindowTumbling StreamingWindowType = iota
	StreamingWindowHopping
	StreamingWindowSession
	StreamingWindowSliding
)

func (w StreamingWindowType) String() string {
	switch w {
	case StreamingWindowTumbling:
		return "tumbling"
	case StreamingWindowHopping:
		return "hopping"
	case StreamingWindowSession:
		return "session"
	case StreamingWindowSliding:
		return "sliding"
	default:
		return "unknown"
	}
}

// HavingClause represents HAVING conditions.
type HavingClause struct {
	Conditions []Condition `json:"conditions"`
}

// EmitClause represents EMIT specifications.
type EmitClause struct {
	Type     EmitType      `json:"type"`
	Interval time.Duration `json:"interval,omitempty"`
}

// EmitType identifies emit types.
type EmitType int

const (
	EmitTypeChanges EmitType = iota
	EmitTypeFinal
	EmitTypeInterval
)

// StreamingResult represents a streaming query result.
type StreamingResult struct {
	QueryID   string                 `json:"query_id"`
	Timestamp int64                  `json:"timestamp"`
	WindowStart int64                `json:"window_start,omitempty"`
	WindowEnd   int64                `json:"window_end,omitempty"`
	GroupKey  string                 `json:"group_key,omitempty"`
	Values    map[string]interface{} `json:"values"`
	Type      ResultType             `json:"type"`
}

// ResultType identifies result types.
type ResultType int

const (
	ResultTypeUpdate ResultType = iota
	ResultTypeFinal
	ResultTypeHeartbeat
)

// StateStore manages aggregation state.
type StateStore struct {
	mu      sync.RWMutex
	windows map[string]*WindowState
	maxSize int
}

// WindowState holds state for a window.
type WindowState struct {
	Key       string
	Start     int64
	End       int64
	Values    map[string]float64
	Count     int64
	Sum       float64
	Min       float64
	Max       float64
	Created   time.Time
	Updated   time.Time
}

// NewStreamingSQLEngine creates a new streaming SQL engine.
func NewStreamingSQLEngine(db *DB, hub *StreamHub, config StreamingSQLConfig) *StreamingSQLEngine {
	ctx, cancel := context.WithCancel(context.Background())

	engine := &StreamingSQLEngine{
		db:      db,
		config:  config,
		hub:     hub,
		queries: make(map[string]*StreamingQuery),
		stateStore: &StateStore{
			windows: make(map[string]*WindowState),
			maxSize: config.StateStoreSize,
		},
		ctx:    ctx,
		cancel: cancel,
	}

	return engine
}

// Start starts the streaming SQL engine.
func (e *StreamingSQLEngine) Start() {
	e.wg.Add(1)
	go e.maintenanceLoop()
}

// Stop stops the streaming SQL engine.
func (e *StreamingSQLEngine) Stop() {
	e.cancel()
	e.wg.Wait()

	// Stop all queries
	e.queryMu.Lock()
	for _, q := range e.queries {
		q.cancel()
	}
	e.queryMu.Unlock()
}

func (e *StreamingSQLEngine) maintenanceLoop() {
	defer e.wg.Done()

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.cleanupExpiredWindows()
		}
	}
}

func (e *StreamingSQLEngine) cleanupExpiredWindows() {
	e.stateStore.mu.Lock()
	defer e.stateStore.mu.Unlock()

	cutoff := time.Now().Add(-e.config.WindowTimeout).UnixNano()

	for key, window := range e.stateStore.windows {
		if window.End < cutoff {
			delete(e.stateStore.windows, key)
		}
	}
}

// ExecuteSQL parses and executes a streaming SQL statement.
func (e *StreamingSQLEngine) ExecuteSQL(sql string) (*StreamingQuery, error) {
	parsed, err := e.ParseSQL(sql)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	return e.Execute(parsed, sql)
}

// Execute executes a parsed streaming SQL query.
func (e *StreamingSQLEngine) Execute(parsed *ParsedStreamingSQL, originalSQL string) (*StreamingQuery, error) {
	e.queryMu.Lock()
	if len(e.queries) >= e.config.MaxConcurrentQueries {
		e.queryMu.Unlock()
		return nil, errors.New("max concurrent queries reached")
	}
	e.queryMu.Unlock()

	ctx, cancel := context.WithCancel(e.ctx)

	query := &StreamingQuery{
		ID:      fmt.Sprintf("ssql-%d", time.Now().UnixNano()),
		SQL:     originalSQL,
		Parsed:  parsed,
		State:   StreamingQueryStateCreated,
		Created: time.Now(),
		Results: make(chan *StreamingResult, e.config.BufferSize),
		cancel:  cancel,
	}

	e.queryMu.Lock()
	e.queries[query.ID] = query
	e.queryMu.Unlock()

	// Start query execution
	go e.runQuery(ctx, query)

	return query, nil
}

func (e *StreamingSQLEngine) runQuery(ctx context.Context, query *StreamingQuery) {
	query.mu.Lock()
	query.State = StreamingQueryStateRunning
	query.mu.Unlock()

	defer func() {
		query.mu.Lock()
		if query.State == StreamingQueryStateRunning {
			query.State = StreamingQueryStateStopped
		}
		close(query.Results)
		query.mu.Unlock()
	}()

	// Subscribe to source stream
	sub := e.hub.Subscribe(query.Parsed.Source, nil)
	if sub == nil {
		query.mu.Lock()
		query.State = StreamingQueryStateError
		query.mu.Unlock()
		return
	}
	defer sub.Close()

	// Window state for this query
	windowStates := make(map[string]*WindowState)

	// Emit ticker if needed
	var emitTicker *time.Ticker
	emitInterval := e.config.EmitInterval
	if query.Parsed.Emit != nil && query.Parsed.Emit.Interval > 0 {
		emitInterval = query.Parsed.Emit.Interval
	}
	emitTicker = time.NewTicker(emitInterval)
	defer emitTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case point, ok := <-sub.C():
			if !ok {
				return
			}

			// Apply WHERE filter
			if query.Parsed.Where != nil && !e.matchesWhere(point, query.Parsed.Where) {
				continue
			}

			// Process aggregation
			if query.Parsed.GroupBy != nil || query.Parsed.Window != nil {
				e.processAggregation(query, point, windowStates)
			} else {
				// Direct projection
				result := e.projectPoint(query, point)
				select {
				case query.Results <- result:
				default:
					// Buffer full, drop
				}
			}

		case <-emitTicker.C:
			// Emit window results
			if query.Parsed.Window != nil {
				e.emitWindowResults(query, windowStates)
			}
		}
	}
}

func (e *StreamingSQLEngine) matchesWhere(point Point, where *WhereClause) bool {
	for _, cond := range where.Conditions {
		if !e.matchesCondition(point, cond) {
			return false
		}
	}
	return true
}

func (e *StreamingSQLEngine) matchesCondition(point Point, cond Condition) bool {
	var value interface{}

	switch cond.Field {
	case "value":
		value = point.Value
	case "metric":
		value = point.Metric
	default:
		// Check tags
		if v, ok := point.Tags[cond.Field]; ok {
			value = v
		} else {
			return false
		}
	}

	// Compare
	switch cond.Operator {
	case "=", "==":
		return fmt.Sprintf("%v", value) == fmt.Sprintf("%v", cond.Value)
	case "!=", "<>":
		return fmt.Sprintf("%v", value) != fmt.Sprintf("%v", cond.Value)
	case ">":
		if v, ok := value.(float64); ok {
			if cv, ok := cond.Value.(float64); ok {
				return v > cv
			}
		}
	case ">=":
		if v, ok := value.(float64); ok {
			if cv, ok := cond.Value.(float64); ok {
				return v >= cv
			}
		}
	case "<":
		if v, ok := value.(float64); ok {
			if cv, ok := cond.Value.(float64); ok {
				return v < cv
			}
		}
	case "<=":
		if v, ok := value.(float64); ok {
			if cv, ok := cond.Value.(float64); ok {
				return v <= cv
			}
		}
	case "LIKE":
		if s, ok := value.(string); ok {
			pattern := strings.ReplaceAll(fmt.Sprintf("%v", cond.Value), "%", ".*")
			matched, _ := regexp.MatchString(pattern, s)
			return matched
		}
	}

	return false
}

func (e *StreamingSQLEngine) processAggregation(query *StreamingQuery, point Point, windowStates map[string]*WindowState) {
	// Calculate window boundaries
	windowSize := e.config.WindowTimeout
	if query.Parsed.Window != nil {
		windowSize = query.Parsed.Window.Size
	}

	windowStart := (point.Timestamp / int64(windowSize)) * int64(windowSize)
	windowEnd := windowStart + int64(windowSize)

	// Build group key
	groupKey := ""
	if query.Parsed.GroupBy != nil {
		var parts []string
		for _, field := range query.Parsed.GroupBy.Fields {
			if v, ok := point.Tags[field]; ok {
				parts = append(parts, v)
			}
		}
		groupKey = strings.Join(parts, "|")
	}

	// State key combines window and group
	stateKey := fmt.Sprintf("%d:%s", windowStart, groupKey)

	// Get or create window state
	state, ok := windowStates[stateKey]
	if !ok {
		state = &WindowState{
			Key:     groupKey,
			Start:   windowStart,
			End:     windowEnd,
			Values:  make(map[string]float64),
			Min:     point.Value,
			Max:     point.Value,
			Created: time.Now(),
		}
		windowStates[stateKey] = state
	}

	// Update aggregations
	state.Count++
	state.Sum += point.Value
	if point.Value < state.Min {
		state.Min = point.Value
	}
	if point.Value > state.Max {
		state.Max = point.Value
	}
	state.Updated = time.Now()

	// Store field values for complex aggregations
	for _, sel := range query.Parsed.Select {
		if sel.Field != "" {
			state.Values[sel.Field] = point.Value
		}
	}
}

func (e *StreamingSQLEngine) emitWindowResults(query *StreamingQuery, windowStates map[string]*WindowState) {
	now := time.Now().UnixNano()

	for key, state := range windowStates {
		// Check if window has closed
		if state.End <= now {
			result := &StreamingResult{
				QueryID:     query.ID,
				Timestamp:   now,
				WindowStart: state.Start,
				WindowEnd:   state.End,
				GroupKey:    state.Key,
				Values:      make(map[string]interface{}),
				Type:        ResultTypeFinal,
			}

			// Calculate final aggregations
			for _, sel := range query.Parsed.Select {
				switch sel.Function {
				case "COUNT":
					result.Values[sel.Alias] = state.Count
				case "SUM":
					result.Values[sel.Alias] = state.Sum
				case "AVG", "MEAN":
					if state.Count > 0 {
						result.Values[sel.Alias] = state.Sum / float64(state.Count)
					}
				case "MIN":
					result.Values[sel.Alias] = state.Min
				case "MAX":
					result.Values[sel.Alias] = state.Max
				default:
					if v, ok := state.Values[sel.Field]; ok {
						result.Values[sel.Alias] = v
					}
				}
			}

			// Apply HAVING
			if query.Parsed.Having == nil || e.matchesHaving(result, query.Parsed.Having) {
				select {
				case query.Results <- result:
					query.mu.Lock()
					query.LastEmit = time.Now()
					query.mu.Unlock()
				default:
				}
			}

			// Remove closed window
			delete(windowStates, key)
		}
	}
}

func (e *StreamingSQLEngine) matchesHaving(result *StreamingResult, having *HavingClause) bool {
	for _, cond := range having.Conditions {
		value, ok := result.Values[cond.Field]
		if !ok {
			return false
		}

		condValue, _ := cond.Value.(float64)
		resultValue, _ := value.(float64)

		switch cond.Operator {
		case ">":
			if resultValue <= condValue {
				return false
			}
		case ">=":
			if resultValue < condValue {
				return false
			}
		case "<":
			if resultValue >= condValue {
				return false
			}
		case "<=":
			if resultValue > condValue {
				return false
			}
		case "=", "==":
			if resultValue != condValue {
				return false
			}
		}
	}
	return true
}

func (e *StreamingSQLEngine) projectPoint(query *StreamingQuery, point Point) *StreamingResult {
	result := &StreamingResult{
		QueryID:   query.ID,
		Timestamp: point.Timestamp,
		Values:    make(map[string]interface{}),
		Type:      ResultTypeUpdate,
	}

	for _, sel := range query.Parsed.Select {
		alias := sel.Alias
		if alias == "" {
			alias = sel.Expression
		}

		switch sel.Expression {
		case "*":
			result.Values["metric"] = point.Metric
			result.Values["value"] = point.Value
			for k, v := range point.Tags {
				result.Values[k] = v
			}
		case "value":
			result.Values[alias] = point.Value
		case "metric":
			result.Values[alias] = point.Metric
		case "timestamp":
			result.Values[alias] = point.Timestamp
		default:
			if v, ok := point.Tags[sel.Expression]; ok {
				result.Values[alias] = v
			}
		}
	}

	return result
}

// ParseSQL parses a streaming SQL statement.
func (e *StreamingSQLEngine) ParseSQL(sql string) (*ParsedStreamingSQL, error) {
	sql = strings.TrimSpace(sql)
	upper := strings.ToUpper(sql)

	parsed := &ParsedStreamingSQL{}

	// Determine statement type
	if strings.HasPrefix(upper, "CREATE STREAM") {
		parsed.Type = StreamingSQLTypeCreateStream
		return e.parseCreateStream(sql, parsed)
	} else if strings.HasPrefix(upper, "SELECT") {
		parsed.Type = StreamingSQLTypeSelect
		return e.parseSelect(sql, parsed)
	} else if strings.HasPrefix(upper, "INSERT INTO") {
		parsed.Type = StreamingSQLTypeInsertInto
		return e.parseInsertInto(sql, parsed)
	}

	return nil, errors.New("unsupported SQL statement")
}

func (e *StreamingSQLEngine) parseSelect(sql string, parsed *ParsedStreamingSQL) (*ParsedStreamingSQL, error) {
	upper := strings.ToUpper(sql)

	// Extract SELECT fields
	selectIdx := strings.Index(upper, "SELECT") + 6
	fromIdx := strings.Index(upper, "FROM")
	if fromIdx == -1 {
		return nil, errors.New("missing FROM clause")
	}

	selectPart := strings.TrimSpace(sql[selectIdx:fromIdx])
	parsed.Select = e.parseSelectFields(selectPart)

	// Extract FROM (source stream)
	remaining := sql[fromIdx+4:]
	parts := strings.Fields(remaining)
	if len(parts) == 0 {
		return nil, errors.New("missing source stream")
	}
	parsed.Source = parts[0]

	// Parse WHERE
	if whereIdx := strings.Index(upper, "WHERE"); whereIdx != -1 {
		whereEnd := len(sql)
		for _, keyword := range []string{"GROUP BY", "WINDOW", "HAVING", "EMIT"} {
			if idx := strings.Index(upper[whereIdx:], keyword); idx != -1 {
				if whereIdx+idx < whereEnd {
					whereEnd = whereIdx + idx
				}
			}
		}
		wherePart := strings.TrimSpace(sql[whereIdx+5 : whereEnd])
		parsed.Where = e.parseWhere(wherePart)
	}

	// Parse GROUP BY
	if groupIdx := strings.Index(upper, "GROUP BY"); groupIdx != -1 {
		groupEnd := len(sql)
		for _, keyword := range []string{"WINDOW", "HAVING", "EMIT"} {
			if idx := strings.Index(upper[groupIdx:], keyword); idx != -1 {
				if groupIdx+idx < groupEnd {
					groupEnd = groupIdx + idx
				}
			}
		}
		groupPart := strings.TrimSpace(sql[groupIdx+8 : groupEnd])
		parsed.GroupBy = &GroupByClause{
			Fields: strings.Split(groupPart, ","),
		}
		for i := range parsed.GroupBy.Fields {
			parsed.GroupBy.Fields[i] = strings.TrimSpace(parsed.GroupBy.Fields[i])
		}
	}

	// Parse WINDOW
	if windowIdx := strings.Index(upper, "WINDOW"); windowIdx != -1 {
		windowEnd := len(sql)
		for _, keyword := range []string{"HAVING", "EMIT"} {
			if idx := strings.Index(upper[windowIdx:], keyword); idx != -1 {
				if windowIdx+idx < windowEnd {
					windowEnd = windowIdx + idx
				}
			}
		}
		windowPart := strings.TrimSpace(sql[windowIdx+6 : windowEnd])
		parsed.Window = e.parseWindow(windowPart)
	}

	// Parse HAVING
	if havingIdx := strings.Index(upper, "HAVING"); havingIdx != -1 {
		havingEnd := len(sql)
		if emitIdx := strings.Index(upper[havingIdx:], "EMIT"); emitIdx != -1 {
			havingEnd = havingIdx + emitIdx
		}
		havingPart := strings.TrimSpace(sql[havingIdx+6 : havingEnd])
		parsed.Having = &HavingClause{
			Conditions: e.parseConditions(havingPart),
		}
	}

	// Parse EMIT
	if emitIdx := strings.Index(upper, "EMIT"); emitIdx != -1 {
		emitPart := strings.TrimSpace(sql[emitIdx+4:])
		parsed.Emit = e.parseEmit(emitPart)
	}

	return parsed, nil
}

func (e *StreamingSQLEngine) parseSelectFields(selectPart string) []SelectField {
	var fields []SelectField

	// Split by comma, handling functions
	parts := splitSelectFields(selectPart)

	for _, part := range parts {
		part = strings.TrimSpace(part)
		field := SelectField{Expression: part}

		// Check for alias
		upperPart := strings.ToUpper(part)
		if asIdx := strings.Index(upperPart, " AS "); asIdx != -1 {
			field.Alias = strings.TrimSpace(part[asIdx+4:])
			part = strings.TrimSpace(part[:asIdx])
			field.Expression = part
		}

		// Check for function
		if parenIdx := strings.Index(part, "("); parenIdx != -1 {
			field.Function = strings.ToUpper(part[:parenIdx])
			endParen := strings.Index(part, ")")
			if endParen > parenIdx {
				field.Field = strings.TrimSpace(part[parenIdx+1 : endParen])
			}
		} else {
			field.Field = part
		}

		if field.Alias == "" {
			if field.Function != "" {
				field.Alias = strings.ToLower(field.Function) + "_" + field.Field
			} else {
				field.Alias = field.Field
			}
		}

		fields = append(fields, field)
	}

	return fields
}

func splitSelectFields(s string) []string {
	var result []string
	var current strings.Builder
	depth := 0

	for _, c := range s {
		if c == '(' {
			depth++
		} else if c == ')' {
			depth--
		} else if c == ',' && depth == 0 {
			result = append(result, current.String())
			current.Reset()
			continue
		}
		current.WriteRune(c)
	}

	if current.Len() > 0 {
		result = append(result, current.String())
	}

	return result
}

func (e *StreamingSQLEngine) parseWhere(wherePart string) *WhereClause {
	return &WhereClause{
		Conditions: e.parseConditions(wherePart),
	}
}

func (e *StreamingSQLEngine) parseConditions(conditionStr string) []Condition {
	var conditions []Condition

	// Simple parsing: split by AND
	parts := strings.Split(strings.ToUpper(conditionStr), " AND ")
	for i, part := range parts {
		_ = i
		part = strings.TrimSpace(part)
		cond := e.parseCondition(part)
		if cond != nil {
			conditions = append(conditions, *cond)
		}
	}

	return conditions
}

func (e *StreamingSQLEngine) parseCondition(condStr string) *Condition {
	condStr = strings.TrimSpace(condStr)

	operators := []string{">=", "<=", "!=", "<>", "=", ">", "<", "LIKE"}

	for _, op := range operators {
		if idx := strings.Index(strings.ToUpper(condStr), op); idx != -1 {
			field := strings.TrimSpace(condStr[:idx])
			valueStr := strings.TrimSpace(condStr[idx+len(op):])
			valueStr = strings.Trim(valueStr, "'\"")

			var value interface{} = valueStr
			// Try to parse as number
			if f, ok := parseFloat(valueStr); ok {
				value = f
			}

			return &Condition{
				Field:    field,
				Operator: op,
				Value:    value,
			}
		}
	}

	return nil
}

func parseFloat(s string) (float64, bool) {
	var f float64
	_, err := fmt.Sscanf(s, "%f", &f)
	return f, err == nil
}

func (e *StreamingSQLEngine) parseWindow(windowPart string) *WindowClause {
	window := &WindowClause{}
	upper := strings.ToUpper(windowPart)

	if strings.Contains(upper, "TUMBLING") {
		window.Type = StreamingWindowTumbling
	} else if strings.Contains(upper, "HOPPING") {
		window.Type = StreamingWindowHopping
	} else if strings.Contains(upper, "SESSION") {
		window.Type = StreamingWindowSession
	} else if strings.Contains(upper, "SLIDING") {
		window.Type = StreamingWindowSliding
	}

	// Parse SIZE
	if sizeIdx := strings.Index(upper, "SIZE"); sizeIdx != -1 {
		window.Size = e.parseDuration(windowPart[sizeIdx+4:])
	}

	// Parse ADVANCE (for hopping)
	if advanceIdx := strings.Index(upper, "ADVANCE"); advanceIdx != -1 {
		window.Advance = e.parseDuration(windowPart[advanceIdx+7:])
	}

	// Parse GRACE
	if graceIdx := strings.Index(upper, "GRACE"); graceIdx != -1 {
		window.Grace = e.parseDuration(windowPart[graceIdx+5:])
	}

	if window.Size == 0 {
		window.Size = time.Minute // Default
	}

	return window
}

func (e *StreamingSQLEngine) parseDuration(s string) time.Duration {
	s = strings.TrimSpace(s)
	s = strings.ToUpper(s)

	// Extract number and unit
	var num int
	var unit string
	fmt.Sscanf(s, "%d %s", &num, &unit)
	if num == 0 {
		fmt.Sscanf(s, "%d%s", &num, &unit)
	}

	switch {
	case strings.HasPrefix(unit, "SEC"):
		return time.Duration(num) * time.Second
	case strings.HasPrefix(unit, "MIN"):
		return time.Duration(num) * time.Minute
	case strings.HasPrefix(unit, "HOUR"):
		return time.Duration(num) * time.Hour
	case strings.HasPrefix(unit, "DAY"):
		return time.Duration(num) * 24 * time.Hour
	default:
		return time.Duration(num) * time.Second
	}
}

func (e *StreamingSQLEngine) parseEmit(emitPart string) *EmitClause {
	emit := &EmitClause{}
	upper := strings.ToUpper(emitPart)

	if strings.Contains(upper, "CHANGES") {
		emit.Type = EmitTypeChanges
	} else if strings.Contains(upper, "FINAL") {
		emit.Type = EmitTypeFinal
	} else if strings.Contains(upper, "EVERY") {
		emit.Type = EmitTypeInterval
		emit.Interval = e.parseDuration(emitPart)
	}

	return emit
}

func (e *StreamingSQLEngine) parseCreateStream(sql string, parsed *ParsedStreamingSQL) (*ParsedStreamingSQL, error) {
	// CREATE STREAM name AS SELECT ...
	upper := strings.ToUpper(sql)

	asIdx := strings.Index(upper, " AS ")
	if asIdx == -1 {
		return nil, errors.New("CREATE STREAM requires AS clause")
	}

	// Extract stream name
	namePart := strings.TrimSpace(sql[14:asIdx]) // After "CREATE STREAM "
	parsed.Sink = namePart

	// Parse the SELECT part
	selectSQL := strings.TrimSpace(sql[asIdx+4:])
	return e.parseSelect(selectSQL, parsed)
}

func (e *StreamingSQLEngine) parseInsertInto(sql string, parsed *ParsedStreamingSQL) (*ParsedStreamingSQL, error) {
	// INSERT INTO sink SELECT ... FROM source
	upper := strings.ToUpper(sql)

	selectIdx := strings.Index(upper, "SELECT")
	if selectIdx == -1 {
		return nil, errors.New("INSERT INTO requires SELECT clause")
	}

	// Extract sink name
	sinkPart := strings.TrimSpace(sql[11:selectIdx]) // After "INSERT INTO "
	parsed.Sink = sinkPart

	// Parse the SELECT part
	selectSQL := strings.TrimSpace(sql[selectIdx:])
	return e.parseSelect(selectSQL, parsed)
}

// StopQuery stops a running query.
func (e *StreamingSQLEngine) StopQuery(queryID string) error {
	e.queryMu.Lock()
	query, ok := e.queries[queryID]
	if !ok {
		e.queryMu.Unlock()
		return errors.New("query not found")
	}
	delete(e.queries, queryID)
	e.queryMu.Unlock()

	query.cancel()
	query.mu.Lock()
	query.State = StreamingQueryStateStopped
	query.mu.Unlock()

	return nil
}

// GetQuery returns a query by ID.
func (e *StreamingSQLEngine) GetQuery(queryID string) (*StreamingQuery, bool) {
	e.queryMu.RLock()
	defer e.queryMu.RUnlock()
	q, ok := e.queries[queryID]
	return q, ok
}

// ListQueries returns all active queries.
func (e *StreamingSQLEngine) ListQueries() []*StreamingQuery {
	e.queryMu.RLock()
	defer e.queryMu.RUnlock()

	queries := make([]*StreamingQuery, 0, len(e.queries))
	for _, q := range e.queries {
		queries = append(queries, q)
	}

	sort.Slice(queries, func(i, j int) bool {
		return queries[i].Created.Before(queries[j].Created)
	})

	return queries
}

// GetStats returns streaming SQL engine statistics.
func (e *StreamingSQLEngine) GetStats() StreamingSQLStats {
	e.queryMu.RLock()
	queryCount := len(e.queries)
	e.queryMu.RUnlock()

	e.stateStore.mu.RLock()
	windowCount := len(e.stateStore.windows)
	e.stateStore.mu.RUnlock()

	return StreamingSQLStats{
		ActiveQueries:    queryCount,
		ActiveWindows:    windowCount,
		MaxConcurrent:    e.config.MaxConcurrentQueries,
		StateStoreSize:   e.config.StateStoreSize,
	}
}

// StreamingSQLStats contains engine statistics.
type StreamingSQLStats struct {
	ActiveQueries    int `json:"active_queries"`
	ActiveWindows    int `json:"active_windows"`
	MaxConcurrent    int `json:"max_concurrent"`
	StateStoreSize   int `json:"state_store_size"`
}
