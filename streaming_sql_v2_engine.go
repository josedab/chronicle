package chronicle

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// StreamingSQLV2Engine – main engine
// ---------------------------------------------------------------------------

// v2Query wraps a streaming query with v2-specific state.
type v2Query struct {
	StreamingQuery
	watermark  *StreamWatermark
	aggregator *WindowedAggregator
	results    []*StreamingResult
	resultMu   sync.Mutex
}

// StreamingSQLV2Engine provides SQL:2016 windowed operations, stream-to-stream
// joins, watermark-based late data handling, and exactly-once semantics.
type StreamingSQLV2Engine struct {
	db         *DB
	config     StreamingSQLV2Config
	hub        *StreamHub
	mu         sync.RWMutex
	running    bool
	stopCh     chan struct{}

	queries    map[string]*v2Query
	joinEngine *StreamJoinEngine
	processor  *ExactlyOnceProcessor

	// Latest checkpoint per query
	checkpoints   map[string]*ExactlyOnceCheckpoint
	checkpointMu  sync.RWMutex
}

// NewStreamingSQLV2Engine constructs a new v2 streaming SQL engine.
func NewStreamingSQLV2Engine(db *DB, hub *StreamHub, config StreamingSQLV2Config) *StreamingSQLV2Engine {
	return &StreamingSQLV2Engine{
		db:          db,
		config:      config,
		hub:         hub,
		queries:     make(map[string]*v2Query),
		joinEngine:  NewStreamJoinEngine(config.JoinBufferSize, config.JoinTimeout),
		processor:   NewExactlyOnceProcessor(100000),
		checkpoints: make(map[string]*ExactlyOnceCheckpoint),
	}
}

// Start begins background goroutines for window triggering, expiry, buffer
// cleanup, and periodic checkpointing.
func (e *StreamingSQLV2Engine) Start() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.running {
		return fmt.Errorf("streaming SQL v2 engine already running")
	}
	if !e.config.Enabled {
		return fmt.Errorf("streaming SQL v2 engine is disabled")
	}
	e.running = true
	e.stopCh = make(chan struct{})
	go e.triggerLoop()
	go e.expiryLoop()
	go e.joinCleanupLoop()
	if e.config.ExactlyOnceEnabled {
		go e.checkpointLoop()
	}
	return nil
}

// Stop gracefully shuts down the engine.
func (e *StreamingSQLV2Engine) Stop() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return nil
	}
	close(e.stopCh)
	e.running = false
	return nil
}

// triggerLoop periodically fires eligible windows based on watermark progress.
func (e *StreamingSQLV2Engine) triggerLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			e.mu.RLock()
			for _, q := range e.queries {
				if q.State != StreamingQueryState(1) { // Running
					continue
				}
				results := q.aggregator.TriggerEligible()
				if len(results) > 0 {
					q.resultMu.Lock()
					q.results = append(q.results, results...)
					q.resultMu.Unlock()
				}
			}
			e.mu.RUnlock()
		}
	}
}

// expiryLoop periodically removes expired windows.
func (e *StreamingSQLV2Engine) expiryLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			e.mu.RLock()
			for _, q := range e.queries {
				q.aggregator.ExpireWindows()
			}
			e.mu.RUnlock()
		}
	}
}

// joinCleanupLoop expires old join buffers periodically.
func (e *StreamingSQLV2Engine) joinCleanupLoop() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			e.joinEngine.ExpireBuffers()
		}
	}
}

// checkpointLoop writes periodic checkpoints for all active queries.
func (e *StreamingSQLV2Engine) checkpointLoop() {
	ticker := time.NewTicker(e.config.CheckpointInterval)
	defer ticker.Stop()
	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			_ = e.Checkpoint(context.Background()) //nolint:errcheck // best-effort cleanup on shutdown
		}
	}
}

// CreateWindowedQuery creates a new SQL:2016 windowed streaming query.
func (e *StreamingSQLV2Engine) CreateWindowedQuery(sql string) (*v2Query, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(e.queries) >= e.config.MaxConcurrentQueries {
		return nil, fmt.Errorf("max concurrent queries reached (%d)", e.config.MaxConcurrentQueries)
	}

	parsed := parseWindowedSQL(sql)
	wm := NewStreamWatermark(e.config.MaxWatermarkLag, e.config.LateDataGracePeriod)
	agg := NewWindowedAggregator(
		parsed.Window.Type,
		parsed.Window.Size,
		parsed.Window.Advance,
		wm,
		e.config.MaxWindowsPerQuery,
		e.config.WindowRetention,
	)

	id := fmt.Sprintf("v2q_%d", time.Now().UnixNano())
	q := &v2Query{
		StreamingQuery: StreamingQuery{
			ID:      id,
			SQL:     sql,
			Parsed:  parsed,
			State:   StreamingQueryState(1), // Running
			Created: time.Now(),
		},
		watermark:  wm,
		aggregator: agg,
		results:    make([]*StreamingResult, 0),
	}
	e.queries[id] = q

	if e.hub != nil && parsed.Source != "" {
		go e.subscribeAndProcess(q)
	}

	return q, nil
}

// CreateJoinQuery creates a stream-to-stream join query.
func (e *StreamingSQLV2Engine) CreateJoinQuery(sql string) (*v2Query, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(e.queries) >= e.config.MaxConcurrentQueries {
		return nil, fmt.Errorf("max concurrent queries reached (%d)", e.config.MaxConcurrentQueries)
	}

	parsed := parseJoinSQL(sql)
	wm := NewStreamWatermark(e.config.MaxWatermarkLag, e.config.LateDataGracePeriod)
	agg := NewWindowedAggregator(
		StreamingWindowTumbling,
		time.Minute,
		0,
		wm,
		e.config.MaxWindowsPerQuery,
		e.config.WindowRetention,
	)

	id := fmt.Sprintf("v2j_%d", time.Now().UnixNano())
	q := &v2Query{
		StreamingQuery: StreamingQuery{
			ID:      id,
			SQL:     sql,
			Parsed:  parsed,
			State:   StreamingQueryState(1), // Running
			Created: time.Now(),
		},
		watermark:  wm,
		aggregator: agg,
		results:    make([]*StreamingResult, 0),
	}

	if len(parsed.Joins) > 0 {
		j := parsed.Joins[0]
		joinType := j.Type
		windowSize := time.Minute
		if j.Window != "" {
			if d, err := time.ParseDuration(j.Window); err == nil {
				windowSize = d
			}
		}
		e.joinEngine.RegisterJoin(id, parsed.Source, j.Stream, joinType, j.Condition, windowSize)
	}

	e.queries[id] = q

	if e.hub != nil {
		go e.subscribeJoin(q)
	}

	return q, nil
}

// EmitResults returns and drains the latest results for a query.
func (e *StreamingSQLV2Engine) EmitResults(queryID string) []*StreamingResult {
	e.mu.RLock()
	q, ok := e.queries[queryID]
	e.mu.RUnlock()
	if !ok {
		return nil
	}
	q.resultMu.Lock()
	defer q.resultMu.Unlock()
	out := q.results
	q.results = make([]*StreamingResult, 0)
	return out
}

// GetWatermark returns the watermark state for a query.
func (e *StreamingSQLV2Engine) GetWatermark(queryID string) (int64, bool) {
	e.mu.RLock()
	q, ok := e.queries[queryID]
	e.mu.RUnlock()
	if !ok {
		return 0, false
	}
	return q.watermark.GetWatermark(), true
}

// Checkpoint captures state for all running queries so processing can be
// resumed after a failure.
func (e *StreamingSQLV2Engine) Checkpoint(_ context.Context) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	e.checkpointMu.Lock()
	defer e.checkpointMu.Unlock()

	for id, q := range e.queries {
		wm := q.watermark.GetWatermark()
		q.aggregator.mu.RLock()
		keys := make([]string, 0, len(q.aggregator.windows))
		for k := range q.aggregator.windows {
			keys = append(keys, k)
		}
		q.aggregator.mu.RUnlock()
		cp := e.processor.CreateCheckpoint(id, wm, keys)
		e.checkpoints[id] = cp
	}
	return nil
}

// Restore restores processing state from the last checkpoint for a query.
func (e *StreamingSQLV2Engine) Restore(_ context.Context, queryID string) error {
	e.checkpointMu.RLock()
	cp, ok := e.checkpoints[queryID]
	e.checkpointMu.RUnlock()
	if !ok {
		return fmt.Errorf("no checkpoint for query %s", queryID)
	}
	e.processor.RestoreCheckpoint(cp)
	return nil
}

// subscribeAndProcess subscribes to the query's source metric on the hub and
// feeds events through the windowed aggregator with exactly-once processing.
func (e *StreamingSQLV2Engine) subscribeAndProcess(q *v2Query) {
	sub := e.hub.Subscribe(q.Parsed.Source, nil)
	defer sub.Close()

	for {
		select {
		case <-e.stopCh:
			return
		case p, ok := <-sub.C():
			if !ok {
				return
			}
			eventID := fmt.Sprintf("%s_%d", p.Metric, p.Timestamp)
			if e.config.ExactlyOnceEnabled && e.processor.IsDuplicate(eventID) {
				continue
			}

			var txnID uint64
			if e.config.ExactlyOnceEnabled {
				txnID = e.processor.Begin()
				_ = e.processor.RecordEvent(txnID, eventID) //nolint:errcheck // best-effort cleanup on shutdown
			}

			q.aggregator.AddEvent(p, q.ID)

			if e.config.ExactlyOnceEnabled {
				_ = e.processor.Commit(txnID) //nolint:errcheck // best-effort cleanup on shutdown
			}
		}
	}
}

// subscribeJoin subscribes to both sides of a join query.
func (e *StreamingSQLV2Engine) subscribeJoin(q *v2Query) {
	if len(q.Parsed.Joins) == 0 {
		return
	}
	j := q.Parsed.Joins[0]
	leftSub := e.hub.Subscribe(q.Parsed.Source, nil)
	rightSub := e.hub.Subscribe(j.Stream, nil)
	defer leftSub.Close()
	defer rightSub.Close()

	for {
		select {
		case <-e.stopCh:
			return
		case p, ok := <-leftSub.C():
			if !ok {
				return
			}
			results := e.joinEngine.ProcessLeft(q.ID, p)
			if len(results) > 0 {
				q.resultMu.Lock()
				q.results = append(q.results, results...)
				q.resultMu.Unlock()
			}
		case p, ok := <-rightSub.C():
			if !ok {
				return
			}
			results := e.joinEngine.ProcessRight(q.ID, p)
			if len(results) > 0 {
				q.resultMu.Lock()
				q.results = append(q.results, results...)
				q.resultMu.Unlock()
			}
		}
	}
}

// ---------------------------------------------------------------------------
// SQL parsing helpers (lightweight, reuses existing types)
// ---------------------------------------------------------------------------

func parseWindowedSQL(sql string) *ParsedStreamingSQL {
	parsed := &ParsedStreamingSQL{
		Type:   StreamingSQLTypeSelect,
		Window: &WindowClause{Type: StreamingWindowTumbling, Size: time.Minute},
	}
	upper := strings.ToUpper(sql)

	// Extract source: FROM <metric>
	if idx := strings.Index(upper, "FROM "); idx >= 0 {
		rest := sql[idx+5:]
		fields := strings.Fields(rest)
		if len(fields) > 0 {
			parsed.Source = fields[0]
		}
	}

	// Detect window type and size
	if strings.Contains(upper, "TUMBLING") {
		parsed.Window.Type = StreamingWindowTumbling
		parsed.Window.Size = extractDuration(upper, "TUMBLING")
	} else if strings.Contains(upper, "HOPPING") {
		parsed.Window.Type = StreamingWindowHopping
		parsed.Window.Size = extractDuration(upper, "HOPPING")
		parsed.Window.Advance = parsed.Window.Size / 2
	} else if strings.Contains(upper, "SESSION") {
		parsed.Window.Type = StreamingWindowSession
		parsed.Window.Size = extractDuration(upper, "SESSION")
	} else if strings.Contains(upper, "SLIDING") {
		parsed.Window.Type = StreamingWindowSliding
		parsed.Window.Size = extractDuration(upper, "SLIDING")
	}

	// GROUP BY
	if idx := strings.Index(upper, "GROUP BY "); idx >= 0 {
		rest := sql[idx+9:]
		end := len(rest)
		for _, kw := range []string{"HAVING", "EMIT", "WINDOW"} {
			if ki := strings.Index(strings.ToUpper(rest), kw); ki >= 0 && ki < end {
				end = ki
			}
		}
		fields := strings.Split(strings.TrimSpace(rest[:end]), ",")
		var groupFields []string
		for _, f := range fields {
			f = strings.TrimSpace(f)
			if f != "" {
				groupFields = append(groupFields, f)
			}
		}
		parsed.GroupBy = &GroupByClause{Fields: groupFields}
	}

	return parsed
}

func parseJoinSQL(sql string) *ParsedStreamingSQL {
	parsed := &ParsedStreamingSQL{
		Type:   StreamingSQLTypeSelect,
		Window: &WindowClause{Type: StreamingWindowTumbling, Size: time.Minute},
	}
	upper := strings.ToUpper(sql)

	// FROM
	if idx := strings.Index(upper, "FROM "); idx >= 0 {
		rest := sql[idx+5:]
		fields := strings.Fields(rest)
		if len(fields) > 0 {
			parsed.Source = fields[0]
		}
	}

	// JOIN
	joinType := JoinTypeInner
	if strings.Contains(upper, "LEFT JOIN") {
		joinType = JoinTypeLeft
	} else if strings.Contains(upper, "OUTER JOIN") || strings.Contains(upper, "FULL JOIN") {
		joinType = JoinTypeOuter
	}

	joinKW := "JOIN "
	if idx := strings.Index(upper, joinKW); idx >= 0 {
		rest := sql[idx+len(joinKW):]
		fields := strings.Fields(rest)
		stream := ""
		if len(fields) > 0 {
			stream = fields[0]
		}
		cond := ""
		if oi := strings.Index(upper[idx:], " ON "); oi >= 0 {
			cond = strings.TrimSpace(sql[idx+oi+4:])
			// Trim trailing keywords.
			for _, kw := range []string{"WITHIN", "WINDOW", "GROUP", "HAVING", "EMIT"} {
				if ki := strings.Index(strings.ToUpper(cond), kw); ki >= 0 {
					cond = strings.TrimSpace(cond[:ki])
				}
			}
		}
		windowSize := time.Minute
		if strings.Contains(upper, "WITHIN") {
			windowSize = extractDuration(upper, "WITHIN")
		}
		parsed.Joins = []StreamJoin{{
			Type:      joinType,
			Stream:    stream,
			Condition: cond,
			Window:    windowSize.String(),
		}}
	}

	return parsed
}

func extractDuration(upper, keyword string) time.Duration {
	idx := strings.Index(upper, keyword)
	if idx < 0 {
		return time.Minute
	}
	rest := upper[idx+len(keyword):]
	rest = strings.TrimSpace(rest)
	// Look for patterns like "1M", "30S", "5M", "1H"
	var num int
	var unit byte
	for i := 0; i < len(rest); i++ {
		if rest[i] >= '0' && rest[i] <= '9' {
			num = num*10 + int(rest[i]-'0')
		} else if rest[i] != '(' && rest[i] != ' ' && rest[i] != ')' {
			unit = rest[i]
			break
		}
	}
	if num == 0 {
		num = 1
	}
	switch unit {
	case 'S':
		return time.Duration(num) * time.Second
	case 'H':
		return time.Duration(num) * time.Hour
	case 'D':
		return time.Duration(num) * 24 * time.Hour
	default: // M or anything else
		return time.Duration(num) * time.Minute
	}
}

func parseWindowKey(key string) (start, end int64) {
	// keys are formatted as "w_<start>_<end>"
	if !strings.HasPrefix(key, "w_") {
		return 0, 0
	}
	rest := key[2:]
	parts := strings.SplitN(rest, "_", 2)
	if len(parts) != 2 {
		return 0, 0
	}
	fmt.Sscanf(parts[0], "%d", &start)
	fmt.Sscanf(parts[1], "%d", &end)
	return
}
