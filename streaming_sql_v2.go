package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

// StreamingSQLV2Config configures the SQL:2016 streaming engine with windowed
// operations, stream-to-stream joins, watermark-based late data handling, and
// exactly-once processing semantics.
type StreamingSQLV2Config struct {
	Enabled              bool          `json:"enabled"`
	MaxWatermarkLag      time.Duration `json:"max_watermark_lag"`
	LateDataGracePeriod  time.Duration `json:"late_data_grace_period"`
	ExactlyOnceEnabled   bool          `json:"exactly_once_enabled"`
	CheckpointInterval   time.Duration `json:"checkpoint_interval"`
	CheckpointDir        string        `json:"checkpoint_dir"`
	MaxWindowsPerQuery   int           `json:"max_windows_per_query"`
	WindowRetention      time.Duration `json:"window_retention"`
	JoinBufferSize       int           `json:"join_buffer_size"`
	JoinTimeout          time.Duration `json:"join_timeout"`
	MaxConcurrentQueries int           `json:"max_concurrent_queries"`
}

// DefaultStreamingSQLV2Config returns sensible defaults.
func DefaultStreamingSQLV2Config() StreamingSQLV2Config {
	return StreamingSQLV2Config{
		Enabled:              true,
		MaxWatermarkLag:      5 * time.Second,
		LateDataGracePeriod:  10 * time.Second,
		ExactlyOnceEnabled:   true,
		CheckpointInterval:   30 * time.Second,
		CheckpointDir:        "/tmp/chronicle-checkpoints",
		MaxWindowsPerQuery:   10000,
		WindowRetention:      time.Hour,
		JoinBufferSize:       50000,
		JoinTimeout:          time.Minute,
		MaxConcurrentQueries: 100,
	}
}

// ---------------------------------------------------------------------------
// Watermark – tracks event-time progress per query
// ---------------------------------------------------------------------------

// StreamWatermark tracks the progress of event time within a streaming query,
// allowing the engine to know when a window can be closed and results emitted.
type StreamWatermark struct {
	mu                sync.Mutex
	currentWatermark  int64 // nanosecond epoch
	maxOutOfOrderness time.Duration
	gracePeriod       time.Duration
	lastAdvance       time.Time
}

// NewStreamWatermark creates a watermark tracker.
func NewStreamWatermark(maxOutOfOrderness, gracePeriod time.Duration) *StreamWatermark {
	return &StreamWatermark{
		maxOutOfOrderness: maxOutOfOrderness,
		gracePeriod:       gracePeriod,
		lastAdvance:       time.Now(),
	}
}

// Advance moves the watermark forward based on the observed event time.
func (w *StreamWatermark) Advance(eventTime int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	candidate := eventTime - w.maxOutOfOrderness.Nanoseconds()
	if candidate > w.currentWatermark {
		w.currentWatermark = candidate
		w.lastAdvance = time.Now()
	}
}

// IsLate returns true when the event arrived after the watermark plus the
// configured grace period.
func (w *StreamWatermark) IsLate(eventTime int64) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	deadline := w.currentWatermark - w.gracePeriod.Nanoseconds()
	return eventTime < deadline
}

// GetWatermark returns the current watermark value.
func (w *StreamWatermark) GetWatermark() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.currentWatermark
}

// ---------------------------------------------------------------------------
// Window state – contents and partial aggregations
// ---------------------------------------------------------------------------

// WindowTriggerType describes when a window should fire.
type WindowTriggerType int

const (
	// TriggerOnWatermark fires when the watermark passes the window end.
	TriggerOnWatermark WindowTriggerType = iota
	// TriggerOnCount fires after a fixed number of events.
	TriggerOnCount
	// TriggerOnProcessingTime fires on wall-clock intervals.
	TriggerOnProcessingTime
)

// V2WindowState holds events and partial aggregation results for a single
// window instance. It is kept separate from the v1 WindowState to allow
// richer trigger semantics and late-data tracking.
type V2WindowState struct {
	Key             string            `json:"key"`
	QueryID         string            `json:"query_id"`
	Start           int64             `json:"start"`
	End             int64             `json:"end"`
	Events          []Point           `json:"-"`
	Count           int64             `json:"count"`
	Sum             float64           `json:"sum"`
	Min             float64           `json:"min"`
	Max             float64           `json:"max"`
	FieldAggs       map[string]*fieldAgg `json:"-"`
	GroupValues     map[string]string `json:"group_values,omitempty"`
	TriggerType     WindowTriggerType `json:"trigger_type"`
	TriggerCount    int64             `json:"trigger_count,omitempty"`
	AllowedLateness time.Duration     `json:"allowed_lateness"`
	Triggered       bool              `json:"triggered"`
	Created         time.Time         `json:"created"`
	Updated         time.Time         `json:"updated"`
}

// fieldAgg maintains running aggregations per field.
type fieldAgg struct {
	Count int64
	Sum   float64
	Min   float64
	Max   float64
	First float64
	Last  float64
	init  bool
}

func newFieldAgg() *fieldAgg { return &fieldAgg{} }

func (fa *fieldAgg) add(v float64) {
	fa.Count++
	fa.Sum += v
	fa.Last = v
	if !fa.init {
		fa.Min = v
		fa.Max = v
		fa.First = v
		fa.init = true
		return
	}
	if v < fa.Min {
		fa.Min = v
	}
	if v > fa.Max {
		fa.Max = v
	}
}

// ---------------------------------------------------------------------------
// WindowedAggregator – SQL:2016 window operations
// ---------------------------------------------------------------------------

// WindowedAggregator manages window lifecycle: assignment, aggregation,
// triggering, and expiry for tumbling, hopping, session, and sliding windows.
type WindowedAggregator struct {
	mu            sync.RWMutex
	windows       map[string]*V2WindowState
	windowType    StreamingWindowType
	windowSize    time.Duration
	windowAdvance time.Duration // for hopping windows
	sessionGap    time.Duration // for session windows
	maxWindows    int
	retention     time.Duration
	watermark     *StreamWatermark
	resultCh      chan *StreamingResult
}

// NewWindowedAggregator creates a windowed aggregator.
func NewWindowedAggregator(wt StreamingWindowType, size, advance time.Duration, wm *StreamWatermark, maxWindows int, retention time.Duration) *WindowedAggregator {
	return &WindowedAggregator{
		windows:       make(map[string]*V2WindowState),
		windowType:    wt,
		windowSize:    size,
		windowAdvance: advance,
		sessionGap:    size, // session gap == size by convention
		maxWindows:    maxWindows,
		retention:     retention,
		watermark:     wm,
		resultCh:      make(chan *StreamingResult, 1024),
	}
}

// windowKeysForEvent returns the window key(s) an event belongs to.
func (wa *WindowedAggregator) windowKeysForEvent(eventTime int64) []string {
	sizeNs := wa.windowSize.Nanoseconds()
	if sizeNs == 0 {
		sizeNs = 1
	}
	switch wa.windowType {
	case StreamingWindowTumbling:
		start := (eventTime / sizeNs) * sizeNs
		return []string{fmt.Sprintf("w_%d_%d", start, start+sizeNs)}

	case StreamingWindowHopping:
		advNs := wa.windowAdvance.Nanoseconds()
		if advNs == 0 {
			advNs = sizeNs
		}
		var keys []string
		// event falls into every window whose [start, start+size) covers it
		latestStart := (eventTime / advNs) * advNs
		for s := latestStart; s > eventTime-sizeNs; s -= advNs {
			if s <= eventTime && eventTime < s+sizeNs {
				keys = append(keys, fmt.Sprintf("w_%d_%d", s, s+sizeNs))
			}
			if s <= 0 {
				break
			}
		}
		if len(keys) == 0 {
			keys = append(keys, fmt.Sprintf("w_%d_%d", latestStart, latestStart+sizeNs))
		}
		return keys

	case StreamingWindowSliding:
		// Sliding windows are conceptually one per event; we bucket by size.
		start := eventTime - sizeNs/2
		end := eventTime + sizeNs/2
		return []string{fmt.Sprintf("w_%d_%d", start, end)}

	case StreamingWindowSession:
		// Session windows are merged dynamically in AddEvent.
		return []string{fmt.Sprintf("session_%d", eventTime)}

	default:
		start := (eventTime / sizeNs) * sizeNs
		return []string{fmt.Sprintf("w_%d_%d", start, start+sizeNs)}
	}
}

// AddEvent assigns the event to the correct window(s) and updates partial
// aggregation state.
func (wa *WindowedAggregator) AddEvent(event Point, queryID string) {
	wa.watermark.Advance(event.Timestamp)

	if wa.watermark.IsLate(event.Timestamp) {
		return // drop events beyond grace period
	}

	if wa.windowType == StreamingWindowSession {
		wa.addSessionEvent(event, queryID)
		return
	}

	keys := wa.windowKeysForEvent(event.Timestamp)
	wa.mu.Lock()
	defer wa.mu.Unlock()

	for _, key := range keys {
		ws, ok := wa.windows[key]
		if !ok {
			if len(wa.windows) >= wa.maxWindows {
				continue
			}
			start, end := parseWindowKey(key)
			ws = &V2WindowState{
				Key:         key,
				QueryID:     queryID,
				Start:       start,
				End:         end,
				FieldAggs:   make(map[string]*fieldAgg),
				TriggerType: TriggerOnWatermark,
				Created:     time.Now(),
			}
			wa.windows[key] = ws
		}
		ws.Events = append(ws.Events, event)
		ws.Count++
		ws.Sum += event.Value
		if ws.Count == 1 {
			ws.Min = event.Value
			ws.Max = event.Value
		} else {
			if event.Value < ws.Min {
				ws.Min = event.Value
			}
			if event.Value > ws.Max {
				ws.Max = event.Value
			}
		}
		// per-metric field agg
		fa, ok := ws.FieldAggs[event.Metric]
		if !ok {
			fa = newFieldAgg()
			ws.FieldAggs[event.Metric] = fa
		}
		fa.add(event.Value)
		ws.Updated = time.Now()
	}
}

// addSessionEvent merges events into session windows based on the gap.
func (wa *WindowedAggregator) addSessionEvent(event Point, queryID string) {
	wa.mu.Lock()
	defer wa.mu.Unlock()

	gapNs := wa.sessionGap.Nanoseconds()
	var merged *V2WindowState

	// Find an existing session window that this event can extend.
	for _, ws := range wa.windows {
		if ws.QueryID != queryID {
			continue
		}
		if event.Timestamp >= ws.Start-gapNs && event.Timestamp <= ws.End+gapNs {
			merged = ws
			break
		}
	}

	if merged == nil {
		if len(wa.windows) >= wa.maxWindows {
			return
		}
		key := fmt.Sprintf("session_%s_%d", queryID, event.Timestamp)
		merged = &V2WindowState{
			Key:         key,
			QueryID:     queryID,
			Start:       event.Timestamp,
			End:         event.Timestamp + gapNs,
			FieldAggs:   make(map[string]*fieldAgg),
			TriggerType: TriggerOnWatermark,
			Created:     time.Now(),
		}
		wa.windows[key] = merged
	}

	// Extend boundaries.
	if event.Timestamp < merged.Start {
		merged.Start = event.Timestamp
	}
	if event.Timestamp+gapNs > merged.End {
		merged.End = event.Timestamp + gapNs
	}

	merged.Events = append(merged.Events, event)
	merged.Count++
	merged.Sum += event.Value
	if merged.Count == 1 {
		merged.Min = event.Value
		merged.Max = event.Value
	} else {
		if event.Value < merged.Min {
			merged.Min = event.Value
		}
		if event.Value > merged.Max {
			merged.Max = event.Value
		}
	}
	fa, ok := merged.FieldAggs[event.Metric]
	if !ok {
		fa = newFieldAgg()
		merged.FieldAggs[event.Metric] = fa
	}
	fa.add(event.Value)
	merged.Updated = time.Now()
}

// TriggerWindow fires a window and returns the aggregated result. It marks
// the window as triggered so it will not fire again unless late data arrives.
func (wa *WindowedAggregator) TriggerWindow(windowKey string) *StreamingResult {
	wa.mu.Lock()
	defer wa.mu.Unlock()

	ws, ok := wa.windows[windowKey]
	if !ok {
		return nil
	}

	ws.Triggered = true

	values := map[string]any{
		"count": ws.Count,
		"sum":   ws.Sum,
		"min":   ws.Min,
		"max":   ws.Max,
	}
	if ws.Count > 0 {
		values["avg"] = ws.Sum / float64(ws.Count)
	}
	for field, fa := range ws.FieldAggs {
		values[field+"_count"] = fa.Count
		values[field+"_sum"] = fa.Sum
		values[field+"_min"] = fa.Min
		values[field+"_max"] = fa.Max
		if fa.Count > 0 {
			values[field+"_avg"] = fa.Sum / float64(fa.Count)
		}
	}

	return &StreamingResult{
		QueryID:     ws.QueryID,
		Timestamp:   time.Now().UnixNano(),
		WindowStart: ws.Start,
		WindowEnd:   ws.End,
		Values:      values,
		Type:        ResultTypeFinal,
	}
}

// TriggerEligible triggers all windows whose end time is at or before the
// current watermark, returning the emitted results.
func (wa *WindowedAggregator) TriggerEligible() []*StreamingResult {
	wa.mu.RLock()
	wm := wa.watermark.GetWatermark()
	var eligible []string
	for key, ws := range wa.windows {
		if !ws.Triggered && ws.End <= wm {
			eligible = append(eligible, key)
		}
	}
	wa.mu.RUnlock()

	var results []*StreamingResult
	for _, key := range eligible {
		if r := wa.TriggerWindow(key); r != nil {
			results = append(results, r)
		}
	}
	return results
}

// ExpireWindows removes windows that have been triggered and whose retention
// has elapsed.
func (wa *WindowedAggregator) ExpireWindows() int {
	wa.mu.Lock()
	defer wa.mu.Unlock()

	cutoff := time.Now().Add(-wa.retention)
	removed := 0
	for key, ws := range wa.windows {
		if ws.Triggered && ws.Updated.Before(cutoff) {
			delete(wa.windows, key)
			removed++
		}
	}
	return removed
}

// ---------------------------------------------------------------------------
// StreamJoinEngine – stream-to-stream joins
// ---------------------------------------------------------------------------

// JoinRegistration describes a registered stream-to-stream join.
type JoinRegistration struct {
	ID          string        `json:"id"`
	LeftStream  string        `json:"left_stream"`
	RightStream string        `json:"right_stream"`
	JoinType    JoinType      `json:"join_type"`
	Condition   string        `json:"condition"`
	WindowSize  time.Duration `json:"window_size"`
}

// joinBuffer holds buffered events for one side of a join.
type joinBuffer struct {
	mu     sync.Mutex
	events []Point
	maxLen int
}

func newJoinBuffer(maxLen int) *joinBuffer {
	return &joinBuffer{events: make([]Point, 0, 256), maxLen: maxLen}
}

func (jb *joinBuffer) add(p Point) {
	jb.mu.Lock()
	defer jb.mu.Unlock()
	if len(jb.events) >= jb.maxLen {
		// evict oldest quarter
		n := jb.maxLen / 4
		jb.events = jb.events[n:]
	}
	jb.events = append(jb.events, p)
}

func (jb *joinBuffer) scan(windowNs int64, eventTime int64) []Point {
	jb.mu.Lock()
	defer jb.mu.Unlock()
	var out []Point
	lo := eventTime - windowNs
	hi := eventTime + windowNs
	for _, e := range jb.events {
		if e.Timestamp >= lo && e.Timestamp <= hi {
			out = append(out, e)
		}
	}
	return out
}

func (jb *joinBuffer) expire(before int64) {
	jb.mu.Lock()
	defer jb.mu.Unlock()
	n := 0
	for _, e := range jb.events {
		if e.Timestamp >= before {
			jb.events[n] = e
			n++
		}
	}
	jb.events = jb.events[:n]
}

// StreamJoinEngine performs stream-to-stream joins within time-bounded windows.
type StreamJoinEngine struct {
	mu          sync.RWMutex
	joins       map[string]*activeJoin
	bufferSize  int
	joinTimeout time.Duration
	resultCh    chan *StreamingResult
}

type activeJoin struct {
	reg   JoinRegistration
	left  *joinBuffer
	right *joinBuffer
}

// NewStreamJoinEngine creates a join engine.
func NewStreamJoinEngine(bufferSize int, joinTimeout time.Duration) *StreamJoinEngine {
	return &StreamJoinEngine{
		joins:       make(map[string]*activeJoin),
		bufferSize:  bufferSize,
		joinTimeout: joinTimeout,
		resultCh:    make(chan *StreamingResult, 1024),
	}
}

// RegisterJoin registers a new stream-to-stream join.
func (sje *StreamJoinEngine) RegisterJoin(id, leftStream, rightStream string, joinType JoinType, condition string, windowSize time.Duration) {
	sje.mu.Lock()
	defer sje.mu.Unlock()
	sje.joins[id] = &activeJoin{
		reg: JoinRegistration{
			ID:          id,
			LeftStream:  leftStream,
			RightStream: rightStream,
			JoinType:    joinType,
			Condition:   condition,
			WindowSize:  windowSize,
		},
		left:  newJoinBuffer(sje.bufferSize),
		right: newJoinBuffer(sje.bufferSize),
	}
}

// ProcessLeft processes an event arriving on the left stream, probing the
// right buffer for matching events within the join window.
func (sje *StreamJoinEngine) ProcessLeft(joinID string, event Point) []*StreamingResult {
	sje.mu.RLock()
	aj, ok := sje.joins[joinID]
	sje.mu.RUnlock()
	if !ok {
		return nil
	}

	aj.left.add(event)
	windowNs := aj.reg.WindowSize.Nanoseconds()
	rightMatches := aj.right.scan(windowNs, event.Timestamp)

	var results []*StreamingResult
	if len(rightMatches) > 0 {
		for _, rm := range rightMatches {
			if matchJoinCondition(event, rm, aj.reg.Condition) {
				results = append(results, buildJoinResult(joinID, event, rm))
			}
		}
	} else if aj.reg.JoinType == JoinTypeLeft || aj.reg.JoinType == JoinTypeOuter {
		results = append(results, buildJoinResult(joinID, event, Point{}))
	}
	return results
}

// ProcessRight processes an event arriving on the right stream.
func (sje *StreamJoinEngine) ProcessRight(joinID string, event Point) []*StreamingResult {
	sje.mu.RLock()
	aj, ok := sje.joins[joinID]
	sje.mu.RUnlock()
	if !ok {
		return nil
	}

	aj.right.add(event)
	windowNs := aj.reg.WindowSize.Nanoseconds()
	leftMatches := aj.left.scan(windowNs, event.Timestamp)

	var results []*StreamingResult
	if len(leftMatches) > 0 {
		for _, lm := range leftMatches {
			if matchJoinCondition(lm, event, aj.reg.Condition) {
				results = append(results, buildJoinResult(joinID, lm, event))
			}
		}
	} else if aj.reg.JoinType == JoinTypeOuter {
		results = append(results, buildJoinResult(joinID, Point{}, event))
	}
	return results
}

// ExpireBuffers removes events older than the join timeout from all buffers.
func (sje *StreamJoinEngine) ExpireBuffers() {
	cutoff := time.Now().Add(-sje.joinTimeout).UnixNano()
	sje.mu.RLock()
	defer sje.mu.RUnlock()
	for _, aj := range sje.joins {
		aj.left.expire(cutoff)
		aj.right.expire(cutoff)
	}
}

// matchJoinCondition evaluates a simple equality condition: "left.tag = right.tag".
func matchJoinCondition(left, right Point, condition string) bool {
	if condition == "" {
		return true
	}
	parts := strings.SplitN(condition, "=", 2)
	if len(parts) != 2 {
		return true
	}
	leftField := strings.TrimSpace(parts[0])
	rightField := strings.TrimSpace(parts[1])

	leftField = stripPrefix(leftField)
	rightField = stripPrefix(rightField)

	lv := resolveField(left, leftField)
	rv := resolveField(right, rightField)
	return lv != "" && lv == rv
}

func stripPrefix(f string) string {
	if idx := strings.Index(f, "."); idx >= 0 {
		return f[idx+1:]
	}
	return f
}

func resolveField(p Point, field string) string {
	if field == "metric" {
		return p.Metric
	}
	if v, ok := p.Tags[field]; ok {
		return v
	}
	return ""
}

func buildJoinResult(joinID string, left, right Point) *StreamingResult {
	vals := map[string]any{
		"left_metric":  left.Metric,
		"left_value":   left.Value,
		"left_time":    left.Timestamp,
		"right_metric": right.Metric,
		"right_value":  right.Value,
		"right_time":   right.Timestamp,
	}
	for k, v := range left.Tags {
		vals["left_"+k] = v
	}
	for k, v := range right.Tags {
		vals["right_"+k] = v
	}
	ts := left.Timestamp
	if right.Timestamp > ts {
		ts = right.Timestamp
	}
	return &StreamingResult{
		QueryID:   joinID,
		Timestamp: ts,
		Values:    vals,
		Type:      ResultTypeUpdate,
	}
}

// ---------------------------------------------------------------------------
// ExactlyOnceProcessor – exactly-once processing semantics
// ---------------------------------------------------------------------------

// TxnState represents the state of a processing transaction.
type TxnState int

const (
	TxnActive TxnState = iota
	TxnCommitted
	TxnAborted
)

// ProcessingTransaction represents a single processing transaction used for
// exactly-once semantics.
type ProcessingTransaction struct {
	ID        uint64    `json:"id"`
	State     TxnState  `json:"state"`
	SeqNum    uint64    `json:"seq_num"`
	EventIDs  []string  `json:"event_ids"`
	OutputIDs []string  `json:"output_ids"`
	Created   time.Time `json:"created"`
	Committed time.Time `json:"committed,omitempty"`
}

// ExactlyOnceCheckpoint stores the state needed to resume processing after a
// failure.
type ExactlyOnceCheckpoint struct {
	QueryID       string            `json:"query_id"`
	SeqNum        uint64            `json:"seq_num"`
	Watermark     int64             `json:"watermark"`
	WindowKeys    []string          `json:"window_keys"`
	ProcessedIDs  map[string]bool   `json:"processed_ids"`
	Timestamp     time.Time         `json:"timestamp"`
}

// ExactlyOnceProcessor provides exactly-once processing guarantees through a
// transaction log, deduplication, and checkpoint/restore.
type ExactlyOnceProcessor struct {
	mu           sync.Mutex
	seqCounter   atomic.Uint64
	activeTxns   map[uint64]*ProcessingTransaction
	txnLog       []*ProcessingTransaction
	processedIDs map[string]bool // deduplication set
	outputIDs    map[string]bool // idempotent output tracking
	maxLogSize   int
}

// NewExactlyOnceProcessor creates an exactly-once processor.
func NewExactlyOnceProcessor(maxLogSize int) *ExactlyOnceProcessor {
	return &ExactlyOnceProcessor{
		activeTxns:   make(map[uint64]*ProcessingTransaction),
		txnLog:       make([]*ProcessingTransaction, 0, 256),
		processedIDs: make(map[string]bool),
		outputIDs:    make(map[string]bool),
		maxLogSize:   maxLogSize,
	}
}

// IsDuplicate returns true if the event ID has already been processed.
func (eop *ExactlyOnceProcessor) IsDuplicate(eventID string) bool {
	eop.mu.Lock()
	defer eop.mu.Unlock()
	return eop.processedIDs[eventID]
}

// Begin starts a new processing transaction, returning its ID.
func (eop *ExactlyOnceProcessor) Begin() uint64 {
	txnID := eop.seqCounter.Add(1)
	eop.mu.Lock()
	defer eop.mu.Unlock()
	txn := &ProcessingTransaction{
		ID:      txnID,
		State:   TxnActive,
		SeqNum:  txnID,
		Created: time.Now(),
	}
	eop.activeTxns[txnID] = txn
	return txnID
}

// RecordEvent records that an event was processed within a transaction.
func (eop *ExactlyOnceProcessor) RecordEvent(txnID uint64, eventID string) error {
	eop.mu.Lock()
	defer eop.mu.Unlock()
	txn, ok := eop.activeTxns[txnID]
	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}
	if txn.State != TxnActive {
		return fmt.Errorf("transaction %d is not active", txnID)
	}
	txn.EventIDs = append(txn.EventIDs, eventID)
	return nil
}

// RecordOutput records an output produced by the transaction to enable
// idempotent replays.
func (eop *ExactlyOnceProcessor) RecordOutput(txnID uint64, outputID string) error {
	eop.mu.Lock()
	defer eop.mu.Unlock()
	txn, ok := eop.activeTxns[txnID]
	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}
	txn.OutputIDs = append(txn.OutputIDs, outputID)
	return nil
}

// Commit commits the transaction, marking all its events as processed and
// outputs as emitted.
func (eop *ExactlyOnceProcessor) Commit(txnID uint64) error {
	eop.mu.Lock()
	defer eop.mu.Unlock()
	txn, ok := eop.activeTxns[txnID]
	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}
	if txn.State != TxnActive {
		return fmt.Errorf("transaction %d is not active", txnID)
	}

	txn.State = TxnCommitted
	txn.Committed = time.Now()

	for _, eid := range txn.EventIDs {
		eop.processedIDs[eid] = true
	}
	for _, oid := range txn.OutputIDs {
		eop.outputIDs[oid] = true
	}

	eop.txnLog = append(eop.txnLog, txn)
	delete(eop.activeTxns, txnID)

	// Trim log if it grows too large.
	if len(eop.txnLog) > eop.maxLogSize {
		eop.txnLog = eop.txnLog[len(eop.txnLog)-eop.maxLogSize/2:]
	}
	return nil
}

// Rollback aborts a transaction; none of its events are marked processed.
func (eop *ExactlyOnceProcessor) Rollback(txnID uint64) error {
	eop.mu.Lock()
	defer eop.mu.Unlock()
	txn, ok := eop.activeTxns[txnID]
	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}
	txn.State = TxnAborted
	eop.txnLog = append(eop.txnLog, txn)
	delete(eop.activeTxns, txnID)
	return nil
}

// CreateCheckpoint captures the current state for later recovery.
func (eop *ExactlyOnceProcessor) CreateCheckpoint(queryID string, watermark int64, windowKeys []string) *ExactlyOnceCheckpoint {
	eop.mu.Lock()
	defer eop.mu.Unlock()
	cp := &ExactlyOnceCheckpoint{
		QueryID:      queryID,
		SeqNum:       eop.seqCounter.Load(),
		Watermark:    watermark,
		WindowKeys:   windowKeys,
		ProcessedIDs: make(map[string]bool, len(eop.processedIDs)),
		Timestamp:    time.Now(),
	}
	for k, v := range eop.processedIDs {
		cp.ProcessedIDs[k] = v
	}
	return cp
}

// RestoreCheckpoint restores previously checkpointed state.
func (eop *ExactlyOnceProcessor) RestoreCheckpoint(cp *ExactlyOnceCheckpoint) {
	eop.mu.Lock()
	defer eop.mu.Unlock()
	eop.seqCounter.Store(cp.SeqNum)
	eop.processedIDs = make(map[string]bool, len(cp.ProcessedIDs))
	for k, v := range cp.ProcessedIDs {
		eop.processedIDs[k] = v
	}
}

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
			_ = e.Checkpoint(context.Background())
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
				_ = e.processor.RecordEvent(txnID, eventID)
			}

			q.aggregator.AddEvent(p, q.ID)

			if e.config.ExactlyOnceEnabled {
				_ = e.processor.Commit(txnID)
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

// ---------------------------------------------------------------------------
// HTTP handlers
// ---------------------------------------------------------------------------

// RegisterHTTPHandlers registers the streaming SQL v2 HTTP routes.
func (e *StreamingSQLV2Engine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/streaming-v2/queries", e.handleQueries)
	mux.HandleFunc("/api/v1/streaming-v2/queries/", e.handleQueryByID)
	mux.HandleFunc("/api/v1/streaming-v2/watermarks", e.handleWatermarks)
	mux.HandleFunc("/api/v1/streaming-v2/checkpoint", e.handleCheckpoint)
	mux.HandleFunc("/api/v1/streaming-v2/status", e.handleStatus)
}

func (e *StreamingSQLV2Engine) handleQueries(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req struct {
			SQL  string `json:"sql"`
			Type string `json:"type"` // "windowed" or "join"
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		var q *v2Query
		var err error
		if req.Type == "join" {
			q, err = e.CreateJoinQuery(req.SQL)
		} else {
			q, err = e.CreateWindowedQuery(req.SQL)
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]any{
			"id":      q.ID,
			"sql":     q.SQL,
			"state":   q.State,
			"created": q.Created,
		})

	case http.MethodGet:
		e.mu.RLock()
		list := make([]map[string]any, 0, len(e.queries))
		for _, q := range e.queries {
			list = append(list, map[string]any{
				"id":      q.ID,
				"sql":     q.SQL,
				"state":   q.State,
				"created": q.Created,
			})
		}
		e.mu.RUnlock()
		writeJSON(w, list)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (e *StreamingSQLV2Engine) handleQueryByID(w http.ResponseWriter, r *http.Request) {
	// Path: /api/v1/streaming-v2/queries/{id}/results
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/streaming-v2/queries/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) == 0 || parts[0] == "" {
		http.Error(w, "missing query id", http.StatusBadRequest)
		return
	}
	queryID := parts[0]
	suffix := ""
	if len(parts) > 1 {
		suffix = parts[1]
	}

	switch {
	case suffix == "results" && r.Method == http.MethodGet:
		results := e.EmitResults(queryID)
		if results == nil {
			results = make([]*StreamingResult, 0)
		}
		writeJSON(w, results)
	default:
		e.mu.RLock()
		q, ok := e.queries[queryID]
		e.mu.RUnlock()
		if !ok {
			http.Error(w, "query not found", http.StatusNotFound)
			return
		}
		writeJSON(w, map[string]any{
			"id":        q.ID,
			"sql":       q.SQL,
			"state":     q.State,
			"created":   q.Created,
			"watermark": q.watermark.GetWatermark(),
		})
	}
}

func (e *StreamingSQLV2Engine) handleWatermarks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	e.mu.RLock()
	wms := make(map[string]int64, len(e.queries))
	for id, q := range e.queries {
		wms[id] = q.watermark.GetWatermark()
	}
	e.mu.RUnlock()
	writeJSON(w, wms)
}

func (e *StreamingSQLV2Engine) handleCheckpoint(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := e.Checkpoint(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	e.checkpointMu.RLock()
	cps := make(map[string]*ExactlyOnceCheckpoint, len(e.checkpoints))
	for k, v := range e.checkpoints {
		cps[k] = v
	}
	e.checkpointMu.RUnlock()
	writeJSON(w, map[string]any{
		"status":      "checkpoint_created",
		"checkpoints": cps,
	})
}

func (e *StreamingSQLV2Engine) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	e.mu.RLock()
	queryCount := len(e.queries)
	running := e.running
	e.mu.RUnlock()

	e.joinEngine.mu.RLock()
	joinCount := len(e.joinEngine.joins)
	e.joinEngine.mu.RUnlock()

	e.checkpointMu.RLock()
	cpCount := len(e.checkpoints)
	e.checkpointMu.RUnlock()

	writeJSON(w, map[string]any{
		"running":          running,
		"query_count":      queryCount,
		"join_count":       joinCount,
		"checkpoint_count": cpCount,
		"exactly_once":     e.config.ExactlyOnceEnabled,
	})
}

