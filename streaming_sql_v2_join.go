package chronicle

import (
	"strings"
	"sync"
	"time"
)

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
