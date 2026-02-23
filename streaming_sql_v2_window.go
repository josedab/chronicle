package chronicle

import (
	"fmt"
	"sync"
	"time"
)

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
