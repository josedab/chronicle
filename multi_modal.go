package chronicle

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// MultiModalStorage provides unified storage for metrics, logs, and traces
// Enables cross-signal correlation and unified querying

// MultiModalConfig configures multi-modal storage
type MultiModalConfig struct {
	// Enable log storage
	LogsEnabled bool

	// Enable trace storage
	TracesEnabled bool

	// Enable cross-signal correlation
	CorrelationEnabled bool

	// Log retention period
	LogRetention time.Duration

	// Trace retention period
	TraceRetention time.Duration

	// Maximum log entries in memory
	MaxLogEntries int

	// Maximum spans in memory
	MaxSpans int

	// Enable full-text indexing for logs
	FullTextIndexing bool

	// Index n-gram size
	NGramSize int

	// Correlation window
	CorrelationWindow time.Duration
}

// DefaultMultiModalConfig returns default configuration
func DefaultMultiModalConfig() *MultiModalConfig {
	return &MultiModalConfig{
		LogsEnabled:        true,
		TracesEnabled:      true,
		CorrelationEnabled: true,
		LogRetention:       7 * 24 * time.Hour,
		TraceRetention:     24 * time.Hour,
		MaxLogEntries:      1000000,
		MaxSpans:           500000,
		FullTextIndexing:   true,
		NGramSize:          3,
		CorrelationWindow:  5 * time.Minute,
	}
}

// SignalType defines the type of observability signal
type SignalType string

const (
	SignalMetric SignalType = "metric"
	SignalLog    SignalType = "log"
	SignalTrace  SignalType = "trace"
)

// MMLogEntry represents a multi-modal log entry
type MMLogEntry struct {
	ID         string            `json:"id"`
	Timestamp  time.Time         `json:"timestamp"`
	Level      LogLevel          `json:"level"`
	Message    string            `json:"message"`
	Service    string            `json:"service"`
	TraceID    string            `json:"trace_id,omitempty"`
	SpanID     string            `json:"span_id,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
	Resource   map[string]string `json:"resource,omitempty"`
}

// LogLevel defines log severity levels
type LogLevel string

const (
	LogLevelDebug LogLevel = "DEBUG"
	LogLevelInfo  LogLevel = "INFO"
	LogLevelWarn  LogLevel = "WARN"
	LogLevelError LogLevel = "ERROR"
	LogLevelFatal LogLevel = "FATAL"
)

// Span represents a distributed trace span
type Span struct {
	TraceID      string            `json:"trace_id"`
	SpanID       string            `json:"span_id"`
	ParentSpanID string            `json:"parent_span_id,omitempty"`
	Name         string            `json:"name"`
	Service      string            `json:"service"`
	Kind         SpanKind          `json:"kind"`
	StartTime    time.Time         `json:"start_time"`
	EndTime      time.Time         `json:"end_time"`
	Duration     time.Duration     `json:"duration"`
	Status       MMSpanStatusInfo  `json:"status"`
	Attributes   map[string]string `json:"attributes,omitempty"`
	Events       []SpanEvent       `json:"events,omitempty"`
	Links        []SpanLink        `json:"links,omitempty"`
	Resource     map[string]string `json:"resource,omitempty"`
}

// SpanKind defines the span type
type SpanKind string

const (
	SpanKindInternal SpanKind = "INTERNAL"
	SpanKindServer   SpanKind = "SERVER"
	SpanKindClient   SpanKind = "CLIENT"
	SpanKindProducer SpanKind = "PRODUCER"
	SpanKindConsumer SpanKind = "CONSUMER"
)

// MMSpanStatusInfo represents span completion status
type MMSpanStatusInfo struct {
	Code    StatusCode `json:"code"`
	Message string     `json:"message,omitempty"`
}

// StatusCode defines status codes
type StatusCode string

const (
	StatusOK    StatusCode = "OK"
	StatusError StatusCode = "ERROR"
	StatusUnset StatusCode = "UNSET"
)

// SpanEvent represents an event within a span
type SpanEvent struct {
	Name       string            `json:"name"`
	Timestamp  time.Time         `json:"timestamp"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// SpanLink represents a link to another span
type SpanLink struct {
	TraceID    string            `json:"trace_id"`
	SpanID     string            `json:"span_id"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// Trace represents a complete trace
type MMTrace struct {
	TraceID   string        `json:"trace_id"`
	RootSpan  *Span         `json:"root_span"`
	Spans     []*Span       `json:"spans"`
	Duration  time.Duration `json:"duration"`
	Services  []string      `json:"services"`
	StartTime time.Time     `json:"start_time"`
	EndTime   time.Time     `json:"end_time"`
}

// CorrelatedSignals represents correlated observability data
type CorrelatedSignals struct {
	TraceID      string        `json:"trace_id,omitempty"`
	TimeRange    MMTimeRange   `json:"time_range"`
	Metrics      []Point       `json:"metrics,omitempty"`
	Logs         []MMLogEntry  `json:"logs,omitempty"`
	Spans        []*Span       `json:"spans,omitempty"`
	Correlations []Correlation `json:"correlations,omitempty"`
}

// MMTimeRange represents a multi-modal time range
type MMTimeRange struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

// Correlation represents a correlation between signals
type Correlation struct {
	Type       string            `json:"type"`
	Signal1    SignalRef         `json:"signal1"`
	Signal2    SignalRef         `json:"signal2"`
	Confidence float64           `json:"confidence"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

// SignalRef references a specific signal
type SignalRef struct {
	Type      SignalType `json:"type"`
	ID        string     `json:"id"`
	Timestamp time.Time  `json:"timestamp"`
}

// MultiModalStorage manages multi-modal observability data
type MultiModalStorage struct {
	db     *DB
	config *MultiModalConfig

	// Log storage
	logs   []MMLogEntry
	logsMu sync.RWMutex

	// Trace storage
	spans    map[string]*Span   // spanID -> span
	traces   map[string][]*Span // traceID -> spans
	tracesMu sync.RWMutex

	// Full-text index for logs
	logIndex *invertedIndex
	indexMu  sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Stats
	totalLogs   int64
	totalSpans  int64
	totalTraces int64
	queries     int64
}

// NewMultiModalStorage creates a new multi-modal storage
func NewMultiModalStorage(db *DB, config *MultiModalConfig) (*MultiModalStorage, error) {
	if config == nil {
		config = DefaultMultiModalConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	mms := &MultiModalStorage{
		db:     db,
		config: config,
		logs:   make([]MMLogEntry, 0),
		spans:  make(map[string]*Span),
		traces: make(map[string][]*Span),
		ctx:    ctx,
		cancel: cancel,
	}

	if config.FullTextIndexing {
		mms.logIndex = newInvertedIndex(config.NGramSize)
	}

	// Start retention worker
	mms.wg.Add(1)
	go mms.retentionWorker()

	return mms, nil
}

// WriteLog writes a log entry
func (mms *MultiModalStorage) WriteLog(entry *MMLogEntry) error {
	if !mms.config.LogsEnabled {
		return nil
	}

	if entry.ID == "" {
		entry.ID = generateLogID()
	}
	if entry.Timestamp.IsZero() {
		entry.Timestamp = time.Now()
	}

	mms.logsMu.Lock()
	mms.logs = append(mms.logs, *entry)

	// Enforce max entries
	if len(mms.logs) > mms.config.MaxLogEntries {
		mms.logs = mms.logs[len(mms.logs)-mms.config.MaxLogEntries:]
	}
	mms.logsMu.Unlock()

	atomic.AddInt64(&mms.totalLogs, 1)

	// Index for full-text search
	if mms.config.FullTextIndexing {
		mms.indexMu.Lock()
		mms.logIndex.add(entry.ID, entry.Message)
		mms.indexMu.Unlock()
	}

	return nil
}

// WriteLogs writes multiple log entries
func (mms *MultiModalStorage) WriteLogs(entries []MMLogEntry) error {
	for i := range entries {
		if err := mms.WriteLog(&entries[i]); err != nil {
			return err
		}
	}
	return nil
}

// WriteSpan writes a trace span
func (mms *MultiModalStorage) WriteSpan(span *Span) error {
	if !mms.config.TracesEnabled {
		return nil
	}

	if span.SpanID == "" {
		span.SpanID = generateSpanID()
	}
	if span.Duration == 0 && !span.EndTime.IsZero() && !span.StartTime.IsZero() {
		span.Duration = span.EndTime.Sub(span.StartTime)
	}

	mms.tracesMu.Lock()
	mms.spans[span.SpanID] = span
	mms.traces[span.TraceID] = append(mms.traces[span.TraceID], span)

	// Enforce max spans
	if len(mms.spans) > mms.config.MaxSpans {
		mms.pruneOldestSpans()
	}
	mms.tracesMu.Unlock()

	atomic.AddInt64(&mms.totalSpans, 1)

	return nil
}

// WriteSpans writes multiple spans
func (mms *MultiModalStorage) WriteSpans(spans []*Span) error {
	for _, span := range spans {
		if err := mms.WriteSpan(span); err != nil {
			return err
		}
	}
	return nil
}

// QueryLogs queries log entries
func (mms *MultiModalStorage) QueryLogs(query *MMLogQuery) ([]MMLogEntry, error) {
	atomic.AddInt64(&mms.queries, 1)

	mms.logsMu.RLock()
	defer mms.logsMu.RUnlock()

	results := make([]MMLogEntry, 0)

	// Full-text search if query specified
	var matchingIDs map[string]bool
	if query.Search != "" && mms.config.FullTextIndexing {
		mms.indexMu.RLock()
		matchingIDs = mms.logIndex.search(query.Search)
		mms.indexMu.RUnlock()
	}

	for _, entry := range mms.logs {
		// Time range filter
		if !query.StartTime.IsZero() && entry.Timestamp.Before(query.StartTime) {
			continue
		}
		if !query.EndTime.IsZero() && entry.Timestamp.After(query.EndTime) {
			continue
		}

		// Level filter
		if query.Level != "" && entry.Level != query.Level {
			if !query.LevelAndAbove || !isLevelAtOrAbove(entry.Level, query.Level) {
				continue
			}
		}

		// Service filter
		if query.Service != "" && entry.Service != query.Service {
			continue
		}

		// Trace ID filter
		if query.TraceID != "" && entry.TraceID != query.TraceID {
			continue
		}

		// Full-text search filter
		if matchingIDs != nil {
			if !matchingIDs[entry.ID] {
				continue
			}
		}

		// Regex filter
		if query.MessageRegex != "" {
			re, err := regexp.Compile(query.MessageRegex)
			if err != nil || !re.MatchString(entry.Message) {
				continue
			}
		}

		// Attribute filters
		if len(query.Attributes) > 0 {
			match := true
			for k, v := range query.Attributes {
				if entry.Attributes[k] != v {
					match = false
					break
				}
			}
			if !match {
				continue
			}
		}

		results = append(results, entry)

		if query.Limit > 0 && len(results) >= query.Limit {
			break
		}
	}

	// Sort by timestamp (newest first)
	sort.Slice(results, func(i, j int) bool {
		return results[i].Timestamp.After(results[j].Timestamp)
	})

	return results, nil
}

// MMLogQuery defines log search criteria
type MMLogQuery struct {
	StartTime     time.Time         `json:"start_time"`
	EndTime       time.Time         `json:"end_time"`
	Level         LogLevel          `json:"level"`
	LevelAndAbove bool              `json:"level_and_above"`
	Service       string            `json:"service"`
	TraceID       string            `json:"trace_id"`
	Search        string            `json:"search"`
	MessageRegex  string            `json:"message_regex"`
	Attributes    map[string]string `json:"attributes"`
	Limit         int               `json:"limit"`
}

// GetTrace retrieves a complete trace
func (mms *MultiModalStorage) GetTrace(traceID string) (*MMTrace, error) {
	mms.tracesMu.RLock()
	spans, exists := mms.traces[traceID]
	mms.tracesMu.RUnlock()

	if !exists || len(spans) == 0 {
		return nil, fmt.Errorf("trace not found: %s", traceID)
	}

	trace := &MMTrace{
		TraceID: traceID,
		Spans:   spans,
	}

	// Find root span and calculate metrics
	serviceSet := make(map[string]bool)
	var minStart, maxEnd time.Time

	for _, span := range spans {
		serviceSet[span.Service] = true

		if span.ParentSpanID == "" {
			trace.RootSpan = span
		}

		if minStart.IsZero() || span.StartTime.Before(minStart) {
			minStart = span.StartTime
		}
		if maxEnd.IsZero() || span.EndTime.After(maxEnd) {
			maxEnd = span.EndTime
		}
	}

	trace.StartTime = minStart
	trace.EndTime = maxEnd
	trace.Duration = maxEnd.Sub(minStart)

	for svc := range serviceSet {
		trace.Services = append(trace.Services, svc)
	}
	sort.Strings(trace.Services)

	atomic.AddInt64(&mms.totalTraces, 1)

	return trace, nil
}

// QuerySpans queries trace spans
func (mms *MultiModalStorage) QuerySpans(query *SpanQuery) ([]*Span, error) {
	atomic.AddInt64(&mms.queries, 1)

	mms.tracesMu.RLock()
	defer mms.tracesMu.RUnlock()

	results := make([]*Span, 0)

	for _, span := range mms.spans {
		// Time range filter
		if !query.StartTime.IsZero() && span.StartTime.Before(query.StartTime) {
			continue
		}
		if !query.EndTime.IsZero() && span.EndTime.After(query.EndTime) {
			continue
		}

		// Service filter
		if query.Service != "" && span.Service != query.Service {
			continue
		}

		// Name filter
		if query.Name != "" && span.Name != query.Name {
			continue
		}

		// Status filter
		if query.Status != "" && span.Status.Code != query.Status {
			continue
		}

		// Kind filter
		if query.Kind != "" && span.Kind != query.Kind {
			continue
		}

		// Duration filter
		if query.MinDuration > 0 && span.Duration < query.MinDuration {
			continue
		}
		if query.MaxDuration > 0 && span.Duration > query.MaxDuration {
			continue
		}

		// Attribute filters
		if len(query.Attributes) > 0 {
			match := true
			for k, v := range query.Attributes {
				if span.Attributes[k] != v {
					match = false
					break
				}
			}
			if !match {
				continue
			}
		}

		results = append(results, span)

		if query.Limit > 0 && len(results) >= query.Limit {
			break
		}
	}

	// Sort by start time (newest first)
	sort.Slice(results, func(i, j int) bool {
		return results[i].StartTime.After(results[j].StartTime)
	})

	return results, nil
}
