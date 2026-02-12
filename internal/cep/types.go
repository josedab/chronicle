package cep

import (
	"container/heap"
	"context"
	"sort"
	"sync"
	"time"

	chronicle "github.com/chronicle-db/chronicle"
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
	db     *chronicle.DB
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
	OnMatch     func(events []chronicle.Point, context map[string]interface{})
	PartitionBy []string
}

// PatternStep defines a single step in an event pattern.
type PatternStep struct {
	Name       string
	Metric     string
	Tags       map[string]string
	Condition  func(chronicle.Point) bool
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
	Function chronicle.AggFunc
	GroupBy  []string

	// State
	buffer     []windowedEvent
	bufferMu   sync.RWMutex
	lastEmit   time.Time
	panes      map[int64]*WindowPane
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
	Point      chronicle.Point
	EventTime  int64
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
	Select  []CEPSelectItem
	From    string
	Where   []CEPCondition
	GroupBy []string
	Having  *CEPCondition
	Window  CEPWindowSpec
	Emit    CEPEmitSpec
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
	Type    WindowType
	Size    time.Duration
	Slide   time.Duration
	GapTime time.Duration
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
	QueryID       string                 `json:"query_id"`
	PatternID     string                 `json:"pattern_id,omitempty"`
	WindowStart   int64                  `json:"window_start"`
	WindowEnd     int64                  `json:"window_end"`
	Timestamp     int64                  `json:"timestamp"`
	Values        map[string]float64     `json:"values"`
	GroupKey      map[string]string      `json:"group_key,omitempty"`
	MatchedEvents []chronicle.Point                `json:"matched_events,omitempty"`
	Metadata      map[string]interface{} `json:"metadata,omitempty"`
}

// CEPStateStore manages state for CEP operations.
type CEPStateStore struct {
	states   map[string][]byte
	statesMu sync.RWMutex
}

// NewCEPEngine creates a new CEP engine.
func NewCEPEngine(db *chronicle.DB, config CEPConfig) *CEPEngine {
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

// WindowConfig configures a window.
type WindowConfig struct {
	Type     WindowType
	Size     time.Duration
	Slide    time.Duration
	Metric   string
	Tags     map[string]string
	Function chronicle.AggFunc
	GroupBy  []string
}

type patternState struct {
	CurrentStep   int
	MatchedEvents []chronicle.Point
	LastMatch     int64
}

func getPartitionKey(p chronicle.Point, partitionBy []string) string {
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

// CEPStats contains CEP engine statistics.
type CEPStats struct {
	WindowCount  int   `json:"window_count"`
	PatternCount int   `json:"pattern_count"`
	QueryCount   int   `json:"query_count"`
	Watermark    int64 `json:"watermark"`
}

// CEPDB wraps a chronicle.DB with CEP capabilities.
type CEPDB struct {
	*chronicle.DB
	cep *CEPEngine
}

// NewCEPDB creates a CEP-enabled database wrapper.
func NewCEPDB(db *chronicle.DB, config CEPConfig) (*CEPDB, error) {
	cep := NewCEPEngine(db, config)

	return &CEPDB{
		DB:  db,
		cep: cep,
	}, nil
}

type eventHeap []windowedEvent

var _ heap.Interface = (*eventHeap)(nil)

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

// Sorted helper for deterministic output
type byTimestamp []chronicle.Point

var _ sort.Interface = byTimestamp{}
