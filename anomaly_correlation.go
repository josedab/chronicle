package chronicle

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

// CorrelationSignalType represents the type of observability signal for correlation.
// Re-uses the same string values as SignalType from multi_modal.go.
type CorrelationSignalType string

const (
	CorrelationSignalMetric CorrelationSignalType = "metric"
	CorrelationSignalLog    CorrelationSignalType = "log"
	CorrelationSignalTrace  CorrelationSignalType = "trace"
)

// AnomalyCorrelationConfig configures the anomaly correlation engine.
type AnomalyCorrelationConfig struct {
	Enabled             bool          `json:"enabled"`
	CorrelationWindow   time.Duration `json:"correlation_window"`
	MinConfidence       float64       `json:"min_confidence"`
	MaxCausalDepth      int           `json:"max_causal_depth"`
	EvaluationInterval  time.Duration `json:"evaluation_interval"`
	MaxIncidents        int           `json:"max_incidents"`
	TagOverlapThreshold float64       `json:"tag_overlap_threshold"`
	TemporalProximityMs int64         `json:"temporal_proximity_ms"`
	EnableCausalGraph   bool          `json:"enable_causal_graph"`
	WebhookURL          string        `json:"webhook_url,omitempty"`
}

// DefaultAnomalyCorrelationConfig returns sensible defaults.
func DefaultAnomalyCorrelationConfig() AnomalyCorrelationConfig {
	return AnomalyCorrelationConfig{
		Enabled:             false,
		CorrelationWindow:   5 * time.Minute,
		MinConfidence:       0.6,
		MaxCausalDepth:      5,
		EvaluationInterval:  30 * time.Second,
		MaxIncidents:        1000,
		TagOverlapThreshold: 0.3,
		TemporalProximityMs: 30000,
		EnableCausalGraph:   true,
	}
}

// AnomalySignal represents an anomalous event from any signal type.
type AnomalySignal struct {
	ID         string                `json:"id"`
	SignalType CorrelationSignalType `json:"signal_type"`
	Metric     string                `json:"metric"`
	Tags       map[string]string     `json:"tags"`
	Value      float64               `json:"value"`
	Severity   float64               `json:"severity"`
	Timestamp  time.Time             `json:"timestamp"`
	Message    string                `json:"message,omitempty"`
	TraceID    string                `json:"trace_id,omitempty"`
}

// CausalEdge represents a directed edge in the causal graph.
type CausalEdge struct {
	FromSignalID string  `json:"from_signal_id"`
	ToSignalID   string  `json:"to_signal_id"`
	Weight       float64 `json:"weight"`
	Relationship string  `json:"relationship"`
}

// CausalGraph represents the dependency graph of anomaly signals.
type CausalGraph struct {
	Nodes []AnomalySignal `json:"nodes"`
	Edges []CausalEdge    `json:"edges"`
}

// CorrelatedIncident represents a group of correlated anomalies with root cause analysis.
type CorrelatedIncident struct {
	ID             string          `json:"id"`
	Title          string          `json:"title"`
	Signals        []AnomalySignal `json:"signals"`
	RootCauses     []RootCause     `json:"root_causes"`
	BlastRadius    int             `json:"blast_radius"`
	Severity       float64         `json:"severity"`
	Confidence     float64         `json:"confidence"`
	StartTime      time.Time       `json:"start_time"`
	LastUpdateTime time.Time       `json:"last_update_time"`
	Status         string          `json:"status"`
	CausalGraph    *CausalGraph    `json:"causal_graph,omitempty"`
}

// RootCause represents a suspected root cause of an incident.
type RootCause struct {
	SignalID    string  `json:"signal_id"`
	Description string  `json:"description"`
	Confidence  float64 `json:"confidence"`
	Evidence    string  `json:"evidence"`
}

// AnomalyCorrelationEngine provides cross-signal anomaly correlation with causal inference.
type AnomalyCorrelationEngine struct {
	config    AnomalyCorrelationConfig
	db        *DB
	incidents map[string]*CorrelatedIncident
	signals   []AnomalySignal
	callbacks []func(*CorrelatedIncident)

	mu   sync.RWMutex
	done chan struct{}
}

// NewAnomalyCorrelationEngine creates a new anomaly correlation engine.
func NewAnomalyCorrelationEngine(db *DB, config AnomalyCorrelationConfig) *AnomalyCorrelationEngine {
	return &AnomalyCorrelationEngine{
		config:    config,
		db:        db,
		incidents: make(map[string]*CorrelatedIncident),
		signals:   make([]AnomalySignal, 0, 256),
		done:      make(chan struct{}),
	}
}

// Start begins the background correlation loop.
func (e *AnomalyCorrelationEngine) Start() {
	if !e.config.Enabled {
		return
	}
	go e.evaluationLoop()
}

// Stop halts the background correlation loop.
func (e *AnomalyCorrelationEngine) Stop() {
	select {
	case <-e.done:
	default:
		close(e.done)
	}
}

// IngestSignal adds an anomaly signal for correlation.
func (e *AnomalyCorrelationEngine) IngestSignal(signal AnomalySignal) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if signal.ID == "" {
		signal.ID = fmt.Sprintf("sig-%d", time.Now().UnixNano())
	}
	if signal.Timestamp.IsZero() {
		signal.Timestamp = time.Now()
	}
	e.signals = append(e.signals, signal)

	// Evict old signals outside the correlation window
	cutoff := time.Now().Add(-e.config.CorrelationWindow * 2)
	idx := 0
	for _, s := range e.signals {
		if s.Timestamp.After(cutoff) {
			e.signals[idx] = s
			idx++
		}
	}
	e.signals = e.signals[:idx]
}

// OnIncident registers a callback for new or updated incidents.
func (e *AnomalyCorrelationEngine) OnIncident(cb func(*CorrelatedIncident)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.callbacks = append(e.callbacks, cb)
}

// Correlate runs a single correlation pass and returns any new/updated incidents.
func (e *AnomalyCorrelationEngine) Correlate(ctx context.Context) ([]*CorrelatedIncident, error) {
	e.mu.Lock()
	signals := make([]AnomalySignal, len(e.signals))
	copy(signals, e.signals)
	e.mu.Unlock()

	if len(signals) < 2 {
		return nil, nil
	}

	clusters := e.clusterSignals(signals)
	var results []*CorrelatedIncident

	for _, cluster := range clusters {
		if len(cluster) < 2 {
			continue
		}

		incident := e.buildIncident(cluster)
		if incident.Confidence < e.config.MinConfidence {
			continue
		}

		if e.config.EnableCausalGraph {
			incident.CausalGraph = e.buildCausalGraph(cluster)
		}

		incident.RootCauses = e.identifyRootCauses(incident)

		e.mu.Lock()
		existing, exists := e.incidents[incident.ID]
		if exists {
			existing.Signals = incident.Signals
			existing.RootCauses = incident.RootCauses
			existing.LastUpdateTime = time.Now()
			existing.Severity = incident.Severity
			existing.CausalGraph = incident.CausalGraph
			incident = existing
		} else {
			if len(e.incidents) >= e.config.MaxIncidents {
				e.evictOldestIncident()
			}
			e.incidents[incident.ID] = incident
		}
		cbs := make([]func(*CorrelatedIncident), len(e.callbacks))
		copy(cbs, e.callbacks)
		e.mu.Unlock()

		for _, cb := range cbs {
			cb(incident)
		}
		results = append(results, incident)
	}

	return results, nil
}

// ListIncidents returns all active incidents.
func (e *AnomalyCorrelationEngine) ListIncidents() []*CorrelatedIncident {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]*CorrelatedIncident, 0, len(e.incidents))
	for _, inc := range e.incidents {
		result = append(result, inc)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Severity > result[j].Severity
	})
	return result
}

// GetIncident returns a specific incident by ID.
func (e *AnomalyCorrelationEngine) GetIncident(id string) (*CorrelatedIncident, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	inc, ok := e.incidents[id]
	return inc, ok
}

// Stats returns engine statistics.
func (e *AnomalyCorrelationEngine) Stats() map[string]interface{} {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return map[string]interface{}{
		"active_incidents": len(e.incidents),
		"buffered_signals": len(e.signals),
		"enabled":          e.config.Enabled,
	}
}

func (e *AnomalyCorrelationEngine) evaluationLoop() {
	ticker := time.NewTicker(e.config.EvaluationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.done:
			return
		case <-ticker.C:
			e.Correlate(context.Background())
		}
	}
}

// clusterSignals groups signals by temporal proximity and tag overlap.
func (e *AnomalyCorrelationEngine) clusterSignals(signals []AnomalySignal) [][]AnomalySignal {
	if len(signals) == 0 {
		return nil
	}

	sort.Slice(signals, func(i, j int) bool {
		return signals[i].Timestamp.Before(signals[j].Timestamp)
	})

	assigned := make([]bool, len(signals))
	var clusters [][]AnomalySignal

	for i := range signals {
		if assigned[i] {
			continue
		}
		cluster := []AnomalySignal{signals[i]}
		assigned[i] = true

		for j := i + 1; j < len(signals); j++ {
			if assigned[j] {
				continue
			}
			if e.areCorrelated(signals[i], signals[j]) {
				cluster = append(cluster, signals[j])
				assigned[j] = true
			}
		}
		clusters = append(clusters, cluster)
	}
	return clusters
}

func (e *AnomalyCorrelationEngine) areCorrelated(a, b AnomalySignal) bool {
	timeDiff := math.Abs(float64(a.Timestamp.Sub(b.Timestamp).Milliseconds()))
	if timeDiff > float64(e.config.TemporalProximityMs) {
		return false
	}

	overlap := tagOverlap(a.Tags, b.Tags)
	if overlap >= e.config.TagOverlapThreshold {
		return true
	}

	if a.TraceID != "" && a.TraceID == b.TraceID {
		return true
	}

	return false
}

func tagOverlap(a, b map[string]string) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	matches := 0
	total := len(a)
	if len(b) > total {
		total = len(b)
	}
	for k, v := range a {
		if bv, ok := b[k]; ok && bv == v {
			matches++
		}
	}
	return float64(matches) / float64(total)
}

func (e *AnomalyCorrelationEngine) buildIncident(signals []AnomalySignal) *CorrelatedIncident {
	var maxSeverity float64
	earliest := signals[0].Timestamp
	signalTypes := make(map[CorrelationSignalType]bool)

	for _, s := range signals {
		if s.Severity > maxSeverity {
			maxSeverity = s.Severity
		}
		if s.Timestamp.Before(earliest) {
			earliest = s.Timestamp
		}
		signalTypes[s.SignalType] = true
	}

	// Confidence increases with signal diversity and count
	confidence := math.Min(1.0, float64(len(signals))/10.0*0.5+float64(len(signalTypes))/3.0*0.5)

	id := fmt.Sprintf("inc-%d", earliest.UnixNano())
	title := fmt.Sprintf("Correlated anomaly: %d signals across %d signal types", len(signals), len(signalTypes))

	return &CorrelatedIncident{
		ID:             id,
		Title:          title,
		Signals:        signals,
		BlastRadius:    len(signals),
		Severity:       maxSeverity,
		Confidence:     confidence,
		StartTime:      earliest,
		LastUpdateTime: time.Now(),
		Status:         "active",
	}
}

func (e *AnomalyCorrelationEngine) buildCausalGraph(signals []AnomalySignal) *CausalGraph {
	graph := &CausalGraph{
		Nodes: signals,
		Edges: make([]CausalEdge, 0),
	}

	for i := 0; i < len(signals); i++ {
		for j := i + 1; j < len(signals); j++ {
			a, b := signals[i], signals[j]
			weight := e.computeEdgeWeight(a, b)
			if weight > 0.2 {
				// Earlier signal is assumed to be the cause
				from, to := a.ID, b.ID
				rel := "temporal"
				if a.Timestamp.After(b.Timestamp) {
					from, to = b.ID, a.ID
				}
				if a.TraceID != "" && a.TraceID == b.TraceID {
					rel = "trace_linked"
				} else if tagOverlap(a.Tags, b.Tags) > 0.5 {
					rel = "tag_correlated"
				}
				graph.Edges = append(graph.Edges, CausalEdge{
					FromSignalID: from,
					ToSignalID:   to,
					Weight:       weight,
					Relationship: rel,
				})
			}
		}
	}
	return graph
}

func (e *AnomalyCorrelationEngine) computeEdgeWeight(a, b AnomalySignal) float64 {
	timeDiff := math.Abs(float64(a.Timestamp.Sub(b.Timestamp).Milliseconds()))
	temporalScore := math.Max(0, 1.0-timeDiff/float64(e.config.TemporalProximityMs))

	tagScore := tagOverlap(a.Tags, b.Tags)

	traceScore := 0.0
	if a.TraceID != "" && a.TraceID == b.TraceID {
		traceScore = 1.0
	}

	return temporalScore*0.4 + tagScore*0.3 + traceScore*0.3
}

func (e *AnomalyCorrelationEngine) identifyRootCauses(incident *CorrelatedIncident) []RootCause {
	if incident.CausalGraph == nil || len(incident.CausalGraph.Edges) == 0 {
		if len(incident.Signals) > 0 {
			earliest := incident.Signals[0]
			for _, s := range incident.Signals[1:] {
				if s.Timestamp.Before(earliest.Timestamp) {
					earliest = s
				}
			}
			return []RootCause{{
				SignalID:    earliest.ID,
				Description: fmt.Sprintf("Earliest signal: %s %s", earliest.SignalType, earliest.Metric),
				Confidence:  0.5,
				Evidence:    "Temporal ordering (first observed anomaly)",
			}}
		}
		return nil
	}

	// Count outgoing edges per node as an indicator of causality
	outDegree := make(map[string]float64)
	inDegree := make(map[string]float64)
	for _, edge := range incident.CausalGraph.Edges {
		outDegree[edge.FromSignalID] += edge.Weight
		inDegree[edge.ToSignalID] += edge.Weight
	}

	type candidate struct {
		signalID string
		score    float64
	}
	var candidates []candidate
	signalMap := make(map[string]AnomalySignal)
	for _, s := range incident.Signals {
		signalMap[s.ID] = s
	}

	for id, out := range outDegree {
		in := inDegree[id]
		// Root causes have high out-degree and low in-degree
		score := out - in
		if score > 0 {
			candidates = append(candidates, candidate{id, score})
		}
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].score > candidates[j].score
	})

	maxCauses := e.config.MaxCausalDepth
	if maxCauses > len(candidates) {
		maxCauses = len(candidates)
	}
	if maxCauses == 0 && len(candidates) > 0 {
		maxCauses = 1
	}

	results := make([]RootCause, 0, maxCauses)
	for i := 0; i < maxCauses; i++ {
		c := candidates[i]
		sig := signalMap[c.signalID]
		maxScore := candidates[0].score
		conf := 0.5
		if maxScore > 0 {
			conf = math.Min(0.95, 0.5+0.45*(c.score/maxScore))
		}
		results = append(results, RootCause{
			SignalID:    c.signalID,
			Description: fmt.Sprintf("Suspected root cause: %s %s (severity=%.2f)", sig.SignalType, sig.Metric, sig.Severity),
			Confidence:  conf,
			Evidence:    fmt.Sprintf("Causal graph analysis: out_degree=%.2f, in_degree=%.2f", outDegree[c.signalID], inDegree[c.signalID]),
		})
	}
	return results
}

func (e *AnomalyCorrelationEngine) evictOldestIncident() {
	var oldestID string
	var oldestTime time.Time
	first := true
	for id, inc := range e.incidents {
		if first || inc.StartTime.Before(oldestTime) {
			oldestID = id
			oldestTime = inc.StartTime
			first = false
		}
	}
	if oldestID != "" {
		delete(e.incidents, oldestID)
	}
}
