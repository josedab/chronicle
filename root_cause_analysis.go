package chronicle

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

// RootCauseAnalysisConfig configures the root cause analysis engine.
type RootCauseAnalysisConfig struct {
	Enabled              bool          `json:"enabled"`
	CausalWindowSize     time.Duration `json:"causal_window_size"`
	MinConfidence        float64       `json:"min_confidence"`
	MaxCausalDepth       int           `json:"max_causal_depth"`
	GrangerLagSteps      int           `json:"granger_lag_steps"`
	CorrelationThreshold float64       `json:"correlation_threshold"`
	TopKCauses           int           `json:"top_k_causes"`
	ExplanationEnabled   bool          `json:"explanation_enabled"`
}

// DefaultRootCauseAnalysisConfig returns sensible defaults for the RCA engine.
func DefaultRootCauseAnalysisConfig() RootCauseAnalysisConfig {
	return RootCauseAnalysisConfig{
		Enabled:              false,
		CausalWindowSize:     5 * time.Minute,
		MinConfidence:        0.7,
		MaxCausalDepth:       5,
		GrangerLagSteps:      10,
		CorrelationThreshold: 0.7,
		TopKCauses:           5,
		ExplanationEnabled:   true,
	}
}

// RelationshipType represents the type of statistical relationship between metrics.
type RelationshipType int

const (
	// RelationGranger indicates a Granger-causal relationship.
	RelationGranger RelationshipType = iota
	// RelationPearson indicates a Pearson correlation.
	RelationPearson
	// RelationSpearman indicates a Spearman rank correlation.
	RelationSpearman
	// RelationTransferEntropy indicates a transfer entropy relationship.
	RelationTransferEntropy
	// RelationMutualInfo indicates a mutual information relationship.
	RelationMutualInfo
)

// MetricRelationship represents a causal or correlational link between two metrics.
type MetricRelationship struct {
	Source      string           `json:"source"`
	Target      string           `json:"target"`
	Type        RelationshipType `json:"type"`
	Strength    float64          `json:"strength"`
	Lag         time.Duration    `json:"lag"`
	Direction   string           `json:"direction"`
	Confidence  float64          `json:"confidence"`
	SampleSize  int              `json:"sample_size"`
	LastUpdated time.Time        `json:"last_updated"`
}

// CausalGraphNode represents a metric in the causal graph.
type CausalGraphNode struct {
	Metric      string    `json:"metric"`
	InDegree    int       `json:"in_degree"`
	OutDegree   int       `json:"out_degree"`
	Centrality  float64   `json:"centrality"`
	IsRoot      bool      `json:"is_root"`
	Cluster     string    `json:"cluster"`
	Anomalous   bool      `json:"anomalous"`
	LastValue   float64   `json:"last_value"`
	LastUpdated time.Time `json:"last_updated"`
}

// RCAGraph is the full causal dependency graph.
type RCAGraph struct {
	Nodes       map[string]*CausalGraphNode `json:"nodes"`
	Edges       []MetricRelationship        `json:"edges"`
	LastBuilt   time.Time                   `json:"last_built"`
	MetricCount int                         `json:"metric_count"`
	EdgeCount   int                         `json:"edge_count"`
}

// RCAIncident represents an incident being analyzed.
type RCAIncident struct {
	ID               string             `json:"id"`
	AnomalousMetrics []string           `json:"anomalous_metrics"`
	StartTime        time.Time          `json:"start_time"`
	EndTime          time.Time          `json:"end_time"`
	State            string             `json:"state"`
	RootCauses       []RankedCause      `json:"root_causes"`
	Timeline         []RCATimelineEntry `json:"timeline"`
	Explanation      string             `json:"explanation"`
	Confidence       float64            `json:"confidence"`
	CreatedAt        time.Time          `json:"created_at"`
	AnalysisDuration time.Duration      `json:"analysis_duration"`
}

// RankedCause is a suspected root cause with ranking.
type RankedCause struct {
	Metric          string        `json:"metric"`
	Score           float64       `json:"score"`
	Confidence      float64       `json:"confidence"`
	Evidence        []string      `json:"evidence"`
	PropagationPath []string      `json:"propagation_path"`
	Lag             time.Duration `json:"lag"`
	Type            string        `json:"type"`
}

// RCATimelineEntry tracks the propagation timeline.
type RCATimelineEntry struct {
	Timestamp   time.Time `json:"timestamp"`
	Metric      string    `json:"metric"`
	Event       string    `json:"event"`
	IsRootCause bool      `json:"is_root_cause"`
	Severity    string    `json:"severity"`
}

// GrangerTestResult holds the result of a Granger causality test.
type GrangerTestResult struct {
	Source     string  `json:"source"`
	Target     string  `json:"target"`
	FStatistic float64 `json:"f_statistic"`
	PValue     float64 `json:"p_value"`
	LagSteps   int     `json:"lag_steps"`
	IsCausal   bool    `json:"is_causal"`
	Confidence float64 `json:"confidence"`
}

// RCAStats holds statistics for the root cause analysis engine.
type RCAStats struct {
	IncidentsAnalyzed   int64         `json:"incidents_analyzed"`
	RootCausesFound     int64         `json:"root_causes_found"`
	AvgAnalysisTime     time.Duration `json:"avg_analysis_time"`
	GraphNodes          int           `json:"graph_nodes"`
	GraphEdges          int           `json:"graph_edges"`
	GrangerTestsRun     int64         `json:"granger_tests_run"`
	TopRootCauseMetrics []string      `json:"top_root_cause_metrics"`
	Accuracy            float64       `json:"accuracy"`
	LastGraphBuild      time.Time     `json:"last_graph_build"`
}

// RootCauseAnalysisEngine provides ML-powered root cause analysis for metric anomalies.
//
// 🧪 EXPERIMENTAL: This API may change or be removed without notice.
// See api_stability.go for stability classifications.
type RootCauseAnalysisEngine struct {
	db            *DB
	config        RootCauseAnalysisConfig
	mu            sync.RWMutex
	graph         *RCAGraph
	incidents     map[string]*RCAIncident
	relationships map[string][]MetricRelationship
	metricHistory map[string][]float64
	stats         RCAStats

	// feedback tracking
	feedbackCorrect int64
	feedbackTotal   int64
	rootCauseCounts map[string]int
}

// NewRootCauseAnalysisEngine creates a new root cause analysis engine.
func NewRootCauseAnalysisEngine(db *DB, cfg RootCauseAnalysisConfig) *RootCauseAnalysisEngine {
	return &RootCauseAnalysisEngine{
		db:     db,
		config: cfg,
		graph: &RCAGraph{
			Nodes: make(map[string]*CausalGraphNode),
			Edges: make([]MetricRelationship, 0),
		},
		incidents:       make(map[string]*RCAIncident),
		relationships:   make(map[string][]MetricRelationship),
		metricHistory:   make(map[string][]float64),
		rootCauseCounts: make(map[string]int),
	}
}

// IngestMetricValue feeds a data point for causal graph building.
func (e *RootCauseAnalysisEngine) IngestMetricValue(metric string, value float64, timestamp time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()

	history := e.metricHistory[metric]
	history = append(history, value)

	// Keep a rolling window; cap at 10000 samples
	const maxHistory = 10000
	if len(history) > maxHistory {
		history = history[len(history)-maxHistory:]
	}
	e.metricHistory[metric] = history

	node, ok := e.graph.Nodes[metric]
	if !ok {
		node = &CausalGraphNode{Metric: metric}
		e.graph.Nodes[metric] = node
	}
	node.LastValue = value
	node.LastUpdated = timestamp
}

// BuildCausalGraph rebuilds the causal graph from metric history using
// pairwise Granger causality tests and Pearson correlation.
func (e *RootCauseAnalysisEngine) BuildCausalGraph() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	metrics := make([]string, 0, len(e.metricHistory))
	for m, h := range e.metricHistory {
		if len(h) >= e.config.GrangerLagSteps+2 {
			metrics = append(metrics, m)
		}
	}
	sort.Strings(metrics)

	if len(metrics) < 2 {
		return fmt.Errorf("need at least 2 metrics with sufficient history, have %d", len(metrics))
	}

	edges := make([]MetricRelationship, 0)
	relationships := make(map[string][]MetricRelationship)

	for i := 0; i < len(metrics); i++ {
		for j := 0; j < len(metrics); j++ {
			if i == j {
				continue
			}
			src := metrics[i]
			tgt := metrics[j]
			srcData := e.metricHistory[src]
			tgtData := e.metricHistory[tgt]

			// Align lengths
			minLen := len(srcData)
			if len(tgtData) < minLen {
				minLen = len(tgtData)
			}
			sx := srcData[len(srcData)-minLen:]
			tx := tgtData[len(tgtData)-minLen:]

			result := e.grangerCausalityTest(sx, tx, e.config.GrangerLagSteps)
			e.stats.GrangerTestsRun++

			if result.IsCausal && result.Confidence >= e.config.MinConfidence {
				corr := math.Abs(pearsonCorrelation(sx, tx))
				direction := "causal"
				if corr < 0 {
					direction = "inverse"
				}

				rel := MetricRelationship{
					Source:      src,
					Target:      tgt,
					Type:        RelationGranger,
					Strength:    result.Confidence,
					Lag:         time.Duration(result.LagSteps) * time.Second,
					Direction:   direction,
					Confidence:  result.Confidence,
					SampleSize:  minLen,
					LastUpdated: time.Now(),
				}
				edges = append(edges, rel)
				key := src + "->" + tgt
				relationships[key] = append(relationships[key], rel)
			}

			if corr := pearsonCorrelation(sx, tx); math.Abs(corr) >= e.config.CorrelationThreshold {
				direction := "correlated"
				if corr < 0 {
					direction = "inverse"
				}
				rel := MetricRelationship{
					Source:      src,
					Target:      tgt,
					Type:        RelationPearson,
					Strength:    math.Abs(corr),
					Direction:   direction,
					Confidence:  math.Abs(corr),
					SampleSize:  minLen,
					LastUpdated: time.Now(),
				}
				edges = append(edges, rel)
				key := src + "->" + tgt
				relationships[key] = append(relationships[key], rel)
			}
		}
	}

	// Rebuild node degrees
	for _, node := range e.graph.Nodes {
		node.InDegree = 0
		node.OutDegree = 0
		node.IsRoot = false
	}
	for _, edge := range edges {
		if n, ok := e.graph.Nodes[edge.Source]; ok {
			n.OutDegree++
		}
		if n, ok := e.graph.Nodes[edge.Target]; ok {
			n.InDegree++
		}
	}

	// Compute centrality and mark roots
	totalEdges := float64(len(edges))
	if totalEdges == 0 {
		totalEdges = 1
	}
	for _, node := range e.graph.Nodes {
		node.Centrality = float64(node.InDegree+node.OutDegree) / totalEdges
		if node.InDegree == 0 && node.OutDegree > 0 {
			node.IsRoot = true
		}
	}

	e.graph.Edges = edges
	e.graph.LastBuilt = time.Now()
	e.graph.MetricCount = len(e.graph.Nodes)
	e.graph.EdgeCount = len(edges)
	e.relationships = relationships
	e.stats.GraphNodes = e.graph.MetricCount
	e.stats.GraphEdges = e.graph.EdgeCount
	e.stats.LastGraphBuild = e.graph.LastBuilt

	return nil
}

// grangerCausalityTest performs a simplified Granger causality test.
// It checks whether past values of source improve prediction of target
// beyond target's own past values.
func (e *RootCauseAnalysisEngine) grangerCausalityTest(source, target []float64, lagSteps int) *GrangerTestResult {
	n := len(target)
	if n <= lagSteps+1 || len(source) < n {
		return &GrangerTestResult{LagSteps: lagSteps}
	}

	// Restricted model: predict target from its own lags
	rssRestricted := 0.0
	// Unrestricted model: predict target from its own lags + source lags
	rssUnrestricted := 0.0

	count := 0
	for t := lagSteps; t < n; t++ {
		// Restricted: simple autoregressive prediction using mean of lags
		autoSum := 0.0
		for l := 1; l <= lagSteps; l++ {
			autoSum += target[t-l]
		}
		autoPred := autoSum / float64(lagSteps)
		residRestricted := target[t] - autoPred
		rssRestricted += residRestricted * residRestricted

		// Unrestricted: autoregressive + source lags
		srcSum := 0.0
		for l := 1; l <= lagSteps; l++ {
			srcSum += source[t-l]
		}
		srcPred := srcSum / float64(lagSteps)
		combinedPred := 0.5*autoPred + 0.5*srcPred
		residUnrestricted := target[t] - combinedPred
		rssUnrestricted += residUnrestricted * residUnrestricted

		count++
	}

	if count == 0 || rssUnrestricted == 0 {
		return &GrangerTestResult{LagSteps: lagSteps}
	}

	// F-statistic: ((RSS_r - RSS_u) / lagSteps) / (RSS_u / (count - 2*lagSteps))
	dfNum := float64(lagSteps)
	dfDen := float64(count) - 2*float64(lagSteps)
	if dfDen <= 0 {
		dfDen = 1
	}

	fStat := ((rssRestricted - rssUnrestricted) / dfNum) / (rssUnrestricted / dfDen)
	if fStat < 0 {
		fStat = 0
	}

	// Approximate p-value using a simple sigmoid transform of the F-statistic
	pValue := 1.0 / (1.0 + fStat)
	isCausal := fStat > 2.0 && pValue < 0.3
	confidence := math.Min(1.0, fStat/10.0)

	return &GrangerTestResult{
		FStatistic: fStat,
		PValue:     pValue,
		LagSteps:   lagSteps,
		IsCausal:   isCausal,
		Confidence: confidence,
	}
}

// pearsonCorrelation computes the Pearson correlation coefficient between x and y.
func pearsonCorrelation(x, y []float64) float64 {
	n := len(x)
	if n == 0 || len(y) != n {
		return 0
	}

	var sumX, sumY, sumXY, sumX2, sumY2 float64
	for i := 0; i < n; i++ {
		sumX += x[i]
		sumY += y[i]
		sumXY += x[i] * y[i]
		sumX2 += x[i] * x[i]
		sumY2 += y[i] * y[i]
	}

	nf := float64(n)
	num := nf*sumXY - sumX*sumY
	den := math.Sqrt((nf*sumX2 - sumX*sumX) * (nf*sumY2 - sumY*sumY))
	if den == 0 {
		return 0
	}
	return num / den
}

// AnalyzeIncident performs root cause analysis on a set of anomalous metrics.
func (e *RootCauseAnalysisEngine) AnalyzeIncident(anomalousMetrics []string, startTime, endTime time.Time) (*RCAIncident, error) {
	start := time.Now()

	if len(anomalousMetrics) == 0 {
		return nil, fmt.Errorf("no anomalous metrics provided")
	}

	id := fmt.Sprintf("rca-%d", time.Now().UnixNano())

	incident := &RCAIncident{
		ID:               id,
		AnomalousMetrics: anomalousMetrics,
		StartTime:        startTime,
		EndTime:          endTime,
		State:            "analyzing",
		CreatedAt:        time.Now(),
	}

	e.mu.Lock()
	// Mark anomalous nodes
	for _, m := range anomalousMetrics {
		if node, ok := e.graph.Nodes[m]; ok {
			node.Anomalous = true
		}
	}
	e.mu.Unlock()

	// Find root causes
	rootCauses := e.findRootCauses(incident)
	incident.RootCauses = rootCauses

	// Build timeline
	incident.Timeline = e.buildTimeline(incident)

	// Generate explanation
	if e.config.ExplanationEnabled {
		incident.Explanation = e.generateExplanation(incident)
	}

	// Compute overall confidence
	if len(rootCauses) > 0 {
		var totalConf float64
		for _, rc := range rootCauses {
			totalConf += rc.Confidence
		}
		incident.Confidence = totalConf / float64(len(rootCauses))
	}

	incident.State = "completed"
	incident.AnalysisDuration = time.Since(start)

	e.mu.Lock()
	e.incidents[id] = incident
	e.stats.IncidentsAnalyzed++
	e.stats.RootCausesFound += int64(len(rootCauses))

	// Update average analysis time
	total := e.stats.AvgAnalysisTime*time.Duration(e.stats.IncidentsAnalyzed-1) + incident.AnalysisDuration
	e.stats.AvgAnalysisTime = total / time.Duration(e.stats.IncidentsAnalyzed)

	// Track top root cause metrics
	for _, rc := range rootCauses {
		e.rootCauseCounts[rc.Metric]++
	}
	e.stats.TopRootCauseMetrics = e.topRootCauseMetrics(5)
	e.mu.Unlock()

	return incident, nil
}
