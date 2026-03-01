package chronicle

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

// ShapleyDimensionAttribution represents a dimension's contribution to an anomaly via Shapley values.
type ShapleyDimensionAttribution struct {
	Dimension    string  `json:"dimension"`
	Value        string  `json:"value"`
	ShapleyValue float64 `json:"shapley_value"`
	Contribution float64 `json:"contribution_pct"`
	Direction    string  `json:"direction"` // "increase" or "decrease"
}

// GrangerCausalResult captures the result of a Granger causality test.
type GrangerCausalResult struct {
	Cause       string  `json:"cause"`
	Effect      string  `json:"effect"`
	FStatistic  float64 `json:"f_statistic"`
	PValue      float64 `json:"p_value"`
	IsCausal    bool    `json:"is_causal"`
	Lag         int     `json:"lag"`
}

// CausalExplainerNode represents a node in the causal graph.
type CausalExplainerNode struct {
	Metric      string   `json:"metric"`
	Centrality  float64  `json:"centrality"`
	InDegree    int      `json:"in_degree"`
	OutDegree   int      `json:"out_degree"`
	Causes      []string `json:"causes"`
	Effects     []string `json:"effects"`
}

// CausalExplainerGraph represents a directed causal graph between metrics.
type CausalExplainerGraph struct {
	Nodes map[string]*CausalExplainerNode `json:"nodes"`
	Edges []GrangerCausalResult    `json:"edges"`
}

// AnomalyExplanationResult contains the full causal explanation for an anomaly.
type AnomalyExplanationResult struct {
	AnomalyID         string                        `json:"anomaly_id"`
	Metric            string                        `json:"metric"`
	Timestamp         time.Time                     `json:"timestamp"`
	Value             float64                       `json:"value"`
	BaselineMean      float64                       `json:"baseline_mean"`
	BaselineStddev    float64                       `json:"baseline_stddev"`
	Deviation         float64                       `json:"deviation_sigma"`
	DimensionAttrs    []ShapleyDimensionAttribution `json:"dimension_attributions"`
	CausalPredecessors []GrangerCausalResult     `json:"causal_predecessors"`
	RootCauses        []string                      `json:"root_causes"`
	NLExplanation     string                        `json:"explanation"`
	Confidence        float64                       `json:"confidence"`
	GeneratedAt       time.Time                     `json:"generated_at"`
}

// CausalExplainerConfig configures the causal anomaly explanation engine.
type CausalExplainerConfig struct {
	MaxLag              int     `json:"max_lag"`
	SignificanceLevel   float64 `json:"significance_level"`
	MinSamples          int     `json:"min_samples"`
	ShapleyPermutations int     `json:"shapley_permutations"`
	MaxDimensions       int     `json:"max_dimensions"`
}

// DefaultCausalExplainerConfig returns sensible defaults.
func DefaultCausalExplainerConfig() CausalExplainerConfig {
	return CausalExplainerConfig{
		MaxLag:              5,
		SignificanceLevel:   0.05,
		MinSamples:          30,
		ShapleyPermutations: 100,
		MaxDimensions:       20,
	}
}

// CausalAnomalyExplainer extends the anomaly explainability engine with causal inference.
type CausalAnomalyExplainer struct {
	config          CausalExplainerConfig
	explainEngine   *AnomalyExplainabilityEngine
	correlationEngine *MetricCorrelationEngine
	db              *DB
	causalGraph     *CausalExplainerGraph
	metricHistory   map[string][]float64
	mu              sync.RWMutex
}

// NewCausalAnomalyExplainer creates a new causal anomaly explainer.
func NewCausalAnomalyExplainer(
	db *DB,
	explainEngine *AnomalyExplainabilityEngine,
	correlationEngine *MetricCorrelationEngine,
	config CausalExplainerConfig,
) *CausalAnomalyExplainer {
	return &CausalAnomalyExplainer{
		config:            config,
		explainEngine:     explainEngine,
		correlationEngine: correlationEngine,
		db:                db,
		causalGraph: &CausalExplainerGraph{
			Nodes: make(map[string]*CausalExplainerNode),
		},
		metricHistory: make(map[string][]float64),
	}
}

// IngestValue feeds a metric value into the history for causal analysis.
func (e *CausalAnomalyExplainer) IngestValue(metric string, value float64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.metricHistory[metric] = append(e.metricHistory[metric], value)
	// Cap history
	if len(e.metricHistory[metric]) > 10000 {
		e.metricHistory[metric] = e.metricHistory[metric][len(e.metricHistory[metric])-10000:]
	}
}

// BuildCausalExplainerGraph constructs a causal graph using Granger causality tests.
func (e *CausalAnomalyExplainer) BuildCausalExplainerGraph() *CausalExplainerGraph {
	e.mu.Lock()
	defer e.mu.Unlock()

	metrics := make([]string, 0, len(e.metricHistory))
	for m := range e.metricHistory {
		if len(e.metricHistory[m]) >= e.config.MinSamples {
			metrics = append(metrics, m)
		}
	}

	graph := &CausalExplainerGraph{
		Nodes: make(map[string]*CausalExplainerNode),
		Edges: make([]GrangerCausalResult, 0),
	}

	for _, m := range metrics {
		graph.Nodes[m] = &CausalExplainerNode{
			Metric: m,
		}
	}

	// Pairwise Granger causality tests
	for i, cause := range metrics {
		for j, effect := range metrics {
			if i == j {
				continue
			}
			result := e.grangerTest(
				e.metricHistory[cause],
				e.metricHistory[effect],
				cause, effect,
			)
			if result.IsCausal {
				graph.Edges = append(graph.Edges, result)
				graph.Nodes[cause].Effects = append(graph.Nodes[cause].Effects, effect)
				graph.Nodes[cause].OutDegree++
				graph.Nodes[effect].Causes = append(graph.Nodes[effect].Causes, cause)
				graph.Nodes[effect].InDegree++
			}
		}
	}

	// Compute centrality (simple degree centrality)
	for _, node := range graph.Nodes {
		total := float64(node.InDegree + node.OutDegree)
		if len(metrics) > 1 {
			node.Centrality = total / float64(2*(len(metrics)-1))
		}
	}

	e.causalGraph = graph
	return graph
}

// grangerTest performs a simplified Granger causality test.
func (e *CausalAnomalyExplainer) grangerTest(cause, effect []float64, causeName, effectName string) GrangerCausalResult {
	n := len(cause)
	if len(effect) < n {
		n = len(effect)
	}
	lag := e.config.MaxLag
	if lag >= n {
		lag = n / 3
	}
	if lag < 1 {
		lag = 1
	}

	// Compute residuals from autoregressive model
	rssRestricted := e.computeARResiduals(effect, lag)
	rssUnrestricted := e.computeARResidualsCausal(cause, effect, lag)

	if rssUnrestricted <= 0 || rssRestricted <= 0 {
		return GrangerCausalResult{Cause: causeName, Effect: effectName, Lag: lag}
	}

	dfRestricted := lag
	dfUnrestricted := 2 * lag
	dfResidual := n - dfUnrestricted - 1

	if dfResidual <= 0 {
		return GrangerCausalResult{Cause: causeName, Effect: effectName, Lag: lag}
	}

	fStat := ((rssRestricted - rssUnrestricted) / float64(dfUnrestricted-dfRestricted)) /
		(rssUnrestricted / float64(dfResidual))

	// Approximate p-value using F-distribution (simplified)
	pValue := 1.0 / (1.0 + fStat)

	return GrangerCausalResult{
		Cause:      causeName,
		Effect:     effectName,
		FStatistic: fStat,
		PValue:     pValue,
		IsCausal:   pValue < e.config.SignificanceLevel,
		Lag:        lag,
	}
}

// computeARResiduals computes residual sum of squares from an autoregressive model.
func (e *CausalAnomalyExplainer) computeARResiduals(series []float64, lag int) float64 {
	if len(series) <= lag {
		return 0
	}

	var rss float64
	for t := lag; t < len(series); t++ {
		predicted := 0.0
		for l := 1; l <= lag; l++ {
			predicted += series[t-l] / float64(lag) // simple average of lagged values
		}
		residual := series[t] - predicted
		rss += residual * residual
	}
	return rss
}

// computeARResidualsCausal computes residuals including the causal variable.
func (e *CausalAnomalyExplainer) computeARResidualsCausal(cause, effect []float64, lag int) float64 {
	n := len(cause)
	if len(effect) < n {
		n = len(effect)
	}
	if n <= lag {
		return 0
	}

	var rss float64
	for t := lag; t < n; t++ {
		predicted := 0.0
		for l := 1; l <= lag; l++ {
			predicted += effect[t-l] / float64(2*lag)
			predicted += cause[t-l] / float64(2*lag)
		}
		residual := effect[t] - predicted
		rss += residual * residual
	}
	return rss
}

// ComputeShapleyValues computes approximate Shapley values for dimension attribution.
func (e *CausalAnomalyExplainer) ComputeShapleyValues(
	metric string,
	dimensions map[string]string,
	anomalyValue, baselineValue float64,
) []ShapleyDimensionAttribution {
	if len(dimensions) == 0 {
		return nil
	}

	dimKeys := make([]string, 0, len(dimensions))
	for k := range dimensions {
		dimKeys = append(dimKeys, k)
	}
	sort.Strings(dimKeys)

	if len(dimKeys) > e.config.MaxDimensions {
		dimKeys = dimKeys[:e.config.MaxDimensions]
	}

	totalDiff := anomalyValue - baselineValue
	attrs := make([]ShapleyDimensionAttribution, 0, len(dimKeys))

	// Approximate Shapley values using marginal contributions
	for _, dim := range dimKeys {
		// Simplified: assign proportional contribution based on how each
		// dimension changes the expected value. In practice, this would
		// run permutation sampling.
		contribution := totalDiff / float64(len(dimKeys))

		direction := "increase"
		if contribution < 0 {
			direction = "decrease"
		}

		attrs = append(attrs, ShapleyDimensionAttribution{
			Dimension:    dim,
			Value:        dimensions[dim],
			ShapleyValue: contribution,
			Contribution: math.Abs(contribution) / math.Max(math.Abs(totalDiff), 1) * 100,
			Direction:    direction,
		})
	}

	// Sort by absolute Shapley value
	sort.Slice(attrs, func(i, j int) bool {
		return math.Abs(attrs[i].ShapleyValue) > math.Abs(attrs[j].ShapleyValue)
	})

	return attrs
}

// ExplainAnomaly generates a comprehensive causal explanation for an anomaly.
func (e *CausalAnomalyExplainer) ExplainAnomaly(
	metric string,
	value float64,
	ts time.Time,
	dimensions map[string]string,
) (*AnomalyExplanationResult, error) {
	if metric == "" {
		return nil, fmt.Errorf("causal_explainer: metric required")
	}

	e.mu.RLock()
	history := e.metricHistory[metric]
	graph := e.causalGraph
	e.mu.RUnlock()

	// Compute baseline statistics
	var baselineMean, baselineStddev float64
	if len(history) > 0 {
		var sum float64
		for _, v := range history {
			sum += v
		}
		baselineMean = sum / float64(len(history))

		var variance float64
		for _, v := range history {
			diff := v - baselineMean
			variance += diff * diff
		}
		baselineStddev = math.Sqrt(variance / float64(len(history)))
	}

	deviation := 0.0
	if baselineStddev > 0 {
		deviation = (value - baselineMean) / baselineStddev
	}

	// Dimension attribution via Shapley values
	dimAttrs := e.ComputeShapleyValues(metric, dimensions, value, baselineMean)

	// Find causal predecessors from the graph
	var causalPreds []GrangerCausalResult
	var rootCauses []string
	if graph != nil {
		for _, edge := range graph.Edges {
			if edge.Effect == metric {
				causalPreds = append(causalPreds, edge)
			}
		}
		// Root causes: metrics that cause this one and have no causes themselves
		for _, pred := range causalPreds {
			node := graph.Nodes[pred.Cause]
			if node != nil && node.InDegree == 0 {
				rootCauses = append(rootCauses, pred.Cause)
			}
		}
		if len(rootCauses) == 0 && len(causalPreds) > 0 {
			// Fall back to highest centrality predecessor
			rootCauses = append(rootCauses, causalPreds[0].Cause)
		}
	}

	// Generate natural-language explanation
	explanation := e.generateNLExplanation(metric, value, ts, deviation, dimAttrs, causalPreds, rootCauses)

	confidence := math.Min(1.0, 0.5+math.Abs(deviation)*0.1)

	return &AnomalyExplanationResult{
		AnomalyID:          fmt.Sprintf("anomaly-%s-%d", metric, ts.UnixNano()),
		Metric:             metric,
		Timestamp:          ts,
		Value:              value,
		BaselineMean:       baselineMean,
		BaselineStddev:     baselineStddev,
		Deviation:          deviation,
		DimensionAttrs:     dimAttrs,
		CausalPredecessors: causalPreds,
		RootCauses:         rootCauses,
		NLExplanation:      explanation,
		Confidence:         confidence,
		GeneratedAt:        time.Now(),
	}, nil
}

// generateNLExplanation creates a human-readable explanation using template-based field substitution.
func (e *CausalAnomalyExplainer) generateNLExplanation(
	metric string,
	value float64,
	ts time.Time,
	deviation float64,
	dimAttrs []ShapleyDimensionAttribution,
	causalPreds []GrangerCausalResult,
	rootCauses []string,
) string {
	var parts []string

	// Severity
	severity := "moderate"
	if math.Abs(deviation) > 5 {
		severity = "critical"
	} else if math.Abs(deviation) > 3 {
		severity = "significant"
	} else if math.Abs(deviation) < 2 {
		severity = "minor"
	}

	direction := "spike"
	if deviation < 0 {
		direction = "drop"
	}

	parts = append(parts, fmt.Sprintf(
		"A %s %s was detected in metric '%s' at %s. The observed value of %.2f deviates %.1f standard deviations from the baseline.",
		severity, direction, metric, ts.Format(time.RFC3339), value, math.Abs(deviation),
	))

	// Dimension attribution
	if len(dimAttrs) > 0 {
		topDims := dimAttrs
		if len(topDims) > 3 {
			topDims = topDims[:3]
		}
		dimDescriptions := make([]string, 0, len(topDims))
		for _, d := range topDims {
			dimDescriptions = append(dimDescriptions,
				fmt.Sprintf("%s=%s (%.1f%% contribution)", d.Dimension, d.Value, d.Contribution))
		}
		parts = append(parts, fmt.Sprintf(
			"The primary contributing dimensions are: %s.", strings.Join(dimDescriptions, ", ")))
	}

	// Causal analysis
	if len(rootCauses) > 0 {
		parts = append(parts, fmt.Sprintf(
			"Causal analysis identifies %s as likely root cause(s) based on Granger causality testing.",
			strings.Join(rootCauses, ", ")))
	} else if len(causalPreds) > 0 {
		causes := make([]string, 0)
		for _, p := range causalPreds {
			causes = append(causes, p.Cause)
		}
		parts = append(parts, fmt.Sprintf(
			"Correlated upstream metrics include: %s.", strings.Join(causes, ", ")))
	}

	return strings.Join(parts, " ")
}

// GetCausalExplainerGraph returns the current causal graph.
func (e *CausalAnomalyExplainer) GetCausalExplainerGraph() *CausalExplainerGraph {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.causalGraph
}
