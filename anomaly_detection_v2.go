package chronicle

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"sync"
	"time"
)

// AnomalyDetectionV2Config configures the adaptive anomaly detection engine.
type AnomalyDetectionV2Config struct {
	Enabled             bool
	SeasonalityPeriod   time.Duration
	MinDataPoints       int
	BaselineWindow      int
	AdaptiveRate        float64
	MaxFalsePositiveRate float64
	FeedbackEnabled     bool
	CorrelationWindow   time.Duration
	AlertCooldown       time.Duration
}

// DefaultAnomalyDetectionV2Config returns sensible defaults.
func DefaultAnomalyDetectionV2Config() AnomalyDetectionV2Config {
	return AnomalyDetectionV2Config{
		Enabled:             true,
		SeasonalityPeriod:   24 * time.Hour,
		MinDataPoints:       30,
		BaselineWindow:      168, // 1 week of hourly data
		AdaptiveRate:        0.1,
		MaxFalsePositiveRate: 0.05,
		FeedbackEnabled:     true,
		CorrelationWindow:   5 * time.Minute,
		AlertCooldown:       15 * time.Minute,
	}
}

// STLDecomposition holds the seasonal-trend decomposition of a time series.
type STLDecomposition struct {
	Trend     []float64 `json:"trend"`
	Seasonal  []float64 `json:"seasonal"`
	Residual  []float64 `json:"residual"`
	Period    int       `json:"period"`
	Timestamp []int64   `json:"timestamps"`
}

// AdaptiveThreshold represents a self-tuning threshold.
type AdaptiveThreshold struct {
	Metric       string    `json:"metric"`
	Upper        float64   `json:"upper"`
	Lower        float64   `json:"lower"`
	Confidence   float64   `json:"confidence"`
	LastUpdated  time.Time `json:"last_updated"`
	SampleCount  int       `json:"sample_count"`
	FalsePositives int     `json:"false_positives"`
	TruePositives  int     `json:"true_positives"`
}

// AnomalyV2 represents a detected anomaly with rich context.
type AnomalyV2 struct {
	ID            string            `json:"id"`
	Metric        string            `json:"metric"`
	Tags          map[string]string `json:"tags"`
	Value         float64           `json:"value"`
	Expected      float64           `json:"expected"`
	Score         float64           `json:"score"`
	Type          string            `json:"type"`
	Method        string            `json:"method"`
	Severity      string            `json:"severity"`
	DetectedAt    time.Time         `json:"detected_at"`
	Seasonal      float64           `json:"seasonal_component"`
	Trend         float64           `json:"trend_component"`
	Residual      float64           `json:"residual_component"`
	CorrelatedIDs []string          `json:"correlated_ids,omitempty"`
	Feedback      *AnomalyFeedback  `json:"feedback,omitempty"`
}

// AnomalyFeedback represents user feedback on an anomaly detection.
type AnomalyFeedback struct {
	AnomalyID  string    `json:"anomaly_id"`
	IsAnomaly  bool      `json:"is_anomaly"`
	Comment    string    `json:"comment,omitempty"`
	ReceivedAt time.Time `json:"received_at"`
}

// MetricBaseline holds the computed baseline for a metric.
type MetricBaseline struct {
	Metric     string         `json:"metric"`
	Mean       float64        `json:"mean"`
	Stddev     float64        `json:"stddev"`
	Min        float64        `json:"min"`
	Max        float64        `json:"max"`
	P50        float64        `json:"p50"`
	P95        float64        `json:"p95"`
	P99        float64        `json:"p99"`
	Values     []float64      `json:"-"`
	UpdatedAt  time.Time      `json:"updated_at"`
	Decomp     *STLDecomposition `json:"decomposition,omitempty"`
}

// CorrelatedAnomaly represents anomalies that occur together.
type CorrelatedAnomaly struct {
	PrimaryID    string      `json:"primary_id"`
	CorrelatedIDs []string   `json:"correlated_ids"`
	Score        float64     `json:"score"`
	TimeWindow   time.Duration `json:"time_window"`
	Metrics      []string    `json:"metrics"`
}

// AnomalyDetectionV2Stats holds engine statistics.
type AnomalyDetectionV2Stats struct {
	TotalDetected      int64   `json:"total_detected"`
	TotalFalsePositives int64  `json:"total_false_positives"`
	TotalTruePositives  int64  `json:"total_true_positives"`
	FalsePositiveRate   float64 `json:"false_positive_rate"`
	MetricsMonitored    int    `json:"metrics_monitored"`
	ActiveThresholds    int    `json:"active_thresholds"`
	AvgDetectionLatency time.Duration `json:"avg_detection_latency"`
}

// AnomalyDetectionV2Engine provides adaptive anomaly detection with STL decomposition.
type AnomalyDetectionV2Engine struct {
	db     *DB
	config AnomalyDetectionV2Config

	mu         sync.RWMutex
	baselines  map[string]*MetricBaseline
	thresholds map[string]*AdaptiveThreshold
	anomalies  []*AnomalyV2
	feedback   map[string]*AnomalyFeedback
	running    bool
	stopCh     chan struct{}
	stats      AnomalyDetectionV2Stats
	anomalySeq int64

	// Online learning components
	learners map[string]*OnlineLearner
	router   *MultiChannelRouter
}

// NewAnomalyDetectionV2Engine creates a new adaptive anomaly detection engine.
func NewAnomalyDetectionV2Engine(db *DB, cfg AnomalyDetectionV2Config) *AnomalyDetectionV2Engine {
	return &AnomalyDetectionV2Engine{
		db:         db,
		config:     cfg,
		baselines:  make(map[string]*MetricBaseline),
		thresholds: make(map[string]*AdaptiveThreshold),
		anomalies:  make([]*AnomalyV2, 0),
		feedback:   make(map[string]*AnomalyFeedback),
		learners:   make(map[string]*OnlineLearner),
		stopCh:     make(chan struct{}),
	}
}

// SetAlertRouter configures the multi-channel alert router.
func (e *AnomalyDetectionV2Engine) SetAlertRouter(router *MultiChannelRouter) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.router = router
}

// Start starts the background detection loop.
func (e *AnomalyDetectionV2Engine) Start() {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true
	e.mu.Unlock()
	go e.detectionLoop()
}

// Stop stops the detection engine.
func (e *AnomalyDetectionV2Engine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	close(e.stopCh)
}

// Ingest processes a new data point for anomaly detection.
func (e *AnomalyDetectionV2Engine) Ingest(metric string, value float64, tags map[string]string, ts time.Time) *AnomalyV2 {
	e.mu.Lock()
	defer e.mu.Unlock()

	baseline := e.getOrCreateBaseline(metric)
	baseline.Values = append(baseline.Values, value)

	// Keep bounded window
	if len(baseline.Values) > e.config.BaselineWindow {
		baseline.Values = baseline.Values[len(baseline.Values)-e.config.BaselineWindow:]
	}

	e.updateBaseline(baseline)

	if len(baseline.Values) < e.config.MinDataPoints {
		return nil
	}

	// Perform STL decomposition
	decomp := e.decompose(baseline)
	baseline.Decomp = decomp

	// Check for anomaly using residual analysis
	residualIdx := len(decomp.Residual) - 1
	if residualIdx < 0 {
		return nil
	}
	residual := decomp.Residual[residualIdx]

	threshold := e.getOrCreateThreshold(metric)
	score := e.computeScore(residual, baseline.Stddev)

	if score < 0.3 {
		return nil
	}

	// Apply adaptive threshold adjustment
	adjustedScore := score * (1.0 - threshold.Confidence*0.1)
	if adjustedScore < 0.3 {
		return nil
	}

	var trendVal, seasonalVal float64
	if len(decomp.Trend) > 0 {
		trendVal = decomp.Trend[len(decomp.Trend)-1]
	}
	if len(decomp.Seasonal) > 0 {
		seasonalVal = decomp.Seasonal[len(decomp.Seasonal)-1]
	}

	e.anomalySeq++
	anomaly := &AnomalyV2{
		ID:         fmt.Sprintf("anomaly-v2-%d", e.anomalySeq),
		Metric:     metric,
		Tags:       tags,
		Value:      value,
		Expected:   baseline.Mean,
		Score:      adjustedScore,
		Type:       classifyAnomalyV2(adjustedScore, value-baseline.Mean),
		Method:     "stl_adaptive",
		Severity:   severityFromScore(adjustedScore),
		DetectedAt: ts,
		Seasonal:   seasonalVal,
		Trend:      trendVal,
		Residual:   residual,
	}

	e.anomalies = append(e.anomalies, anomaly)
	e.stats.TotalDetected++

	// Cap anomaly history
	if len(e.anomalies) > 10000 {
		e.anomalies = e.anomalies[len(e.anomalies)-5000:]
	}

	// Online learning: update learner and check for drift
	learner := e.getOrCreateLearner(metric)
	learner.Update(value)
	predMean, _ := learner.Predict()
	predError := value - predMean
	if learner.DetectDrift(predError) {
		// Drift detected: widen thresholds to reduce false positives
		threshold.Upper *= 1.1
		threshold.Lower *= 0.9
		threshold.Confidence = math.Max(0.1, threshold.Confidence-0.1)
	}

	// Route alert through multi-channel router
	if e.router != nil {
		e.router.Route(anomaly)
	}

	return anomaly
}

// SubmitFeedback records user feedback for adaptive threshold tuning.
func (e *AnomalyDetectionV2Engine) SubmitFeedback(fb AnomalyFeedback) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.config.FeedbackEnabled {
		return fmt.Errorf("feedback not enabled")
	}

	fb.ReceivedAt = time.Now()
	e.feedback[fb.AnomalyID] = &fb

	// Find the anomaly and update its feedback
	for _, a := range e.anomalies {
		if a.ID == fb.AnomalyID {
			a.Feedback = &fb
			// Update threshold based on feedback
			threshold := e.getOrCreateThreshold(a.Metric)
			if fb.IsAnomaly {
				threshold.TruePositives++
				e.stats.TotalTruePositives++
				// Increase confidence
				threshold.Confidence = math.Min(1.0, threshold.Confidence+e.config.AdaptiveRate)
			} else {
				threshold.FalsePositives++
				e.stats.TotalFalsePositives++
				// Decrease confidence and widen threshold
				threshold.Confidence = math.Max(0.0, threshold.Confidence-e.config.AdaptiveRate*2)
				threshold.Upper *= 1.1
				threshold.Lower *= 0.9
			}
			threshold.LastUpdated = time.Now()
			break
		}
	}

	e.updateFalsePositiveRate()
	return nil
}

// GetBaseline returns the baseline for a metric.
func (e *AnomalyDetectionV2Engine) GetBaseline(metric string) *MetricBaseline {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if b, ok := e.baselines[metric]; ok {
		cp := *b
		return &cp
	}
	return nil
}

// GetThreshold returns the adaptive threshold for a metric.
func (e *AnomalyDetectionV2Engine) GetThreshold(metric string) *AdaptiveThreshold {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if t, ok := e.thresholds[metric]; ok {
		cp := *t
		return &cp
	}
	return nil
}

// ListAnomalies returns recent anomalies.
func (e *AnomalyDetectionV2Engine) ListAnomalies(limit int) []*AnomalyV2 {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if limit <= 0 || limit > len(e.anomalies) {
		limit = len(e.anomalies)
	}

	start := len(e.anomalies) - limit
	result := make([]*AnomalyV2, limit)
	copy(result, e.anomalies[start:])
	return result
}

// CorrelateAnomalies finds anomalies that occurred within the correlation window.
func (e *AnomalyDetectionV2Engine) CorrelateAnomalies() []CorrelatedAnomaly {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if len(e.anomalies) < 2 {
		return nil
	}

	window := e.config.CorrelationWindow
	var correlated []CorrelatedAnomaly

	for i, primary := range e.anomalies {
		var ids []string
		var metrics []string
		for j, other := range e.anomalies {
			if i == j {
				continue
			}
			diff := primary.DetectedAt.Sub(other.DetectedAt)
			if diff < 0 {
				diff = -diff
			}
			if diff <= window && primary.Metric != other.Metric {
				ids = append(ids, other.ID)
				metrics = append(metrics, other.Metric)
			}
		}
		if len(ids) > 0 {
			correlated = append(correlated, CorrelatedAnomaly{
				PrimaryID:     primary.ID,
				CorrelatedIDs: ids,
				Score:         primary.Score,
				TimeWindow:    window,
				Metrics:       metrics,
			})
		}
	}

	return correlated
}

// Decompose performs STL-like decomposition on a metric's data.
func (e *AnomalyDetectionV2Engine) Decompose(metric string) *STLDecomposition {
	e.mu.RLock()
	baseline, ok := e.baselines[metric]
	e.mu.RUnlock()
	if !ok {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.decompose(baseline)
}

// Stats returns engine statistics.
func (e *AnomalyDetectionV2Engine) GetStats() AnomalyDetectionV2Stats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	stats := e.stats
	stats.MetricsMonitored = len(e.baselines)
	stats.ActiveThresholds = len(e.thresholds)
	return stats
}

func (e *AnomalyDetectionV2Engine) getOrCreateBaseline(metric string) *MetricBaseline {
	if b, ok := e.baselines[metric]; ok {
		return b
	}
	b := &MetricBaseline{
		Metric: metric,
		Values: make([]float64, 0, e.config.BaselineWindow),
	}
	e.baselines[metric] = b
	return b
}

func (e *AnomalyDetectionV2Engine) getOrCreateThreshold(metric string) *AdaptiveThreshold {
	if t, ok := e.thresholds[metric]; ok {
		return t
	}
	t := &AdaptiveThreshold{
		Metric:      metric,
		Upper:       3.0,
		Lower:       -3.0,
		Confidence:  0.5,
		LastUpdated: time.Now(),
	}
	e.thresholds[metric] = t
	return t
}

func (e *AnomalyDetectionV2Engine) getOrCreateLearner(metric string) *OnlineLearner {
	if l, ok := e.learners[metric]; ok {
		return l
	}
	l := NewOnlineLearner(0.01)
	e.learners[metric] = l
	return l
}

func (e *AnomalyDetectionV2Engine) updateBaseline(b *MetricBaseline) {
	n := len(b.Values)
	if n == 0 {
		return
	}

	var sum float64
	b.Min = b.Values[0]
	b.Max = b.Values[0]
	for _, v := range b.Values {
		sum += v
		if v < b.Min {
			b.Min = v
		}
		if v > b.Max {
			b.Max = v
		}
	}
	b.Mean = sum / float64(n)

	var variance float64
	for _, v := range b.Values {
		diff := v - b.Mean
		variance += diff * diff
	}
	if n > 1 {
		b.Stddev = math.Sqrt(variance / float64(n-1))
	}

	sorted := make([]float64, n)
	copy(sorted, b.Values)
	sort.Float64s(sorted)
	b.P50 = percentileV2(sorted, 50)
	b.P95 = percentileV2(sorted, 95)
	b.P99 = percentileV2(sorted, 99)
	b.UpdatedAt = time.Now()
}

func percentileV2(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := (p / 100.0) * float64(len(sorted)-1)
	lower := int(math.Floor(idx))
	upper := int(math.Ceil(idx))
	if lower == upper || upper >= len(sorted) {
		return sorted[lower]
	}
	frac := idx - float64(lower)
	return sorted[lower]*(1-frac) + sorted[upper]*frac
}

// decompose performs simplified STL decomposition.
// Uses moving average for trend, then extracts seasonal pattern from detrended data.
func (e *AnomalyDetectionV2Engine) decompose(b *MetricBaseline) *STLDecomposition {
	n := len(b.Values)
	if n < 4 {
		return &STLDecomposition{
			Trend:    b.Values,
			Seasonal: make([]float64, n),
			Residual: make([]float64, n),
			Period:   1,
		}
	}

	// Determine period (default to what config suggests or auto-detect)
	period := n / 4
	if period < 2 {
		period = 2
	}
	if period > n/2 {
		period = n / 2
	}

	// Step 1: Extract trend via centered moving average
	trend := make([]float64, n)
	halfWindow := period / 2
	for i := 0; i < n; i++ {
		start := i - halfWindow
		end := i + halfWindow + 1
		if start < 0 {
			start = 0
		}
		if end > n {
			end = n
		}
		var sum float64
		count := 0
		for j := start; j < end; j++ {
			sum += b.Values[j]
			count++
		}
		trend[i] = sum / float64(count)
	}

	// Step 2: Detrend and extract seasonal component
	detrended := make([]float64, n)
	for i := range detrended {
		detrended[i] = b.Values[i] - trend[i]
	}

	// Average seasonal pattern
	seasonal := make([]float64, n)
	seasonalAvg := make([]float64, period)
	seasonalCount := make([]int, period)
	for i := 0; i < n; i++ {
		idx := i % period
		seasonalAvg[idx] += detrended[i]
		seasonalCount[idx]++
	}
	for i := range seasonalAvg {
		if seasonalCount[i] > 0 {
			seasonalAvg[i] /= float64(seasonalCount[i])
		}
	}
	for i := 0; i < n; i++ {
		seasonal[i] = seasonalAvg[i%period]
	}

	// Step 3: Compute residual
	residual := make([]float64, n)
	for i := range residual {
		residual[i] = b.Values[i] - trend[i] - seasonal[i]
	}

	return &STLDecomposition{
		Trend:    trend,
		Seasonal: seasonal,
		Residual: residual,
		Period:   period,
	}
}

func (e *AnomalyDetectionV2Engine) computeScore(residual, stddev float64) float64 {
	if stddev == 0 {
		if residual == 0 {
			return 0
		}
		return 1.0
	}
	zscore := math.Abs(residual / stddev)
	// Normalize to 0-1 using sigmoid
	score := 1.0 / (1.0 + math.Exp(-1.5*(zscore-2.0)))
	return math.Min(1.0, score)
}

func (e *AnomalyDetectionV2Engine) updateFalsePositiveRate() {
	total := e.stats.TotalTruePositives + e.stats.TotalFalsePositives
	if total > 0 {
		e.stats.FalsePositiveRate = float64(e.stats.TotalFalsePositives) / float64(total)
	}
}

func classifyAnomalyV2(score float64, drift float64) string {
	if score >= 0.8 {
		if drift > 0 {
			return "spike"
		}
		return "dip"
	}
	if score >= 0.5 {
		return "drift"
	}
	return "fluctuation"
}

func severityFromScore(score float64) string {
	if score >= 0.8 {
		return "critical"
	}
	if score >= 0.5 {
		return "warning"
	}
	return "info"
}

// --- DBSCAN Multivariate Anomaly Detection ---

// DBSCANConfig configures the DBSCAN clustering algorithm.
type DBSCANConfig struct {
	Epsilon    float64 // neighborhood radius
	MinPoints  int     // minimum points to form a cluster
}

// DBSCANResult holds the output of a DBSCAN clustering run.
type DBSCANResult struct {
	Clusters   [][]int // indices of points in each cluster
	Noise      []int   // indices of noise points (anomalies)
	NumClusters int
}

// DBSCAN performs density-based spatial clustering for multivariate anomaly detection.
// Points not belonging to any cluster are classified as anomalies.
func DBSCAN(points [][]float64, config DBSCANConfig) *DBSCANResult {
	n := len(points)
	if n == 0 {
		return &DBSCANResult{}
	}

	labels := make([]int, n) // -1 = unvisited, 0 = noise, >0 = cluster ID
	for i := range labels {
		labels[i] = -1
	}

	clusterID := 0
	for i := 0; i < n; i++ {
		if labels[i] != -1 {
			continue
		}

		neighbors := regionQuery(points, i, config.Epsilon)
		if len(neighbors) < config.MinPoints {
			labels[i] = 0 // noise
			continue
		}

		clusterID++
		labels[i] = clusterID
		seeds := make([]int, len(neighbors))
		copy(seeds, neighbors)

		for j := 0; j < len(seeds); j++ {
			q := seeds[j]
			if q == i {
				continue
			}
			if labels[q] == 0 {
				labels[q] = clusterID
			}
			if labels[q] != -1 {
				continue
			}
			labels[q] = clusterID

			qNeighbors := regionQuery(points, q, config.Epsilon)
			if len(qNeighbors) >= config.MinPoints {
				seeds = append(seeds, qNeighbors...)
			}
		}
	}

	// Build result
	result := &DBSCANResult{NumClusters: clusterID}
	clusterMap := make(map[int][]int)
	for i, label := range labels {
		if label == 0 {
			result.Noise = append(result.Noise, i)
		} else if label > 0 {
			clusterMap[label] = append(clusterMap[label], i)
		}
	}
	for _, indices := range clusterMap {
		result.Clusters = append(result.Clusters, indices)
	}
	return result
}

func regionQuery(points [][]float64, idx int, epsilon float64) []int {
	var neighbors []int
	for i := range points {
		if euclideanDistF64(points[idx], points[i]) <= epsilon {
			neighbors = append(neighbors, i)
		}
	}
	return neighbors
}

func euclideanDistF64(a, b []float64) float64 {
	var sum float64
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		d := a[i] - b[i]
		sum += d * d
	}
	return math.Sqrt(sum)
}

// --- Isolation Forest for High-Cardinality Anomaly Detection ---

// IForestDetector implements the Isolation Forest algorithm for anomaly detection.
type IForestDetector struct {
	iTrees       []*iforestTree
	iNumTrees    int
	iSampleSize  int
	iThreshold   float64 // anomaly score threshold (0-1)
}

type iforestTree struct {
	left      *iforestTree
	right     *iforestTree
	splitAttr int
	splitVal  float64
	size      int
	depth     int
}

// NewIsolationForest creates a new Isolation Forest.
func NewIForestDetector(numTrees, sampleSize int, threshold float64) *IForestDetector {
	if numTrees <= 0 {
		numTrees = 100
	}
	if sampleSize <= 0 {
		sampleSize = 256
	}
	if threshold <= 0 || threshold > 1 {
		threshold = 0.6
	}
	return &IForestDetector{
		iNumTrees:    numTrees,
		iSampleSize:  sampleSize,
		iThreshold:   threshold,
	}
}

// Fit builds the isolation forest from training data.
func (f *IForestDetector) Fit(data [][]float64) {
	if len(data) == 0 {
		return
	}
	maxDepth := int(math.Ceil(math.Log2(float64(f.iSampleSize))))
	f.iTrees = make([]*iforestTree, f.iNumTrees)

	for i := 0; i < f.iNumTrees; i++ {
		// Subsample
		sample := subsample(data, f.iSampleSize)
		f.iTrees[i] = buildITree(sample, 0, maxDepth)
	}
}

// Score computes the anomaly score for a point (higher = more anomalous).
func (f *IForestDetector) Score(point []float64) float64 {
	if len(f.iTrees) == 0 {
		return 0
	}

	var totalDepth float64
	for _, tree := range f.iTrees {
		totalDepth += float64(pathLength(tree, point, 0))
	}
	avgDepth := totalDepth / float64(len(f.iTrees))

	// Normalize using the expected path length
	n := float64(f.iSampleSize)
	c := 2.0*(math.Log(n-1.0)+0.5772156649) - (2.0 * (n - 1.0) / n)
	if c == 0 {
		return 0
	}
	return math.Pow(2.0, -avgDepth/c)
}

// IsAnomaly returns true if the point's anomaly score exceeds the threshold.
func (f *IForestDetector) IsAnomaly(point []float64) bool {
	return f.Score(point) > f.iThreshold
}

func buildITree(data [][]float64, depth, maxDepth int) *iforestTree {
	node := &iforestTree{size: len(data), depth: depth}
	if len(data) <= 1 || depth >= maxDepth {
		return node
	}

	dims := len(data[0])
	if dims == 0 {
		return node
	}

	// Pick random attribute and split value
	attr := depth % dims // deterministic for reproducibility
	minVal, maxVal := data[0][attr], data[0][attr]
	for _, d := range data[1:] {
		if attr < len(d) {
			if d[attr] < minVal {
				minVal = d[attr]
			}
			if d[attr] > maxVal {
				maxVal = d[attr]
			}
		}
	}

	if minVal == maxVal {
		return node
	}

	splitVal := (minVal + maxVal) / 2.0
	node.splitAttr = attr
	node.splitVal = splitVal

	var left, right [][]float64
	for _, d := range data {
		if attr < len(d) && d[attr] < splitVal {
			left = append(left, d)
		} else {
			right = append(right, d)
		}
	}

	if len(left) > 0 {
		node.left = buildITree(left, depth+1, maxDepth)
	}
	if len(right) > 0 {
		node.right = buildITree(right, depth+1, maxDepth)
	}
	return node
}

func pathLength(node *iforestTree, point []float64, depth int) int {
	if node == nil || (node.left == nil && node.right == nil) {
		if node != nil && node.size > 1 {
			return depth + int(math.Ceil(math.Log2(float64(node.size))))
		}
		return depth
	}

	if node.splitAttr < len(point) && point[node.splitAttr] < node.splitVal {
		return pathLength(node.left, point, depth+1)
	}
	return pathLength(node.right, point, depth+1)
}

func subsample(data [][]float64, n int) [][]float64 {
	if len(data) <= n {
		result := make([][]float64, len(data))
		copy(result, data)
		return result
	}
	// Take first n items (deterministic for reproducibility)
	result := make([][]float64, n)
	copy(result, data[:n])
	return result
}

// --- Online Learning for Drift Adaptation ---

// OnlineLearner adapts anomaly thresholds in real-time based on incoming data.
type OnlineLearner struct {
	mu            sync.RWMutex
	mean          float64
	variance      float64
	count         int64
	learningRate  float64
	driftDetector *DriftDetector
}

// DriftDetector monitors for concept drift in streaming data.
type DriftDetector struct {
	windowSize   int
	recentErrors []float64
	baselineErr  float64
	driftCount   int64
}

// NewOnlineLearner creates a new online learning component.
func NewOnlineLearner(learningRate float64) *OnlineLearner {
	if learningRate <= 0 || learningRate > 1 {
		learningRate = 0.01
	}
	return &OnlineLearner{
		learningRate: learningRate,
		driftDetector: &DriftDetector{
			windowSize: 100,
		},
	}
}

// Update incorporates a new data point using exponential weighted moving average.
func (ol *OnlineLearner) Update(value float64) {
	ol.mu.Lock()
	defer ol.mu.Unlock()

	ol.count++
	if ol.count == 1 {
		ol.mean = value
		ol.variance = 0
		return
	}

	// Welford's online algorithm
	delta := value - ol.mean
	ol.mean += ol.learningRate * delta
	delta2 := value - ol.mean
	ol.variance = (1-ol.learningRate)*ol.variance + ol.learningRate*delta*delta2
}

// Predict returns the expected value and standard deviation.
func (ol *OnlineLearner) Predict() (mean, stddev float64) {
	ol.mu.RLock()
	defer ol.mu.RUnlock()
	return ol.mean, math.Sqrt(math.Max(0, ol.variance))
}

// DetectDrift checks if concept drift has occurred.
func (ol *OnlineLearner) DetectDrift(predictionError float64) bool {
	ol.mu.Lock()
	defer ol.mu.Unlock()

	dd := ol.driftDetector
	dd.recentErrors = append(dd.recentErrors, math.Abs(predictionError))
	if len(dd.recentErrors) > dd.windowSize {
		dd.recentErrors = dd.recentErrors[1:]
	}

	if len(dd.recentErrors) < dd.windowSize/2 {
		return false
	}

	// Compare recent error rate to baseline
	var recentSum float64
	for _, e := range dd.recentErrors {
		recentSum += e
	}
	recentAvg := recentSum / float64(len(dd.recentErrors))

	if dd.baselineErr == 0 {
		dd.baselineErr = recentAvg
		return false
	}

	// Drift detected if recent error is 2x baseline
	if recentAvg > 2*dd.baselineErr {
		dd.driftCount++
		dd.baselineErr = recentAvg // adapt
		return true
	}

	// Slowly adapt baseline
	dd.baselineErr = 0.95*dd.baselineErr + 0.05*recentAvg
	return false
}

// DriftCount returns the number of drift events detected.
func (ol *OnlineLearner) DriftCount() int64 {
	ol.mu.RLock()
	defer ol.mu.RUnlock()
	return ol.driftDetector.driftCount
}

// --- Multi-Channel Alert Routing ---

// AlertChannel defines a destination for anomaly alerts.
type AlertChannel struct {
	Name     string `json:"name"`
	Type     string `json:"type"` // "webhook", "email", "slack", "pagerduty"
	Endpoint string `json:"endpoint"`
	Severity string `json:"min_severity"` // minimum severity to trigger
	Enabled  bool   `json:"enabled"`
}

// MultiChannelRouter routes anomaly alerts to multiple channels based on severity.
type MultiChannelRouter struct {
	mu       sync.RWMutex
	channels []AlertChannel
	stats    MultiChannelStats
}

// MultiChannelStats tracks alert routing metrics.
type MultiChannelStats struct {
	TotalAlerts   int64            `json:"total_alerts"`
	AlertsByChannel map[string]int64 `json:"alerts_by_channel"`
	AlertsBySeverity map[string]int64 `json:"alerts_by_severity"`
}

// NewMultiChannelRouter creates a new multi-channel alert router.
func NewMultiChannelRouter(channels []AlertChannel) *MultiChannelRouter {
	return &MultiChannelRouter{
		channels: channels,
		stats: MultiChannelStats{
			AlertsByChannel:  make(map[string]int64),
			AlertsBySeverity: make(map[string]int64),
		},
	}
}

// Route sends an anomaly alert to all matching channels.
func (r *MultiChannelRouter) Route(anomaly *AnomalyV2) []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.stats.TotalAlerts++
	r.stats.AlertsBySeverity[anomaly.Severity]++

	var routed []string
	severityOrder := map[string]int{"info": 0, "warning": 1, "critical": 2}
	anomalySev := severityOrder[anomaly.Severity]

	for _, ch := range r.channels {
		if !ch.Enabled {
			continue
		}
		channelMinSev := severityOrder[ch.Severity]
		if anomalySev >= channelMinSev {
			r.stats.AlertsByChannel[ch.Name]++
			routed = append(routed, ch.Name)
		}
	}
	return routed
}

// Stats returns alert routing statistics.
func (r *MultiChannelRouter) Stats() MultiChannelStats {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.stats
}

func (e *AnomalyDetectionV2Engine) detectionLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			// Periodic threshold recalibration based on feedback
			e.mu.Lock()
			for metric, threshold := range e.thresholds {
				total := threshold.TruePositives + threshold.FalsePositives
				if total > 10 {
					fpRate := float64(threshold.FalsePositives) / float64(total)
					if fpRate > e.config.MaxFalsePositiveRate {
						threshold.Upper *= 1.05
						threshold.Lower *= 0.95
						threshold.Confidence = math.Max(0.1, threshold.Confidence-0.05)
					}
				}
				_ = metric //nolint:errcheck // metric used for iteration
			}
			e.mu.Unlock()
		}
	}
}

// AsPostWriteHook returns a WriteHook that feeds points into the anomaly detector.
func (e *AnomalyDetectionV2Engine) AsPostWriteHook() WriteHook {
	return WriteHook{
		Name:  "anomaly-detection-v2",
		Phase: "post",
		Handler: func(p Point) (Point, error) {
			e.Ingest(p.Metric, p.Value, p.Tags, time.Now())
			return p, nil // pass-through: don't modify the point
		},
	}
}

// RegisterHTTPHandlers registers HTTP endpoints for the anomaly v2 engine.
func (e *AnomalyDetectionV2Engine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/anomaly/v2/ingest", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Metric string            `json:"metric"`
			Value  float64           `json:"value"`
			Tags   map[string]string `json:"tags"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		result := e.Ingest(req.Metric, req.Value, req.Tags, time.Now())
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"anomaly": result,
		})
	})

	mux.HandleFunc("/api/v1/anomaly/v2/anomalies", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.ListAnomalies(100))
	})

	mux.HandleFunc("/api/v1/anomaly/v2/feedback", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var fb AnomalyFeedback
		if err := json.NewDecoder(r.Body).Decode(&fb); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if err := e.SubmitFeedback(fb); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	mux.HandleFunc("/api/v1/anomaly/v2/baselines", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		metric := r.URL.Query().Get("metric")
		if metric != "" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(e.GetBaseline(metric))
			return
		}
		e.mu.RLock()
		baselines := make([]*MetricBaseline, 0, len(e.baselines))
		for _, b := range e.baselines {
			cp := *b
			cp.Values = nil // Don't expose raw values
			baselines = append(baselines, &cp)
		}
		e.mu.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(baselines)
	})

	mux.HandleFunc("/api/v1/anomaly/v2/correlations", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.CorrelateAnomalies())
	})

	mux.HandleFunc("/api/v1/anomaly/v2/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetStats())
	})

	mux.HandleFunc("/api/v1/anomaly/v2/decompose", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		metric := r.URL.Query().Get("metric")
		if metric == "" {
			http.Error(w, "metric parameter required", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.Decompose(metric))
	})
}
