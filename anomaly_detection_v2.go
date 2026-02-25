package chronicle

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

// AnomalyDetectionV2Config configures the adaptive anomaly detection engine.
type AnomalyDetectionV2Config struct {
	Enabled              bool
	SeasonalityPeriod    time.Duration
	MinDataPoints        int
	BaselineWindow       int
	AdaptiveRate         float64
	MaxFalsePositiveRate float64
	FeedbackEnabled      bool
	CorrelationWindow    time.Duration
	AlertCooldown        time.Duration
}

// DefaultAnomalyDetectionV2Config returns sensible defaults.
func DefaultAnomalyDetectionV2Config() AnomalyDetectionV2Config {
	return AnomalyDetectionV2Config{
		Enabled:              true,
		SeasonalityPeriod:    24 * time.Hour,
		MinDataPoints:        30,
		BaselineWindow:       168, // 1 week of hourly data
		AdaptiveRate:         0.1,
		MaxFalsePositiveRate: 0.05,
		FeedbackEnabled:      true,
		CorrelationWindow:    5 * time.Minute,
		AlertCooldown:        15 * time.Minute,
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
	Metric         string    `json:"metric"`
	Upper          float64   `json:"upper"`
	Lower          float64   `json:"lower"`
	Confidence     float64   `json:"confidence"`
	LastUpdated    time.Time `json:"last_updated"`
	SampleCount    int       `json:"sample_count"`
	FalsePositives int       `json:"false_positives"`
	TruePositives  int       `json:"true_positives"`
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
	Metric    string            `json:"metric"`
	Mean      float64           `json:"mean"`
	Stddev    float64           `json:"stddev"`
	Min       float64           `json:"min"`
	Max       float64           `json:"max"`
	P50       float64           `json:"p50"`
	P95       float64           `json:"p95"`
	P99       float64           `json:"p99"`
	Values    []float64         `json:"-"`
	UpdatedAt time.Time         `json:"updated_at"`
	Decomp    *STLDecomposition `json:"decomposition,omitempty"`
}

// CorrelatedAnomaly represents anomalies that occur together.
type CorrelatedAnomaly struct {
	PrimaryID     string        `json:"primary_id"`
	CorrelatedIDs []string      `json:"correlated_ids"`
	Score         float64       `json:"score"`
	TimeWindow    time.Duration `json:"time_window"`
	Metrics       []string      `json:"metrics"`
}

// AnomalyDetectionV2Stats holds engine statistics.
type AnomalyDetectionV2Stats struct {
	TotalDetected       int64         `json:"total_detected"`
	TotalFalsePositives int64         `json:"total_false_positives"`
	TotalTruePositives  int64         `json:"total_true_positives"`
	FalsePositiveRate   float64       `json:"false_positive_rate"`
	MetricsMonitored    int           `json:"metrics_monitored"`
	ActiveThresholds    int           `json:"active_thresholds"`
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
