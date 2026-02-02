package chronicle

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

// AnomalyType represents the classification of an anomaly.
type AnomalyType int

const (
	// AnomalyTypeNormal indicates no anomaly detected.
	AnomalyTypeNormal AnomalyType = iota
	// AnomalyTypeSpike indicates a sudden sharp increase.
	AnomalyTypeSpike
	// AnomalyTypeDip indicates a sudden sharp decrease.
	AnomalyTypeDip
	// AnomalyTypeDrift indicates a gradual shift from baseline.
	AnomalyTypeDrift
	// AnomalyTypeLevelShift indicates a permanent level change.
	AnomalyTypeLevelShift
	// AnomalyTypeSeasonalDeviation indicates deviation from expected seasonal pattern.
	AnomalyTypeSeasonalDeviation
	// AnomalyTypeTrendChange indicates a change in the trend direction.
	AnomalyTypeTrendChange
	// AnomalyTypeMissing indicates missing or null values where data expected.
	AnomalyTypeMissing
	// AnomalyTypeOutlier indicates a statistical outlier.
	AnomalyTypeOutlier
	// AnomalyTypeVarianceChange indicates a change in data variability.
	AnomalyTypeVarianceChange
)

func (t AnomalyType) String() string {
	switch t {
	case AnomalyTypeNormal:
		return "normal"
	case AnomalyTypeSpike:
		return "spike"
	case AnomalyTypeDip:
		return "dip"
	case AnomalyTypeDrift:
		return "drift"
	case AnomalyTypeLevelShift:
		return "level_shift"
	case AnomalyTypeSeasonalDeviation:
		return "seasonal_deviation"
	case AnomalyTypeTrendChange:
		return "trend_change"
	case AnomalyTypeMissing:
		return "missing"
	case AnomalyTypeOutlier:
		return "outlier"
	case AnomalyTypeVarianceChange:
		return "variance_change"
	default:
		return "unknown"
	}
}

// AnomalySeverity represents the severity level of an anomaly.
type AnomalySeverity int

const (
	AnomalySeverityInfo AnomalySeverity = iota
	AnomalySeverityWarning
	AnomalySeverityCritical
	AnomalySeverityEmergency
)

func (s AnomalySeverity) String() string {
	switch s {
	case AnomalySeverityInfo:
		return "info"
	case AnomalySeverityWarning:
		return "warning"
	case AnomalySeverityCritical:
		return "critical"
	case AnomalySeverityEmergency:
		return "emergency"
	default:
		return "unknown"
	}
}

// ClassifiedAnomaly contains a fully classified anomaly with remediation suggestions.
type ClassifiedAnomaly struct {
	// Basic anomaly result
	AnomalyResult

	// Classification fields
	Type        AnomalyType     `json:"type"`
	TypeName    string          `json:"type_name"`
	Severity    AnomalySeverity `json:"severity"`
	SeverityStr string          `json:"severity_str"`

	// Confidence in classification (0-1)
	ClassificationConfidence float64 `json:"classification_confidence"`

	// Additional context
	Timestamp     int64   `json:"timestamp"`
	Metric        string  `json:"metric,omitempty"`
	Value         float64 `json:"value"`
	BaselineValue float64 `json:"baseline_value"`
	BaselineStd   float64 `json:"baseline_std"`

	// Pattern analysis
	PatternAnalysis *PatternAnalysis `json:"pattern_analysis,omitempty"`

	// Remediation suggestions
	Remediations []Remediation `json:"remediations,omitempty"`

	// Related anomalies (if part of a pattern)
	RelatedAnomalies []int64 `json:"related_anomalies,omitempty"`
}

// PatternAnalysis provides detailed pattern information for the anomaly.
type PatternAnalysis struct {
	// Trend analysis
	TrendDirection   string  `json:"trend_direction"` // "increasing", "decreasing", "stable"
	TrendStrength    float64 `json:"trend_strength"`
	TrendChangePoint int64   `json:"trend_change_point,omitempty"`

	// Seasonality analysis
	SeasonalPeriod     int     `json:"seasonal_period,omitempty"`
	SeasonalStrength   float64 `json:"seasonal_strength"`
	ExpectedSeasonalValue float64 `json:"expected_seasonal_value,omitempty"`

	// Volatility analysis
	RecentVolatility   float64 `json:"recent_volatility"`
	BaselineVolatility float64 `json:"baseline_volatility"`
	VolatilityRatio    float64 `json:"volatility_ratio"`

	// Duration analysis (for sustained anomalies)
	Duration        time.Duration `json:"duration,omitempty"`
	ConsecutiveCount int           `json:"consecutive_count"`
}

// Remediation represents a suggested action to address the anomaly.
type Remediation struct {
	Action      string  `json:"action"`
	Description string  `json:"description"`
	Priority    int     `json:"priority"` // 1 = highest
	AutoExecute bool    `json:"auto_execute"`
	Confidence  float64 `json:"confidence"`
}

// AnomalyClassifier provides AI-powered anomaly classification.
type AnomalyClassifier struct {
	config       AnomalyClassifierConfig
	detector     *AnomalyDetector
	mu           sync.RWMutex

	// Historical data for pattern analysis
	recentValues    []float64
	recentTimestamps []int64
	maxHistory      int

	// Baseline statistics
	baselineMean   float64
	baselineStd    float64
	baselineMin    float64
	baselineMax    float64
	baselineTrend  float64

	// Seasonal decomposition
	seasonalPattern []float64
	seasonalPeriod  int

	// Recent anomaly tracking
	recentAnomalies []ClassifiedAnomaly
}

// AnomalyClassifierConfig configures the anomaly classifier.
type AnomalyClassifierConfig struct {
	// Spike detection threshold (number of std devs for sudden change)
	SpikeThreshold float64

	// Drift detection window size
	DriftWindowSize int

	// Level shift detection threshold
	LevelShiftThreshold float64

	// Seasonal period for seasonal deviation detection
	SeasonalPeriod int

	// Minimum consecutive points to confirm pattern
	MinConsecutivePoints int

	// Enable automatic remediation suggestions
	EnableRemediation bool

	// Custom remediation rules
	RemediationRules []RemediationRule
}

// RemediationRule defines a custom rule for generating remediations.
type RemediationRule struct {
	AnomalyType AnomalyType
	Condition   func(anomaly *ClassifiedAnomaly) bool
	Action      string
	Description string
	Priority    int
	AutoExecute bool
}

// DefaultAnomalyClassifierConfig returns default configuration.
func DefaultAnomalyClassifierConfig() AnomalyClassifierConfig {
	return AnomalyClassifierConfig{
		SpikeThreshold:       3.0,
		DriftWindowSize:      20,
		LevelShiftThreshold:  2.0,
		SeasonalPeriod:       24,
		MinConsecutivePoints: 3,
		EnableRemediation:    true,
	}
}

// NewAnomalyClassifier creates a new anomaly classifier.
func NewAnomalyClassifier(detector *AnomalyDetector, config AnomalyClassifierConfig) *AnomalyClassifier {
	if config.SpikeThreshold <= 0 {
		config.SpikeThreshold = 3.0
	}
	if config.DriftWindowSize <= 0 {
		config.DriftWindowSize = 20
	}
	if config.SeasonalPeriod <= 0 {
		config.SeasonalPeriod = 24
	}

	return &AnomalyClassifier{
		config:          config,
		detector:        detector,
		maxHistory:      1000,
		recentValues:    make([]float64, 0, 1000),
		recentTimestamps: make([]int64, 0, 1000),
		seasonalPeriod:  config.SeasonalPeriod,
		recentAnomalies: make([]ClassifiedAnomaly, 0),
	}
}

// UpdateBaseline updates the baseline statistics with new data.
func (c *AnomalyClassifier) UpdateBaseline(data TimeSeriesData) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(data.Values) == 0 {
		return
	}

	// Update recent history
	c.recentValues = append(c.recentValues, data.Values...)
	c.recentTimestamps = append(c.recentTimestamps, data.Timestamps...)

	// Trim to max history
	if len(c.recentValues) > c.maxHistory {
		excess := len(c.recentValues) - c.maxHistory
		c.recentValues = c.recentValues[excess:]
		c.recentTimestamps = c.recentTimestamps[excess:]
	}

	// Calculate baseline statistics
	c.baselineMean = mean(c.recentValues)
	c.baselineStd = stdDevSingle(c.recentValues)
	c.baselineMin, c.baselineMax = minMax(c.recentValues)

	// Calculate trend
	c.baselineTrend = c.calculateTrend(c.recentValues)

	// Extract seasonal pattern
	if len(c.recentValues) >= c.seasonalPeriod*2 {
		c.seasonalPattern = c.extractSeasonalPattern(c.recentValues, c.seasonalPeriod)
	}
}

// Classify performs anomaly detection and classification on a value.
func (c *AnomalyClassifier) Classify(value float64, timestamp int64, context []float64) (*ClassifiedAnomaly, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Get base anomaly detection result
	result, err := c.detector.Detect(value, context)
	if err != nil {
		// Use statistical fallback
		result = &AnomalyResult{
			Score:         c.calculateZScore(value),
			ExpectedValue: c.baselineMean,
			Deviation:     value - c.baselineMean,
		}
	}

	classified := &ClassifiedAnomaly{
		AnomalyResult: *result,
		Timestamp:     timestamp,
		Value:         value,
		BaselineValue: c.baselineMean,
		BaselineStd:   c.baselineStd,
	}

	// Classify the anomaly type
	c.classifyType(classified, context)

	// Determine severity
	c.determineSeverity(classified)

	// Analyze patterns
	classified.PatternAnalysis = c.analyzePatterns(value, context)

	// Generate remediations
	if c.config.EnableRemediation {
		classified.Remediations = c.generateRemediations(classified)
	}

	// Find related anomalies
	classified.RelatedAnomalies = c.findRelatedAnomalies(classified)

	// Store in recent anomalies if it's an actual anomaly
	if classified.Type != AnomalyTypeNormal {
		c.recentAnomalies = append(c.recentAnomalies, *classified)
		if len(c.recentAnomalies) > 100 {
			c.recentAnomalies = c.recentAnomalies[1:]
		}
	}

	// Update history
	c.recentValues = append(c.recentValues, value)
	c.recentTimestamps = append(c.recentTimestamps, timestamp)
	if len(c.recentValues) > c.maxHistory {
		c.recentValues = c.recentValues[1:]
		c.recentTimestamps = c.recentTimestamps[1:]
	}

	return classified, nil
}

func (c *AnomalyClassifier) classifyType(anomaly *ClassifiedAnomaly, context []float64) {
	// Check if it's actually anomalous
	if !anomaly.IsAnomaly && anomaly.Score < 0.3 {
		anomaly.Type = AnomalyTypeNormal
		anomaly.TypeName = anomaly.Type.String()
		anomaly.ClassificationConfidence = 1.0 - anomaly.Score
		return
	}

	value := anomaly.Value
	deviation := anomaly.Deviation
	
	// Calculate various features for classification
	zScore := c.calculateZScore(value)
	
	var typeScores = make(map[AnomalyType]float64)

	// Spike detection: sudden large positive deviation
	if deviation > 0 && zScore > c.config.SpikeThreshold {
		spikeScore := math.Min(zScore/c.config.SpikeThreshold, 1.0)
		if len(context) > 1 {
			// Check if previous values were normal
			prevMean := mean(context[max(0, len(context)-3):])
			if math.Abs(prevMean-c.baselineMean) < c.baselineStd {
				spikeScore *= 1.2 // Boost if coming from normal
			}
		}
		typeScores[AnomalyTypeSpike] = math.Min(spikeScore, 1.0)
	}

	// Dip detection: sudden large negative deviation
	if deviation < 0 && zScore > c.config.SpikeThreshold {
		dipScore := math.Min(zScore/c.config.SpikeThreshold, 1.0)
		if len(context) > 1 {
			prevMean := mean(context[max(0, len(context)-3):])
			if math.Abs(prevMean-c.baselineMean) < c.baselineStd {
				dipScore *= 1.2
			}
		}
		typeScores[AnomalyTypeDip] = math.Min(dipScore, 1.0)
	}

	// Drift detection: gradual shift over time
	if len(context) >= c.config.DriftWindowSize {
		driftScore := c.detectDrift(context, value)
		typeScores[AnomalyTypeDrift] = driftScore
	}

	// Level shift detection: permanent change in baseline
	if len(context) >= c.config.DriftWindowSize*2 {
		levelShiftScore := c.detectLevelShift(context, value)
		typeScores[AnomalyTypeLevelShift] = levelShiftScore
	}

	// Seasonal deviation detection
	if len(c.seasonalPattern) > 0 {
		seasonalScore := c.detectSeasonalDeviation(value, len(c.recentValues)%c.seasonalPeriod)
		typeScores[AnomalyTypeSeasonalDeviation] = seasonalScore
	}

	// Trend change detection
	if len(context) >= 10 {
		trendChangeScore := c.detectTrendChange(context, value)
		typeScores[AnomalyTypeTrendChange] = trendChangeScore
	}

	// Variance change detection
	if len(context) >= c.config.DriftWindowSize {
		varianceScore := c.detectVarianceChange(context)
		typeScores[AnomalyTypeVarianceChange] = varianceScore
	}

	// Outlier (generic statistical outlier if no specific pattern)
	typeScores[AnomalyTypeOutlier] = math.Min(zScore/2.0, 1.0) * 0.5 // Lower weight

	// Find the highest scoring type
	maxScore := 0.0
	selectedType := AnomalyTypeOutlier
	for t, score := range typeScores {
		if score > maxScore {
			maxScore = score
			selectedType = t
		}
	}

	anomaly.Type = selectedType
	anomaly.TypeName = selectedType.String()
	anomaly.ClassificationConfidence = math.Min(maxScore, 1.0)
}

func (c *AnomalyClassifier) calculateZScore(value float64) float64 {
	if c.baselineStd == 0 {
		return 0
	}
	return math.Abs(value-c.baselineMean) / c.baselineStd
}

func (c *AnomalyClassifier) detectDrift(context []float64, value float64) float64 {
	if len(context) < c.config.DriftWindowSize {
		return 0
	}

	// Compare early window to recent window
	windowSize := c.config.DriftWindowSize / 2
	earlyMean := mean(context[:windowSize])
	recentMean := mean(context[len(context)-windowSize:])

	// Check for consistent direction of change
	drift := (recentMean - earlyMean) / (c.baselineStd + 1e-10)

	// Drift should be gradual (not sudden)
	if math.Abs(drift) > 1.0 && math.Abs(drift) < c.config.SpikeThreshold {
		return sigmoid(math.Abs(drift) - 0.5)
	}
	return 0
}

func (c *AnomalyClassifier) detectLevelShift(context []float64, value float64) float64 {
	if len(context) < c.config.DriftWindowSize*2 {
		return 0
	}

	midpoint := len(context) / 2
	beforeMean := mean(context[:midpoint])
	afterMean := mean(context[midpoint:])
	beforeStd := stdDevSingle(context[:midpoint])
	afterStd := stdDevSingle(context[midpoint:])

	// Level shift: significant change in mean with similar variance
	meanShift := math.Abs(afterMean-beforeMean) / (c.baselineStd + 1e-10)
	varianceRatio := (afterStd + 1e-10) / (beforeStd + 1e-10)

	if meanShift > c.config.LevelShiftThreshold && varianceRatio > 0.5 && varianceRatio < 2.0 {
		return sigmoid(meanShift - c.config.LevelShiftThreshold)
	}
	return 0
}

func (c *AnomalyClassifier) detectSeasonalDeviation(value float64, position int) float64 {
	if len(c.seasonalPattern) == 0 || position >= len(c.seasonalPattern) {
		return 0
	}

	expectedSeasonal := c.baselineMean * c.seasonalPattern[position]
	deviation := math.Abs(value - expectedSeasonal)
	normalizedDev := deviation / (c.baselineStd + 1e-10)

	if normalizedDev > 2.0 {
		return sigmoid(normalizedDev - 2.0)
	}
	return 0
}

func (c *AnomalyClassifier) detectTrendChange(context []float64, value float64) float64 {
	if len(context) < 10 {
		return 0
	}

	// Calculate trend in first and second half
	mid := len(context) / 2
	trend1 := c.calculateTrend(context[:mid])
	trend2 := c.calculateTrend(context[mid:])

	// Detect sign change or significant magnitude change
	if (trend1 > 0 && trend2 < 0) || (trend1 < 0 && trend2 > 0) {
		return 0.8 // Strong signal for trend reversal
	}

	magnitudeChange := math.Abs(trend2-trend1) / (math.Abs(trend1) + 1e-10)
	if magnitudeChange > 2.0 {
		return sigmoid(magnitudeChange - 1.0) * 0.6
	}
	return 0
}

func (c *AnomalyClassifier) detectVarianceChange(context []float64) float64 {
	if len(context) < c.config.DriftWindowSize {
		return 0
	}

	windowSize := c.config.DriftWindowSize / 2
	recentStd := stdDevSingle(context[len(context)-windowSize:])
	
	ratio := recentStd / (c.baselineStd + 1e-10)
	if ratio > 2.0 || ratio < 0.5 {
		return sigmoid(math.Abs(math.Log(ratio)))
	}
	return 0
}

func (c *AnomalyClassifier) calculateTrend(values []float64) float64 {
	if len(values) < 2 {
		return 0
	}

	// Simple linear regression slope
	n := float64(len(values))
	sumX, sumY, sumXY, sumX2 := 0.0, 0.0, 0.0, 0.0

	for i, y := range values {
		x := float64(i)
		sumX += x
		sumY += y
		sumXY += x * y
		sumX2 += x * x
	}

	denominator := n*sumX2 - sumX*sumX
	if denominator == 0 {
		return 0
	}

	return (n*sumXY - sumX*sumY) / denominator
}

func (c *AnomalyClassifier) extractSeasonalPattern(values []float64, period int) []float64 {
	if len(values) < period*2 {
		return nil
	}

	pattern := make([]float64, period)
	counts := make([]int, period)

	for i, v := range values {
		pos := i % period
		pattern[pos] += v
		counts[pos]++
	}

	// Normalize to get multiplicative seasonal factors
	overallMean := mean(values)
	for i := range pattern {
		if counts[i] > 0 {
			pattern[i] = (pattern[i] / float64(counts[i])) / (overallMean + 1e-10)
		} else {
			pattern[i] = 1.0
		}
	}

	return pattern
}

func (c *AnomalyClassifier) determineSeverity(anomaly *ClassifiedAnomaly) {
	// Base severity on score and type
	score := anomaly.Score

	switch {
	case score < 0.3:
		anomaly.Severity = AnomalySeverityInfo
	case score < 0.6:
		anomaly.Severity = AnomalySeverityWarning
	case score < 0.85:
		anomaly.Severity = AnomalySeverityCritical
	default:
		anomaly.Severity = AnomalySeverityEmergency
	}

	// Adjust based on type
	switch anomaly.Type {
	case AnomalyTypeSpike, AnomalyTypeDip:
		// Sudden changes may be more critical
		if anomaly.Severity < AnomalySeverityCritical && score > 0.5 {
			anomaly.Severity = AnomalySeverityCritical
		}
	case AnomalyTypeMissing:
		// Missing data is always at least warning
		if anomaly.Severity < AnomalySeverityWarning {
			anomaly.Severity = AnomalySeverityWarning
		}
	}

	anomaly.SeverityStr = anomaly.Severity.String()
}

func (c *AnomalyClassifier) analyzePatterns(value float64, context []float64) *PatternAnalysis {
	analysis := &PatternAnalysis{}

	// Trend analysis
	if len(context) >= 5 {
		trend := c.calculateTrend(context)
		analysis.TrendStrength = math.Abs(trend)
		if trend > 0.01 {
			analysis.TrendDirection = "increasing"
		} else if trend < -0.01 {
			analysis.TrendDirection = "decreasing"
		} else {
			analysis.TrendDirection = "stable"
		}
	}

	// Volatility analysis
	if len(context) >= 10 {
		recentWindow := context[max(0, len(context)-10):]
		analysis.RecentVolatility = stdDevSingle(recentWindow)
		analysis.BaselineVolatility = c.baselineStd
		if c.baselineStd > 0 {
			analysis.VolatilityRatio = analysis.RecentVolatility / c.baselineStd
		}
	}

	// Seasonal analysis
	if len(c.seasonalPattern) > 0 {
		pos := len(c.recentValues) % c.seasonalPeriod
		if pos < len(c.seasonalPattern) {
			analysis.SeasonalPeriod = c.seasonalPeriod
			analysis.SeasonalStrength = stdDevSingle(c.seasonalPattern)
			analysis.ExpectedSeasonalValue = c.baselineMean * c.seasonalPattern[pos]
		}
	}

	// Consecutive anomaly count
	analysis.ConsecutiveCount = c.countConsecutiveAnomalies()

	return analysis
}

func (c *AnomalyClassifier) countConsecutiveAnomalies() int {
	count := 0
	for i := len(c.recentAnomalies) - 1; i >= 0; i-- {
		if c.recentAnomalies[i].Type != AnomalyTypeNormal {
			count++
		} else {
			break
		}
	}
	return count
}

func (c *AnomalyClassifier) generateRemediations(anomaly *ClassifiedAnomaly) []Remediation {
	var remediations []Remediation

	// Apply custom rules first
	for _, rule := range c.config.RemediationRules {
		if rule.AnomalyType == anomaly.Type {
			if rule.Condition == nil || rule.Condition(anomaly) {
				remediations = append(remediations, Remediation{
					Action:      rule.Action,
					Description: rule.Description,
					Priority:    rule.Priority,
					AutoExecute: rule.AutoExecute,
					Confidence:  anomaly.ClassificationConfidence,
				})
			}
		}
	}

	// Generate default remediations based on type
	switch anomaly.Type {
	case AnomalyTypeSpike:
		remediations = append(remediations,
			Remediation{
				Action:      "investigate_spike",
				Description: fmt.Sprintf("Investigate sudden increase of %.2f%% above baseline", (anomaly.Value/anomaly.BaselineValue-1)*100),
				Priority:    1,
				Confidence:  anomaly.ClassificationConfidence,
			},
			Remediation{
				Action:      "check_upstream_services",
				Description: "Check upstream services for sudden load increase",
				Priority:    2,
				Confidence:  0.7,
			},
		)

	case AnomalyTypeDip:
		remediations = append(remediations,
			Remediation{
				Action:      "investigate_dip",
				Description: fmt.Sprintf("Investigate sudden decrease of %.2f%% below baseline", (1-anomaly.Value/anomaly.BaselineValue)*100),
				Priority:    1,
				Confidence:  anomaly.ClassificationConfidence,
			},
			Remediation{
				Action:      "check_service_health",
				Description: "Verify all service components are healthy",
				Priority:    2,
				Confidence:  0.8,
			},
		)

	case AnomalyTypeDrift:
		remediations = append(remediations,
			Remediation{
				Action:      "monitor_drift",
				Description: "Continue monitoring for drift progression",
				Priority:    2,
				Confidence:  anomaly.ClassificationConfidence,
			},
			Remediation{
				Action:      "baseline_review",
				Description: "Consider updating baseline if drift represents new normal",
				Priority:    3,
				Confidence:  0.6,
			},
		)

	case AnomalyTypeLevelShift:
		remediations = append(remediations,
			Remediation{
				Action:      "confirm_level_shift",
				Description: "Confirm if level shift is intentional (deployment, config change)",
				Priority:    1,
				Confidence:  anomaly.ClassificationConfidence,
			},
			Remediation{
				Action:      "update_baseline",
				Description: "Update baseline to reflect new level if change is permanent",
				Priority:    2,
				AutoExecute: false,
				Confidence:  0.7,
			},
		)

	case AnomalyTypeSeasonalDeviation:
		remediations = append(remediations,
			Remediation{
				Action:      "check_seasonal_factors",
				Description: "Review external factors affecting seasonal pattern",
				Priority:    2,
				Confidence:  anomaly.ClassificationConfidence,
			},
		)

	case AnomalyTypeTrendChange:
		remediations = append(remediations,
			Remediation{
				Action:      "analyze_trend_cause",
				Description: "Analyze root cause of trend direction change",
				Priority:    1,
				Confidence:  anomaly.ClassificationConfidence,
			},
		)

	case AnomalyTypeVarianceChange:
		remediations = append(remediations,
			Remediation{
				Action:      "investigate_volatility",
				Description: "Investigate cause of increased/decreased volatility",
				Priority:    2,
				Confidence:  anomaly.ClassificationConfidence,
			},
		)

	case AnomalyTypeOutlier:
		remediations = append(remediations,
			Remediation{
				Action:      "validate_data",
				Description: "Validate data point for potential measurement error",
				Priority:    2,
				Confidence:  0.5,
			},
		)
	}

	// Sort by priority
	sort.Slice(remediations, func(i, j int) bool {
		return remediations[i].Priority < remediations[j].Priority
	})

	return remediations
}

func (c *AnomalyClassifier) findRelatedAnomalies(anomaly *ClassifiedAnomaly) []int64 {
	var related []int64

	// Look for recent anomalies of the same type or correlated timing
	threshold := time.Hour.Nanoseconds()
	for _, past := range c.recentAnomalies {
		if past.Timestamp == anomaly.Timestamp {
			continue
		}
		timeDiff := anomaly.Timestamp - past.Timestamp
		if timeDiff > 0 && timeDiff < threshold {
			if past.Type == anomaly.Type || c.isCorrelated(past, *anomaly) {
				related = append(related, past.Timestamp)
			}
		}
	}

	return related
}

func (c *AnomalyClassifier) isCorrelated(a, b ClassifiedAnomaly) bool {
	// Simple correlation check based on deviation direction and magnitude
	if (a.Deviation > 0) == (b.Deviation > 0) {
		// Same direction
		ratio := math.Abs(a.Deviation) / (math.Abs(b.Deviation) + 1e-10)
		return ratio > 0.5 && ratio < 2.0
	}
	return false
}

// ClassifyBatch classifies multiple data points.
func (c *AnomalyClassifier) ClassifyBatch(data TimeSeriesData) ([]ClassifiedAnomaly, error) {
	results := make([]ClassifiedAnomaly, len(data.Values))

	for i, value := range data.Values {
		// Build context from previous values
		contextStart := max(0, i-c.detector.config.WindowSize)
		context := data.Values[contextStart:i]

		classified, err := c.Classify(value, data.Timestamps[i], context)
		if err != nil {
			return nil, err
		}
		results[i] = *classified
	}

	return results, nil
}

// GetRecentAnomalies returns recent classified anomalies.
func (c *AnomalyClassifier) GetRecentAnomalies(limit int) []ClassifiedAnomaly {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if limit <= 0 || limit > len(c.recentAnomalies) {
		limit = len(c.recentAnomalies)
	}

	start := len(c.recentAnomalies) - limit
	result := make([]ClassifiedAnomaly, limit)
	copy(result, c.recentAnomalies[start:])
	return result
}

// GetAnomalySummary returns a summary of recent anomalies by type.
func (c *AnomalyClassifier) GetAnomalySummary() map[string]int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	summary := make(map[string]int)
	for _, a := range c.recentAnomalies {
		summary[a.TypeName]++
	}
	return summary
}

// helper for Go < 1.21
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
