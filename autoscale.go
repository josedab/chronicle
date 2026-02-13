package chronicle

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

// AutoscaleConfig configures the predictive autoscaling engine.
type AutoscaleConfig struct {
	Enabled              bool
	CollectionInterval   time.Duration
	ForecastHorizon      time.Duration
	EvaluationInterval   time.Duration
	ScaleUpThreshold     float64 // utilization ratio to trigger scale-up (e.g., 0.80)
	ScaleDownThreshold   float64 // utilization ratio to trigger scale-down (e.g., 0.30)
	CooldownPeriod       time.Duration
	MinBufferPool        int
	MaxBufferPool        int
	MinWriteWorkers      int
	MaxWriteWorkers      int
	MinCompactionWorkers int
	MaxCompactionWorkers int
	ForecastConfig       ForecastConfig
}

// DefaultAutoscaleConfig returns production-ready defaults.
func DefaultAutoscaleConfig() AutoscaleConfig {
	return AutoscaleConfig{
		Enabled:              true,
		CollectionInterval:   30 * time.Second,
		ForecastHorizon:      1 * time.Hour,
		EvaluationInterval:   1 * time.Minute,
		ScaleUpThreshold:     0.80,
		ScaleDownThreshold:   0.30,
		CooldownPeriod:       5 * time.Minute,
		MinBufferPool:        4,
		MaxBufferPool:        256,
		MinWriteWorkers:      1,
		MaxWriteWorkers:      32,
		MinCompactionWorkers: 1,
		MaxCompactionWorkers: 8,
		ForecastConfig:       DefaultForecastConfig(),
	}
}

// ScaleDimension identifies a tunable resource axis.
type ScaleDimension int

const (
	ScaleDimensionBufferPool ScaleDimension = iota
	ScaleDimensionWriteWorkers
	ScaleDimensionCompactionWorkers
	ScaleDimensionQueryConcurrency
	ScaleDimensionCacheSize
)

func (d ScaleDimension) String() string {
	switch d {
	case ScaleDimensionBufferPool:
		return "buffer_pool"
	case ScaleDimensionWriteWorkers:
		return "write_workers"
	case ScaleDimensionCompactionWorkers:
		return "compaction_workers"
	case ScaleDimensionQueryConcurrency:
		return "query_concurrency"
	case ScaleDimensionCacheSize:
		return "cache_size"
	default:
		return "unknown"
	}
}

// ScaleDirection indicates whether to increase or decrease resources.
type ScaleDirection int

const (
	ScaleDirectionNone ScaleDirection = iota
	ScaleDirectionUp
	ScaleDirectionDown
)

func (d ScaleDirection) String() string {
	switch d {
	case ScaleDirectionUp:
		return "up"
	case ScaleDirectionDown:
		return "down"
	default:
		return "none"
	}
}

// ScaleDecision represents an autoscaling action taken or proposed.
type ScaleDecision struct {
	Timestamp       time.Time      `json:"timestamp"`
	Dimension       ScaleDimension `json:"dimension"`
	Direction       ScaleDirection `json:"direction"`
	CurrentValue    int            `json:"current_value"`
	TargetValue     int            `json:"target_value"`
	Reason          string         `json:"reason"`
	PredictedLoad   float64        `json:"predicted_load"`
	ConfidenceLower float64        `json:"confidence_lower"`
	ConfidenceUpper float64        `json:"confidence_upper"`
	Applied         bool           `json:"applied"`
}

// AutoscaleSnapshot represents the current state of all tuned parameters.
type AutoscaleSnapshot struct {
	Timestamp             time.Time       `json:"timestamp"`
	BufferPoolSize        int             `json:"buffer_pool_size"`
	WriteWorkers          int             `json:"write_workers"`
	CompactionWorkers     int             `json:"compaction_workers"`
	WriteRatePerSec       float64         `json:"write_rate_per_sec"`
	QueryRatePerSec       float64         `json:"query_rate_per_sec"`
	MemoryUtilization     float64         `json:"memory_utilization"`
	StorageUtilization    float64         `json:"storage_utilization"`
	PendingDecisions      int             `json:"pending_decisions"`
	TotalScaleUps         int64           `json:"total_scale_ups"`
	TotalScaleDowns       int64           `json:"total_scale_downs"`
	RecentDecisions       []ScaleDecision `json:"recent_decisions,omitempty"`
}

// InternalMetricSample holds a single observation of an internal metric.
type InternalMetricSample struct {
	Timestamp time.Time
	Value     float64
}

// AutoscaleEngine monitors internal metrics, forecasts workload, and adjusts parameters.
type AutoscaleEngine struct {
	db     *DB
	config AutoscaleConfig

	forecaster *Forecaster

	mu             sync.RWMutex
	metricHistory  map[string][]InternalMetricSample
	decisions      []ScaleDecision
	lastScaleTime  map[ScaleDimension]time.Time
	currentValues  map[ScaleDimension]int
	totalScaleUps  int64
	totalScaleDown int64

	onDecision func(*ScaleDecision)

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewAutoscaleEngine creates a predictive autoscaling engine.
func NewAutoscaleEngine(db *DB, config AutoscaleConfig) *AutoscaleEngine {
	if config.ScaleUpThreshold <= 0 {
		config.ScaleUpThreshold = 0.80
	}
	if config.ScaleDownThreshold <= 0 {
		config.ScaleDownThreshold = 0.30
	}
	if config.CooldownPeriod <= 0 {
		config.CooldownPeriod = 5 * time.Minute
	}
	if config.CollectionInterval <= 0 {
		config.CollectionInterval = 30 * time.Second
	}
	if config.EvaluationInterval <= 0 {
		config.EvaluationInterval = time.Minute
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &AutoscaleEngine{
		db:            db,
		config:        config,
		forecaster:    NewForecaster(config.ForecastConfig),
		metricHistory: make(map[string][]InternalMetricSample),
		decisions:     make([]ScaleDecision, 0, 128),
		lastScaleTime: make(map[ScaleDimension]time.Time),
		currentValues: map[ScaleDimension]int{
			ScaleDimensionBufferPool:        config.MinBufferPool * 2,
			ScaleDimensionWriteWorkers:      config.MinWriteWorkers * 2,
			ScaleDimensionCompactionWorkers: config.MinCompactionWorkers,
		},
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start begins the background collection and evaluation loops.
func (e *AutoscaleEngine) Start() {
	e.wg.Add(2)
	go e.collectionLoop()
	go e.evaluationLoop()
}

// Stop gracefully shuts down the autoscaler.
func (e *AutoscaleEngine) Stop() {
	e.cancel()
	e.wg.Wait()
}

// OnDecision registers a callback for scale decisions.
func (e *AutoscaleEngine) OnDecision(fn func(*ScaleDecision)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.onDecision = fn
}

// Snapshot returns the current autoscale state.
func (e *AutoscaleEngine) Snapshot() AutoscaleSnapshot {
	e.mu.RLock()
	defer e.mu.RUnlock()

	snap := AutoscaleSnapshot{
		Timestamp:         time.Now(),
		BufferPoolSize:    e.currentValues[ScaleDimensionBufferPool],
		WriteWorkers:      e.currentValues[ScaleDimensionWriteWorkers],
		CompactionWorkers: e.currentValues[ScaleDimensionCompactionWorkers],
		TotalScaleUps:     e.totalScaleUps,
		TotalScaleDowns:   e.totalScaleDown,
		PendingDecisions:  0,
	}

	// Latest rates from history
	if samples, ok := e.metricHistory["write_rate"]; ok && len(samples) > 0 {
		snap.WriteRatePerSec = samples[len(samples)-1].Value
	}
	if samples, ok := e.metricHistory["query_rate"]; ok && len(samples) > 0 {
		snap.QueryRatePerSec = samples[len(samples)-1].Value
	}
	if samples, ok := e.metricHistory["memory_util"]; ok && len(samples) > 0 {
		snap.MemoryUtilization = samples[len(samples)-1].Value
	}
	if samples, ok := e.metricHistory["storage_util"]; ok && len(samples) > 0 {
		snap.StorageUtilization = samples[len(samples)-1].Value
	}

	// Recent decisions (last 20)
	n := len(e.decisions)
	start := 0
	if n > 20 {
		start = n - 20
	}
	snap.RecentDecisions = make([]ScaleDecision, n-start)
	copy(snap.RecentDecisions, e.decisions[start:])

	return snap
}

// Decisions returns a copy of all historical scaling decisions.
func (e *AutoscaleEngine) Decisions() []ScaleDecision {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]ScaleDecision, len(e.decisions))
	copy(out, e.decisions)
	return out
}

func (e *AutoscaleEngine) collectionLoop() {
	defer e.wg.Done()
	ticker := time.NewTicker(e.config.CollectionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.collectMetrics()
		}
	}
}

func (e *AutoscaleEngine) evaluationLoop() {
	defer e.wg.Done()
	ticker := time.NewTicker(e.config.EvaluationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.evaluate()
		}
	}
}

// collectMetrics gathers internal DB metrics.
func (e *AutoscaleEngine) collectMetrics() {
	now := time.Now()

	// Gather metrics from DB internals (simulated via db.Metrics())
	sample := func(name string, value float64) {
		e.mu.Lock()
		defer e.mu.Unlock()
		e.metricHistory[name] = append(e.metricHistory[name], InternalMetricSample{
			Timestamp: now,
			Value:     value,
		})
		// Keep history bounded
		if len(e.metricHistory[name]) > 10000 {
			e.metricHistory[name] = e.metricHistory[name][5000:]
		}
	}

	// Collect real metrics if DB is available
	if e.db != nil {
		sample("write_rate", e.estimateWriteRate())
		sample("query_rate", e.estimateQueryRate())
		sample("memory_util", e.estimateMemoryUtil())
		sample("storage_util", e.estimateStorageUtil())
	}
}

// estimateWriteRate returns estimated writes/sec from recent activity.
func (e *AutoscaleEngine) estimateWriteRate() float64 {
	metrics := e.db.Metrics()
	return float64(len(metrics)) * 0.1 // heuristic based on metric count
}

func (e *AutoscaleEngine) estimateQueryRate() float64 {
	return 0.0 // placeholder — real impl reads from query engine stats
}

func (e *AutoscaleEngine) estimateMemoryUtil() float64 {
	return 0.5 // placeholder — real impl reads runtime.MemStats
}

func (e *AutoscaleEngine) estimateStorageUtil() float64 {
	return 0.3 // placeholder — real impl reads disk usage
}

// evaluate runs forecast-based scaling decisions.
func (e *AutoscaleEngine) evaluate() {
	e.mu.RLock()
	writeHistory := e.getTimeSeries("write_rate")
	memHistory := e.getTimeSeries("memory_util")
	e.mu.RUnlock()

	now := time.Now()

	// Forecast write rate
	if len(writeHistory.Values) >= 10 {
		forecast, err := e.forecaster.Forecast(writeHistory, 6)
		if err == nil && len(forecast.Predictions) > 0 {
			predicted := forecast.Predictions[len(forecast.Predictions)-1].Value
			e.evaluateDimension(ScaleDimensionWriteWorkers, predicted, now,
				e.config.MinWriteWorkers, e.config.MaxWriteWorkers)
			e.evaluateDimension(ScaleDimensionBufferPool, predicted,
				now, e.config.MinBufferPool, e.config.MaxBufferPool)
		}
	}

	// Forecast memory utilization for compaction workers
	if len(memHistory.Values) >= 10 {
		forecast, err := e.forecaster.Forecast(memHistory, 6)
		if err == nil && len(forecast.Predictions) > 0 {
			predicted := forecast.Predictions[len(forecast.Predictions)-1].Value
			e.evaluateDimension(ScaleDimensionCompactionWorkers, predicted, now,
				e.config.MinCompactionWorkers, e.config.MaxCompactionWorkers)
		}
	}
}

func (e *AutoscaleEngine) getTimeSeries(name string) TimeSeriesData {
	samples, ok := e.metricHistory[name]
	if !ok {
		return TimeSeriesData{}
	}
	ts := TimeSeriesData{
		Timestamps: make([]int64, len(samples)),
		Values:     make([]float64, len(samples)),
	}
	for i, s := range samples {
		ts.Timestamps[i] = s.Timestamp.UnixMilli()
		ts.Values[i] = s.Value
	}
	return ts
}

func (e *AutoscaleEngine) evaluateDimension(dim ScaleDimension, predicted float64, now time.Time, minVal, maxVal int) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Cooldown check
	if last, ok := e.lastScaleTime[dim]; ok {
		if now.Sub(last) < e.config.CooldownPeriod {
			return
		}
	}

	current := e.currentValues[dim]
	var direction ScaleDirection
	var target int
	var reason string

	if predicted >= e.config.ScaleUpThreshold {
		direction = ScaleDirectionUp
		// Scale up by 50% or at least 1
		increment := int(math.Ceil(float64(current) * 0.5))
		if increment < 1 {
			increment = 1
		}
		target = current + increment
		if target > maxVal {
			target = maxVal
		}
		reason = fmt.Sprintf("predicted load %.2f exceeds threshold %.2f", predicted, e.config.ScaleUpThreshold)
	} else if predicted <= e.config.ScaleDownThreshold && current > minVal {
		direction = ScaleDirectionDown
		decrement := int(math.Ceil(float64(current) * 0.25))
		if decrement < 1 {
			decrement = 1
		}
		target = current - decrement
		if target < minVal {
			target = minVal
		}
		reason = fmt.Sprintf("predicted load %.2f below threshold %.2f", predicted, e.config.ScaleDownThreshold)
	}

	if direction == ScaleDirectionNone || target == current {
		return
	}

	decision := ScaleDecision{
		Timestamp:     now,
		Dimension:     dim,
		Direction:     direction,
		CurrentValue:  current,
		TargetValue:   target,
		Reason:        reason,
		PredictedLoad: predicted,
		Applied:       true,
	}

	e.currentValues[dim] = target
	e.lastScaleTime[dim] = now
	e.decisions = append(e.decisions, decision)

	if direction == ScaleDirectionUp {
		e.totalScaleUps++
	} else {
		e.totalScaleDown++
	}

	if e.onDecision != nil {
		e.onDecision(&decision)
	}
}

// HistogramBuckets computes a histogram from metric samples for visualization.
func HistogramBuckets(samples []InternalMetricSample, buckets int) []struct {
	Lower float64
	Upper float64
	Count int
} {
	if len(samples) == 0 || buckets <= 0 {
		return nil
	}

	values := make([]float64, len(samples))
	for i, s := range samples {
		values[i] = s.Value
	}
	sort.Float64s(values)

	min, max := values[0], values[len(values)-1]
	if min == max {
		return []struct {
			Lower float64
			Upper float64
			Count int
		}{{Lower: min, Upper: max, Count: len(values)}}
	}

	width := (max - min) / float64(buckets)
	result := make([]struct {
		Lower float64
		Upper float64
		Count int
	}, buckets)

	for i := range result {
		result[i].Lower = min + float64(i)*width
		result[i].Upper = min + float64(i+1)*width
	}

	for _, v := range values {
		idx := int((v - min) / width)
		if idx >= buckets {
			idx = buckets - 1
		}
		result[idx].Count++
	}

	return result
}
