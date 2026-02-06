package chronicle

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

// AnomalyPipelineConfig configures the streaming anomaly detection pipeline.
type AnomalyPipelineConfig struct {
	Enabled            bool          `json:"enabled"`
	Sensitivity        float64       `json:"sensitivity"`
	WindowSize         int           `json:"window_size"`
	EvaluationInterval time.Duration `json:"evaluation_interval"`
	ZScoreThreshold    float64       `json:"zscore_threshold"`
	IQRMultiplier      float64       `json:"iqr_multiplier"`
	MinDataPoints      int           `json:"min_data_points"`
	EnableAutoAlert    bool          `json:"enable_auto_alert"`
	AlertWebhookURL    string        `json:"alert_webhook_url,omitempty"`
}

// DefaultAnomalyPipelineConfig returns sensible defaults for the anomaly pipeline.
func DefaultAnomalyPipelineConfig() AnomalyPipelineConfig {
	return AnomalyPipelineConfig{
		Enabled:            false,
		Sensitivity:        0.5,
		WindowSize:         100,
		EvaluationInterval: 10 * time.Second,
		ZScoreThreshold:    3.0,
		IQRMultiplier:      1.5,
		MinDataPoints:      20,
		EnableAutoAlert:    true,
	}
}

// metricBaseline holds rolling statistics for a single metric.
type metricBaseline struct {
	values      []float64
	mean        float64
	stddev      float64
	q1          float64
	q3          float64
	count       int64
	lastUpdated time.Time
}

// PipelineAnomaly represents an anomaly detected by the streaming pipeline.
type PipelineAnomaly struct {
	ID        string            `json:"id"`
	Metric    string            `json:"metric"`
	Tags      map[string]string `json:"tags,omitempty"`
	Timestamp time.Time         `json:"timestamp"`
	Value     float64           `json:"value"`
	Expected  float64           `json:"expected"`
	Score     float64           `json:"score"`
	Type      string            `json:"type"`
	Method    string            `json:"method"`
	Severity  string            `json:"severity"`
}

// AnomalyPipelineStats holds pipeline statistics.
type AnomalyPipelineStats struct {
	TotalProcessed int64 `json:"total_processed"`
	TotalAnomalies int64 `json:"total_anomalies"`
	ActiveMetrics  int   `json:"active_metrics"`
	Running        bool  `json:"running"`
}

// AnomalyPipeline provides streaming anomaly detection over real-time metric points.
type AnomalyPipeline struct {
	config      AnomalyPipelineConfig
	db          *DB
	hub         *StreamHub
	alertMgr    *AlertManager
	correlation *AnomalyCorrelationEngine

	baselines map[string]*metricBaseline
	anomalies []PipelineAnomaly
	mu        sync.RWMutex
	running   bool
	cancel    context.CancelFunc

	totalProcessed int64
	totalAnomalies int64
}

// NewAnomalyPipeline creates a new streaming anomaly detection pipeline.
func NewAnomalyPipeline(db *DB, config AnomalyPipelineConfig) *AnomalyPipeline {
	p := &AnomalyPipeline{
		config:    config,
		db:        db,
		baselines: make(map[string]*metricBaseline),
		anomalies: make([]PipelineAnomaly, 0, 256),
	}
	if db != nil && db.features != nil {
		p.alertMgr = db.features.AlertManager()
		p.correlation = db.features.AnomalyCorrelation()
	}
	return p
}

// SetStreamHub sets the stream hub for subscribing to real-time points.
func (p *AnomalyPipeline) SetStreamHub(hub *StreamHub) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.hub = hub
}

// Start begins the background anomaly detection goroutine.
func (p *AnomalyPipeline) Start(ctx context.Context) error {
	p.mu.Lock()
	if p.running {
		p.mu.Unlock()
		return fmt.Errorf("anomaly pipeline already running")
	}
	if p.hub == nil {
		p.mu.Unlock()
		return fmt.Errorf("stream hub not configured")
	}
	p.running = true
	ctx, cancel := context.WithCancel(ctx)
	p.cancel = cancel
	p.mu.Unlock()

	sub := p.hub.Subscribe("", nil)

	go func() {
		defer sub.Close()
		for {
			select {
			case <-ctx.Done():
				return
			case pt, ok := <-sub.C():
				if !ok {
					return
				}
				p.processPoint(pt)
			}
		}
	}()

	return nil
}

// Stop stops the anomaly detection pipeline.
func (p *AnomalyPipeline) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.running {
		return
	}
	p.running = false
	if p.cancel != nil {
		p.cancel()
		p.cancel = nil
	}
}

func (p *AnomalyPipeline) processPoint(pt Point) {
	p.mu.Lock()
	p.totalProcessed++

	b, ok := p.baselines[pt.Metric]
	if !ok {
		b = &metricBaseline{
			values: make([]float64, 0, p.config.WindowSize),
		}
		p.baselines[pt.Metric] = b
	}

	p.updateBaseline(b, pt.Value)

	if b.count < int64(p.config.MinDataPoints) {
		p.mu.Unlock()
		return
	}

	var detected bool
	var method string
	var score float64

	// Z-score detection
	if b.stddev > 0 {
		z := math.Abs(pt.Value-b.mean) / b.stddev
		if z > p.config.ZScoreThreshold {
			detected = true
			method = "zscore"
			score = math.Min(1.0, z/10.0)
		}
	}

	// IQR detection
	if !detected {
		iqr := b.q3 - b.q1
		if iqr > 0 {
			lower := b.q1 - p.config.IQRMultiplier*iqr
			upper := b.q3 + p.config.IQRMultiplier*iqr
			if pt.Value < lower || pt.Value > upper {
				detected = true
				method = "iqr"
				var dist float64
				if pt.Value < lower {
					dist = lower - pt.Value
				} else {
					dist = pt.Value - upper
				}
				score = math.Min(1.0, dist/(iqr+1e-9))
			}
		}
	}

	if !detected {
		p.mu.Unlock()
		return
	}

	// Apply sensitivity adjustment
	score = score * p.config.Sensitivity

	anomalyType, severity := p.classifyAnomaly(pt.Value, b.mean, score)

	anomaly := PipelineAnomaly{
		ID:        fmt.Sprintf("anom-%d", time.Now().UnixNano()),
		Metric:    pt.Metric,
		Tags:      pt.Tags,
		Timestamp: time.Now(),
		Value:     pt.Value,
		Expected:  b.mean,
		Score:     score,
		Type:      anomalyType,
		Method:    method,
		Severity:  severity,
	}

	p.anomalies = append(p.anomalies, anomaly)
	p.totalAnomalies++

	// Keep anomalies list bounded
	if len(p.anomalies) > 10000 {
		p.anomalies = p.anomalies[len(p.anomalies)-5000:]
	}

	correlation := p.correlation
	p.mu.Unlock()

	// Feed to correlation engine for warning+ severity
	if p.config.EnableAutoAlert && correlation != nil && (severity == "warning" || severity == "critical") {
		correlation.IngestSignal(AnomalySignal{
			ID:         anomaly.ID,
			SignalType: CorrelationSignalMetric,
			Metric:     anomaly.Metric,
			Tags:       anomaly.Tags,
			Value:      anomaly.Value,
			Severity:   anomaly.Score,
			Timestamp:  anomaly.Timestamp,
			Message:    fmt.Sprintf("%s anomaly on %s: value=%.4f expected=%.4f", anomaly.Type, anomaly.Metric, anomaly.Value, anomaly.Expected),
		})
	}
}

func (p *AnomalyPipeline) updateBaseline(b *metricBaseline, value float64) {
	b.values = append(b.values, value)
	if len(b.values) > p.config.WindowSize {
		b.values = b.values[len(b.values)-p.config.WindowSize:]
	}
	b.count++
	b.lastUpdated = time.Now()

	n := float64(len(b.values))
	sum := 0.0
	for _, v := range b.values {
		sum += v
	}
	b.mean = sum / n

	variance := 0.0
	for _, v := range b.values {
		d := v - b.mean
		variance += d * d
	}
	if n > 1 {
		variance /= (n - 1)
	}
	b.stddev = math.Sqrt(variance)

	// Compute quartiles
	sorted := make([]float64, len(b.values))
	copy(sorted, b.values)
	sort.Float64s(sorted)
	b.q1 = percentile(sorted, 25)
	b.q3 = percentile(sorted, 75)
}

func percentile(sorted []float64, pct float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := pct / 100.0 * float64(len(sorted)-1)
	lower := int(rank)
	upper := lower + 1
	if upper >= len(sorted) {
		return sorted[len(sorted)-1]
	}
	frac := rank - float64(lower)
	return sorted[lower]*(1-frac) + sorted[upper]*frac
}

func (p *AnomalyPipeline) classifyAnomaly(value, expected, score float64) (string, string) {
	// Determine anomaly type
	diff := value - expected
	anomalyType := "outlier"
	if diff > 0 {
		if score > 0.7 {
			anomalyType = "spike"
		}
	} else {
		if score > 0.7 {
			anomalyType = "dip"
		}
	}
	if math.Abs(diff) > 0 && score > 0.3 && score <= 0.7 {
		anomalyType = "drift"
	}

	// Determine severity
	severity := "info"
	if score >= 0.3 {
		severity = "warning"
	}
	if score >= 0.7 {
		severity = "critical"
	}

	return anomalyType, severity
}

// ListAnomalies returns detected anomalies filtered by metric, time, and limit.
func (p *AnomalyPipeline) ListAnomalies(metric string, since time.Time, limit int) []PipelineAnomaly {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var result []PipelineAnomaly
	for i := len(p.anomalies) - 1; i >= 0; i-- {
		a := p.anomalies[i]
		if metric != "" && a.Metric != metric {
			continue
		}
		if !since.IsZero() && a.Timestamp.Before(since) {
			continue
		}
		result = append(result, a)
		if limit > 0 && len(result) >= limit {
			break
		}
	}
	return result
}

// GetBaseline returns the current baseline statistics for a metric.
func (p *AnomalyPipeline) GetBaseline(metric string) *metricBaseline {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.baselines[metric]
}

// Stats returns pipeline statistics.
func (p *AnomalyPipeline) Stats() AnomalyPipelineStats {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return AnomalyPipelineStats{
		TotalProcessed: p.totalProcessed,
		TotalAnomalies: p.totalAnomalies,
		ActiveMetrics:  len(p.baselines),
		Running:        p.running,
	}
}
