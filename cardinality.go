package chronicle

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// CardinalityTracker monitors and limits series cardinality to prevent
// cardinality explosions that can cause memory exhaustion.
type CardinalityTracker struct {
	db     *DB
	config CardinalityConfig

	// Global tracking
	totalSeries  atomic.Int64
	metricCounts sync.Map // map[metric]int64

	// HyperLogLog sketches for estimation (simplified - uses exact count for now)
	labelValues sync.Map // map[labelKey]map[string]struct{}

	// Alerts
	alerts   []CardinalityAlert
	alertsMu sync.RWMutex

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// CardinalityConfig configures cardinality tracking and limits.
type CardinalityConfig struct {
	// Enabled enables cardinality tracking.
	Enabled bool

	// MaxTotalSeries is the maximum number of unique series allowed globally.
	// 0 means unlimited.
	MaxTotalSeries int64

	// MaxSeriesPerMetric is the maximum series per metric name.
	// 0 means unlimited.
	MaxSeriesPerMetric int64

	// MaxLabelValues is the maximum unique values per label key.
	// 0 means unlimited.
	MaxLabelValues int64

	// AlertThresholdPercent triggers an alert when usage exceeds this percentage.
	// Default: 80
	AlertThresholdPercent int

	// CheckInterval is how often to check for cardinality issues.
	// Default: 1 minute
	CheckInterval time.Duration

	// OnAlert is called when a cardinality alert is triggered.
	OnAlert func(alert CardinalityAlert)
}

// CardinalityAlert represents a cardinality warning or limit breach.
type CardinalityAlert struct {
	Type      CardinalityAlertType
	Metric    string
	LabelKey  string
	Current   int64
	Limit     int64
	Timestamp time.Time
	Message   string
}

// CardinalityAlertType categorizes cardinality alerts.
type CardinalityAlertType int

const (
	AlertHighCardinality CardinalityAlertType = iota
	AlertLimitReached
	AlertLabelExplosion
)

func (t CardinalityAlertType) String() string {
	switch t {
	case AlertHighCardinality:
		return "high_cardinality"
	case AlertLimitReached:
		return "limit_reached"
	case AlertLabelExplosion:
		return "label_explosion"
	default:
		return "unknown"
	}
}

// CardinalityStats contains cardinality statistics.
type CardinalityStats struct {
	TotalSeries   int64
	MetricCount   int
	TopMetrics    []MetricCardinality
	TopLabels     []LabelCardinality
	ActiveAlerts  []CardinalityAlert
	LastCheckTime time.Time
}

// MetricCardinality shows series count for a metric.
type MetricCardinality struct {
	Metric      string
	SeriesCount int64
	Percentage  float64
}

// LabelCardinality shows unique value count for a label.
type LabelCardinality struct {
	LabelKey   string
	ValueCount int64
	TopValues  []string
}

// DefaultCardinalityConfig returns sensible defaults.
func DefaultCardinalityConfig() CardinalityConfig {
	return CardinalityConfig{
		Enabled:               true,
		MaxTotalSeries:        1_000_000,
		MaxSeriesPerMetric:    100_000,
		MaxLabelValues:        10_000,
		AlertThresholdPercent: 80,
		CheckInterval:         time.Minute,
	}
}

// NewCardinalityTracker creates a new cardinality tracker.
func NewCardinalityTracker(db *DB, config CardinalityConfig) *CardinalityTracker {
	if config.AlertThresholdPercent <= 0 {
		config.AlertThresholdPercent = 80
	}
	if config.CheckInterval <= 0 {
		config.CheckInterval = time.Minute
	}

	return &CardinalityTracker{
		db:     db,
		config: config,
		stopCh: make(chan struct{}),
	}
}

// Start begins background cardinality monitoring.
func (ct *CardinalityTracker) Start() {
	if !ct.config.Enabled {
		return
	}

	ct.wg.Add(1)
	go ct.monitorLoop()
}

// Stop stops the cardinality tracker.
func (ct *CardinalityTracker) Stop() {
	close(ct.stopCh)
	ct.wg.Wait()
}

func (ct *CardinalityTracker) monitorLoop() {
	defer ct.wg.Done()

	ticker := time.NewTicker(ct.config.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ct.stopCh:
			return
		case <-ticker.C:
			ct.checkCardinality()
		}
	}
}

// TrackPoint records a point for cardinality tracking.
// Returns an error if cardinality limits would be exceeded.
func (ct *CardinalityTracker) TrackPoint(p Point) error {
	if !ct.config.Enabled {
		return nil
	}

	seriesKey := makeSeriesKey(p.Metric, p.Tags)

	// Check global limit
	if ct.config.MaxTotalSeries > 0 {
		current := ct.totalSeries.Load()
		if current >= ct.config.MaxTotalSeries {
			// Check if this is a new series
			if !ct.seriesExists(seriesKey) {
				return fmt.Errorf("cardinality limit exceeded: %d/%d total series",
					current, ct.config.MaxTotalSeries)
			}
		}
	}

	// Check per-metric limit
	if ct.config.MaxSeriesPerMetric > 0 {
		count := ct.getMetricCount(p.Metric)
		if count >= ct.config.MaxSeriesPerMetric {
			if !ct.seriesExistsForMetric(p.Metric, seriesKey) {
				return fmt.Errorf("cardinality limit exceeded for metric %s: %d/%d series",
					p.Metric, count, ct.config.MaxSeriesPerMetric)
			}
		}
	}

	// Track label values
	for k, v := range p.Tags {
		if ct.config.MaxLabelValues > 0 {
			if err := ct.trackLabelValue(k, v); err != nil {
				return err
			}
		}
	}

	return nil
}

// RecordSeries records that a series was written.
func (ct *CardinalityTracker) RecordSeries(metric string, tags map[string]string) {
	if !ct.config.Enabled {
		return
	}

	ct.totalSeries.Add(1)
	ct.incrementMetricCount(metric)

	for k, v := range tags {
		ct.recordLabelValue(k, v)
	}
}

func (ct *CardinalityTracker) seriesExists(key string) bool {
	// In a full implementation, this would check the index
	// For now, we allow it (optimistic)
	return false
}

func (ct *CardinalityTracker) seriesExistsForMetric(metric, key string) bool {
	return false
}

func (ct *CardinalityTracker) getMetricCount(metric string) int64 {
	if v, ok := ct.metricCounts.Load(metric); ok {
		return v.(int64)
	}
	return 0
}

func (ct *CardinalityTracker) incrementMetricCount(metric string) {
	for {
		old, _ := ct.metricCounts.LoadOrStore(metric, int64(0))
		oldVal := old.(int64)
		if ct.metricCounts.CompareAndSwap(metric, oldVal, oldVal+1) {
			break
		}
	}
}

func (ct *CardinalityTracker) trackLabelValue(key, value string) error {
	valuesI, _ := ct.labelValues.LoadOrStore(key, &sync.Map{})
	values := valuesI.(*sync.Map)

	// Count current values
	count := int64(0)
	values.Range(func(_, _ any) bool {
		count++
		return true
	})

	// Check if this is a new value
	if _, exists := values.Load(value); !exists {
		if count >= ct.config.MaxLabelValues {
			return fmt.Errorf("label cardinality limit exceeded for %s: %d/%d values",
				key, count, ct.config.MaxLabelValues)
		}
	}

	return nil
}

func (ct *CardinalityTracker) recordLabelValue(key, value string) {
	valuesI, _ := ct.labelValues.LoadOrStore(key, &sync.Map{})
	values := valuesI.(*sync.Map)
	values.Store(value, struct{}{})
}

func (ct *CardinalityTracker) checkCardinality() {
	ct.alertsMu.Lock()
	defer ct.alertsMu.Unlock()

	// Clear old alerts
	ct.alerts = ct.alerts[:0]

	total := ct.totalSeries.Load()

	// Check global threshold
	if ct.config.MaxTotalSeries > 0 {
		threshold := int64(float64(ct.config.MaxTotalSeries) * float64(ct.config.AlertThresholdPercent) / 100)
		if total >= threshold {
			alert := CardinalityAlert{
				Type:      AlertHighCardinality,
				Current:   total,
				Limit:     ct.config.MaxTotalSeries,
				Timestamp: time.Now(),
				Message:   fmt.Sprintf("Total series count %d exceeds %d%% of limit %d", total, ct.config.AlertThresholdPercent, ct.config.MaxTotalSeries),
			}
			ct.alerts = append(ct.alerts, alert)
			if ct.config.OnAlert != nil {
				ct.config.OnAlert(alert)
			}
		}
	}

	// Check per-metric thresholds
	if ct.config.MaxSeriesPerMetric > 0 {
		threshold := int64(float64(ct.config.MaxSeriesPerMetric) * float64(ct.config.AlertThresholdPercent) / 100)
		ct.metricCounts.Range(func(k, v any) bool {
			metric := k.(string)
			count := v.(int64)
			if count >= threshold {
				alert := CardinalityAlert{
					Type:      AlertHighCardinality,
					Metric:    metric,
					Current:   count,
					Limit:     ct.config.MaxSeriesPerMetric,
					Timestamp: time.Now(),
					Message:   fmt.Sprintf("Metric %s has %d series, exceeds %d%% of limit", metric, count, ct.config.AlertThresholdPercent),
				}
				ct.alerts = append(ct.alerts, alert)
				if ct.config.OnAlert != nil {
					ct.config.OnAlert(alert)
				}
			}
			return true
		})
	}

	// Check label value thresholds
	if ct.config.MaxLabelValues > 0 {
		threshold := int64(float64(ct.config.MaxLabelValues) * float64(ct.config.AlertThresholdPercent) / 100)
		ct.labelValues.Range(func(k, v any) bool {
			labelKey := k.(string)
			values := v.(*sync.Map)
			count := int64(0)
			values.Range(func(_, _ any) bool {
				count++
				return true
			})
			if count >= threshold {
				alert := CardinalityAlert{
					Type:      AlertLabelExplosion,
					LabelKey:  labelKey,
					Current:   count,
					Limit:     ct.config.MaxLabelValues,
					Timestamp: time.Now(),
					Message:   fmt.Sprintf("Label %s has %d unique values, exceeds %d%% of limit", labelKey, count, ct.config.AlertThresholdPercent),
				}
				ct.alerts = append(ct.alerts, alert)
				if ct.config.OnAlert != nil {
					ct.config.OnAlert(alert)
				}
			}
			return true
		})
	}
}

// Stats returns current cardinality statistics.
func (ct *CardinalityTracker) Stats() CardinalityStats {
	stats := CardinalityStats{
		TotalSeries:   ct.totalSeries.Load(),
		LastCheckTime: time.Now(),
	}

	// Collect metric counts
	var metrics []MetricCardinality
	ct.metricCounts.Range(func(k, v any) bool {
		metrics = append(metrics, MetricCardinality{
			Metric:      k.(string),
			SeriesCount: v.(int64),
		})
		return true
	})

	// Sort by series count descending
	sort.Slice(metrics, func(i, j int) bool {
		return metrics[i].SeriesCount > metrics[j].SeriesCount
	})

	// Calculate percentages and take top 10
	for i := range metrics {
		if stats.TotalSeries > 0 {
			metrics[i].Percentage = float64(metrics[i].SeriesCount) / float64(stats.TotalSeries) * 100
		}
	}
	if len(metrics) > 10 {
		metrics = metrics[:10]
	}
	stats.TopMetrics = metrics
	stats.MetricCount = len(metrics)

	// Collect label cardinalities
	var labels []LabelCardinality
	ct.labelValues.Range(func(k, v any) bool {
		labelKey := k.(string)
		values := v.(*sync.Map)

		var valueList []string
		count := int64(0)
		values.Range(func(vk, _ any) bool {
			count++
			if len(valueList) < 5 {
				valueList = append(valueList, vk.(string))
			}
			return true
		})

		labels = append(labels, LabelCardinality{
			LabelKey:   labelKey,
			ValueCount: count,
			TopValues:  valueList,
		})
		return true
	})

	// Sort by value count descending
	sort.Slice(labels, func(i, j int) bool {
		return labels[i].ValueCount > labels[j].ValueCount
	})
	if len(labels) > 10 {
		labels = labels[:10]
	}
	stats.TopLabels = labels

	// Copy alerts
	ct.alertsMu.RLock()
	stats.ActiveAlerts = make([]CardinalityAlert, len(ct.alerts))
	copy(stats.ActiveAlerts, ct.alerts)
	ct.alertsMu.RUnlock()

	return stats
}

// GetAlerts returns active cardinality alerts.
func (ct *CardinalityTracker) GetAlerts() []CardinalityAlert {
	ct.alertsMu.RLock()
	defer ct.alertsMu.RUnlock()

	alerts := make([]CardinalityAlert, len(ct.alerts))
	copy(alerts, ct.alerts)
	return alerts
}

// Reset clears all cardinality tracking data.
func (ct *CardinalityTracker) Reset() {
	ct.totalSeries.Store(0)
	ct.metricCounts = sync.Map{}
	ct.labelValues = sync.Map{}

	ct.alertsMu.Lock()
	ct.alerts = ct.alerts[:0]
	ct.alertsMu.Unlock()
}

func makeSeriesKey(metric string, tags map[string]string) string {
	return NewSeriesKey(metric, tags).PrometheusString()
}
