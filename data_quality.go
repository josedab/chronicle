package chronicle

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sync"
	"time"
)

// DataQualityConfig configures the data quality monitor.
type DataQualityConfig struct {
	Enabled          bool
	MaxGapDuration   time.Duration
	DuplicateWindow  time.Duration
	OutlierStddevs   float64
	MaxClockSkew     time.Duration
	CheckInterval    time.Duration
	MaxIssues        int
}

// DefaultDataQualityConfig returns sensible defaults.
func DefaultDataQualityConfig() DataQualityConfig {
	return DataQualityConfig{
		Enabled:         true,
		MaxGapDuration:  5 * time.Minute,
		DuplicateWindow: time.Second,
		OutlierStddevs:  4.0,
		MaxClockSkew:    time.Minute,
		CheckInterval:   30 * time.Second,
		MaxIssues:       10000,
	}
}

// QualityIssueType represents the type of quality issue.
type QualityIssueType string

const (
	QualityGap       QualityIssueType = "gap"
	QualityDuplicate QualityIssueType = "duplicate"
	QualityOutlier   QualityIssueType = "outlier"
	QualityClockSkew QualityIssueType = "clock_skew"
	QualityNaN       QualityIssueType = "nan_value"
	QualityInf       QualityIssueType = "inf_value"
)

// QualityIssue represents a detected data quality issue.
type QualityIssue struct {
	ID        string           `json:"id"`
	Metric    string           `json:"metric"`
	Type      QualityIssueType `json:"type"`
	Severity  string           `json:"severity"` // info, warning, critical
	Message   string           `json:"message"`
	Timestamp int64            `json:"timestamp"`
	Value     float64          `json:"value,omitempty"`
	DetectedAt time.Time       `json:"detected_at"`
}

// MetricQualityScore represents the quality score for a metric.
type MetricQualityScore struct {
	Metric      string  `json:"metric"`
	Score       float64 `json:"score"` // 0-100
	Gaps        int     `json:"gaps"`
	Duplicates  int     `json:"duplicates"`
	Outliers    int     `json:"outliers"`
	ClockSkews  int     `json:"clock_skews"`
	TotalPoints int64   `json:"total_points"`
	TotalIssues int     `json:"total_issues"`
}

// DataQualityStats holds engine statistics.
type DataQualityStats struct {
	MetricsMonitored int   `json:"metrics_monitored"`
	TotalIssues      int   `json:"total_issues"`
	GapCount         int64 `json:"gap_count"`
	DuplicateCount   int64 `json:"duplicate_count"`
	OutlierCount     int64 `json:"outlier_count"`
	AvgQualityScore  float64 `json:"avg_quality_score"`
}

type metricTracker struct {
	lastTimestamp int64
	lastValue    float64
	values       []float64
	pointCount   int64
	issues       int
}

// DataQualityEngine monitors and reports data quality issues.
type DataQualityEngine struct {
	db     *DB
	config DataQualityConfig
	mu     sync.RWMutex
	issues []QualityIssue
	trackers map[string]*metricTracker
	running bool
	stopCh  chan struct{}
	stats   DataQualityStats
	issueSeq int64
}

// NewDataQualityEngine creates a new data quality engine.
func NewDataQualityEngine(db *DB, cfg DataQualityConfig) *DataQualityEngine {
	return &DataQualityEngine{
		db:       db,
		config:   cfg,
		issues:   make([]QualityIssue, 0),
		trackers: make(map[string]*metricTracker),
		stopCh:   make(chan struct{}),
	}
}

func (e *DataQualityEngine) Start() {
	e.mu.Lock(); if e.running { e.mu.Unlock(); return }; e.running = true; e.mu.Unlock()
}

func (e *DataQualityEngine) Stop() {
	e.mu.Lock(); defer e.mu.Unlock(); if !e.running { return }; e.running = false; select { case <-e.stopCh: default: close(e.stopCh) }
}

// Check inspects a point for quality issues. Returns any detected issues.
func (e *DataQualityEngine) Check(p Point) []QualityIssue {
	e.mu.Lock()
	defer e.mu.Unlock()

	tracker := e.getOrCreateTracker(p.Metric)
	var detected []QualityIssue

	// Check NaN
	if math.IsNaN(p.Value) {
		issue := e.createIssue(p.Metric, QualityNaN, "critical", "NaN value detected", p.Timestamp, p.Value)
		detected = append(detected, issue)
	}

	// Check Inf
	if math.IsInf(p.Value, 0) {
		issue := e.createIssue(p.Metric, QualityInf, "critical", "Infinite value detected", p.Timestamp, p.Value)
		detected = append(detected, issue)
	}

	// Check gap
	if tracker.lastTimestamp > 0 {
		gap := time.Duration(p.Timestamp - tracker.lastTimestamp)
		if gap > e.config.MaxGapDuration {
			msg := fmt.Sprintf("data gap of %s detected", gap)
			issue := e.createIssue(p.Metric, QualityGap, "warning", msg, p.Timestamp, 0)
			detected = append(detected, issue)
			e.stats.GapCount++
		}
	}

	// Check duplicate
	if tracker.lastTimestamp > 0 {
		timeDiff := time.Duration(absInt64(p.Timestamp - tracker.lastTimestamp))
		if timeDiff < e.config.DuplicateWindow && p.Value == tracker.lastValue {
			issue := e.createIssue(p.Metric, QualityDuplicate, "info", "duplicate point detected", p.Timestamp, p.Value)
			detected = append(detected, issue)
			e.stats.DuplicateCount++
		}
	}

	// Check clock skew (timestamp in future)
	now := time.Now().UnixNano()
	if p.Timestamp > now+e.config.MaxClockSkew.Nanoseconds() {
		msg := fmt.Sprintf("timestamp %dns in future", p.Timestamp-now)
		issue := e.createIssue(p.Metric, QualityClockSkew, "warning", msg, p.Timestamp, 0)
		detected = append(detected, issue)
	}

	// Check outlier
	tracker.values = append(tracker.values, p.Value)
	if len(tracker.values) > 1000 { tracker.values = tracker.values[500:] }
	if len(tracker.values) >= 20 && !math.IsNaN(p.Value) && !math.IsInf(p.Value, 0) {
		mean, stddev := meanAndStddev(tracker.values[:len(tracker.values)-1])
		if stddev > 0 && math.Abs(p.Value-mean) > e.config.OutlierStddevs*stddev {
			msg := fmt.Sprintf("outlier: value %.2f is %.1f stddevs from mean %.2f", p.Value, math.Abs(p.Value-mean)/stddev, mean)
			issue := e.createIssue(p.Metric, QualityOutlier, "warning", msg, p.Timestamp, p.Value)
			detected = append(detected, issue)
			e.stats.OutlierCount++
		}
	}

	tracker.lastTimestamp = p.Timestamp
	tracker.lastValue = p.Value
	tracker.pointCount++

	return detected
}

func absInt64(x int64) int64 {
	if x < 0 { return -x }
	return x
}

func meanAndStddev(values []float64) (float64, float64) {
	n := len(values)
	if n == 0 { return 0, 0 }
	var sum float64
	for _, v := range values { sum += v }
	mean := sum / float64(n)
	var variance float64
	for _, v := range values { d := v - mean; variance += d * d }
	return mean, math.Sqrt(variance / float64(n))
}

func (e *DataQualityEngine) createIssue(metric string, issueType QualityIssueType, severity, message string, ts int64, value float64) QualityIssue {
	e.issueSeq++
	issue := QualityIssue{
		ID: fmt.Sprintf("dq-%d", e.issueSeq),
		Metric: metric, Type: issueType, Severity: severity,
		Message: message, Timestamp: ts, Value: value,
		DetectedAt: time.Now(),
	}
	e.issues = append(e.issues, issue)
	if len(e.issues) > e.config.MaxIssues {
		e.issues = e.issues[len(e.issues)-e.config.MaxIssues:]
	}
	e.stats.TotalIssues = len(e.issues)
	if t, ok := e.trackers[metric]; ok { t.issues++ }
	return issue
}

func (e *DataQualityEngine) getOrCreateTracker(metric string) *metricTracker {
	if t, ok := e.trackers[metric]; ok { return t }
	t := &metricTracker{values: make([]float64, 0, 100)}
	e.trackers[metric] = t
	e.stats.MetricsMonitored = len(e.trackers)
	return t
}

// GetScore returns the quality score for a metric.
func (e *DataQualityEngine) GetScore(metric string) *MetricQualityScore {
	e.mu.RLock(); defer e.mu.RUnlock()
	tracker, ok := e.trackers[metric]
	if !ok { return nil }

	score := &MetricQualityScore{Metric: metric, TotalPoints: tracker.pointCount, TotalIssues: tracker.issues}
	for _, issue := range e.issues {
		if issue.Metric != metric { continue }
		switch issue.Type {
		case QualityGap: score.Gaps++
		case QualityDuplicate: score.Duplicates++
		case QualityOutlier: score.Outliers++
		case QualityClockSkew: score.ClockSkews++
		}
	}
	if tracker.pointCount > 0 {
		score.Score = math.Max(0, 100-float64(tracker.issues)/float64(tracker.pointCount)*100)
	} else {
		score.Score = 100
	}
	return score
}

// ListIssues returns quality issues.
func (e *DataQualityEngine) ListIssues(metric string, limit int) []QualityIssue {
	e.mu.RLock(); defer e.mu.RUnlock()
	var result []QualityIssue
	for _, issue := range e.issues {
		if metric != "" && issue.Metric != metric { continue }
		result = append(result, issue)
	}
	if limit > 0 && len(result) > limit { result = result[len(result)-limit:] }
	return result
}

// GetStats returns engine stats.
func (e *DataQualityEngine) GetStats() DataQualityStats {
	e.mu.RLock(); defer e.mu.RUnlock(); return e.stats
}

// RegisterHTTPHandlers registers HTTP endpoints.
func (e *DataQualityEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/quality/issues", func(w http.ResponseWriter, r *http.Request) {
		metric := r.URL.Query().Get("metric")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.ListIssues(metric, 100))
	})
	mux.HandleFunc("/api/v1/quality/score", func(w http.ResponseWriter, r *http.Request) {
		metric := r.URL.Query().Get("metric")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetScore(metric))
	})
	mux.HandleFunc("/api/v1/quality/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.GetStats())
	})
}
