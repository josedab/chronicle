// Bridge: anomaly_bridge.go
//
// This file bridges internal/anomaly/ into the public chronicle package.
// It re-exports types via type aliases so that callers use the top-level
// chronicle API while implementation stays private.
//
// Pattern: internal/anomaly/ (implementation) → anomaly_bridge.go (public API)
// Related files: anomaly_correlation.go, anomaly_explainability.go, anomaly_pipeline.go

package chronicle

import (
	"github.com/chronicle-db/chronicle/internal/anomaly"
)

// Type aliases re-export anomaly types for backward compatibility.
type AnomalyConfig = anomaly.AnomalyConfig
type AnomalyModel = anomaly.AnomalyModel
type AnomalyDetector = anomaly.AnomalyDetector
type AnomalyResult = anomaly.AnomalyResult
type AnomalyStats = anomaly.AnomalyStats
type ContributingFactor = anomaly.ContributingFactor
type Prediction = anomaly.Prediction
type IsolationForest = anomaly.IsolationForest
type IsolationTree = anomaly.IsolationTree
type IsolationNode = anomaly.IsolationNode
type LSTMModel = anomaly.LSTMModel
type Autoencoder = anomaly.Autoencoder
type TransformerModel = anomaly.TransformerModel
type StatisticalModel = anomaly.StatisticalModel

// Classification types
type AnomalyType = anomaly.AnomalyType
type AnomalySeverity = anomaly.AnomalySeverity
type ClassifiedAnomaly = anomaly.ClassifiedAnomaly
type PatternAnalysis = anomaly.PatternAnalysis
type Remediation = anomaly.Remediation
type AnomalyClassifier = anomaly.AnomalyClassifier
type AnomalyClassifierConfig = anomaly.AnomalyClassifierConfig
type RemediationRule = anomaly.RemediationRule

// Re-export constants.
const (
	AnomalyModelIsolationForest = anomaly.AnomalyModelIsolationForest
	AnomalyModelLSTM            = anomaly.AnomalyModelLSTM
	AnomalyModelAutoencoder     = anomaly.AnomalyModelAutoencoder
	AnomalyModelTransformer     = anomaly.AnomalyModelTransformer
	AnomalyModelEnsemble        = anomaly.AnomalyModelEnsemble
	AnomalyModelStatistical     = anomaly.AnomalyModelStatistical
)

const (
	AnomalyTypeNormal            = anomaly.AnomalyTypeNormal
	AnomalyTypeSpike             = anomaly.AnomalyTypeSpike
	AnomalyTypeDip               = anomaly.AnomalyTypeDip
	AnomalyTypeDrift             = anomaly.AnomalyTypeDrift
	AnomalyTypeLevelShift        = anomaly.AnomalyTypeLevelShift
	AnomalyTypeSeasonalDeviation = anomaly.AnomalyTypeSeasonalDeviation
	AnomalyTypeTrendChange       = anomaly.AnomalyTypeTrendChange
	AnomalyTypeMissing           = anomaly.AnomalyTypeMissing
	AnomalyTypeOutlier           = anomaly.AnomalyTypeOutlier
	AnomalyTypeVarianceChange    = anomaly.AnomalyTypeVarianceChange
)

const (
	AnomalySeverityInfo      = anomaly.AnomalySeverityInfo
	AnomalySeverityWarning   = anomaly.AnomalySeverityWarning
	AnomalySeverityCritical  = anomaly.AnomalySeverityCritical
	AnomalySeverityEmergency = anomaly.AnomalySeverityEmergency
)

// Re-export constructor functions.
var (
	DefaultAnomalyConfig          = anomaly.DefaultAnomalyConfig
	NewIsolationForest            = anomaly.NewIsolationForest
	NewLSTMModel                  = anomaly.NewLSTMModel
	NewAutoencoder                = anomaly.NewAutoencoder
	NewTransformerModel           = anomaly.NewTransformerModel
	NewStatisticalModel           = anomaly.NewStatisticalModel
	DefaultAnomalyClassifierConfig = anomaly.DefaultAnomalyClassifierConfig
	NewAnomalyClassifier          = anomaly.NewAnomalyClassifier
)

// NewAnomalyDetector creates a new anomaly detector. If ds is nil, the detector
// operates without a data source (no auto-retraining).
func NewAnomalyDetector(db *DB, config AnomalyConfig) *AnomalyDetector {
	var ds anomaly.DataSource
	if db != nil {
		ds = &dbAnomalyAdapter{db: db}
	}
	return anomaly.NewAnomalyDetector(ds, config)
}

// AnomalyDetectionDB wraps a DB with anomaly detection capabilities.
type AnomalyDetectionDB struct {
	*DB
	inner *anomaly.AnomalyDetectionDB
}

// NewAnomalyDetectionDB creates a new anomaly detection enabled database.
func NewAnomalyDetectionDB(db *DB, config AnomalyConfig) *AnomalyDetectionDB {
	adapter := &dbAnomalyAdapter{db: db}
	inner := anomaly.NewAnomalyDetectionDB(adapter, adapter, config)
	return &AnomalyDetectionDB{
		DB:    db,
		inner: inner,
	}
}

// Start starts anomaly detection.
func (adb *AnomalyDetectionDB) Start() { adb.inner.Start() }

// Stop stops anomaly detection.
func (adb *AnomalyDetectionDB) Stop() { adb.inner.Stop() }

// Detector returns the anomaly detector.
func (adb *AnomalyDetectionDB) Detector() *AnomalyDetector { return adb.inner.Detector() }

// WriteAndDetect writes a point and performs anomaly detection.
func (adb *AnomalyDetectionDB) WriteAndDetect(p Point, context []float64) (*AnomalyResult, error) {
	// Write the point via the root DB directly
	if err := adb.DB.Write(p); err != nil {
		return nil, err
	}
	// Detect anomaly
	return adb.inner.Detector().Detect(p.Value, context)
}

// dbAnomalyAdapter makes *DB satisfy anomaly.DataSource and anomaly.PointWriter.
type dbAnomalyAdapter struct {
	db *DB
}

func (a *dbAnomalyAdapter) Metrics() []string {
	return a.db.Metrics()
}

func (a *dbAnomalyAdapter) QueryRange(metric string, start, end int64) ([]anomaly.DataPoint, error) {
	result, err := a.db.Execute(&Query{Metric: metric, Start: start, End: end})
	if err != nil {
		return nil, err
	}
	points := make([]anomaly.DataPoint, len(result.Points))
	for i, p := range result.Points {
		points[i] = anomaly.DataPoint{Value: p.Value, Timestamp: p.Timestamp}
	}
	return points, nil
}

func (a *dbAnomalyAdapter) WritePoint(p anomaly.PointData) error {
	return a.db.Write(Point{Metric: p.Metric, Timestamp: p.Timestamp, Value: p.Value, Tags: p.Tags})
}
